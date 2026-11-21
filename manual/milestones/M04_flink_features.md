# M04 — Flink feature job (the hardest milestone)

**Timebox:** 28 h fast / 30 h full · **Triggers:** (1) walking skeleton not done by hour 12; (2) > 40 h total. Fallbacks in §8.
**Role signal:** SWE / streaming (event time, watermarks, keyed state, timers, exactly-once, recovery), training–serving parity.

## Inputs
`tx.canonical` (M3), `features/spec.yaml`, `feature_names_v1.txt`, `tests/golden/*.json` (M2), `contracts/feature_vector.avsc`, Spark `features_tx` for parity.

## Outputs
| Output | Where |
|---|---|
| Maven module, fat jar | `stream/flink-features/` → `target/flink-features-<v>.jar` |
| Job | `FeatureJob.java` (main), `DedupeFunction`, `LegSplitter`, `AccountFeatureFunction`, `JoinHalvesFunction`, `FeatureMath` (pure, unit-tested) |
| Deploy / stop | `make flink-build`, `make flink-deploy MODE=eos CKPT=5s`, `make flink-stop` (stop with savepoint, `--drain`) |
| Golden-vector tests on the Flink test harness | `src/test/java/.../GoldenVectorTest.java` |
| Parity report (T6) | `results/M04/parity.json` |
| Restart / exactly-once check | `results/M04/restart.json` |
| Local throughput + checkpoint stats | `results/M04/local_bench.json` |

## 1. Topology

```
KafkaSource<CanonicalTx>  (read_committed, offsets committed on checkpoint)
  .assignTimestampsAndWatermarks( boundedOutOfOrderness(OOO), idleness(10s) )
  .keyBy(tx_id) → DedupeFunction               // drop repeats; event-time timer clears state at ts+48h
  .flatMap(LegSplitter)                         // OUT leg (sender), IN leg (receiver)
  .keyBy(account_key) → AccountFeatureFunction  // buffer → sort by (ts, tx_id, dir) at watermark → features → append → evict
        └─ side output: late legs → tx.late
  .keyBy(tx_id) → JoinHalvesFunction            // wait for S and R halves → FeatureVector
  .sinkTo(KafkaSink<FeatureVector>)             // EXACTLY_ONCE or AT_LEAST_ONCE (flag)
```
Parallelism: local 4 (TaskManager slots); EKS = partitions (24).

## 2. Walking skeleton first (hours 0–12)

Build the **whole topology** with only `out_count_1h` and `in_count_1h`:
1. **[CC]** Maven: Java 17, `flink-streaming-java 1.20.x` (provided), `flink-connector-kafka 3.4.0-1.20`, `flink-avro`, `flink-avro-confluent-registry`, `avro-maven-plugin` (generate `CanonicalTx`, `FeatureVector` from `contracts/`), shade plugin, JUnit 5, `flink-test-utils`.
2. **[CC]** `FeatureJob` with config from CLI args / properties: bootstrap servers, registry URL, OOO bound, delivery mode, checkpoint interval, checkpoint dir, parallelism.
3. **[YOU]** `AccountFeatureFunction` for just those two counts (the buffering logic is the same as the full version).
4. **[YOU]** `JoinHalvesFunction`; **[YOU]** the checkpoint + sink config block.
5. **[CC]** Deploy locally, replay one day, see records in `tx.features`.
6. **[CC + YOU]** Restart test (§6).

When the skeleton passes the restart test, the remaining work is "add features", which is far less risky.

## 3. State and logic design (what you implement)

### `DedupeFunction` (keyed by `tx_id`)
- `ValueState<Boolean> seen`.
- On element: if `seen`, increment `duplicates_dropped` and return. Otherwise set `seen`, register an event-time timer at `event_ts + 48h`, and emit.
- `onTimer`: `seen.clear()`.
- Why event-time timers instead of state TTL: TTL is processing-time based. At 2,400× replay speed, 48 h of event time passes in about a minute of wall time, so a processing-time TTL would either keep state for the whole run or expire it far too early.

### `LegSplitter`
Emits `Leg{account_key, direction, tx_id, event_ts_ms, counterparty_key, amount_usd_cents, payment_currency, receiving_currency, payment_format, tx_ref}`. `tx_ref` carries the fields JoinHalves needs to build the output: bank_id, sender/receiver keys, amount, replay/connector timestamps.

### `AccountFeatureFunction` (keyed by `account_key`): the core
State (HashMap backend: plain Java objects, no per-access serialization):
- `MapState<Long, List<Leg>> pending`: legs buffered by `event_ts_ms` until the watermark passes.
- `ValueState<AccountLog> log`: a deque of entries plus running aggregates per window and direction: counts, cent sums, hold-time sum/count, cross-currency count, per-format OUT counts (72h), counterparty multisets for distinct counts, `first_seen_ts`, `last_in_ts`.
- `ValueState<Long> cleanupTimer`.

`processElement(leg)`:
```
if leg.ts <= ctx.timerService().currentWatermark():   // late
    ctx.output(LATE_TAG, leg); lateCounter.inc(); return
pending[leg.ts].add(leg)
ctx.timerService().registerEventTimeTimer(leg.ts)     // deduplicated per (key, ts) by Flink
```
`onTimer(ts)`, which fires when the watermark ≥ ts:
```
legs = pending[ts] sorted by (tx_id, dir_order OUT<IN); pending.remove(ts)
for each group of legs with the same tx_id:
    for leg in group: evict(log, leg.ts) per window; emit(Partial(tx_id, role, features(log, leg)))
    for leg in group: append(log, leg)               // hold_minutes computed here for OUT legs
re-register cleanup timer at lastTs + maxWindow (+1 ms); onTimer for cleanup with empty pending → clear the window deque/aggregates but KEEP first_seen_ts and last_in_ts (spec: account_age and hold_minutes outlive the window)
```
**Why this is correct** (be able to say this): the watermark guarantees no more legs with `ts ≤ watermark` will arrive, unless they're late, and late ones go to the side output. So when the timer for `ts` fires, the account has every leg with that timestamp and all earlier ones. Processing them in `(ts, tx_id)` order gives exactly the oracle's order, whatever order Kafka partitions delivered them in.

**Key memory question:** accounts that go silent would keep their deque forever. The cleanup timer evicts their old entries 72 h (event time) after their last activity.

### `JoinHalvesFunction` (keyed by `tx_id`)
- `ValueState<Partial> first`. When the second half arrives, merge into a `FeatureVector` (values in `feature_names_v1.txt` order, with `null` for null features), emit, clear, and delete the timer.
- Register a **processing-time** timer of 5 minutes when the first half arrives. If it fires, increment `orphan_halves` and drop. This should never happen; if it does, a leg was late or lost.
- A self-transfer produces both halves from the same account and the same timer callback. That's fine: both still go through the join.

### Feature math
Put all arithmetic in `FeatureMath` as pure static methods (no Flink types). Unit-test it directly against the golden JSON. Port the oracle's null rules **exactly**: `null` when the denominator is 0, 0 for empty counts and sums.

## 4. Watermarks and lateness: decide and document (D18)
- Timestamps are minute-resolution and replayed at speed. `forBoundedOutOfOrderness(OOO)` with **OOO in event time**. At `--speedup 1440`, 1 minute of event time ≈ 42 ms of wall time.
- Two connectors and many partitions → arrivals interleave. Start with **OOO = 2 min** of event time and measure `late` counts at each replay speed. Raise it only if late > 0. Every minute of OOO adds OOO/speedup to latency.
- `withIdleness(10s)` so an empty partition doesn't stall the watermark.
- End of stream: the final timestamp's legs wait until the watermark passes them. For parity and benchmark runs, finish with `flink stop --drain`, which emits the max watermark and flushes everything.
- Late events: side output to `tx.late` and count. In parity runs the count must be **0**. At `--asap` it may not be; report it.

## 5. Exactly-once configuration (YOU write it; know every line)
```java
env.enableCheckpointing(ckptMs, CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(ckptMs / 2);
env.getCheckpointConfig().setCheckpointTimeout(120_000);
env.getCheckpointConfig().setExternalizedCheckpointRetention(RETAIN_ON_CANCELLATION); // name per 1.20 API; check docs
// state backend: hashmap locally; checkpoint storage: file:///flink-checkpoints (local) | s3://…/checkpoints (EKS)

KafkaSink.<FeatureVector>builder()
  .setBootstrapServers(bootstrap)
  .setRecordSerializer(/* key = tx_id, value = Confluent Avro */)
  .setDeliveryGuarantee(mode == EOS ? DeliveryGuarantee.EXACTLY_ONCE : DeliveryGuarantee.AT_LEAST_ONCE)
  .setTransactionalIdPrefix("aml-features-" + runId)          // unique per job deployment
  .setProperty("transaction.timeout.ms", "600000")            // must be <= broker transaction.max.timeout.ms (Kafka default 15 min)
  .build();
// Source: .setProperty("isolation.level", "read_committed"); commit offsets on checkpoint
```
The API names above are from memory ([Likely]). Have Claude Code check each against the **Flink 1.20** Javadoc before you trust them.

What to be able to explain:
1. The source's Kafka offsets are part of the checkpoint, so after a restart, consumption resumes exactly where the last completed checkpoint was.
2. The sink pre-commits a Kafka transaction per checkpoint and commits on checkpoint completion (two-phase commit). After a crash, uncommitted transactions are aborted.
3. Consumers of `tx.features` **must** use `read_committed`, or they'll see aborted data (duplicates). librdkafka (the Python client) defaults to `read_committed` ([Likely]); set it explicitly anyway.
4. Latency cost: output becomes visible about one checkpoint interval later. That's why ALO mode exists (D03).
5. What is *not* exactly-once: anything Flink doesn't control (the scorer and alert service). They're made effectively-once by idempotency (M5).

## 6. Tests

**a. Golden vectors on the harness** (CC writes, you debug): `KeyedOneInputStreamOperatorTestHarness` around `AccountFeatureFunction`. Feed the golden scenarios' legs in a **shuffled** order, advance the watermark, and assert the outputs equal `tests/golden/*.json`. The shuffle is the important part: it proves the ordering logic.

**b. Feature-name parity:** the Java feature list equals `feature_names_v1.txt`.

**c. Parity, T6** (`tests/parity/run_parity.py`, CC):
1. Fresh topics. Deploy the job (EOS, 5 s checkpoints).
2. Replay **all of hi_small** at `--speedup 20000` (or a contiguous range **plus 72 h of warm-up history before it**; without warm-up, Flink's windows are empty and nothing matches).
3. `make flink-stop` (drain).
4. Consume `tx.features` (`read_committed`) into Parquet.
5. Join with Spark `features_tx` on `tx_id` and compare every feature: **counts and cent sums exact**, ratios and averages exact or relative 1e-12. Null must match null.
6. Write `parity.json`: rows compared, % rows fully equal, mismatches per feature, the first 20 diffs, late count, orphan halves, duplicates dropped.
- Target **≥ 99.9% of rows fully equal**. With D05/D06 you should reach 100%. Any mismatch is a bug or a spec ambiguity: fix the spec or one implementation, and log it in decisions.

**d. Restart, part of T7** (`tests/parity/run_restart.py`):
1. Start a replay of 2 days. At a random point, `docker kill` the TaskManager. Compose restarts it; the job recovers from the last checkpoint.
2. Drain at the end. Consume `tx.features` with `read_committed`.
3. Assert: every input `tx_id` appears **exactly once**; feature values equal a clean run's.
4. Repeat 5 times. Then run in ALO mode and **record** the duplicates (they're expected there).

**e. Local bench:** replay hi_small `--asap`. Record `numRecordsInPerSecond`, checkpoint duration and size, max watermark lag and late count → `local_bench.json`.

## 7. EOS vs ALO latency (needs M5 for end-to-end; do the Flink-only part here)
Measure `features_ts_ms − connector_ts_ms` (p50/p99) for EOS {1 s, 5 s, 10 s} and ALO, at `--speedup 1440`. Expect EOS p99 to be roughly the interval plus checkpoint duration. Save it to `results/M04/latency_flink.json`. M5 extends this to ingest-to-alert.

## 8. Fallbacks (in this order)
1. Behind schedule at hour 20: ship without `distinct_*` and `format_share_*` (spec v1-lite → bump `spec_version` to 2 and regenerate). Add them back after M11 if there's time.
2. EOS still fighting you (transaction timeouts, fencing errors) at hour 30: run ALO everywhere, rely on idempotent alerts, and document what failed and why. A diagnosed failure is still a good story.
3. Java blocking you entirely (> 40 h): PyFlink DataStream with the same design, recorded in decisions. Last resort; it costs you the typed-language signal.

## Claude Code prompts
> /milestone-start M04 — then: Build the walking skeleton per §2 with only out_count_1h and in_count_1h. Put TODO(human) in AccountFeatureFunction.processElement/onTimer, JoinHalvesFunction, and the checkpoint/sink block in FeatureJob. Generate Avro classes from contracts/. Write GoldenVectorTest using the test harness with shuffled input order. Verify every Flink API you use against the 1.20 docs and list the classes you checked.

> Before I implement onTimer: explain, with a timeline diagram in text, what the watermark is at each step when legs for one account arrive from 3 partitions out of order, and when each timer fires.

> The restart test shows N duplicates in tx.features under EOS. Give me ranked hypotheses (consumer not read_committed? transactional id reuse? sink mode not applied?) and a 2-minute check for each. Don't change code yet.

## Definition of Done
- [ ] Golden-vector harness tests green (shuffled input)
- [ ] Parity ≥ 99.9% (target 100%) on hi_small, late = 0, orphans = 0 → `parity.json`
- [ ] Restart test: 5/5 runs exactly-once under EOS; ALO duplicates recorded → `restart.json`
- [ ] `local_bench.json` and `latency_flink.json`
- [ ] D18 (watermark/OOO), D19 (checkpoint interval default) in decisions
- [ ] `/quiz M04` passed without looking at the code

## Interview questions
- How does exactly-once work here end to end? Where could a duplicate still sneak in?
- What's your watermark and why? What happens to a late event? What did you measure?
- Why key by `tx_id` before splitting into legs? Why event-time timers instead of TTL?
- Your state at HI-Large scale: how big is it, where does it live, how long do checkpoints take?
- Why does a receiver's feature need buffering when the sender's mostly doesn't?
