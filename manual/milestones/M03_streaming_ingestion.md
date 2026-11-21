# M03 — Replayer + two banks + config-driven connectors

**Timebox:** 10 h fast / 14 h full · **Trigger:** > 14 h → Bank B keeps only the name/unit differences.
**Role signal:** FDE (heterogeneous customer data, config-only onboarding, dead-letter handling), SWE (Kafka producers/consumers, schema registry, delivery semantics).

## Inputs
`raw_transactions` (with `owner_bank_id`), `connectors/banks/bank_a.yaml`, `bank_b.yaml`, `contracts/canonical_tx.avsc`, `features/money.py`, `fx_rates.yaml`.

## Outputs
| Output | Where |
|---|---|
| Replayer (event-time pacing, speed control, two bank formats) | `stream/replayer/replayer.py`, `make replay DS=hi_small SPEEDUP=1440` |
| Mapping engine + runner | `stream/connectors/engine.py`, `run.py`, `make connectors` (compose `apps` profile, one service per bank) |
| Canonical stream | `tx.canonical` (Avro via schema registry, key `sender_key`) |
| DLQ | `tx.dlq` (JSON with reason) |
| Ingest benchmark | `results/M03/ingest_bench.json` |
| Onboarding check tool (used again in M10) | `make onboard-check BANK=<x> SAMPLE=<file>` |

## Bank formats

**Bank A**: the IBM CSV format plus a leading transaction reference (banks have references; IBM rows don't):
```
txn_ref,timestamp,from_bank,from_account,to_bank,to_account,amount_received,receiving_currency,amount_paid,payment_currency,payment_format
123,2022/09/01 00:20,10,8000EBD30,10,8000EBD30,3697.34,US Dollar,3697.34,US Dollar,Reinvestment
```
No header in the stream; the column order comes from `bank_a.yaml`.

**Bank B**: JSON with its own names, minor units, epoch millis and ISO currencies:
```json
{"transactionId":123,"bookedAt":1661991600000,
 "originator":{"bankCode":"10","accountNo":"8000EBD30"},
 "beneficiary":{"bankCode":"10","accountNo":"8000EBD30"},
 "debit":{"amountMinor":369734,"currency":"USD"},
 "credit":{"amountMinor":369734,"currency":"USD"},
 "method":"INTERNAL_REINVEST"}
```
**Neither format carries the label.** `is_laundering` never leaves the lake.

## Steps

1. **[CC] Replayer.**
   - Reads `raw_transactions` with PyIceberg (`scan(row_filter=…, selected_fields=…)` → Arrow batches) in `tx_id` order, optionally for a date range (`--from 2022-09-01 --to 2022-09-03`).
   - Pacing modes:
     - `--speedup S`: wall time = start + (event_ts − first_ts)/S. 1440 = one day per minute.
     - `--rate R`: a fixed messages/s cap.
     - `--asap`: as fast as possible.
   - Renders each row in its owner bank's format and produces to `bank_a.raw` / `bank_b.raw`, key = sender account. The Kafka header `replay_emit_ts_ms` holds the wall-clock emit time.
   - Producer: `enable.idempotence=true`, `acks=all`, `linger.ms=5`, `compression.type=zstd`, `batch.size` 256 KB.
   - Sharding for throughput (M9): `--bank A|B` so two replayer processes each own one bank stream; each still emits in `tx_id` order.
   - Writes `results/M03/replay_<run>.json` (messages, duration, achieved rate).
2. **[CC] Mapping engine** (`engine.py`). A pure function `map_record(raw_bytes, bank_cfg) -> CanonicalTx | MappingError`. Supported operations (enough for A, B and C):
   - parse: `csv` (delimiter, column list, quote char, decimal comma), `json` (dotted paths)
   - `type`: string, long, decimal
   - `parse.timestamp_format` + `timezone`; `parse.epoch` (s or ms); `parse.iso8601`
   - `map`: value maps (vocabulary → canonical enum strings)
   - `parse.decimal_to_minor` with `currency_from: <canonical field>`, or `type: long` for sources already in minor units
   - common real-world cleanup, designed generically now (M10 reuses it without code changes): `default_from: <field>` (use another field when empty), `extract: <regex group>` / `strip_prefix: <sep>` (e.g. a branch prefix on account numbers), `decimal_locale` (`1.234,56`), `trim`/`upper`, `ignore` (drop a column such as a free-text memo)
   - `derive`: `sender_key`, `receiver_key`, `amount_*_usd_cents` (via `features/money.py`)
   - `validate`: required fields, allowed currencies and formats, `amount > 0`, timestamp in range

   Fields are processed **in the order listed in the YAML**, so later fields can use earlier canonical values (e.g. the currency before the amount).
3. **[CC] Connector runner** (`run.py --bank A`):
   - Consumer group `connector-<bank>`; batch poll; map; Avro-serialize with the registry serializer; produce to `tx.canonical` keyed by `sender_key`; copy `replay_emit_ts_ms` from the header into the record; set `connector_ts_ms` and `mapping_version`.
   - Bad records → `tx.dlq` as `{raw, error_code, error_detail, source_topic, partition, offset, mapping_version}`.
   - **Delivery:** at-least-once. Commit offsets only after the producer's delivery callbacks for the batch succeed. Duplicates are possible after a crash. Flink removes them by `tx_id` (D15). Count them.
   - Metrics: `records_in_total`, `records_out_total`, `dlq_total{error_code}`, `batch_latency_seconds`.
4. **[CC] Schema registry.** Register `canonical_tx.avsc` (subject `tx.canonical-value`, compatibility BACKWARD). Add a CI check that a changed `.avsc` stays backward compatible: post it to the registry's compatibility endpoint in an integration test, or use `fastavro`-based reader/writer resolution in a unit test.
5. **[CC] Onboarding dry-run tool.** `make onboard-check BANK=b SAMPLE=tests/fixtures/bank_b_sample.jsonl` maps a sample file **without Kafka** and prints field coverage, DLQ rate by error code, unmapped vocabulary values and 3 example canonical records. This is the FDE tool you'll time in M10.
6. **[YOU] Prove "no code change":** rename a Bank B field (`beneficiary` → `payee`) in the replayer's Bank B renderer **and** in `bank_b.yaml` only. Run the tests. The diff should touch zero `.py` files under `stream/connectors/`.

## Tests
- `test_mapping_equivalence`: for 10k sample rows, render as A and as B, map both → identical canonical records except `bank_id`, `mapping_version` and timestamps.
- `test_dlq_cases`: missing field, unknown currency, unknown method, negative amount, unparseable timestamp, malformed JSON. Each lands in the DLQ with the right code.
- `test_money_parity`: canonical USD cents equal `raw_transactions.amount_*_usd_cents` for the same `tx_id` (connectors and M1 share `money.py`, so this must be exact).
- Integration `test_end_to_end_ingest`: replay 1 day of hi_small `--asap` → count(`tx.canonical`) == rows in the range; DLQ empty.

## Definition of Done
- [ ] Both banks flow into `tx.canonical`; counts reconcile with the raw table for the replayed range
- [ ] DLQ works with reasons; `onboard-check` works on a sample
- [ ] The "no code change" demonstration recorded in `docs/decisions.md`
- [ ] `ingest_bench.json`: messages/s per connector process at `--asap` (find the bottleneck: parse, Avro serialization or produce)

## Gotchas
- **Python Avro throughput:** per-record serialization is the likely bottleneck ([Likely]). Batch, reuse the serializer, and use more connector processes (one per partition) before you optimize code.
- **Ordering:** two connectors writing into one topic means event times interleave out of order across partitions. That's expected. Flink's watermark bound handles it (M4).
- **Idempotent producer ≠ exactly-once.** It prevents duplicates from producer retries within a session, not duplicates from reprocessing after a consumer crash.

## Interview questions
- A new bank sends a different schema tomorrow. Walk me through onboarding. (YAML → `onboard-check` → register bank → start connector instance. No deploy of new code.)
- What happens to a record your mapping can't parse? Who looks at the DLQ and how is it replayed?
- Where can duplicates enter, and where do you remove them?
