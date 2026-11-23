# stream/

Streaming side.

- `replayer/`: lake rows -> two banks' native records on `bank_a.raw` / `bank_b.raw` (M3). `columns.py` is the allow-list of lake columns it reads (no label, no split); `render.py` holds each bank's own format and vocabulary.
- `connectors/`: config-driven connectors, bank format -> `tx.canonical` (Avro, registry subject `tx.canonical-value`, BACKWARD) and `tx.dlq` (M3). `engine.py` maps one record from a bank YAML (`connectors/README.md`), `run.py` is the runner, `onboard_check.py` the dry-run tool, `bench.py` the ingest benchmark, `registry.py` registers the schema.
- `flink-features/`: Flink/Java event-time features, package `com.streamaml.features` (M4)
- `settings.py`: brokers, registry, topic names, producer settings and the IPv4 client setting (D53).

`is_laundering`, pattern data and `split` never enter Kafka topics: the replayer reads an explicit column list, and `tests/leakage/test_stream_no_labels.py` pins it.

## Run it

```bash
make up && make topics                      # Redpanda, Postgres, ... and the topics
make connectors                             # two connector containers (apps profile; rebuilds the image)
make replay DS=hi_small FROM=2022-09-01 TO=2022-09-02 ASAP=1     # one day; SPEEDUP=1440 = a day per minute
make onboard-check BANK=b SAMPLE=tests/fixtures/bank_b_sample.jsonl      # no Kafka
make ingest-bench DS=hi_small               # stage costs and msgs/s per connector process (stop the containers first)
```

`make connector-local BANK=A` runs one connector as a host process (its own consumer group `local-connector-a`). Replays and tests can use scratch topics with `PREFIX=` / `--topic-prefix`.

## What to know

- **Delivery is at least once.** A connector commits an input offset only after every output of the batch was acknowledged, so a crash can re-read a batch and put duplicates on `tx.canonical`. Flink removes them by `tx_id` (D15). A clean run has none (`results/M03/hi_small/ingest_reconcile.json`).
- **Bad records go to `tx.dlq`**, with a code and the raw text. See `docs/runbook_dlq.md` for how to look at them and send them through again.
- **Order.** Each bank's stream is emitted in `(event_ts_ms, tx_id)` order, but two connectors writing one topic interleave across partitions; Flink's watermark handles that (M4).
- **Zero amounts are valid** (D53): they exist in the data and the feature oracle keeps them.
