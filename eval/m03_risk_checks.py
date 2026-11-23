"""M03 pre-implementation checks (the plan's Risk 1 and Risk 2).

Writes results/M03/<ds>/risk_checks.json.

    uv run python -m eval.m03_risk_checks --ds hi_small            # needs `make up` and the SSD

Check 1, `scan_order`: does a PyIceberg scan of `raw_transactions` return rows in
(event_ts_ms, tx_id) order, and how fast? Run on a 1-day and a 3-day range, with an explicit column
allow-list (no label columns). If the order is not guaranteed the replayer must sort each day slice.

Check 2, `avro_produce`: per-record time of confluent-kafka `AvroSerializer` against
`fastavro.schemaless_writer` with Confluent framing, and the time to produce the same number of
serialized records to a scratch topic. It uses a scratch topic and a scratch registry subject (both
removed at the end, the topic only if `--delete-topic` is given: `rpk topic delete` is user-gated).
This is a microbenchmark of two stages on one machine in whatever state it is in, NOT the
ingest rate: the parse+map stage does not exist yet, so the file cannot say which stage is the
largest overall.
"""

import argparse
import io
import json
import os
import platform
import subprocess
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import fastavro
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pyiceberg.catalog.sql import SqlCatalog

from batch.spark import common

BROKERS = os.environ.get("AML_KAFKA_BROKERS", "localhost:19092")
REGISTRY = os.environ.get("AML_SCHEMA_REGISTRY", "http://localhost:18081")
SCHEMA_PATH = common.ROOT / "contracts" / "canonical_tx.avsc"
SCRATCH_TOPIC = "scratch.m03.risk2"
DAY_MS = 86_400_000
DAY1_START_MS = 1_661_990_400_000  # 2022-09-01T00:00:00Z, the first day of HI-Small (eda.json)

# The columns the checks may read. No label, split or pattern column, ever (CLAUDE.md rule 2).
ALLOW_LIST = (
    "tx_id",
    "event_ts_ms",
    "sender_bank",
    "sender_account",
    "sender_key",
    "receiver_bank",
    "receiver_account",
    "receiver_key",
    "amount_paid_minor",
    "payment_currency",
    "amount_received_minor",
    "receiving_currency",
    "amount_paid_usd_cents",
    "amount_received_usd_cents",
    "payment_format",
    "owner_bank_id",
)


def _table(ds: str) -> Any:
    catalog = SqlCatalog("lake", uri=common.pyiceberg_uri(), warehouse=f"file://{common.WAREHOUSE}")
    return catalog.load_table(f"aml_{ds}.raw_transactions")


def check_order(
    prev: tuple[int, int], ts: list[int], ids: list[int]
) -> tuple[tuple[int, int], int, int]:
    """Count rows whose event_ts_ms goes backwards and whose tx_id does not increase.

    `prev` is the last (event_ts_ms, tx_id) seen; returns the new last pair and the two counts.
    """
    ts_backsteps = 0
    id_not_increasing = 0
    for key in zip(ts, ids, strict=True):
        if key[0] < prev[0]:
            ts_backsteps += 1
        if key[1] <= prev[1]:
            id_not_increasing += 1
        prev = key
    return prev, ts_backsteps, id_not_increasing


def scan_order(ds: str, days: int) -> dict[str, Any]:
    lo, hi = DAY1_START_MS, DAY1_START_MS + days * DAY_MS
    scan = _table(ds).scan(
        row_filter=f"event_ts_ms >= {lo} AND event_ts_ms < {hi}", selected_fields=ALLOW_LIST
    )
    files = len(list(scan.plan_files()))
    started = time.perf_counter()
    prev = (-1, -1)
    rows = batches = ts_back = id_bad = 0
    ts_all: list[int] = []
    id_all: list[int] = []
    for batch in scan.to_arrow_batch_reader():
        ts = batch.column("event_ts_ms").to_pylist()
        ids = batch.column("tx_id").to_pylist()
        prev, tb, ib = check_order(prev, ts, ids)
        ts_back += tb
        id_bad += ib
        rows += len(ids)
        batches += 1
        ts_all += ts
        id_all += ids
    seconds = time.perf_counter() - started
    # The fallback the replayer would use: sort by (event_ts_ms, tx_id), then re-check.
    pairs = sorted(zip(ts_all, id_all, strict=True))
    _, ts_back_sorted, id_bad_sorted = check_order(
        (-1, -1), [p[0] for p in pairs], [p[1] for p in pairs]
    )
    return {
        "days": days,
        "range_ms": [lo, hi],
        "data_files_planned": files,
        "arrow_batches": batches,
        "rows": rows,
        "scan_seconds": round(seconds, 3),
        "rows_per_second": round(rows / seconds) if seconds else None,
        "event_ts_backsteps_in_scan_order": ts_back,
        "tx_id_not_increasing_in_scan_order": id_bad,
        "event_ts_backsteps_after_sort": ts_back_sorted,
        "tx_id_not_increasing_after_sort": id_bad_sorted,
        "distinct_tx_id": len(set(id_all)),
    }


def canonical_records(ds: str, n: int) -> list[dict[str, Any]]:
    """The first n rows of day 1 as dicts shaped like CanonicalTx (labels never selected)."""
    scan = _table(ds).scan(
        row_filter=f"event_ts_ms >= {DAY1_START_MS} AND event_ts_ms < {DAY1_START_MS + DAY_MS}",
        selected_fields=ALLOW_LIST,
    )
    out: list[dict[str, Any]] = []
    now = int(time.time() * 1000)
    for batch in scan.to_arrow_batch_reader():
        for row in batch.to_pylist():
            out.append(
                {
                    "tx_id": row["tx_id"],
                    "bank_id": row["owner_bank_id"],
                    "event_ts_ms": row["event_ts_ms"],
                    "sender_bank": row["sender_bank"],
                    "sender_account": row["sender_account"],
                    "sender_key": row["sender_key"],
                    "receiver_bank": row["receiver_bank"],
                    "receiver_account": row["receiver_account"],
                    "receiver_key": row["receiver_key"],
                    "payment_currency": row["payment_currency"],
                    "amount_paid_minor": row["amount_paid_minor"],
                    "receiving_currency": row["receiving_currency"],
                    "amount_received_minor": row["amount_received_minor"],
                    "amount_paid_usd_cents": row["amount_paid_usd_cents"],
                    "amount_received_usd_cents": row["amount_received_usd_cents"],
                    "payment_format": row["payment_format"],
                    "fx_version": 1,
                    "mapping_version": "x-1",
                    "replay_emit_ts_ms": now,
                    "connector_ts_ms": now,
                }
            )
            if len(out) >= n:
                return out
    return out


def _timed(fn: Any) -> float:
    started = time.perf_counter()
    fn()
    return time.perf_counter() - started


def avro_produce(ds: str, n: int, delete_topic: bool) -> dict[str, Any]:
    records = canonical_records(ds, n)
    if len(records) < n:
        raise RuntimeError(f"only {len(records)} rows available, wanted {n}")
    schema_str = SCHEMA_PATH.read_text()
    registry = SchemaRegistryClient({"url": REGISTRY})
    ctx = SerializationContext(SCRATCH_TOPIC, MessageField.VALUE)
    subject = f"{SCRATCH_TOPIC}-value"

    # --- stage: confluent-kafka AvroSerializer (the first serialization also registers the schema)
    serializer = AvroSerializer(registry, schema_str, conf={"auto.register.schemas": True})
    serializer(records[0], ctx)
    confluent_s = _timed(lambda: [serializer(r, ctx) for r in records])
    confluent_bytes = [serializer(r, ctx) for r in records[:1000]]

    # --- stage: fastavro.schemaless_writer with Confluent framing (magic byte 0 + 4-byte schema id)
    schema_id = registry.lookup_schema(subject, serializer._schema).schema_id  # noqa: SLF001
    parsed = fastavro.parse_schema(json.loads(schema_str))
    header = b"\x00" + schema_id.to_bytes(4, "big")

    def frame(rec: dict[str, Any]) -> bytes:
        buf = io.BytesIO()
        buf.write(header)
        fastavro.schemaless_writer(buf, parsed, rec)
        return buf.getvalue()

    fastavro_s = _timed(lambda: [frame(r) for r in records])
    bytes_equal = [frame(r) for r in records[:1000]] == confluent_bytes

    # --- stage: produce the serialized records to a scratch topic (same settings as the replayer)
    admin = AdminClient({"bootstrap.servers": BROKERS})
    for fut in admin.create_topics(
        [NewTopic(SCRATCH_TOPIC, num_partitions=6, replication_factor=1)]
    ).values():
        try:
            fut.result()
        except Exception as exc:  # noqa: BLE001 - an existing scratch topic is fine
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
    payloads = [(r["sender_key"].encode(), frame(r)) for r in records]
    delivered = 0
    errors: list[str] = []

    def on_delivery(err: Any, _msg: Any) -> None:
        nonlocal delivered
        if err is not None:
            errors.append(str(err))
        else:
            delivered += 1

    producer = Producer(
        {
            "bootstrap.servers": BROKERS,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
            "compression.type": "zstd",
            "batch.size": 262144,
        }
    )

    def produce_all() -> None:
        for key, value in payloads:
            while True:
                try:
                    producer.produce(SCRATCH_TOPIC, value=value, key=key, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.05)
            producer.poll(0)
        producer.flush()

    produce_s = _timed(produce_all)

    registry.delete_subject(subject)  # soft delete, then permanent
    registry.delete_subject(subject, permanent=True)
    topic_deleted = False
    if delete_topic:
        subprocess.run(
            ["rpk", "topic", "delete", SCRATCH_TOPIC, "-X", f"brokers={BROKERS}"], check=True
        )
        topic_deleted = True

    def per_record(seconds: float) -> dict[str, float]:
        return {
            "seconds_total": round(seconds, 3),
            "microseconds_per_record": round(seconds / n * 1e6, 2),
            "records_per_second": round(n / seconds),
        }

    return {
        "records": n,
        "avro_confluent_serializer": per_record(confluent_s),
        "avro_fastavro_schemaless_framed": per_record(fastavro_s),
        "framed_bytes_equal_to_confluent_on_first_1000": bytes_equal,
        "avro_message_bytes_mean": round(sum(len(v) for _, v in payloads) / n, 1),
        "produce": per_record(produce_s),
        "produce_delivered": delivered,
        "produce_errors": errors[:5],
        "producer_config": {
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
            "compression.type": "zstd",
            "batch.size": 262144,
            "partitions": 6,
        },
        "scratch_topic": SCRATCH_TOPIC,
        "scratch_topic_deleted": topic_deleted,
        "scratch_subject_deleted_permanently": True,
        "not_measured": (
            "parse+map stage (the engine does not exist yet): "
            "the largest stage overall is decided from ingest_bench.json"
        ),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", default="hi_small")
    parser.add_argument("--records", type=int, default=100_000)
    parser.add_argument("--delete-topic", action="store_true")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    common.require_data_root()
    out = args.out or common.ROOT / "results" / "M03" / args.ds / "risk_checks.json"
    doc = {
        "dataset": args.ds,
        "check": "M03 risk checks 1 and 2 (pre-implementation microbenchmarks)",
        "machine": {"platform": platform.platform(), "python": platform.python_version()},
        "scan_order": [scan_order(args.ds, 1), scan_order(args.ds, 3)],
        "avro_produce": avro_produce(args.ds, args.records, args.delete_topic),
        **common.provenance(),
    }
    common.write_json(out, doc)
    print(json.dumps(doc, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
