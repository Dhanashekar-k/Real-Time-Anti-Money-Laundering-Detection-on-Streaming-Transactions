"""End to end: replay one day of HI-Small -> two connectors -> `tx.canonical`, vs the lake.

Needs `make up`. `make topics` is NOT required: the test creates scratch topics
(`it<time>.bank_a.raw` and so on) so a repeated run never sees an earlier run's messages, and it
deletes them afterwards. That is also why `duplicates == 0` can be asserted: this is a clean run
with no fault injected (duplicates after a crash are expected, and are reported but not asserted,
by the M08 fault runs, D15).

What is compared, for EVERY message of the day: the lake row with the same `tx_id` (owner bank,
event time, sender and receiver key, currencies, minor units, USD cents, payment format). The USD
cents come from `features/money.py` inside the connector and from Spark in the lake: they must be
equal. Writes results/M03/hi_small/ingest_reconcile.json.
"""

import json
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from batch.spark import common
from stream.connectors import registry
from stream.replayer import render
from stream.settings import BROKERS, SCHEMA_REGISTRY

pytestmark = [pytest.mark.integration, pytest.mark.slow]

ROOT = Path(__file__).resolve().parents[2]
DAY_FROM, DAY_TO = "2022-09-01", "2022-09-02"
LAKE_COLUMNS = (
    "tx_id",
    "event_ts_ms",
    "sender_key",
    "receiver_key",
    "payment_currency",
    "receiving_currency",
    "amount_paid_minor",
    "amount_received_minor",
    "amount_paid_usd_cents",
    "amount_received_usd_cents",
    "payment_format",
    "owner_bank_id",
)


@pytest.fixture
def scratch() -> Any:
    """Scratch topics with a unique prefix, deleted afterwards."""
    prefix = f"it{int(time.time())}."
    topics = {
        f"{prefix}bank_a.raw": 6,
        f"{prefix}bank_b.raw": 6,
        f"{prefix}tx.canonical": 12,
        f"{prefix}tx.dlq": 1,
    }
    admin = AdminClient({"bootstrap.servers": BROKERS})
    new = [NewTopic(t, num_partitions=n, replication_factor=1) for t, n in topics.items()]
    for future in admin.create_topics(new).values():
        future.result(timeout=30)
    yield prefix
    for future in admin.delete_topics(list(topics)).values():
        try:
            future.result(timeout=30)
        except Exception:  # noqa: BLE001 - cleanup must not hide the test's own failure
            pass


def read_topic(topic: str, handle: Callable[[Any], None]) -> int:
    """Read every message currently in `topic` (from the start) and hand it to `handle`.

    Stops when every partition has reported its end (`_PARTITION_EOF`); the topic must not be
    written to meanwhile.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": f"verify-{topic}-{time.time_ns()}",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        partitions = list(consumer.list_topics(topic, timeout=30).topics[topic].partitions)
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions])
        pending, count = set(partitions), 0
        while pending:
            for msg in consumer.consume(5000, 2.0):
                error = msg.error()
                if error is None:
                    handle(msg)
                    count += 1
                elif error.code() == KafkaError._PARTITION_EOF:
                    pending.discard(msg.partition())
                else:
                    raise RuntimeError(str(error))
        return count
    finally:
        consumer.close()


def run_module(module: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", module, *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=900,
    )


def lake_day(frm_ms: int, to_ms: int) -> pa.Table:
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog("lake", uri=common.pyiceberg_uri(), warehouse=f"file://{common.WAREHOUSE}")
    table = catalog.load_table("aml_hi_small.raw_transactions")
    scan = table.scan(
        row_filter=f"event_ts_ms >= {frm_ms} AND event_ts_ms < {to_ms}",
        selected_fields=LAKE_COLUMNS,
    )
    return scan.to_arrow().sort_by("tx_id")


def test_replayed_day_reconciles_with_the_lake(scratch: str, tmp_path: Path) -> None:
    common.require_data_root()
    prefix = scratch
    from stream.replayer.replayer import parse_range

    frm_ms, to_ms = parse_range(DAY_FROM, DAY_TO)
    lake = lake_day(frm_ms, to_ms)
    cols = {name: lake.column(name).to_pylist() for name in LAKE_COLUMNS}
    first, total = cols["tx_id"][0], lake.num_rows
    assert cols["tx_id"] == list(range(first, first + total)), "tx_id is not contiguous in the day"
    lake_rows = Counter(cols["owner_bank_id"])
    zero_rows = sum(
        1
        for a, b in zip(cols["amount_paid_minor"], cols["amount_received_minor"], strict=True)
        if a == 0 or b == 0
    )

    # 1. replay the day as fast as possible into the scratch bank topics
    emitted_from = int(time.time() * 1000)
    replay_summary = tmp_path / "replay"
    run_module(
        "stream.replayer.replayer",
        "--ds",
        "hi_small",
        "--from",
        DAY_FROM,
        "--to",
        DAY_TO,
        "--asap",
        "--topic-prefix",
        prefix,
        "--run",
        "it",
        "--out-dir",
        str(replay_summary),
    )
    emitted_to = int(time.time() * 1000)
    replay = json.loads((replay_summary / "replay_it.json").read_text())
    assert replay["messages"] == dict(lake_rows)

    # 2. both connectors, concurrently, until their topic has been quiet for a few seconds
    procs = {}
    for bank in ("A", "B"):
        procs[bank] = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "stream.connectors.run",
                "--bank",
                bank,
                "--topic-prefix",
                prefix,
                "--idle-exit",
                "8",
                "--summary",
                str(tmp_path / f"connector_{bank}.json"),
            ],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    for bank, proc in procs.items():
        out, _ = proc.communicate(timeout=1200)
        assert proc.returncode == 0, (bank, out[-2000:])
    connectors = {b: json.loads((tmp_path / f"connector_{b}.json").read_text()) for b in procs}

    # 3. read `tx.canonical` back and compare every message with the lake row of the same tx_id
    deserializer = AvroDeserializer(registry.client(), registry.canonical_schema().schema_str)
    context = SerializationContext(f"{prefix}tx.canonical", MessageField.VALUE)
    seen = [0] * total
    per_bank: Counter[str] = Counter()
    mismatches: list[dict[str, Any]] = []
    stats = {"outside_day": 0, "key_not_sender_key": 0, "emit_after_connector": 0, "no_emit_ts": 0}

    def check(msg: Any) -> None:
        rec = deserializer(msg.value(), context)
        i = rec["tx_id"] - first
        if not 0 <= i < total:
            stats["outside_day"] += 1
            return
        seen[i] += 1
        per_bank[rec["bank_id"]] += 1
        if msg.key() != rec["sender_key"].encode():
            stats["key_not_sender_key"] += 1
        emit = rec["replay_emit_ts_ms"]
        if emit is None:
            stats["no_emit_ts"] += 1
        elif not emitted_from <= emit <= emitted_to or emit > rec["connector_ts_ms"]:
            stats["emit_after_connector"] += 1
        expected = {
            "bank_id": cols["owner_bank_id"][i],
            "event_ts_ms": cols["event_ts_ms"][i],
            "sender_key": cols["sender_key"][i],
            "receiver_key": cols["receiver_key"][i],
            "payment_currency": cols["payment_currency"][i],
            "receiving_currency": cols["receiving_currency"][i],
            "amount_paid_minor": cols["amount_paid_minor"][i],
            "amount_received_minor": cols["amount_received_minor"][i],
            "amount_paid_usd_cents": cols["amount_paid_usd_cents"][i],
            "amount_received_usd_cents": cols["amount_received_usd_cents"][i],
            "payment_format": cols["payment_format"][i],
            "fx_version": 1,
        }
        wrong = {k: (rec[k], v) for k, v in expected.items() if rec[k] != v}
        if wrong and len(mismatches) < 10:
            mismatches.append({"tx_id": rec["tx_id"], "different": wrong})
        elif wrong:
            stats.setdefault("mismatches_beyond_examples", 0)
            stats["mismatches_beyond_examples"] += 1

    canonical_messages = read_topic(f"{prefix}tx.canonical", check)
    dlq_messages = read_topic(f"{prefix}tx.dlq", lambda m: None)

    distinct = sum(1 for n in seen if n)
    duplicates = sum(n - 1 for n in seen if n > 1)
    missing = sum(1 for n in seen if n == 0)
    registry_ok = registry.is_compatible()

    doc = {
        "dataset": "hi_small",
        "range": {"from": DAY_FROM, "to": DAY_TO, "range_ms": [frm_ms, to_ms]},
        "scratch_topic_prefix": prefix,
        "lake_rows": total,
        "lake_rows_by_bank": dict(lake_rows),
        "lake_rows_with_a_zero_minor_amount": zero_rows,
        "canonical_messages": canonical_messages,
        "canonical_messages_by_bank": dict(per_bank),
        "distinct_tx_ids": distinct,
        "duplicates": duplicates,
        "missing_tx_ids": missing,
        "dlq_messages": dlq_messages,
        "field_mismatches_examples": mismatches,
        "other_checks": stats,
        "connectors": {
            b: {k: c[k] for k in ("records_in", "records_out", "dlq", "seconds", "mapping_version")}
            for b, c in connectors.items()
        },
        "replay": {
            k: replay[k]
            for k in (
                "messages",
                "total_messages",
                "duration_seconds",
                "achieved_messages_per_second",
            )
        },
        "schema_registry_compatible": registry_ok,
        "passed": not (
            duplicates or missing or dlq_messages or mismatches or canonical_messages != total
        ),
        **common.provenance(),
    }
    common.write_json(ROOT / "results" / "M03" / "hi_small" / "ingest_reconcile.json", doc)

    assert canonical_messages == total, doc
    assert dict(per_bank) == dict(lake_rows)
    assert missing == 0 and distinct == total
    assert duplicates == 0, "a clean run must not produce duplicates"
    assert dlq_messages == 0
    assert not mismatches and not stats["outside_day"] and not stats["key_not_sender_key"]
    assert not stats["emit_after_connector"] and not stats["no_emit_ts"]
    for bank, summary in connectors.items():
        assert summary["records_in"] == summary["records_out"] == lake_rows[bank], summary
        assert summary["dlq"] == {}
    assert registry_ok


def test_bad_records_reach_the_dlq_with_reasons(scratch: str, tmp_path: Path) -> None:
    prefix = scratch
    good = {
        "tx_id": 5,
        "event_ts_ms": 1661991600000,
        "sender_bank": "10",
        "sender_account": "AAA",
        "receiver_bank": "20",
        "receiver_account": "BBB",
        "payment_currency": "USD",
        "amount_paid_minor": 0,
        "receiving_currency": "USD",
        "amount_received_minor": 0,
        "payment_format": "WIRE",
    }
    unknown_method = json.loads(render.render_bank_b({**good, "tx_id": 6}))
    unknown_method["method"] = "HAWALA"
    inputs = [
        render.render_bank_b(good),  # a zero amount is a valid record
        b'{"transactionId":7,',
        b'{"transactionId":8,"bookedAt":1661991600000}',
        json.dumps(unknown_method).encode(),
    ]
    producer = Producer({"bootstrap.servers": BROKERS})
    for value in inputs:
        producer.produce(f"{prefix}bank_b.raw", value=value, key=b"k")
    producer.flush(30)

    run_module(
        "stream.connectors.run",
        "--bank",
        "B",
        "--topic-prefix",
        prefix,
        "--idle-exit",
        "6",
        "--summary",
        str(tmp_path / "b.json"),
    )

    dead: list[dict[str, Any]] = []
    read_topic(f"{prefix}tx.dlq", lambda m: dead.append(json.loads(m.value())))
    assert sorted(d["error_code"] for d in dead) == [
        "MALFORMED_RECORD",
        "MISSING_FIELD",
        "UNKNOWN_VALUE",
    ]
    for record in dead:
        assert set(record) == {
            "raw",
            "error_code",
            "error_detail",
            "source_topic",
            "partition",
            "offset",
            "mapping_version",
        }
        assert (
            record["source_topic"] == f"{prefix}bank_b.raw" and record["mapping_version"] == "b-1"
        )
    good_ids: list[int] = []
    deserializer = AvroDeserializer(registry.client(), registry.canonical_schema().schema_str)
    context = SerializationContext(f"{prefix}tx.canonical", MessageField.VALUE)
    read_topic(
        f"{prefix}tx.canonical",
        lambda m: good_ids.append(deserializer(m.value(), context)["tx_id"]),
    )
    assert good_ids == [5]
    summary = json.loads((tmp_path / "b.json").read_text())
    assert (
        summary["records_in"] == 4
        and summary["records_out"] == 1
        and sum(summary["dlq"].values()) == 3
    )


def test_the_registry_enforces_backward_compatibility() -> None:
    first = registry.register_canonical()
    assert registry.register_canonical() == first  # idempotent: the same text keeps its id
    assert registry.client().get_compatibility(registry.SUBJECT) == "BACKWARD"
    assert registry.is_compatible()

    schema = json.loads(registry.SCHEMA_PATH.read_text())
    required = {**schema, "fields": [*schema["fields"], {"name": "branch", "type": "string"}]}
    defaulted = {
        **schema,
        "fields": [
            *schema["fields"],
            {"name": "branch", "type": ["null", "string"], "default": None},
        ],
    }
    client = registry.client(SCHEMA_REGISTRY)
    assert not client.test_compatibility(registry.SUBJECT, Schema(json.dumps(required), "AVRO"))
    assert client.test_compatibility(registry.SUBJECT, Schema(json.dumps(defaulted), "AVRO"))
