"""The connector's batch logic: map, serialize, produce, commit only after delivery (M03 step 5).

At-least-once means: an input offset is committed only after every output of the batch (canonical
records and DLQ records) was acknowledged. These tests use a producer and a consumer that log each
call, so the ORDER of produce, flush and commit is what is asserted. Real Kafka is in
tests/integration.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from stream.connectors.engine import load_config
from stream.connectors.run import (
    BatchProcessor,
    ConsumerError,
    DeliveryError,
    Metrics,
    dlq_record,
    parse_emit_header,
)
from stream.replayer import render

pytestmark = pytest.mark.unit

BANK_B = Path(__file__).resolve().parents[2] / "connectors" / "banks" / "bank_b.yaml"

ROW = {
    "tx_id": 41,
    "event_ts_ms": 1661991600000,
    "sender_bank": "10",
    "sender_account": "ACC1",
    "receiver_bank": "20",
    "receiver_account": "ACC2",
    "payment_currency": "EUR",
    "amount_paid_minor": 12345,
    "receiving_currency": "EUR",
    "amount_received_minor": 12345,
    "payment_format": "WIRE",
}
NOW = 1_700_000_000_999


class FakeMsg:
    def __init__(
        self,
        value: bytes | None,
        offset: int,
        partition: int = 0,
        headers: list[tuple[str, bytes]] | None = None,
        topic: str = "bank_b.raw",
        key: bytes = b"10:ACC1",
        error: Any = None,
    ) -> None:
        self._v, self._o, self._p, self._h, self._t, self._k, self._e = (
            value,
            offset,
            partition,
            headers,
            topic,
            key,
            error,
        )

    def value(self) -> bytes | None:
        return self._v

    def offset(self) -> int:
        return self._o

    def partition(self) -> int:
        return self._p

    def headers(self) -> list[tuple[str, bytes]] | None:
        return self._h

    def topic(self) -> str:
        return self._t

    def key(self) -> bytes:
        return self._k

    def error(self) -> Any:
        return self._e


class Log:
    def __init__(self) -> None:
        self.events: list[tuple[Any, ...]] = []


class FakeProducer:
    def __init__(self, log: Log, fail: bool = False) -> None:
        self.log, self.fail, self.pending, self.sent = log, fail, [], []

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Any, **_: Any) -> None:
        self.log.events.append(("produce", topic))
        self.pending.append(on_delivery)
        self.sent.append((topic, key, value))

    def poll(self, _timeout: float) -> int:
        return 0

    def flush(self, _timeout: float | None = None) -> int:
        self.log.events.append(("flush",))
        for callback in self.pending:
            callback("broker down" if self.fail else None, None)
        self.pending = []
        return 0


class FakeConsumer:
    def __init__(self, log: Log) -> None:
        self.log = log

    def commit(self, offsets: list[Any], asynchronous: bool = True) -> None:
        assert asynchronous is False, "the commit must be synchronous"
        self.log.events.append(
            ("commit", sorted((o.topic, o.partition, o.offset) for o in offsets))
        )


def make(fail: bool = False) -> tuple[BatchProcessor, Log, FakeProducer, CollectorRegistry]:
    log = Log()
    producer = FakeProducer(log, fail)
    registry = CollectorRegistry()
    processor = BatchProcessor(
        cfg=load_config(BANK_B),
        serialize=lambda rec: json.dumps(rec, sort_keys=True).encode(),
        producer=producer,
        consumer=FakeConsumer(log),
        metrics=Metrics("B", registry),
        now_ms=lambda: NOW,
    )
    return processor, log, producer, registry


def good(offset: int, tx_id: int = 41, **kwargs: Any) -> FakeMsg:
    return FakeMsg(render.render_bank_b({**ROW, "tx_id": tx_id}), offset, **kwargs)


# --- commit after delivery ------------------------------------------------------------------------


def test_commit_comes_after_every_produce_and_the_flush() -> None:
    processor, log, _, _ = make()
    processor.process([good(10, 1), good(11, 2), FakeMsg(b"{not json", 12)])
    kinds = [e[0] for e in log.events]
    assert kinds == ["produce", "produce", "produce", "flush", "commit"]
    assert log.events[-1] == ("commit", [("bank_b.raw", 0, 13)])  # last offset + 1


def test_a_failed_delivery_raises_and_commits_nothing() -> None:
    processor, log, _, registry = make(fail=True)
    with pytest.raises(DeliveryError):
        processor.process([good(10), good(11)])
    assert all(e[0] != "commit" for e in log.events)
    assert registry.get_sample_value("records_out_total", {"bank": "B"}) in (None, 0.0)


def test_offsets_are_committed_per_partition() -> None:
    processor, log, _, _ = make()
    processor.process([good(5, 1, partition=0), good(9, 2, partition=3), good(6, 3, partition=0)])
    assert log.events[-1] == ("commit", [("bank_b.raw", 0, 7), ("bank_b.raw", 3, 10)])


def test_an_empty_batch_commits_nothing() -> None:
    processor, log, _, _ = make()
    processor.process([])
    assert all(e[0] != "commit" for e in log.events)


def test_a_consumer_error_stops_the_batch_without_a_commit() -> None:
    processor, log, _, _ = make()

    class Boom:
        def code(self) -> int:
            return 1

        def __str__(self) -> str:
            return "broker transport failure"

    with pytest.raises(ConsumerError):
        processor.process([good(1), FakeMsg(None, 2, error=Boom())])
    assert all(e[0] != "commit" for e in log.events)


def test_a_serializer_failure_is_a_crash_not_a_dlq_record() -> None:
    processor, log, producer, _ = make()

    def broken(_: dict) -> bytes:
        raise RuntimeError("schema registry unreachable")

    processor.serialize = broken
    with pytest.raises(RuntimeError, match="schema registry"):
        processor.process([good(1)])
    assert producer.sent == [] and all(e[0] != "commit" for e in log.events)


# --- what is produced -----------------------------------------------------------------------------


def test_canonical_record_key_headers_and_timestamps() -> None:
    processor, _, producer, _ = make()
    processor.process([good(10, headers=[("replay_emit_ts_ms", b"1699999999000")])])
    (topic, key, value) = producer.sent[0]
    record = json.loads(value)
    assert topic == "tx.canonical" and key == b"10:ACC1"  # keyed by sender_key
    assert record["replay_emit_ts_ms"] == 1699999999000
    assert record["connector_ts_ms"] == NOW
    assert record["bank_id"] == "B" and record["tx_id"] == 41 and record["mapping_version"] == "b-1"


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        (None, None),
        ([], None),
        ([("other", b"1")], None),
        ([("replay_emit_ts_ms", b"123")], 123),
        ([("replay_emit_ts_ms", b"abc")], None),  # a bad header is not a bad record
        ([("replay_emit_ts_ms", b"")], None),
        ([("replay_emit_ts_ms", None)], None),
    ],
)
def test_emit_header(headers: Any, expected: int | None) -> None:
    assert parse_emit_header(headers) == expected


def test_a_record_without_the_header_still_maps() -> None:
    processor, _, producer, _ = make()
    processor.process([good(10, headers=None)])
    assert json.loads(producer.sent[0][2])["replay_emit_ts_ms"] is None


def test_dlq_record_shape_and_content() -> None:
    processor, _, producer, _ = make()
    bad = b'{"transactionId":1,"bookedAt":1661991600000}'
    processor.process([FakeMsg(bad, 77, partition=2, key=b"k")])
    (topic, key, value) = producer.sent[0]
    record = json.loads(value)
    assert topic == "tx.dlq" and key == b"k"
    assert set(record) == {
        "raw",
        "error_code",
        "error_detail",
        "source_topic",
        "partition",
        "offset",
        "mapping_version",
    }
    assert record["raw"] == bad.decode() and record["source_topic"] == "bank_b.raw"
    assert (
        record["partition"] == 2 and record["offset"] == 77 and record["mapping_version"] == "b-1"
    )
    assert record["error_code"] == "MISSING_FIELD" and "sender_bank" in record["error_detail"]


def test_dlq_record_keeps_a_non_utf8_message_readable() -> None:
    rec = dlq_record(b"\xff\xfe abc", "MALFORMED_RECORD", "not UTF-8", "bank_a.raw", 0, 5, "a-1")
    assert "abc" in rec["raw"] and isinstance(rec["raw"], str)
    json.dumps(rec)  # serializable


def test_a_tombstone_goes_to_the_dlq() -> None:
    processor, _, producer, _ = make()
    processor.process([FakeMsg(None, 3)])
    assert producer.sent[0][0] == "tx.dlq"
    assert json.loads(producer.sent[0][2])["error_code"] == "MALFORMED_RECORD"


# --- metrics --------------------------------------------------------------------------------------


def test_metrics_count_in_out_and_dlq_by_code() -> None:
    processor, _, _, registry = make()
    result = processor.process(
        [good(1, 1), good(2, 2), FakeMsg(b"[]", 3), FakeMsg(b"{", 4), FakeMsg(b"{}", 5)]
    )
    assert (result.records_in, result.records_out, result.dlq) == (
        5,
        2,
        {"MALFORMED_RECORD": 2, "MISSING_FIELD": 1},
    )
    value = registry.get_sample_value
    assert value("records_in_total", {"bank": "B"}) == 5
    assert value("records_out_total", {"bank": "B"}) == 2
    assert value("dlq_total", {"bank": "B", "error_code": "MALFORMED_RECORD"}) == 2
    assert value("dlq_total", {"bank": "B", "error_code": "MISSING_FIELD"}) == 1
    assert value("batch_latency_seconds_count", {"bank": "B"}) == 1
