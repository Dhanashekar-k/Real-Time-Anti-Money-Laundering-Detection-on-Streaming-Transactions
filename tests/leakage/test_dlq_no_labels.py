"""CLAUDE.md rule 2 and the DLQ's raw-bytes boundary (D53).

`tx.dlq` stores the raw text of a record it could not map. The connector does not look inside it, so
a label field that a bank sent would be copied there. In THIS system no label can reach it: the
replayer reads an explicit column list (`stream.replayer.columns`) and its renderers read fixed
keys, so nothing a bank topic holds carries a label. That upstream guard is the only one. A
production feed, where the bank is not ours, would redact known label keys before the DLQ write;
that is not built (D53).

What these tests show: over a large corpus of records that fail mapping in every way the engine has
(both banks, every bad fixture line, labelled lake rows made to fail), no DLQ `raw` payload contains
a known label key as a substring. They cannot show that a production feed is safe. The positive
control below shows the check can fail, and that a label in the input WOULD be carried verbatim.
"""

import json
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from stream.connectors.engine import load_config
from stream.connectors.run import BatchProcessor, Metrics
from stream.replayer import render

pytestmark = pytest.mark.unit

LABEL_KEYS = ("is_laundering", "laundering", "pattern_type", "attempt_id", "split")


def label_keys_in(text: str) -> list[str]:
    """Every known label key found as a substring, case-insensitively (so `isLaundering` counts)."""
    lowered = text.lower()
    return [key for key in LABEL_KEYS if key in lowered]


class _Msg:
    def __init__(self, value: bytes, offset: int) -> None:
        self._v, self._o = value, offset

    def value(self) -> bytes:
        return self._v

    def offset(self) -> int:
        return self._o

    def partition(self) -> int:
        return 0

    def topic(self) -> str:
        return "bank.raw"

    def key(self) -> bytes:
        return b"k"

    def headers(self) -> None:
        return None

    def error(self) -> None:
        return None


class _Producer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []
        self._callbacks: list[Any] = []

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Any) -> None:
        self.sent.append((topic, value))
        self._callbacks.append(on_delivery)

    def poll(self, _t: float) -> int:
        return 0

    def flush(self, _t: float | None = None) -> int:
        for callback in self._callbacks:
            callback(None, None)
        self._callbacks = []
        return 0


class _Consumer:
    def commit(self, offsets: list[Any], asynchronous: bool = True) -> None:
        pass


def dlq_payloads(bank: str, raws: list[bytes]) -> list[dict[str, Any]]:
    """Run `raws` through the connector's batch logic and return the DLQ records it produced."""
    producer = _Producer()
    processor = BatchProcessor(
        cfg=load_config(f"connectors/banks/bank_{bank.lower()}.yaml"),
        serialize=lambda record: json.dumps(record).encode(),
        producer=producer,
        consumer=_Consumer(),
        metrics=Metrics(bank, CollectorRegistry()),
    )
    for start in range(0, len(raws), 500):
        processor.process([_Msg(r, start + i) for i, r in enumerate(raws[start : start + 500])])
    return [json.loads(value) for topic, value in producer.sent if topic == "tx.dlq"]


def _failing_corpus(bank: str, rows: list[dict[str, Any]]) -> list[bytes]:
    """Valid rendered records, each broken in a different way, plus the fixtures' bad lines."""
    from pathlib import Path

    from stream.connectors.onboard_check import read_sample

    sample = "bank_a_sample.csv" if bank == "A" else "bank_b_sample.jsonl"
    corpus = list(read_sample(Path(__file__).resolve().parents[1] / "fixtures" / sample))
    for i, row in enumerate(rows):
        text = render.render(bank, row).decode()
        corpus += [
            text[: len(text) // 2].encode(),  # truncated
            text.replace("USD", "ZZZ")
            .replace("US Dollar", "Zloty")
            .replace("Euro", "Zloty")
            .encode(),
            text.replace("2022", "2099").replace("1661", "9999").encode(),
            (text + ",extra").encode() if bank == "A" else text.replace("method", "mode").encode(),
        ]
        if i % 7 == 0:
            corpus.append(b"\xff\xfe" + text.encode())
    return corpus


LABELLED = {
    "tx_id": 1,
    "event_ts_ms": 1661991600000,
    "sender_bank": "10",
    "sender_account": "AAA",
    "receiver_bank": "20",
    "receiver_account": "BBB",
    "payment_currency": "USD",
    "amount_paid_minor": 100,
    "receiving_currency": "USD",
    "amount_received_minor": 100,
    "payment_format": "WIRE",
    "is_laundering": 1,
    "split": "test",
    "pattern_type": "CYCLE",
    "attempt_id": 7,
}


@pytest.mark.parametrize("bank", ["A", "B"])
def test_no_label_key_appears_in_any_dlq_raw_payload(
    bank: str, synthetic_rows: list[dict[str, Any]]
) -> None:
    rows = [LABELLED, *synthetic_rows[:600]]  # the first is a lake row WITH its label columns
    payloads = dlq_payloads(bank, _failing_corpus(bank, rows))
    assert len(payloads) >= 1000, "the corpus must really exercise the DLQ"
    assert {p["error_code"] for p in payloads} >= {
        "MALFORMED_RECORD",
        "UNKNOWN_VALUE",
        "BAD_TIMESTAMP",
    }
    leaks = [
        (p["error_code"], label_keys_in(p["raw"])) for p in payloads if label_keys_in(p["raw"])
    ]
    assert leaks == []


def test_the_check_can_fail_and_the_dlq_carries_a_bank_s_label_verbatim() -> None:
    """Positive control: the guard is upstream; a label a bank DID send is carried to the DLQ."""
    sent = json.loads(render.render_bank_b(LABELLED))
    sent["isLaundering"] = True
    sent["method"] = "HAWALA"  # makes the record fail mapping, so it goes to the DLQ
    (record,) = dlq_payloads("B", [json.dumps(sent).encode()])
    assert label_keys_in(record["raw"]) == ["laundering"]  # no redaction exists yet (D53)
    assert label_keys_in("x is_laundering y") == ["is_laundering", "laundering"]
    assert label_keys_in("clean text") == []
