"""The real bank files, record by record: what maps, and what lands in the DLQ with which code.

The two base records are the manual's examples (M03, "Bank formats"), written out by hand and not
produced by the renderer, so this file does not depend on the replayer.

Error codes (D53): MALFORMED_RECORD, MISSING_FIELD, UNKNOWN_VALUE, BAD_NUMBER, BAD_TIMESTAMP,
TIMESTAMP_OUT_OF_RANGE, NEGATIVE_AMOUNT, BAD_FORMAT.
Zero amounts are valid (D53): 1,066 HI-Small rows have a zero minor-unit amount and the feature
oracle keeps them, so a stream without them could not match Spark.
"""

import io
import json
from pathlib import Path
from typing import Any

import fastavro
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from stream.connectors.engine import MappingError, map_record

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]

A_COLUMNS = [
    "txn_ref",
    "timestamp",
    "from_bank",
    "from_account",
    "to_bank",
    "to_account",
    "amount_received",
    "receiving_currency",
    "amount_paid",
    "payment_currency",
    "payment_format",
]
A_BASE = [
    "123",
    "2022/09/01 00:20",
    "10",
    "8000EBD30",
    "10",
    "8000EBD30",
    "3697.34",
    "US Dollar",
    "3697.34",
    "US Dollar",
    "Reinvestment",
]
B_BASE: dict[str, Any] = {
    "transactionId": 123,
    "bookedAt": 1661991600000,
    "originator": {"bankCode": "10", "accountNo": "8000EBD30"},
    "beneficiary": {"bankCode": "10", "accountNo": "8000EBD30"},
    "debit": {"amountMinor": 369734, "currency": "USD"},
    "credit": {"amountMinor": 369734, "currency": "USD"},
    "method": "INTERNAL_REINVEST",
}


def expected(bank_id: str, mapping_version: str) -> dict[str, Any]:
    return {
        "tx_id": 123,
        "bank_id": bank_id,
        "event_ts_ms": 1661991600000,
        "sender_bank": "10",
        "sender_account": "8000EBD30",
        "sender_key": "10:8000EBD30",
        "receiver_bank": "10",
        "receiver_account": "8000EBD30",
        "receiver_key": "10:8000EBD30",
        "payment_currency": "USD",
        "amount_paid_minor": 369734,
        "receiving_currency": "USD",
        "amount_received_minor": 369734,
        "amount_paid_usd_cents": 369734,
        "amount_received_usd_cents": 369734,
        "payment_format": "REINVESTMENT",
        "fx_version": 1,
        "mapping_version": mapping_version,
    }


def a_line(**changes: str) -> bytes:
    cols = dict(zip(A_COLUMNS, A_BASE, strict=True))
    cols.update(changes)
    return ",".join(cols[c] for c in A_COLUMNS).encode()


def b_doc(*path_and_value: Any) -> bytes:
    """B_BASE with the value at a dotted path replaced; `...` as the value deletes the key."""
    doc = json.loads(json.dumps(B_BASE))
    for path, value in zip(path_and_value[::2], path_and_value[1::2], strict=True):
        *parents, last = path.split(".")
        node = doc
        for part in parents:
            node = node[part]
        if value is ...:
            del node[last]
        else:
            node[last] = value
    return json.dumps(doc).encode()


# --- the two manual examples map to the full canonical record -----------------------------------


def test_bank_a_manual_example(bank_a_cfg: Any) -> None:
    assert map_record(a_line(), bank_a_cfg) == expected("A", "a-1")


def test_bank_b_manual_example(bank_b_cfg: Any) -> None:
    assert map_record(json.dumps(B_BASE).encode(), bank_b_cfg) == expected("B", "b-1")


def test_canonical_record_has_no_runner_fields(bank_a_cfg: Any) -> None:
    out = map_record(a_line(), bank_a_cfg)
    assert "connector_ts_ms" not in out and "replay_emit_ts_ms" not in out  # the runner sets these


# --- the six DLQ cases of the manual, for both banks ----------------------------------------------

CASES: list[tuple[str, str, bytes, str, str | None]] = [
    # (id, bank, raw, code, field)
    ("a-missing-field", "A", a_line(from_account=""), "MISSING_FIELD", "sender_account"),
    ("b-missing-field", "B", b_doc("originator.accountNo", ...), "MISSING_FIELD", "sender_account"),
    ("b-null-field", "B", b_doc("originator.accountNo", None), "MISSING_FIELD", "sender_account"),
    (
        "a-unknown-currency",
        "A",
        a_line(payment_currency="Zloty"),
        "UNKNOWN_VALUE",
        "payment_currency",
    ),
    (
        "b-unknown-currency",
        "B",
        b_doc("debit.currency", "PLN"),
        "UNKNOWN_VALUE",
        "payment_currency",
    ),
    (
        "b-unknown-receiving-currency",
        "B",
        b_doc("credit.currency", "XXX"),
        "UNKNOWN_VALUE",
        "receiving_currency",
    ),
    ("a-unknown-format", "A", a_line(payment_format="Crypto"), "UNKNOWN_VALUE", "payment_format"),
    ("b-unknown-method", "B", b_doc("method", "HAWALA"), "UNKNOWN_VALUE", "payment_format"),
    ("a-negative-amount", "A", a_line(amount_paid="-5.00"), "NEGATIVE_AMOUNT", "amount_paid_minor"),
    (
        "b-negative-amount",
        "B",
        b_doc("debit.amountMinor", -1),
        "NEGATIVE_AMOUNT",
        "amount_paid_minor",
    ),
    ("a-bad-number", "A", a_line(amount_paid="abc"), "BAD_NUMBER", "amount_paid_minor"),
    ("b-bad-number", "B", b_doc("debit.amountMinor", "abc"), "BAD_NUMBER", "amount_paid_minor"),
    (
        "b-fractional-minor",
        "B",
        b_doc("debit.amountMinor", 12.5),
        "BAD_NUMBER",
        "amount_paid_minor",
    ),
    ("a-bad-txn-ref", "A", a_line(txn_ref="x1"), "BAD_NUMBER", "tx_id"),
    ("a-bad-timestamp", "A", a_line(timestamp="2022/13/45 25:61"), "BAD_TIMESTAMP", "event_ts_ms"),
    ("b-bad-timestamp", "B", b_doc("bookedAt", "yesterday"), "BAD_TIMESTAMP", "event_ts_ms"),
    (
        "a-ts-out-of-range",
        "A",
        a_line(timestamp="2030/01/01 00:00"),
        "TIMESTAMP_OUT_OF_RANGE",
        "event_ts_ms",
    ),
    (
        "b-ts-out-of-range",
        "B",
        b_doc("bookedAt", 1_000_000_000_000),
        "TIMESTAMP_OUT_OF_RANGE",
        "event_ts_ms",
    ),
    ("a-too-few-columns", "A", b",".join(a_line().split(b",")[:-1]), "MALFORMED_RECORD", None),
    ("a-too-many-columns", "A", a_line() + b",extra", "MALFORMED_RECORD", None),
    ("a-empty", "A", b"", "MALFORMED_RECORD", None),
    ("b-malformed-json", "B", b'{"transactionId":123,', "MALFORMED_RECORD", None),
    ("b-json-array", "B", b"[1, 2]", "MALFORMED_RECORD", None),
    ("b-not-utf8", "B", b"\xff\xfe\x00{", "MALFORMED_RECORD", None),
    ("b-empty", "B", b"", "MALFORMED_RECORD", None),
]


@pytest.mark.parametrize(
    ("bank", "raw", "code", "field"), [c[1:] for c in CASES], ids=[c[0] for c in CASES]
)
def test_dlq_case(
    bank: str, raw: bytes, code: str, field: str | None, request: pytest.FixtureRequest
) -> None:
    cfg = request.getfixturevalue("bank_a_cfg" if bank == "A" else "bank_b_cfg")
    out = map_record(raw, cfg)
    assert isinstance(out, MappingError), out
    assert out.code == code
    if field is not None:
        assert out.field == field
    assert out.detail  # a reason a person can read


def test_every_documented_code_is_exercised() -> None:
    codes = {c[3] for c in CASES}
    assert codes == {
        "MISSING_FIELD",
        "UNKNOWN_VALUE",
        "NEGATIVE_AMOUNT",
        "BAD_NUMBER",
        "BAD_TIMESTAMP",
        "TIMESTAMP_OUT_OF_RANGE",
        "MALFORMED_RECORD",
    }  # BAD_FORMAT (extract without a match) is covered in test_engine_ops.py


# --- zero amounts are valid -----------------------------------------------------------------------


def test_zero_amounts_map_in_both_banks(bank_a_cfg: Any, bank_b_cfg: Any) -> None:
    a = map_record(a_line(amount_paid="0.00", amount_received="0.00"), bank_a_cfg)
    b = map_record(b_doc("debit.amountMinor", 0, "credit.amountMinor", 0), bank_b_cfg)
    for out in (a, b):
        assert not isinstance(out, MappingError), out
        assert out["amount_paid_minor"] == 0 and out["amount_paid_usd_cents"] == 0
        assert out["amount_received_minor"] == 0 and out["amount_received_usd_cents"] == 0


def test_a_japanese_amount_that_rounds_to_zero_yen_maps(bank_a_cfg: Any) -> None:
    out = map_record(
        a_line(
            amount_paid="0.4",
            payment_currency="Yen",
            amount_received="0.4",
            receiving_currency="Yen",
        ),
        bank_a_cfg,
    )
    assert not isinstance(out, MappingError), out
    assert out["amount_paid_minor"] == 0 and out["payment_currency"] == "JPY"


# --- the first problem in YAML order wins ---------------------------------------------------------


def test_two_problems_report_the_one_listed_first(bank_a_cfg: Any) -> None:
    out = map_record(a_line(payment_currency="Zloty", amount_paid="-5.00"), bank_a_cfg)
    assert isinstance(out, MappingError) and out.field == "payment_currency"


# --- map_record never raises, and what it returns can be sent -----------------------------------


def _sendable(out: Any) -> None:
    """A result is either a MappingError that is not an engine bug, or a record Avro can encode."""
    if isinstance(out, MappingError):
        assert out.code != "INTERNAL_ERROR", out
        return
    out = {**out, "replay_emit_ts_ms": None, "connector_ts_ms": 1}
    schema = fastavro.parse_schema(
        json.loads((ROOT / "contracts" / "canonical_tx.avsc").read_text())
    )
    fastavro.schemaless_writer(io.BytesIO(), schema, out)
    out["sender_key"].encode("utf-8")


@settings(
    max_examples=300, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(raw=st.binary(max_size=200))
def test_arbitrary_bytes_never_raise(raw: bytes, bank_a_cfg: Any, bank_b_cfg: Any) -> None:
    for cfg in (bank_a_cfg, bank_b_cfg):
        _sendable(map_record(raw, cfg))


@settings(
    max_examples=300, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(position=st.integers(min_value=0, max_value=200), char=st.characters(), cut=st.booleans())
def test_mutated_valid_records_never_raise(
    position: int, char: str, cut: bool, bank_a_cfg: Any, bank_b_cfg: Any
) -> None:
    for cfg, raw in ((bank_a_cfg, a_line().decode()), (bank_b_cfg, json.dumps(B_BASE))):
        i = position % len(raw)
        mutated = raw[:i] if cut else raw[:i] + char + raw[i + 1 :]
        _sendable(map_record(mutated.encode("utf-8", errors="replace"), cfg))


@settings(
    max_examples=200, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(
    account=st.lists(
        st.sampled_from(["a", "Z", "9", "\ud800", "\udfff", "é", "\u0000", ":", '"']), max_size=8
    )
)
def test_json_text_with_escapes_and_lone_surrogates_is_sendable_or_rejected(
    account: list[str], bank_b_cfg: Any
) -> None:
    doc = json.loads(json.dumps(B_BASE))
    doc["originator"]["accountNo"] = "".join(account)
    raw = json.dumps(doc).encode()  # ensure_ascii: a surrogate becomes a \\ud800 escape
    _sendable(map_record(raw, bank_b_cfg))
