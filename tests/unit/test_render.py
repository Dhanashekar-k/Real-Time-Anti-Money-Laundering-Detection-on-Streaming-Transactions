"""The replayer's two bank renderers (M03 step 3): lake row -> the bank's native record bytes.

The renderers hold each bank's own vocabulary as constants, independent of the connector YAML, so a
wrong name in `bank_a.yaml` makes the equivalence test fail instead of being copied from one place
to the other. Bank A's CSV is rendered from minor units (the lake keeps no original amount text,
D53), so what these tests prove is the format, not parity with the original CSV.
"""

import csv
import io
import json
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from features import money
from features.spec import load_spec
from stream.replayer import render

pytestmark = pytest.mark.unit

ROW: dict[str, Any] = {
    "tx_id": 123,
    "event_ts_ms": 1661991600000,
    "sender_bank": "10",
    "sender_account": "8000EBD30",
    "receiver_bank": "10",
    "receiver_account": "8000EBD30",
    "payment_currency": "USD",
    "amount_paid_minor": 369734,
    "receiving_currency": "USD",
    "amount_received_minor": 369734,
    "payment_format": "REINVESTMENT",
}


def test_bank_a_matches_the_manual_example() -> None:
    assert render.render_bank_a(ROW) == (
        b"123,2022/09/01 00:20,10,8000EBD30,10,8000EBD30,3697.34,US Dollar,3697.34,US Dollar,"
        b"Reinvestment"
    )


def test_bank_b_matches_the_manual_example() -> None:
    assert json.loads(render.render_bank_b(ROW)) == {
        "transactionId": 123,
        "bookedAt": 1661991600000,
        "originator": {"bankCode": "10", "accountNo": "8000EBD30"},
        "beneficiary": {"bankCode": "10", "accountNo": "8000EBD30"},
        "debit": {"amountMinor": 369734, "currency": "USD"},
        "credit": {"amountMinor": 369734, "currency": "USD"},
        "method": "INTERNAL_REINVEST",
    }


def test_render_dispatches_on_the_bank() -> None:
    assert render.render("A", ROW) == render.render_bank_a(ROW)
    assert render.render("B", ROW) == render.render_bank_b(ROW)
    with pytest.raises(ValueError):
        render.render("C", ROW)


def test_rendering_is_deterministic_and_has_no_newline() -> None:
    for bank in ("A", "B"):
        assert render.render(bank, ROW) == render.render(bank, ROW)
        assert b"\n" not in render.render(bank, ROW)


# --- amounts --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("minor", "currency", "text"),
    [
        (369734, "USD", "3697.34"),
        (0, "USD", "0.00"),
        (5, "USD", "0.05"),
        (100, "EUR", "1.00"),
        (123, "JPY", "123"),
        (0, "JPY", "0"),
        (123456, "BTC", "0.00123456"),
        (0, "BTC", "0.00000000"),
        (10**11, "USD", "1000000000.00"),  # never exponent notation
        (2**63 - 1, "BTC", "92233720368.54775807"),
    ],
)
def test_amount_text(minor: int, currency: str, text: str) -> None:
    assert render.amount_text(minor, currency) == text


@settings(max_examples=300, deadline=None)
@given(
    minor=st.integers(min_value=0, max_value=2**63 - 1),
    currency=st.sampled_from(money.CURRENCIES),
)
def test_amount_text_round_trips_through_money_py(minor: int, currency: str) -> None:
    assert money.to_minor(render.amount_text(minor, currency), currency) == minor


def test_negative_minor_units_are_refused() -> None:
    with pytest.raises(ValueError):
        render.amount_text(-1, "USD")


# --- vocabularies ---------------------------------------------------------------------------------


def test_every_spec_currency_and_format_has_a_name_in_both_banks() -> None:
    spec = load_spec()
    assert set(render.BANK_A_CURRENCY_NAMES) == set(spec.currencies)
    assert set(render.BANK_A_FORMAT_NAMES) == set(spec.payment_formats)
    assert set(render.BANK_B_METHODS) == set(spec.payment_formats)
    for table in (
        render.BANK_A_CURRENCY_NAMES,
        render.BANK_A_FORMAT_NAMES,
        render.BANK_B_METHODS,
    ):
        assert len(set(table.values())) == len(table)  # no two codes share a bank name


# --- Bank A specifics -----------------------------------------------------------------------------


def test_bank_a_has_eleven_columns_and_no_header() -> None:
    fields = next(csv.reader(io.StringIO(render.render_bank_a(ROW).decode())))
    assert len(fields) == 11 and fields[0] == "123"


def test_bank_a_quotes_a_delimiter_inside_an_account() -> None:
    row = {**ROW, "sender_account": 'AB,"C'}
    fields = next(csv.reader(io.StringIO(render.render_bank_a(row).decode())))
    assert fields[3] == 'AB,"C' and len(fields) == 11


def test_bank_a_cannot_carry_seconds() -> None:
    with pytest.raises(ValueError):
        render.render_bank_a({**ROW, "event_ts_ms": 1661991600000 + 1000})


def test_bank_b_carries_milliseconds() -> None:
    doc = json.loads(render.render_bank_b({**ROW, "event_ts_ms": 1661991600123}))
    assert doc["bookedAt"] == 1661991600123


# --- Bank B field names live in one dict ----------------------------------------------------------


def _json_keys(node: Any) -> set[str]:
    if isinstance(node, dict):
        return set(node) | {k for v in node.values() for k in _json_keys(v)}
    return set()


def test_every_bank_b_json_key_comes_from_the_names_dict() -> None:
    keys = _json_keys(json.loads(render.render_bank_b(ROW)))
    assert keys == set(render.BANK_B_NAMES.values())


def test_renaming_one_entry_renames_the_field_everywhere(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(render.BANK_B_NAMES, "receiver", "payee")
    doc = json.loads(render.render_bank_b(ROW))
    assert "payee" in doc and "beneficiary" not in doc
    assert doc["payee"] == {"bankCode": "10", "accountNo": "8000EBD30"}
