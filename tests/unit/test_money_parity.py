"""Connector money equals `features/money.py` (the one implementation shared with M1), exactly.

Unit part of the plan's `test_money_parity`: the engine against `money.py` for every currency and
the awkward amounts (half-even ties, JPY decimals, BTC satoshis, zero, huge). The part that
compares against the lake, every `tx_id` of a replayed day, is in
tests/integration/test_end_to_end_ingest.py.
"""

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from stream.connectors.engine import MappingError, map_record

from features import money

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
AMOUNTS = [
    "0",
    "0.00",
    "0.004",
    "0.005",  # half-even tie at 2 decimals: 0.5 cent -> 0
    "0.015",  # 1.5 cents -> 2
    "0.025",  # 2.5 cents -> 2
    "0.5",
    "1",
    "1.5",
    "2.5",
    "99.995",
    "1234567.891",
    "0.00000001",
    "0.000000005",
    "9999999999.99",
]


def _a_names() -> dict[str, str]:
    """Canonical code -> Bank A's name for it, read from the YAML (this test is about money)."""
    doc = yaml.safe_load((ROOT / "connectors" / "banks" / "bank_a.yaml").read_text())
    return {code: name for name, code in doc["value_maps"]["currency_names"].items()}


def _a_line(currency: str, amount: str) -> bytes:
    name = _a_names()[currency]
    cols = ["1", "2022/09/01 00:20", "10", "A1", "10", "B1", amount, name, amount, name, "Wire"]
    return ",".join(cols).encode()


@pytest.mark.parametrize("currency", money.CURRENCIES)
def test_bank_a_strings_equal_money_py(currency: str, bank_a_cfg: Any) -> None:
    for amount in AMOUNTS:
        out = map_record(_a_line(currency, amount), bank_a_cfg)
        minor = money.to_minor(amount, currency)
        assert not isinstance(out, MappingError), (currency, amount, out)
        assert out["amount_paid_minor"] == out["amount_received_minor"] == minor
        cents = money.minor_to_usd_cents(minor, currency)
        assert out["amount_paid_usd_cents"] == out["amount_received_usd_cents"] == cents


@pytest.mark.parametrize("currency", money.CURRENCIES)
def test_bank_b_minor_units_equal_money_py(currency: str, bank_b_cfg: Any) -> None:
    for amount in AMOUNTS:
        minor = money.to_minor(amount, currency)
        doc = {
            "transactionId": 1,
            "bookedAt": 1661991600000,
            "originator": {"bankCode": "10", "accountNo": "A1"},
            "beneficiary": {"bankCode": "10", "accountNo": "B1"},
            "debit": {"amountMinor": minor, "currency": currency},
            "credit": {"amountMinor": minor, "currency": currency},
            "method": "WIRE_TRANSFER",
        }
        out = map_record(json.dumps(doc).encode(), bank_b_cfg)
        assert not isinstance(out, MappingError), (currency, amount, out)
        assert out["amount_paid_minor"] == minor
        assert out["amount_paid_usd_cents"] == money.minor_to_usd_cents(minor, currency)


def test_cross_currency_row_converts_each_side_with_its_own_rate(bank_b_cfg: Any) -> None:
    doc = {
        "transactionId": 1,
        "bookedAt": 1661991600000,
        "originator": {"bankCode": "10", "accountNo": "A1"},
        "beneficiary": {"bankCode": "20", "accountNo": "B1"},
        "debit": {"amountMinor": 100000, "currency": "EUR"},
        "credit": {"amountMinor": 12345, "currency": "JPY"},
        "method": "WIRE_TRANSFER",
    }
    out = map_record(json.dumps(doc).encode(), bank_b_cfg)
    assert not isinstance(out, MappingError)
    assert out["amount_paid_usd_cents"] == money.minor_to_usd_cents(100000, "EUR")
    assert out["amount_received_usd_cents"] == money.minor_to_usd_cents(12345, "JPY")
    assert out["amount_paid_usd_cents"] != out["amount_received_usd_cents"]


def test_fx_version_is_the_frozen_table_version(bank_a_cfg: Any) -> None:
    fx_doc = yaml.safe_load(money.FX_PATH.read_text())
    out = map_record(_a_line("USD", "1.00"), bank_a_cfg)
    assert not isinstance(out, MappingError)
    assert out["fx_version"] == fx_doc["fx_version"] == money.FX_VERSION_REQUIRED


def test_usd_cents_outside_int64_is_an_error_not_a_wrapped_number(bank_b_cfg: Any) -> None:
    # 2**63 - 1 minor units of EUR is more than 2**63 - 1 USD cents (EUR is worth more than USD)
    big = 2**63 - 1
    doc = {
        "transactionId": 1,
        "bookedAt": 1661991600000,
        "originator": {"bankCode": "10", "accountNo": "A1"},
        "beneficiary": {"bankCode": "10", "accountNo": "B1"},
        "debit": {"amountMinor": big, "currency": "EUR"},
        "credit": {"amountMinor": 1, "currency": "EUR"},
        "method": "WIRE_TRANSFER",
    }
    out = map_record(json.dumps(doc).encode(), bank_b_cfg)
    assert isinstance(out, MappingError) and out.code == "BAD_NUMBER"
    assert out.field == "amount_paid_usd_cents"
