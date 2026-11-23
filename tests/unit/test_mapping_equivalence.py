"""Bank A (CSV) and Bank B (JSON) describe the same transactions, so they must map to the same
canonical records (M03 `test_mapping_equivalence`).

10,000 synthetic rows cover all 15 currencies and 7 formats, zero amounts and large amounts. Each is
rendered as A and as B, mapped, and compared. The renderers' vocabularies are independent of the
YAML (see test_render.py), so this test fails if a bank file has a wrong or missing name.
"""

from typing import Any

import pytest
from stream.connectors.engine import MappingError, map_record

from features import money
from stream.replayer import render

pytestmark = pytest.mark.unit

DIFFERENT_BY_DESIGN = {"bank_id", "mapping_version"}


def test_the_sample_covers_the_vocabularies(synthetic_rows: list[dict[str, Any]]) -> None:
    assert len(synthetic_rows) == 10_000
    currencies = {r["payment_currency"] for r in synthetic_rows} | {
        r["receiving_currency"] for r in synthetic_rows
    }
    assert currencies == set(money.CURRENCIES)
    assert len({r["payment_format"] for r in synthetic_rows}) == 7
    assert any(r["amount_paid_minor"] == 0 for r in synthetic_rows)
    assert any(r["payment_currency"] != r["receiving_currency"] for r in synthetic_rows)


def test_a_and_b_map_to_identical_canonical_records(
    synthetic_rows: list[dict[str, Any]], bank_a_cfg: Any, bank_b_cfg: Any
) -> None:
    mismatches: list[tuple[int, dict, dict]] = []
    for row in synthetic_rows:
        a = map_record(render.render_bank_a(row), bank_a_cfg)
        b = map_record(render.render_bank_b(row), bank_b_cfg)
        assert not isinstance(a, MappingError), (row, a)
        assert not isinstance(b, MappingError), (row, b)
        assert a["bank_id"] == "A" and b["bank_id"] == "B"
        assert a["mapping_version"] != b["mapping_version"]
        strip = {k: v for k, v in a.items() if k not in DIFFERENT_BY_DESIGN}
        if strip != {k: v for k, v in b.items() if k not in DIFFERENT_BY_DESIGN}:
            mismatches.append((row["tx_id"], a, b))
    assert not mismatches, mismatches[:3]


def test_both_banks_reproduce_the_lake_row(
    synthetic_rows: list[dict[str, Any]], bank_a_cfg: Any, bank_b_cfg: Any
) -> None:
    """Not only A == B: each equals what the lake row says, computed with money.py directly."""
    for row in synthetic_rows[:2000]:
        for cfg, text in (
            (bank_a_cfg, render.render_bank_a(row)),
            (bank_b_cfg, render.render_bank_b(row)),
        ):
            out = map_record(text, cfg)
            assert not isinstance(out, MappingError), (row, out)
            assert out["tx_id"] == row["tx_id"]
            assert out["event_ts_ms"] == row["event_ts_ms"]
            assert out["sender_key"] == f"{row['sender_bank']}:{row['sender_account']}"
            assert out["receiver_key"] == f"{row['receiver_bank']}:{row['receiver_account']}"
            for side, ccy in (("paid", "payment_currency"), ("received", "receiving_currency")):
                minor = row[f"amount_{side}_minor"]
                assert out[ccy] == row[ccy]
                assert out[f"amount_{side}_minor"] == minor
                assert out[f"amount_{side}_usd_cents"] == money.minor_to_usd_cents(minor, row[ccy])
            assert out["payment_format"] == row["payment_format"]


def test_a_sender_with_the_same_account_in_two_banks_stays_two_accounts(bank_a_cfg: Any) -> None:
    """Account ids are only unique within a bank, so the key carries the bank code."""
    base = {
        "tx_id": 1,
        "event_ts_ms": 1661991600000,
        "sender_bank": "10",
        "sender_account": "SAME",
        "receiver_bank": "20",
        "receiver_account": "SAME",
        "payment_currency": "USD",
        "amount_paid_minor": 1,
        "receiving_currency": "USD",
        "amount_received_minor": 1,
        "payment_format": "ACH",
    }
    out = map_record(render.render_bank_a(base), bank_a_cfg)
    assert not isinstance(out, MappingError)
    assert out["sender_key"] == "10:SAME" and out["receiver_key"] == "20:SAME"
    assert out["sender_key"] != out["receiver_key"]
