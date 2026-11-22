"""EDA on the 16-row fixture. Every expected value was worked out by hand from the file.

Order by (event_ts, src_row): src 8, 0, 1, 7, 2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14, 15 = tx 0..15.
Laundering rows: src 0, 4, 5, 7, 11. Self-transfer: src 3 (10:ACC001 -> 10:ACC001).
Legs per account (sender rows + receiver rows): 10:ACC001 7, 10:ACC002 4, 10:ACC003 1,
20:ACC004 5, 30:ACC010 5, 40:ACC011 2, 40:ACC012 2, 50:ACC020 4, 50:ACC021 2 (sum 32 = 2 x 16).
10:ACC001 is in 6 transactions (src 0, 2, 3, 6, 7, 12; the self-transfer is one transaction).
"""

import hashlib
import json
import shutil
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest

from batch.spark import common
from batch.spark.csv_to_iceberg import ingest
from batch.spark.eda import render_markdown, run_eda
from batch.spark.patterns import build_patterns

pytestmark = [pytest.mark.integration, pytest.mark.slow]

ROOT = Path(__file__).resolve().parents[2]
TRANS = ROOT / "tests" / "fixtures" / "aml" / "mini_trans.csv"
PATTERNS = ROOT / "tests" / "fixtures" / "aml" / "mini_patterns.txt"
NAMESPACE = "aml_mini_eda"
FX = {c: Decimal("1") for c in ["USD", "EUR", "GBP", "CHF", "JPY", "CNY", "INR", "CAD", "AUD",
                                  "MXN", "BRL", "RUB", "SAR", "ILS", "BTC"]}  # fmt: skip


@pytest.fixture(scope="module")
def eda() -> Iterator[dict]:
    spark = common.get_spark("eda-test", max_partition_bytes=256)
    try:
        common.drop_namespace(spark, NAMESPACE)
        results = ROOT / "var" / "eda-test-results"
        ingest(spark, ds="mini", trans_path=TRANS, namespace=NAMESPACE, fx=FX, results_dir=results)
        build_patterns(
            spark,
            ds="mini",
            patterns_path=PATTERNS,
            namespace=NAMESPACE,
            fx=FX,
            results_dir=results,
        )
        yield run_eda(spark, ds="mini", namespace=NAMESPACE, trans_path=TRANS)
    finally:
        try:
            common.drop_namespace(spark, NAMESPACE)
        finally:
            shutil.rmtree(common.WAREHOUSE / NAMESPACE, ignore_errors=True)
            shutil.rmtree(ROOT / "var" / "eda-test-results", ignore_errors=True)
            spark.stop()


def test_totals(eda: dict) -> None:
    t = eda["totals"]
    assert (t["rows"], t["laundering"]) == (16, 5)
    assert t["laundering_rate"] == pytest.approx(5 / 16)
    assert t["one_laundering_in"] == pytest.approx(16 / 5)
    assert t["sender_banks"] == 5  # 10, 20, 30, 40, 50


def test_transactions_per_day(eda: dict) -> None:
    days = {d["day"]: (d["rows"], d["laundering"]) for d in eda["per_day"]}
    assert days == {
        "2022-08-31": (1, 0),
        "2022-09-01": (9, 4),
        "2022-09-02": (3, 1),
        "2022-09-03": (3, 0),
    }
    assert eda["sparse_days"]["leading"] == [] and eda["sparse_days"]["trailing"] == []
    assert eda["sparse_days"]["totals"]["trailing"]["rows"] == 0
    assert eda["sparse_days"]["by_split"] == []


def test_accounts_and_max_transactions_per_account(eda: dict) -> None:
    a = eda["accounts"]
    assert (a["distinct"], a["distinct_senders"], a["distinct_receivers"]) == (9, 9, 8)
    assert a["tx_per_account"]["max"] == 6
    assert a["tx_per_account"]["max_account"] == "10:ACC001"
    assert a["legs_per_account"]["max"] == 7
    assert a["tx_per_account"]["top"][0] == {"account": "10:ACC001", "tx": 6, "legs": 7}
    assert a["max_entries_in_retention_window"]["max"] == 7
    assert a["max_entries_in_retention_window"]["account"] == "10:ACC001"
    assert a["max_entries_in_retention_window"]["window_ms"] == 259_200_000


def test_window_is_start_exclusive_and_counts_per_account() -> None:
    """20:ACC004 has 5 legs spanning exactly 72 h (08/31 23:59 ... 09/03 23:59). The first falls
    on the excluded edge of the window ending at the last one, so no window holds all five."""
    from pyspark.sql import functions as F

    from batch.spark.eda import _legs, window_entries

    spark = common.get_spark("eda-test", max_partition_bytes=256)
    legs = _legs(spark.table(f"lake.{NAMESPACE}.raw_transactions"))
    per_account = {
        r["account"]: r["m"]
        for r in window_entries(legs, 259_200_000)
        .groupBy("account")
        .agg(F.max("entries").alias("m"))
        .collect()
    }
    assert per_account["20:ACC004"] == 4
    assert per_account["10:ACC001"] == 7
    assert per_account["30:ACC010"] == 5
    # a window one millisecond longer admits the edge leg
    longer = {
        r["account"]: r["m"]
        for r in window_entries(legs, 259_200_001)
        .groupBy("account")
        .agg(F.max("entries").alias("m"))
        .collect()
    }
    assert longer["20:ACC004"] == 5


def test_self_transfers(eda: dict) -> None:
    s = eda["self_transfers"]
    assert (s["rows"], s["laundering"]) == (1, 0)
    assert s["share"] == pytest.approx(1 / 16)
    assert [(r["key"], r["rows"]) for r in s["by_payment_format"]] == [("REINVESTMENT", 1)]


def _by_key(rows: list[dict]) -> dict:
    return {r["key"]: (r["rows"], r["laundering"]) for r in rows}


def test_rates_by_format_and_currency(eda: dict) -> None:
    assert _by_key(eda["by_payment_format"]) == {
        "ACH": (6, 2), "WIRE": (4, 1), "BITCOIN": (2, 2), "CHEQUE": (1, 0),
        "REINVESTMENT": (1, 0), "CREDIT_CARD": (1, 0), "CASH": (1, 0),
    }  # fmt: skip
    assert _by_key(eda["by_payment_currency"]) == {
        "USD": (9, 2), "BTC": (2, 2), "EUR": (2, 0), "JPY": (1, 1), "GBP": (1, 0), "CHF": (1, 0),
    }  # fmt: skip


def test_cross_currency_split(eda: dict) -> None:
    assert _by_key(list(eda["cross_currency"].values())) == {True: (2, 1), False: (14, 4)}
    assert set(eda["cross_currency"]) == {"true", "false"}


def test_owner_bank_shares_match_an_independent_hash(eda: dict) -> None:
    banks = {  # sender bank per source row, read off the file
        "10": 7, "20": 3, "30": 2, "40": 2, "50": 2,
    }  # fmt: skip
    expected = {"A": 0, "B": 0}
    for bank, rows in banks.items():
        digit = int(hashlib.md5(bank.encode()).hexdigest()[-1], 16)  # noqa: S324
        expected["A" if digit % 2 == 0 else "B"] += rows
    got = {o["key"]: o["rows"] for o in eda["owner_bank"] if o["rows"]}
    assert got == {k: v for k, v in expected.items() if v}
    assert sum(o["share"] for o in eda["owner_bank"]) == pytest.approx(1)


def test_patterns_section(eda: dict) -> None:
    p = eda["patterns"]
    assert (p["attempts"], p["lines"]) == (2, 3)
    types = {t["pattern_type"]: t for t in p["by_type"]}
    assert types["FAN-OUT"]["lines"] == 2 and types["FAN-OUT"]["distinct_laundering_tx"] == 2
    assert types["CYCLE"]["ambiguous_lines"] == 1 and types["CYCLE"]["distinct_laundering_tx"] == 1
    assert (p["laundering_tx_in_some_pattern"], p["laundering_tx_total"]) == (3, 5)
    assert p["laundering_tx_in_no_pattern"] == 2
    assert p["coverage"] == pytest.approx(3 / 5)


def test_vocabularies_and_amounts(eda: dict) -> None:
    v = eda["vocabularies"]
    assert v["currencies_not_in_spec"] == [] and v["formats_not_in_spec"] == []
    assert "CNY" in v["currencies_in_spec_not_seen"]
    assert eda["amounts"]["zero_paid_minor_by_currency"] == {}


def test_source_precision_counts_rounded_amounts(eda: dict) -> None:
    # 0.125 USD (src 2) and 99.995 USD (src 12) have three decimals; JPY 1000000.00 is integral.
    p = eda["source_precision"]
    assert p["paid_over_precision_by_currency"] == {"USD": 2}
    assert p["received_over_precision_by_currency"] == {"USD": 2}


def test_markdown_is_generated_from_the_json_only(eda: dict) -> None:
    md = render_markdown(eda)
    assert md.startswith("# EDA: mini")
    assert "10:ACC001" in md and "FAN-OUT" in md and "Sparse-day rule" in md
    json.dumps(eda)  # the whole document must be JSON-serialisable
