"""FX variance analysis (M01 step 6): measures implied rates, never writes fx_rates.yaml.

Fixture cross-currency rows (worked out by hand): row 4 pays 1000000.00 Yen and receives 7000.00
US Dollar (0.007 USD per JPY); row 6 pays 92.00 Euro and receives 100.00 US Dollar. Note the
file's column order: "Amount Received ... Amount Paid" - row 6 below is written as received 100.00
USD, paid 92.00 Euro, so the implied rate for the pair EUR->USD is 100.00 / 92.00.
"""

import json
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest

from batch.spark import common
from batch.spark.fx import analyze

pytestmark = [pytest.mark.integration, pytest.mark.slow]

FIXTURE = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "aml" / "mini_trans.csv"


@pytest.fixture(scope="module")
def table(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict]:
    fx_file = common.ROOT / "features" / "fx_rates.yaml"
    before = fx_file.read_bytes() if fx_file.exists() else None
    spark = common.get_spark("fx-test", catalog=False, max_partition_bytes=256)
    try:
        results_dir = tmp_path_factory.mktemp("results")
        doc = analyze(spark, ds="mini", trans_path=FIXTURE, results_dir=results_dir)
        after = fx_file.read_bytes() if fx_file.exists() else None
        yield {
            "doc": doc,
            "results_dir": results_dir,
            "fx_file_before": before,
            "fx_file_after": after,
        }
    finally:
        spark.stop()


def _pair(doc: dict, paid: str, received: str) -> dict:
    (match,) = [
        p for p in doc["pairs"] if p["paid_currency"] == paid and p["received_currency"] == received
    ]
    return match


def test_only_cross_currency_rows_are_analysed(table: dict) -> None:
    doc = table["doc"]
    assert doc["cross_currency_rows"] == 2
    assert sorted((p["paid_currency"], p["received_currency"]) for p in doc["pairs"]) == [
        ("EUR", "USD"),
        ("JPY", "USD"),
    ]


def test_weighted_rate_is_exact_decimal_division_of_sums(table: dict) -> None:
    jpy = _pair(table["doc"], "JPY", "USD")
    assert jpy["n"] == 1
    assert Decimal(jpy["weighted_rate"]) == Decimal("7000.00") / Decimal("1000000.00")
    eur = _pair(table["doc"], "EUR", "USD")
    assert Decimal(eur["weighted_rate"]).quantize(Decimal("0.0000001")) == Decimal("1.0869565")


def test_rates_are_strings_not_floats(table: dict) -> None:
    for pair in table["doc"]["pairs"]:
        assert isinstance(pair["weighted_rate"], str)


def test_single_row_pairs_have_no_variance_estimate(table: dict) -> None:
    for pair in table["doc"]["pairs"]:
        assert pair["cv"] is None  # one observation: no spread to report


def test_json_has_provenance_and_does_not_touch_the_fx_file(table: dict) -> None:
    doc = json.loads((table["results_dir"] / "mini" / "fx_variance.json").read_text())
    for key in ("git_sha", "dataset", "timestamp", "pairs", "cross_currency_rows"):
        assert key in doc, key
    # analyze() must never write the frozen table (only `make fx-freeze` does): compare against the
    # bytes the file had before this module ran `analyze`.
    assert table["fx_file_after"] == table["fx_file_before"]
