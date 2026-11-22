"""Patterns -> `aml_<ds>.patterns`: each pattern line is matched to a tx_id in raw_transactions.

Fixture facts worked out by hand (tests/fixtures/aml): the FAN-OUT lines are source rows 4 and 5
(tx_id 6 and 7, unique); the CYCLE line equals source rows 0 and 7, exact duplicates (tx_id 1 and
3), so it is an AMBIGUOUS match: one pattern line, two candidate transactions.
"""

import json
import shutil
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest

from batch.spark import common
from batch.spark.csv_to_iceberg import ingest
from batch.spark.patterns import build_patterns

pytestmark = [pytest.mark.integration, pytest.mark.slow]

ROOT = Path(__file__).resolve().parents[2]
TRANS = ROOT / "tests" / "fixtures" / "aml" / "mini_trans.csv"
PATTERNS = ROOT / "tests" / "fixtures" / "aml" / "mini_patterns.txt"
NAMESPACE = "aml_mini_pat"
FX = {c: Decimal("1") for c in ["USD", "EUR", "GBP", "CHF", "JPY", "CNY", "INR", "CAD", "AUD",
                                  "MXN", "BRL", "RUB", "SAR", "ILS", "BTC"]}  # fmt: skip


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict]:
    results_dir = tmp_path_factory.mktemp("results")
    spark = common.get_spark("patterns-test", max_partition_bytes=256)
    try:
        common.drop_namespace(spark, NAMESPACE)
        ingest(spark, ds="mini", trans_path=TRANS, namespace=NAMESPACE, fx=FX,
               results_dir=results_dir)  # fmt: skip
        summary = build_patterns(
            spark, ds="mini", patterns_path=PATTERNS, namespace=NAMESPACE, fx=FX,
            results_dir=results_dir,
        )  # fmt: skip
        rows = [r.asDict() for r in spark.table(f"lake.{NAMESPACE}.patterns").collect()]
        rows.sort(key=lambda r: (r["attempt_id"], r["line_in_attempt"]))
        yield {"summary": summary, "rows": rows, "results_dir": results_dir}
    finally:
        try:
            common.drop_namespace(spark, NAMESPACE)
        finally:
            shutil.rmtree(common.WAREHOUSE / NAMESPACE, ignore_errors=True)
            spark.stop()


def test_every_pattern_line_gets_one_row(built: dict) -> None:
    assert len(built["rows"]) == 3
    assert [r["attempt_id"] for r in built["rows"]] == [0, 0, 1]


def test_unique_matches_get_their_tx_id_and_are_not_ambiguous(built: dict) -> None:
    fan_out = built["rows"][:2]
    assert [r["tx_id"] for r in fan_out] == [6, 7]
    assert [r["ambiguous"] for r in fan_out] == [False, False]
    assert {r["pattern_type"] for r in fan_out} == {"FAN-OUT"}


def test_duplicate_rows_are_flagged_ambiguous_not_dropped(built: dict) -> None:
    cycle = built["rows"][2]
    assert cycle["ambiguous"] is True
    assert cycle["tx_id"] == 1  # the first candidate by tx_id; the other duplicate is tx_id 3
    assert cycle["pattern_type"] == "CYCLE"


def test_a_candidate_transaction_is_used_by_at_most_one_pattern_line(built: dict) -> None:
    ids = [r["tx_id"] for r in built["rows"] if r["tx_id"] is not None]
    assert len(ids) == len(set(ids))


def test_coverage_counts_laundering_rows_only(built: dict) -> None:
    s = built["summary"]
    assert s["laundering_total"] == 5
    assert s["laundering_in_pattern_assigned"] == 3  # tx 6, 7, 1
    assert s["coverage_assigned"] == pytest.approx(3 / 5)
    # Upper bound counts every candidate of an ambiguous key: tx 1 and 3 -> 4 of 5.
    assert s["laundering_in_pattern_upper_bound"] == 4
    assert s["coverage_upper_bound"] == pytest.approx(4 / 5)
    assert s["laundering_outside_any_pattern_lower_bound"] == 1  # source row 11 (tx_id 11)


def test_counts_by_type_come_from_the_file(built: dict) -> None:
    s = built["summary"]
    assert s["attempts"] == 2
    assert s["lines"] == 3
    assert s["by_type"] == {
        "CYCLE": {"attempts": 1, "lines": 1},
        "FAN-OUT": {"attempts": 1, "lines": 2},
    }
    assert s["unmatched_lines"] == 0
    assert s["ambiguous_lines"] == 1


def test_patterns_json_has_provenance(built: dict) -> None:
    doc = json.loads((built["results_dir"] / "mini" / "patterns.json").read_text())
    for key in ("git_sha", "dataset", "timestamp", "coverage_assigned", "by_type"):
        assert key in doc, key
