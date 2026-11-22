"""Pattern matching when lines and transactions do not pair up one to one.

Transactions (tx_id after sorting by time then file row): tx 0 and tx 1 are exact duplicates (key
X), tx 2 is a lone row (key Y, not in any pattern), tx 3 is a lone row (key Z).
Pattern lines: attempt 0 has THREE lines with key X (two candidates), attempt 1 has one line whose
key matches no transaction, attempt 2 has TWO lines with key Z (one candidate).
"""

import shutil
from collections.abc import Iterator
from decimal import Decimal

import pytest

from batch.spark import common
from batch.spark.csv_to_iceberg import ingest
from batch.spark.patterns import build_patterns

pytestmark = [pytest.mark.integration, pytest.mark.slow]

NAMESPACE = "aml_mini_edge"
FX = {c: Decimal("1") for c in ["USD", "EUR", "GBP", "CHF", "JPY", "CNY", "INR", "CAD", "AUD",
                                  "MXN", "BRL", "RUB", "SAR", "ILS", "BTC"]}  # fmt: skip
HEADER = (
    "Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,"
    "Amount Paid,Payment Currency,Payment Format,Is Laundering"
)
X = "2022/09/01 00:00,10,A1,20,B1,5.00,US Dollar,5.00,US Dollar,ACH,1"
Y = "2022/09/01 00:01,10,A2,20,B2,7.00,US Dollar,7.00,US Dollar,ACH,0"
Z = "2022/09/01 00:02,10,A3,20,B3,9.00,US Dollar,9.00,US Dollar,ACH,1"
NO_MATCH = "2022/09/01 00:09,10,A9,20,B9,1.00,US Dollar,1.00,US Dollar,ACH,1"


def _block(kind: str, lines: list[str]) -> str:
    return "\n".join(
        [f"BEGIN LAUNDERING ATTEMPT - {kind}:  x", *lines, f"END LAUNDERING ATTEMPT - {kind}", ""]
    )


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict]:
    work = tmp_path_factory.mktemp("edge")
    trans = work / "trans.csv"
    trans.write_text("\n".join([HEADER, X, X, Y, Z, ""]))
    patterns = work / "patterns.txt"
    patterns.write_text(
        _block("STACK", [X, X, X]) + _block("RANDOM", [NO_MATCH]) + _block("CYCLE", [Z, Z])
    )
    spark = common.get_spark("patterns-edge-test", max_partition_bytes=256)
    try:
        common.drop_namespace(spark, NAMESPACE)
        results = work / "results"
        ingest(spark, ds="edge", trans_path=trans, namespace=NAMESPACE, fx=FX, results_dir=results)
        summary = build_patterns(
            spark,
            ds="edge",
            patterns_path=patterns,
            namespace=NAMESPACE,
            fx=FX,
            results_dir=results,
        )
        rows = [r.asDict() for r in spark.table(f"lake.{NAMESPACE}.patterns").collect()]
        rows.sort(key=lambda r: (r["attempt_id"], r["line_in_attempt"]))
        yield {"summary": summary, "rows": rows}
    finally:
        try:
            common.drop_namespace(spark, NAMESPACE)
        finally:
            shutil.rmtree(common.WAREHOUSE / NAMESPACE, ignore_errors=True)
            spark.stop()


def test_more_lines_than_candidates_leaves_the_extra_line_unmatched_but_ambiguous(
    built: dict,
) -> None:
    stack = [r for r in built["rows"] if r["attempt_id"] == 0]
    assert [r["tx_id"] for r in stack] == [0, 1, None]
    assert [r["ambiguous"] for r in stack] == [True, True, True]  # the key has two candidates


def test_a_key_with_no_transaction_is_unmatched_and_not_ambiguous(built: dict) -> None:
    (line,) = [r for r in built["rows"] if r["attempt_id"] == 1]
    assert line["tx_id"] is None and line["ambiguous"] is False


def test_two_lines_one_candidate_gives_the_first_line_the_tx_and_flags_both(built: dict) -> None:
    cycle = [r for r in built["rows"] if r["attempt_id"] == 2]
    assert [r["tx_id"] for r in cycle] == [3, None]
    assert [r["ambiguous"] for r in cycle] == [True, True]


def test_summary_counts(built: dict) -> None:
    s = built["summary"]
    assert s["lines"] == 6
    assert s["unmatched_lines"] == 3  # third X line, the no-match line, second Z line
    assert s["ambiguous_lines"] == 5
    assert s["laundering_total"] == 3  # X, X and Z; the no-match row is only in the patterns file
    assert s["laundering_in_pattern_assigned"] == 3
