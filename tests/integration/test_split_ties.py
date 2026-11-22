"""The split boundary is a timestamp, through the real ingest path (not only in split.py).

10 rows: tx 0-4 at minutes 0-4, tx 5, 6, 7 all at minute 5, tx 8 at minute 6, tx 9 at minute 7.
N = 10, so t1 is the timestamp at index 6 (minute 5) and t2 the one at index 8 (minute 6).
train = minute < 5: tx 0-4 (5 rows). val = minute 5 or later and < 6: tx 5, 6, 7 (3 rows).
test = tx 8, 9. A pure index split would put tx 5 in train (index < 6): exactly one row differs.
"""

import shutil
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest

from batch.spark import common, split
from batch.spark.csv_to_iceberg import ingest

pytestmark = [pytest.mark.integration, pytest.mark.slow]

NAMESPACE = "aml_mini_ties"
FX = {c: Decimal("1") for c in ["USD", "EUR", "GBP", "CHF", "JPY", "CNY", "INR", "CAD", "AUD",
                                  "MXN", "BRL", "RUB", "SAR", "ILS", "BTC"]}  # fmt: skip
HEADER = (
    "Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,"
    "Amount Paid,Payment Currency,Payment Format,Is Laundering"
)
MINUTES = [0, 1, 2, 3, 4, 5, 5, 5, 6, 7]


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict]:
    work = tmp_path_factory.mktemp("ties")
    trans = work / "trans.csv"
    rows = [
        f"2022/09/01 00:{m:02d},10,A{i},20,B{i},1.00,US Dollar,1.00,US Dollar,ACH,0"
        for i, m in enumerate(MINUTES)
    ]
    trans.write_text("\n".join([HEADER, *rows, ""]))
    spark = common.get_spark("split-ties-test", max_partition_bytes=256)
    try:
        common.drop_namespace(spark, NAMESPACE)
        summary = ingest(
            spark, ds="ties", trans_path=trans, namespace=NAMESPACE, fx=FX, results_dir=work
        )
        doc = split.write_split_json(spark, ds="ties", namespace=NAMESPACE, results_dir=work)
        by_tx = {
            r["tx_id"]: r["split"]
            for r in spark.table(f"lake.{NAMESPACE}.raw_transactions").collect()
        }
        yield {"summary": summary, "doc": doc, "by_tx": by_tx}
    finally:
        try:
            common.drop_namespace(spark, NAMESPACE)
        finally:
            shutil.rmtree(common.WAREHOUSE / NAMESPACE, ignore_errors=True)
            spark.stop()


def test_equal_timestamps_stay_together_through_ingest(built: dict) -> None:
    assert [built["by_tx"][i] for i in range(10)] == ["train"] * 5 + ["val"] * 3 + ["test"] * 2
    assert built["summary"]["split_counts"] == {"train": 5, "val": 3, "test": 2}


def test_the_deviation_from_an_index_split_is_reported(built: Path) -> None:
    assert built["doc"]["boundary_indexes"] == {"t1": 6, "t2": 8}
    assert built["doc"]["rows_differing_from_pure_index_split"] == 1
    assert built["doc"]["stored_split_mismatches"] == 0
