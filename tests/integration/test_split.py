"""Temporal 60/20/20 split: boundaries are timestamps taken at index floor(0.6N) / floor(0.8N)."""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from batch.spark import common, split

pytestmark = [pytest.mark.integration, pytest.mark.slow]


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = common.get_spark("split-test", catalog=False)
    yield session
    session.stop()


@pytest.mark.parametrize(
    ("n", "expected"),
    [(1, (0, 0)), (5, (3, 4)), (10, (6, 8)), (16, (9, 12)), (5_078_345, (3_047_007, 4_062_676))],
)
def test_boundary_indexes_use_integer_arithmetic(n: int, expected: tuple[int, int]) -> None:
    assert split.boundary_indexes(n) == expected


def test_empty_table_is_an_error() -> None:
    with pytest.raises(ValueError, match="empty"):
        split.boundary_indexes(0)


def _frame(spark: SparkSession, timestamps: list[int]):  # noqa: ANN202
    rows = [(i, ts) for i, ts in enumerate(timestamps)]
    return spark.createDataFrame(rows, "tx_id long, event_ts_ms long")


def _counts(df) -> dict[str, int]:  # noqa: ANN001
    t1, t2 = split.compute_boundaries(df, df.count())
    out = df.withColumn("split", split.split_col(F.col("event_ts_ms"), t1, t2))
    return {r["split"]: r["n"] for r in out.groupBy("split").agg(F.count("*").alias("n")).collect()}


def test_distinct_timestamps_give_the_index_split(spark: SparkSession) -> None:
    df = _frame(spark, [i * 60_000 for i in range(10)])
    assert _counts(df) == {"train": 6, "val": 2, "test": 2}


def test_a_timestamp_never_straddles_two_splits(spark: SparkSession) -> None:
    # N=10: t1 is the timestamp at index 6. Indexes 5 and 6 share it, so BOTH go to val: train
    # has 5 rows, not the 6 a pure index split would give. This is the documented deviation (D17).
    ts = [0, 1, 2, 3, 4, 5, 5, 7, 8, 9]
    df = _frame(spark, [t * 60_000 for t in ts])
    assert _counts(df) == {"train": 5, "val": 3, "test": 2}


def test_every_row_is_in_exactly_one_split(spark: SparkSession) -> None:
    df = _frame(spark, [t * 60_000 for t in [0, 0, 0, 1, 1, 2, 2, 2, 2, 3, 4, 4, 5]])
    assert sum(_counts(df).values()) == 13


def test_splits_are_ordered_in_time(spark: SparkSession) -> None:
    ts = [t * 60_000 for t in [0, 0, 1, 2, 2, 3, 5, 5, 5, 8, 9, 9]]
    df = _frame(spark, ts)
    t1, t2 = split.compute_boundaries(df, df.count())
    out = df.withColumn("split", split.split_col(F.col("event_ts_ms"), t1, t2)).collect()
    by_split = {
        s: [r["event_ts_ms"] for r in out if r["split"] == s] for s in ("train", "val", "test")
    }
    assert max(by_split["train"]) < t1 <= min(by_split["val"])
    assert max(by_split["val"]) < t2 <= min(by_split["test"])
