"""Temporal 60/20/20 split by transaction order (Egressy et al., AAAI 2024, App. E.1).

t1 = event_ts at index floor(0.6 N) and t2 = event_ts at index floor(0.8 N) in (event_ts, tx_id)
order, where tx_id IS that index. The boundary is a *timestamp*, not an index: train is
`event_ts < t1`, val `t1 <= event_ts < t2`, test `event_ts >= t2`, so transactions in the same
minute never straddle two splits (small deviation from a pure index split; see D17).
"""

import argparse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from batch.spark import common


def boundary_indexes(n: int) -> tuple[int, int]:
    """floor(0.6 n) and floor(0.8 n) in integer arithmetic (no float rounding at the edge)."""
    if n < 1:
        raise ValueError("cannot split an empty table")
    return (6 * n) // 10, (8 * n) // 10


def compute_boundaries(df: DataFrame, n: int) -> tuple[int, int]:
    """(t1_ms, t2_ms) from a frame with `tx_id` and `event_ts_ms`; tx_id must be 0..n-1."""
    k1, k2 = boundary_indexes(n)
    rows = df.filter(F.col("tx_id").isin(k1, k2)).select("tx_id", "event_ts_ms").collect()
    at = {r["tx_id"]: r["event_ts_ms"] for r in rows}
    return at[k1], at[k2]


def split_col(event_ts_ms: Column, t1_ms: int, t2_ms: int) -> Column:
    return (
        F.when(event_ts_ms < F.lit(t1_ms), F.lit("train"))
        .when(event_ts_ms < F.lit(t2_ms), F.lit("val"))
        .otherwise(F.lit("test"))
    )


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat()


def write_split_json(
    spark: SparkSession, *, ds: str, namespace: str, results_dir: Path
) -> dict[str, Any]:
    """Recompute the boundaries from the stored table, check the stored `split` column against
    them, and write `<results_dir>/<ds>/split.json` (the numbers D17 quotes)."""
    df = spark.table(f"lake.{namespace}.raw_transactions")
    n = df.count()
    k1, k2 = boundary_indexes(n)
    t1_ms, t2_ms = compute_boundaries(df.select("tx_id", "event_ts_ms"), n)
    expected = split_col(F.col("event_ts_ms"), t1_ms, t2_ms)
    by_index = (
        F.when(F.col("tx_id") < k1, F.lit("train"))
        .when(F.col("tx_id") < k2, F.lit("val"))
        .otherwise(F.lit("test"))
    )
    checked = df.select(
        "split",
        "is_laundering",
        "event_ts_ms",
        expected.alias("expected_split"),
        by_index.alias("index_split"),
    )
    mismatches = checked.filter(F.col("split") != F.col("expected_split")).count()
    differing = checked.filter(F.col("expected_split") != F.col("index_split")).count()
    splits: dict[str, Any] = {}
    for row in (
        checked.groupBy("split")
        .agg(
            F.count("*").alias("rows"),
            F.sum("is_laundering").alias("laundering"),
            F.min("event_ts_ms").alias("first_ts_ms"),
            F.max("event_ts_ms").alias("last_ts_ms"),
        )
        .collect()
    ):
        splits[row["split"]] = {
            "rows": row["rows"],
            "laundering": row["laundering"],
            "first_ts_ms": row["first_ts_ms"],
            "last_ts_ms": row["last_ts_ms"],
        }
    doc: dict[str, Any] = {
        **common.provenance(),
        "dataset": ds,
        "config": {"table": f"lake.{namespace}.raw_transactions", "fractions": [0.6, 0.2, 0.2]},
        "rows": n,
        "boundary_indexes": {"t1": k1, "t2": k2},
        "t1_ms": t1_ms,
        "t1_utc": _iso(t1_ms),
        "t2_ms": t2_ms,
        "t2_utc": _iso(t2_ms),
        "splits": splits,
        "stored_split_mismatches": mismatches,
        "rows_differing_from_pure_index_split": differing,
    }
    common.write_json(results_dir / ds / "split.json", doc)
    return doc


def main(argv: list[str] | None = None) -> None:
    from batch.spark.csv_to_iceberg import DATASET_FILES  # here: csv_to_iceberg imports this module

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", required=True, choices=sorted(DATASET_FILES))
    parser.add_argument("--results-dir", type=Path, default=common.ROOT / "results" / "M01")
    args = parser.parse_args(argv)
    spark = common.get_spark(f"split-{args.ds}")
    try:
        doc = write_split_json(
            spark, ds=args.ds, namespace=f"aml_{args.ds}", results_dir=args.results_dir
        )
    finally:
        spark.stop()
    print(f"{args.ds}: t1={doc['t1_utc']} t2={doc['t2_utc']} "
          f"{ {k: v['rows'] for k, v in doc['splits'].items()} } "
          f"mismatches={doc['stored_split_mismatches']}")  # fmt: skip


if __name__ == "__main__":
    main()
