"""Temporal 60/20/20 split by transaction order (Egressy et al., AAAI 2024, App. E.1).

t1 = event_ts at index floor(0.6 N) and t2 = event_ts at index floor(0.8 N) in (event_ts, tx_id)
order, where tx_id IS that index. The boundary is a *timestamp*, not an index: train is
`event_ts < t1`, val `t1 <= event_ts < t2`, test `event_ts >= t2`, so transactions in the same
minute never straddle two splits (small deviation from a pure index split; see D17).
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


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
