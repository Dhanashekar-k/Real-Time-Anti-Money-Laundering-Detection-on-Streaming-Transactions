"""Implied FX rates from cross-currency rows (M01 step 4 of the manual, our step 6).

For rows where payment currency != receiving currency, `amount_received / amount_paid` is the
rate the generator used for that pair: units of the receiving currency per unit of the paid one.
This script MEASURES only: it writes `results/M01/<ds>/fx_variance.json` and prints a table. It
never writes `features/fx_rates.yaml`; the rates are frozen after a person reviews the table.

Rates that end up in the frozen table are exact Decimals (ratio of sums, a string). The spread
statistics (stddev, CV) are diagnostics computed in floating point; they are never used as money.

Usage: python -m batch.spark.fx --ds hi_small
"""

import argparse
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from batch.spark import common, csv_to_iceberg

ROOT = common.ROOT
SIGNIFICANT_DIGITS = 12
LARGE_PAID_THRESHOLD = Decimal(100)  # rows paying >= 100 units: rounding noise in the rate is small


def _ratio(numerator: Decimal, denominator: Decimal) -> str | None:
    if denominator == 0:
        return None
    with localcontext() as ctx:
        ctx.prec = SIGNIFICANT_DIGITS
        ctx.rounding = ROUND_HALF_EVEN
        return str(+(numerator / denominator))  # unary plus applies the context precision


def analyze(spark: SparkSession, *, ds: str, trans_path: Path, results_dir: Path) -> dict[str, Any]:
    raw = csv_to_iceberg.read_transactions(spark, trans_path)
    currency_names, _ = csv_to_iceberg.value_maps()
    lookup = csv_to_iceberg._lookup  # noqa: SLF001 - same mapping code as the ingest
    rows = raw.select(
        lookup(F.col("raw_payment_currency"), currency_names).alias("paid_ccy"),
        lookup(F.col("raw_receiving_currency"), currency_names).alias("recv_ccy"),
        F.col("raw_amount_paid").cast("decimal(38,18)").alias("paid"),
        F.col("raw_amount_received").cast("decimal(38,18)").alias("recv"),
        F.to_date(F.unix_timestamp("raw_ts_raw", "yyyy/MM/dd HH:mm").cast("timestamp")).alias(
            "day"
        ),
    )
    unmapped = rows.filter(
        "paid_ccy IS NULL OR recv_ccy IS NULL OR paid IS NULL OR recv IS NULL"
    ).count()
    if unmapped:
        raise ValueError(f"{unmapped} rows have an unmapped currency name or unparsable amount")
    cross = rows.filter("paid_ccy <> recv_ccy")
    positive = cross.filter("paid > 0 AND recv > 0")
    excluded_nonpositive = cross.count() - positive.count()

    keys = ["paid_ccy", "recv_ccy"]
    overall = (
        positive.groupBy(*keys)
        .agg(
            F.count("*").alias("n"),
            F.sum("paid").cast("decimal(38,18)").alias("sum_paid"),
            F.sum("recv").cast("decimal(38,18)").alias("sum_recv"),
            F.mean(F.col("recv").cast("double") / F.col("paid").cast("double")).alias("mean_rate"),
            F.stddev(F.col("recv").cast("double") / F.col("paid").cast("double")).alias("sd_rate"),
            F.min(F.col("recv").cast("double") / F.col("paid").cast("double")).alias("min_rate"),
            F.max(F.col("recv").cast("double") / F.col("paid").cast("double")).alias("max_rate"),
        )
        .collect()
    )
    large = {
        (r["paid_ccy"], r["recv_ccy"]): r
        for r in positive.filter(F.col("paid") >= F.lit(LARGE_PAID_THRESHOLD))
        .groupBy(*keys)
        .agg(
            F.count("*").alias("n"),
            F.sum("paid").cast("decimal(38,18)").alias("sum_paid"),
            F.sum("recv").cast("decimal(38,18)").alias("sum_recv"),
            F.mean(F.col("recv").cast("double") / F.col("paid").cast("double")).alias("mean_rate"),
            F.stddev(F.col("recv").cast("double") / F.col("paid").cast("double")).alias("sd_rate"),
        )
        .collect()
    }
    daily = (
        positive.groupBy(*keys, "day")
        .agg(F.sum("paid").alias("p"), F.sum("recv").alias("r"), F.count("*").alias("n"))
        .withColumn("day_rate", F.col("r").cast("double") / F.col("p").cast("double"))
        .groupBy(*keys)
        .agg(
            F.count("*").alias("n_days"),
            F.mean("day_rate").alias("mean_day_rate"),
            F.stddev("day_rate").alias("sd_day_rate"),
            F.min("day_rate").alias("min_day_rate"),
            F.max("day_rate").alias("max_day_rate"),
        )
        .collect()
    )
    daily_by_pair = {(r["paid_ccy"], r["recv_ccy"]): r for r in daily}

    pairs = []
    for r in sorted(overall, key=lambda x: (x["paid_ccy"], x["recv_ccy"])):
        pair = (r["paid_ccy"], r["recv_ccy"])
        big = large.get(pair)
        day = daily_by_pair[pair]
        pairs.append(
            {
                "paid_currency": pair[0],
                "received_currency": pair[1],
                "n": r["n"],
                "weighted_rate": _ratio(r["sum_recv"], r["sum_paid"]),
                "mean_rate": r["mean_rate"],
                "min_rate": r["min_rate"],
                "max_rate": r["max_rate"],
                "cv": (r["sd_rate"] / r["mean_rate"]) if r["sd_rate"] is not None else None,
                "n_paid_ge_100": big["n"] if big else 0,
                "weighted_rate_paid_ge_100": _ratio(big["sum_recv"], big["sum_paid"])
                if big
                else None,
                "cv_paid_ge_100": (
                    big["sd_rate"] / big["mean_rate"]
                    if big and big["sd_rate"] is not None
                    else None
                ),
                "n_days": day["n_days"],
                "daily_rate_min": day["min_day_rate"],
                "daily_rate_max": day["max_day_rate"],
                "daily_cv": (
                    day["sd_day_rate"] / day["mean_day_rate"]
                    if day["sd_day_rate"] is not None
                    else None
                ),
            }
        )
    doc: dict[str, Any] = {
        **common.provenance(),
        "dataset": ds,
        "config": {
            "source": str(trans_path),
            "significant_digits": SIGNIFICANT_DIGITS,
            "large_paid_threshold_units": str(LARGE_PAID_THRESHOLD),
            "note": "weighted_rate = sum(received)/sum(paid), exact decimal; cv and daily_cv are "
            "float diagnostics",
        },
        "rows": rows.count(),
        "cross_currency_rows": cross.count(),
        "cross_currency_rows_excluded_nonpositive": excluded_nonpositive,
        "pairs": pairs,
    }
    common.write_json(results_dir / ds / "fx_variance.json", doc)
    return doc


def format_table(doc: dict[str, Any]) -> str:
    columns = [("pair", 9), ("n", 10), ("weighted", 16), ("cv", 10), ("cv(>=100)", 11)]
    columns += [("days", 6), ("daily cv", 10)]
    header = "".join(f"{name:<9}" if name == "pair" else f"{name:>{w}}" for name, w in columns)

    def f(x: float | None) -> str:
        return "-" if x is None else f"{x:.2e}"

    lines = [header]
    for p in doc["pairs"]:
        lines.append(
            f"{p['paid_currency'] + '>' + p['received_currency']:<9}{p['n']:>10}"
            f"{(p['weighted_rate'] or '-'):>16}{f(p['cv']):>10}{f(p['cv_paid_ge_100']):>11}"
            f"{p['n_days']:>6}{f(p['daily_cv']):>10}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", required=True, choices=sorted(csv_to_iceberg.DATASET_FILES))
    parser.add_argument("--trans", type=Path)
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results" / "M01")
    args = parser.parse_args(argv)
    common.require_data_root()
    trans = args.trans or common.RAW_DIR / csv_to_iceberg.DATASET_FILES[args.ds][0]
    spark = common.get_spark(f"fx-{args.ds}", catalog=False)
    try:
        doc = analyze(spark, ds=args.ds, trans_path=trans, results_dir=args.results_dir)
    finally:
        spark.stop()
    print(format_table(doc))


if __name__ == "__main__":
    main()
