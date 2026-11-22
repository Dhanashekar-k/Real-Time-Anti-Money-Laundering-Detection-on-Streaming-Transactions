"""Spark feature job (M02 step 8): `aml_<ds>.raw_transactions` -> `aml_<ds>.features_tx`.

    uv run python -m batch.spark.features --ds hi_small      (or: make features DS=hi_small)

The features are defined once, in `features/spec.yaml`, and computed by the same Python code the
tests use:
- account features: every transaction becomes an OUT leg on the sender and an IN leg on the
  receiver; `groupBy(account_key).applyInPandas(compute_account_legs)` runs the oracle
  (`features/reference.py`) once per account; the S row and the R row of a transaction are joined
  on `tx_id` and given the `s_` / `r_` prefix;
- tx features: expressed in Spark SQL (integer arithmetic, vocabulary codes), EXCEPT
  `log10_amount_paid_usd`, which goes through a pandas UDF that calls
  `features.tx_features.log10_feature` (the D42 rounding is never re-expressed in Spark SQL, and
  nothing here uses `round` or `bround`);
- columns come out in the order of the committed `features/feature_names_v1.txt`, which this job
  reads as the authority and refuses to run if it is stale.

Only the columns in `RAW_COLUMNS` are read from the raw table: the label (`is_laundering`), `split`
and the pattern data never enter this job (CLAUDE.md rule 2). The output table has no label.

A hot account is one group in one Python process (HI-Small's largest has 169,756 transactions,
HI-Medium's 1,083,821, D36), so the run time is set by that group, not by the cluster.
"""

import argparse
import functools
import os
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from batch.spark import common
from features import expand, reference, tx_features
from features.spec import Spec, load_spec

RAW_COLUMNS = (
    "tx_id",
    "event_ts_ms",
    "sender_bank",
    "sender_key",
    "receiver_bank",
    "receiver_key",
    "amount_paid_usd_cents",
    "amount_received_usd_cents",
    "payment_currency",
    "receiving_currency",
    "payment_format",
    "owner_bank_id",
)
KEY_COLUMNS = ("tx_id", "event_ts_ms", "owner_bank_id")
_SPARK_TYPE = {"int64": "long", "object": "string", "float64": "double", "Int64": "long"}
_HOUR_MS = 3_600_000
_DAY_MS = 86_400_000


def get_session(app_name: str, catalog: bool = False) -> SparkSession:
    """Local Spark whose Python workers can `import features` (the repo root is on PYTHONPATH)."""
    paths = [str(common.ROOT), os.environ.get("PYTHONPATH", "")]
    os.environ["PYTHONPATH"] = os.pathsep.join(p for p in paths if p)
    spark = common.get_spark(app_name, catalog=catalog)
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    return spark


@functools.lru_cache(maxsize=1)
def _worker_spec() -> Spec:
    return load_spec()


def load_feature_names(spec: Spec) -> list[str]:
    """The committed names file is the authority; refuse to run if it is stale."""
    path = expand.names_path(spec)
    if path.read_text() != expand.render(spec):
        raise RuntimeError(f"{path} is out of date: run `uv run python -m features.expand`")
    return path.read_text().split()


# --- account features ---------------------------------------------------------------------------


def legs_df(raw: DataFrame) -> DataFrame:
    """One OUT leg per transaction on the sender and one IN leg on the receiver."""
    common_cols = [
        "tx_id",
        "event_ts_ms",
        "payment_currency",
        "receiving_currency",
        "payment_format",
    ]
    out = raw.select(
        F.col("sender_key").alias("account_key"),
        F.lit("OUT").alias("direction"),
        F.col("receiver_key").alias("counterparty_key"),
        F.col("amount_paid_usd_cents").alias("amount_usd_cents"),
        *common_cols,
    )
    inn = raw.select(
        F.col("receiver_key").alias("account_key"),
        F.lit("IN").alias("direction"),
        F.col("sender_key").alias("counterparty_key"),
        F.col("amount_received_usd_cents").alias("amount_usd_cents"),
        *common_cols,
    )
    return out.unionByName(inn)


def _account_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """Runs in a Python worker: the oracle on one account's legs."""
    return reference.compute_account_legs(pdf[list(reference.LEG_COLUMNS)], _worker_spec())


def account_features_df(legs: DataFrame, spec: Spec) -> DataFrame:
    schema = ", ".join(f"{c} {_SPARK_TYPE[d]}" for c, d in reference.output_dtypes(spec).items())
    return legs.groupBy("account_key").applyInPandas(_account_features, schema)


def _role_frame(result: DataFrame, role: str, prefix: str, spec: Spec) -> DataFrame:
    cols = reference.role_columns(spec, role)
    return result.where(F.col("role") == role).select(
        "tx_id", *[F.col(c).alias(prefix + c) for c in cols]
    )


# --- tx features --------------------------------------------------------------------------------


@F.pandas_udf(DoubleType())
def _log10_feature(cents: pd.Series) -> pd.Series:
    spec = _worker_spec()
    values = [tx_features.log10_feature(int(c), spec) for c in cents]
    return pd.Series(values, index=cents.index, dtype="float64")


def _code(col: Column, vocabulary: tuple[str, ...]) -> Column:
    pairs = [x for i, v in enumerate(vocabulary) for x in (F.lit(v), F.lit(i))]
    return F.element_at(F.create_map(*pairs), col).cast("long")  # unknown -> NULL, caught later


def tx_features_df(raw: DataFrame, spec: Spec) -> DataFrame:
    """`tx_id` plus the nine tx features, in spec order."""
    return raw.select(
        "tx_id",
        _log10_feature(F.col("amount_paid_usd_cents")).alias("log10_amount_paid_usd"),
        (F.col("payment_currency") != F.col("receiving_currency"))
        .cast("long")
        .alias("is_cross_currency"),
        _code(F.col("payment_format"), spec.payment_formats).alias("payment_format_code"),
        _code(F.col("payment_currency"), spec.currencies).alias("payment_currency_code"),
        _code(F.col("receiving_currency"), spec.currencies).alias("receiving_currency_code"),
        # integer arithmetic only (DIV), as in features/tx_features.py
        F.expr(f"(event_ts_ms DIV {_HOUR_MS}) % 24").cast("long").alias("hour_of_day"),
        F.expr(f"(event_ts_ms DIV {_DAY_MS} + 3) % 7").cast("long").alias("day_of_week"),
        (F.col("sender_bank") == F.col("receiver_bank")).cast("long").alias("same_bank"),
        (F.col("sender_key") == F.col("receiver_key")).cast("long").alias("self_transfer"),
    ).select("tx_id", *[c for c in _tx_names(spec)])


def _tx_names(spec: Spec) -> list[str]:
    return [n for t in spec.tx_features for n in expand.expand_template(t, spec)]


def raw_frame_from_rows(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    """A frame with the `RAW_COLUMNS` of the raw table from plain dicts (tests and slices)."""
    fields = [
        ("tx_id", "long"),
        ("event_ts_ms", "long"),
        ("sender_bank", "string"),
        ("sender_key", "string"),
        ("receiver_bank", "string"),
        ("receiver_key", "string"),
        ("amount_paid_usd_cents", "long"),
        ("amount_received_usd_cents", "long"),
        ("payment_currency", "string"),
        ("receiving_currency", "string"),
        ("payment_format", "string"),
    ]
    schema = ", ".join(f"{n} {t}" for n, t in fields)
    df = spark.createDataFrame([tuple(r[n] for n, _ in fields) for r in rows], schema)
    return df.withColumn("owner_bank_id", common.owner_bank_id_col(F.col("sender_bank")))


# --- the job --------------------------------------------------------------------------------


def compute_features(raw: DataFrame, spec: Spec) -> DataFrame:
    """`tx_id`, `event_ts_ms`, `owner_bank_id`, then the features in `feature_names_v<N>.txt` order.

    Only RAW_COLUMNS are used; any other column on `raw` (a label, `split`) is dropped first.
    """
    names = load_feature_names(spec)
    raw = raw.select(*RAW_COLUMNS)
    result = account_features_df(legs_df(raw), spec)
    s = _role_frame(result, "S", "s_", spec)
    r = _role_frame(result, "R", "r_", spec)
    keys = raw.select(*KEY_COLUMNS)
    df = keys.join(tx_features_df(raw, spec), "tx_id").join(s, "tx_id").join(r, "tx_id")
    return df.select(*KEY_COLUMNS, *names)


def quality_checks(df: DataFrame, n_expected: int) -> dict[str, Any]:
    """Row count, unique tx_id, nulls in the columns that must never be null, NaN anywhere."""
    dtypes = dict(df.dtypes)
    must_not_be_null = [c for c, t in dtypes.items() if t != "double"]
    aggs = [F.count("*").alias("_rows"), F.countDistinct("tx_id").alias("_distinct")]
    aggs += [F.sum(F.col(c).isNull().cast("int")).alias(f"null:{c}") for c in dtypes]
    aggs += [
        F.sum(F.isnan(c).cast("int")).alias(f"nan:{c}") for c, t in dtypes.items() if t == "double"
    ]
    row = df.agg(*aggs).first().asDict()
    null_counts = {c: int(row[f"null:{c}"] or 0) for c in dtypes}
    bad_nulls = {c: n for c, n in null_counts.items() if c in must_not_be_null and n}
    nan_counts = {k[4:]: int(v or 0) for k, v in row.items() if k.startswith("nan:")}
    out = {
        "rows": int(row["_rows"]),
        "distinct_tx_id": int(row["_distinct"]),
        "rows_expected": n_expected,
        "null_counts_nullable_columns": {
            c: n for c, n in null_counts.items() if c not in must_not_be_null
        },
        "unexpected_nulls": bad_nulls,
        "nan_cells": {c: n for c, n in nan_counts.items() if n},
    }
    if out["rows"] != n_expected or out["distinct_tx_id"] != n_expected:
        raise RuntimeError(f"row count mismatch: {out}")
    if bad_nulls or out["nan_cells"]:
        raise RuntimeError(f"unexpected NULL or NaN cells: {bad_nulls} {out['nan_cells']}")
    return out


def run(ds: str, results_dir: Path) -> dict[str, Any]:
    common.require_data_root()
    spec = load_spec()
    spark = get_session(f"features-{ds}", catalog=True)
    try:
        source = f"lake.aml_{ds}.raw_transactions"
        target = f"lake.aml_{ds}.features_tx"
        snapshot = spark.sql(
            f"SELECT snapshot_id FROM {source}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()[0]
        raw = spark.table(source)
        n_raw = raw.count()
        df = compute_features(raw, spec)
        # one snapshot only (D36's glob-read caveat): drop the old table, then create
        spark.sql(f"DROP TABLE IF EXISTS {target} PURGE")
        started = time.perf_counter()
        df.writeTo(target).using("iceberg").create()
        seconds = time.perf_counter() - started
        checks = quality_checks(spark.table(target), n_raw)
        conf = spark.sparkContext.getConf()
        doc = {
            "dataset": ds,
            **common.provenance(),
            "config": {
                "source": source,
                "source_snapshot_id": snapshot,
                "target": target,
                "spec_version": spec.spec_version,
                "fx_version_required": spec.fx_version_required,
                "n_features": len(load_feature_names(spec)),
                "spark_master": spark.sparkContext.master,
                "driver_memory": conf.get("spark.driver.memory"),
                "shuffle_partitions": conf.get("spark.sql.shuffle.partitions"),
                "python": sys.version.split()[0],
            },
            "write_seconds": round(seconds, 1),
            "checks": checks,
        }
        common.write_json(results_dir / ds / "features.json", doc)
        return doc
    finally:
        spark.stop()


def smoke(n: int = 200) -> int:
    """The job's code path on a small synthetic table, with no lake and no catalog."""
    from features import synthetic

    spark = get_session("features-smoke")
    try:
        spec = load_spec()
        rows = synthetic.random_transactions(n, seed=1, n_accounts=8)
        return compute_features(raw_frame_from_rows(spark, rows), spec).count()
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", choices=["hi_small", "hi_medium"])
    parser.add_argument("--smoke", action="store_true", help="tiny synthetic run, no lake")
    parser.add_argument("--results-dir", type=Path, default=common.ROOT / "results" / "M02")
    args = parser.parse_args(argv)
    if args.smoke:
        print(f"SMOKE_ROWS {smoke()}")
        return
    if not args.ds:
        parser.error("--ds is required unless --smoke")
    doc = run(args.ds, args.results_dir)
    print(
        f"{args.ds}: {doc['checks']['rows']:,} rows, {doc['config']['n_features']} features, "
        f"{doc['write_seconds']} s"
    )


if __name__ == "__main__":
    # Run through the importable module, not `__main__`: the functions shipped to the Python
    # workers are pickled by reference, and in a worker `__main__` is Spark's daemon, not this file.
    from batch.spark import features as _job

    _job.main()
