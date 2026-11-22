"""IBM AML transactions CSV -> Iceberg `lake.aml_<ds>.raw_transactions` (M01 steps 2-3, 5-6, 8).

Read the file as text lines, give each line its exact index, drop the header line, split on commas
into 11 fields by position (the file has two columns both named `Account`, so header names are
never used), parse amounts as decimals (never floats), order by (event_ts_ms, src_row), and assign
tx_id = position in that order. Money is integer minor units and USD cents via common.py.

Usage: python -m batch.spark.csv_to_iceberg --ds hi_small
"""

import argparse
import hashlib
import re
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from batch.spark import common, split
from features import money

ROOT = common.ROOT

RAW_COLUMNS = [
    "ts_raw", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
    "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering",
]  # fmt: skip
N_FIELDS = len(RAW_COLUMNS)

# Column order of the table (manual/02 section 4) plus `src_row`, which keeps the row's position
# in the source file so the tie-break behind tx_id can be audited.
TABLE_COLUMNS = [
    "tx_id", "src_row", "event_ts", "event_ts_ms", "sender_bank", "sender_account", "sender_key",
    "receiver_bank", "receiver_account", "receiver_key", "amount_paid_minor", "payment_currency",
    "amount_received_minor", "receiving_currency", "amount_paid_usd_cents",
    "amount_received_usd_cents", "payment_format", "owner_bank_id", "is_laundering", "split",
]  # fmt: skip

DATASET_FILES = {
    "hi_small": ("HI-Small_Trans.csv", "HI-Small_Patterns.txt"),
    "hi_medium": ("HI-Medium_Trans.csv", "HI-Medium_Patterns.txt"),
    "hi_large": ("HI-Large_Trans.csv", "HI-Large_Patterns.txt"),
}
_TS_FORMAT = "yyyy/MM/dd HH:mm"
_LOOKS_LIKE_A_ROW = re.compile(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2},")


def value_maps() -> tuple[dict[str, str], dict[str, str]]:
    """IBM currency and payment-format names -> codes, from the Bank A connector mapping (the
    single place they are written down; the connectors use the same file)."""
    doc = yaml.safe_load((ROOT / "connectors" / "banks" / "bank_a.yaml").read_text())
    return doc["value_maps"]["currency_names"], doc["value_maps"]["format_names"]


def _lookup(column: Column, mapping: Mapping[str, str]) -> Column:
    pairs = [F.lit(x) for kv in mapping.items() for x in kv]
    return F.create_map(*pairs)[column]


def read_transactions(spark: SparkSession, path: Path) -> DataFrame:
    """One row per data line: `src_row` (0-based file row, header excluded) + 11 string fields.

    Raises if the first line looks like data (no header to drop) or any line has != 11 fields.
    The returned frame is persisted (see common.add_row_index).
    """
    lines = spark.read.text(str(path))
    indexed = common.add_row_index(lines, "line_no")
    first = indexed.filter(F.col("line_no") == 0).first()
    if first is None:
        raise ValueError(f"{path} is empty")
    if _LOOKS_LIKE_A_ROW.match(first["value"]):
        raise ValueError(f"{path}: first line looks like data, not a header: {first['value']!r}")
    data = indexed.filter(F.col("line_no") > 0).withColumn("src_row", F.col("line_no") - 1)
    parts = F.split(F.col("value"), ",", -1)
    wrong = data.filter(F.size(parts) != N_FIELDS)
    if (n_wrong := wrong.count()) > 0:
        sample = [r["value"] for r in wrong.limit(3).collect()]
        raise ValueError(f"{path}: {n_wrong} lines do not have {N_FIELDS} fields, e.g. {sample}")
    # `raw_` prefix: an output column that shares its name with an input column in the same select
    # made a dependent expression silently evaluate to NULL (seen on Spark 3.5.9), so no output
    # of build_typed may reuse an input name.
    fields = [F.trim(parts[i]).alias(f"raw_{name}") for i, name in enumerate(RAW_COLUMNS)]
    return data.select("src_row", *fields)


def build_typed(raw: DataFrame, fx: Mapping[str, Decimal | str]) -> DataFrame:
    """Typed columns, keys, minor units, USD cents, owner bank. No ordering or split yet."""
    currency_names, format_names = value_maps()
    event_ts_s = F.unix_timestamp(F.col("raw_ts_raw"), _TS_FORMAT)
    paid = F.col("raw_amount_paid").cast("decimal(38,18)")
    received = F.col("raw_amount_received").cast("decimal(38,18)")
    paid_ccy = _lookup(F.col("raw_payment_currency"), currency_names)
    recv_ccy = _lookup(F.col("raw_receiving_currency"), currency_names)
    paid_minor = common.to_minor_col(paid, paid_ccy)
    recv_minor = common.to_minor_col(received, recv_ccy)
    return raw.select(
        "src_row",
        F.timestamp_seconds(event_ts_s).alias("event_ts"),
        (event_ts_s * 1000).alias("event_ts_ms"),
        F.col("raw_from_bank").alias("sender_bank"),
        F.col("raw_from_account").alias("sender_account"),
        F.concat_ws(":", F.col("raw_from_bank"), F.col("raw_from_account")).alias("sender_key"),
        F.col("raw_to_bank").alias("receiver_bank"),
        F.col("raw_to_account").alias("receiver_account"),
        F.concat_ws(":", F.col("raw_to_bank"), F.col("raw_to_account")).alias("receiver_key"),
        paid_minor.alias("amount_paid_minor"),
        paid_ccy.alias("payment_currency"),
        recv_minor.alias("amount_received_minor"),
        recv_ccy.alias("receiving_currency"),
        common.usd_cents_col(paid_minor, paid_ccy, fx).alias("amount_paid_usd_cents"),
        common.usd_cents_col(recv_minor, recv_ccy, fx).alias("amount_received_usd_cents"),
        _lookup(F.col("raw_payment_format"), format_names).alias("payment_format"),
        common.owner_bank_id_col(F.col("raw_from_bank")).alias("owner_bank_id"),
        F.col("raw_is_laundering").cast("int").alias("is_laundering"),
    )


def check_no_nulls(typed: DataFrame, raw: DataFrame) -> None:
    """Refuse the whole load if any derived column is NULL (unparsable timestamp or amount,
    unmapped currency or format name, amount beyond int64) or the label is not 0/1."""
    counts = typed.select(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in typed.columns],
        F.sum((~F.col("is_laundering").isin(0, 1)).cast("int")).alias("_bad_label"),
    ).first()
    bad = {k: v for k, v in counts.asDict().items() if v}
    if not bad:
        return
    unmapped: dict[str, list[str]] = {}
    for derived in ("payment_currency", "receiving_currency", "payment_format"):
        if derived in bad:
            joined = typed.join(raw.select("src_row", f"raw_{derived}"), "src_row")
            names = joined.filter(F.col(derived).isNull()).select(f"raw_{derived}").distinct()
            unmapped[derived] = [r[0] for r in names.limit(10).collect()]
    raise ValueError(f"NULLs in derived columns (column: rows): {bad}; unmapped names: {unmapped}")


def _file_facts(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    lines = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
            lines += chunk.count(b"\n")
        # a final line without a newline still counts as a line
        handle.seek(max(handle.tell() - 1, 0))
        if handle.read(1) not in (b"\n", b""):
            lines += 1
    return {
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
        "lines_minus_header": lines - 1,
    }


def ingest(
    spark: SparkSession,
    *,
    ds: str,
    trans_path: Path,
    namespace: str,
    fx: Mapping[str, Decimal | str],
    results_dir: Path,
    fx_version: int | None = None,
) -> dict[str, Any]:
    """Write `lake.<namespace>.raw_transactions` and `<results_dir>/<ds>/ingest.json`."""
    source = _file_facts(trans_path)
    raw = read_transactions(spark, trans_path)
    typed = build_typed(raw, fx)
    check_no_nulls(typed, raw)

    ordered = common.add_row_index(typed.orderBy("event_ts_ms", "src_row"), "tx_id")
    n = ordered.count()
    t1_ms, t2_ms = split.compute_boundaries(ordered, n)
    final = ordered.withColumn("split", split.split_col(F.col("event_ts_ms"), t1_ms, t2_ms)).select(
        *TABLE_COLUMNS
    )

    table = f"lake.{namespace}.raw_transactions"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{namespace}")
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    # tx_id order is event-time order, so each write task sees days in order (clustered by day)
    # and rows in tx_id order. The sort order is declared afterwards so later writers keep it.
    final.writeTo(table).partitionedBy(F.days("event_ts")).option(
        "fanout-enabled", "true"
    ).createOrReplace()
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY tx_id")

    written = spark.table(table)
    rows = written.count()
    if rows != n or rows != source["lines_minus_header"]:
        raise RuntimeError(
            f"row count mismatch: table {rows}, parsed {n}, "
            f"source lines {source['lines_minus_header']}"
        )
    split_counts = {
        r["split"]: r["n"] for r in written.groupBy("split").agg(F.count("*").alias("n")).collect()
    }
    summary: dict[str, Any] = {
        **common.provenance(),
        "dataset": ds,
        "config": {
            "namespace": namespace,
            "table": table,
            "timezone": "UTC",
            "ts_format": _TS_FORMAT,
            "spark_master": spark.sparkContext.master,
            "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory"),
        },
        "source": source,
        "source_partitions": spark.read.text(str(trans_path)).rdd.getNumPartitions(),
        "fx_version": fx_version,
        "rows": rows,
        "source_lines_minus_header": source["lines_minus_header"],
        "laundering": written.filter("is_laundering = 1").count(),
        "split_counts": split_counts,
        "split_boundaries_ms": {"t1": t1_ms, "t2": t2_ms},
    }
    common.write_json(results_dir / ds / "ingest.json", summary)
    return summary


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", required=True, choices=sorted(DATASET_FILES))
    parser.add_argument("--trans", type=Path, help="override the transactions CSV path")
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results" / "M01")
    args = parser.parse_args(argv)
    common.require_data_root()
    trans = args.trans or common.RAW_DIR / DATASET_FILES[args.ds][0]
    fx = money.load_fx()  # fails until features/fx_rates.yaml is frozen (M01 step 6)
    spark = common.get_spark(f"csv-to-iceberg-{args.ds}")
    try:
        summary = ingest(
            spark,
            ds=args.ds,
            trans_path=trans,
            namespace=f"aml_{args.ds}",
            fx=fx,
            results_dir=args.results_dir,
            fx_version=money.FX_VERSION_REQUIRED,
        )
    finally:
        spark.stop()
    print(f"{args.ds}: {summary['rows']} rows, {summary['laundering']} laundering, "
          f"splits {summary['split_counts']}")  # fmt: skip


if __name__ == "__main__":
    main()
