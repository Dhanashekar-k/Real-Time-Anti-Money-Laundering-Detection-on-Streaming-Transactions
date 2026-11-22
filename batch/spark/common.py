"""Shared Spark helpers: session, Iceberg catalog, Spark-native money, exact row index, results.

The Spark money functions must give integers identical to `features/money.py` (tested on random
rows in tests/integration/test_money_spark_parity.py). They use integer arithmetic on
decimal(38,0) columns: decimal division would round by Spark's precision rules and could differ
from Python's exact Decimal in the last digit.
"""

import hashlib
import json
import os
import subprocess
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from features import money

ROOT = Path(__file__).resolve().parents[2]

# All big data lives under one root, on the external SSD (D37): raw files, the Iceberg warehouse and
# Spark's spill space. The Makefile exports the same default, so `make` and a bare `pytest` agree.
DEFAULT_DATA_ROOT = "/Volumes/Shekar_ssd/project_2"
DATA_ROOT = Path(os.environ.get("AML_DATA_ROOT") or DEFAULT_DATA_ROOT)
RAW_DIR = DATA_ROOT / "raw"
WAREHOUSE = DATA_ROOT / "warehouse"
SPILL_DIR = DATA_ROOT / "spill"
PG = "localhost:5432/iceberg_catalog"
PG_PASSWORD = os.environ.get("AML_PG_PASSWORD", "postgres")

# Pinned in docs/decisions.md (D-versions). iceberg-spark-runtime-3.5_2.12 = Spark 3.5 / Scala 2.12.
ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0"
POSTGRES_JDBC_PKG = "org.postgresql:postgresql:42.7.14"


class DataRootMissing(RuntimeError):
    """The big-data root (the external SSD) is not there."""


def require_data_root() -> Path:
    """Fail fast, with a clear message, if the SSD is not mounted. Nothing is created on the way."""
    if not DATA_ROOT.is_dir():
        raise DataRootMissing(
            f"SSD not mounted at {DATA_ROOT} (set AML_DATA_ROOT to use another location)"
        )
    return DATA_ROOT


def pyiceberg_uri() -> str:
    return f"postgresql+psycopg2://postgres:{PG_PASSWORD}@{PG}"


def get_spark(
    app_name: str, *, catalog: bool = True, max_partition_bytes: int | None = None
) -> SparkSession:
    """Local Spark, UTC, with the Iceberg JDBC catalog `lake` (compose Postgres) unless disabled.

    `AML_SPARK_DRIVER_MEMORY` (default 4g) must be set before the first session: HI-Medium wants
    8g to 12g. Spill space goes to $AML_DATA_ROOT/spill (the SSD), not /tmp. Raises
    `DataRootMissing` if the SSD is not mounted.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    require_data_root()
    scratch = SPILL_DIR
    scratch.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("AML_SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("AML_SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.local.dir", str(scratch))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.shuffle.partitions", os.environ.get("AML_SPARK_SHUFFLE", "32"))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
    )
    if max_partition_bytes is not None:
        builder = builder.config("spark.sql.files.maxPartitionBytes", str(max_partition_bytes))
    # The jars are loaded when the JVM launches and cannot be added to a JVM that a stopped
    # session left running (pytest runs several sessions in one process), so they are always on
    # the classpath, even for `catalog=False`.
    builder = builder.config("spark.jars.packages", f"{ICEBERG_PKG},{POSTGRES_JDBC_PKG}")
    if catalog:
        builder = (
            builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
            .config("spark.sql.catalog.lake.uri", f"jdbc:postgresql://{PG}")
            .config("spark.sql.catalog.lake.jdbc.user", "postgres")
            .config("spark.sql.catalog.lake.jdbc.password", PG_PASSWORD)
            .config("spark.sql.catalog.lake.warehouse", f"file://{WAREHOUSE}")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def drop_namespace(spark: SparkSession, namespace: str) -> None:
    """Drop every table (with files) in `lake.<namespace>`, then the namespace. Idempotent."""
    existing = {row[0] for row in spark.sql("SHOW NAMESPACES IN lake").collect()}
    if namespace not in existing:
        return
    for row in spark.sql(f"SHOW TABLES IN lake.{namespace}").collect():
        spark.sql(f"DROP TABLE IF EXISTS lake.{namespace}.{row['tableName']} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS lake.{namespace}")


# --- money, Spark-native -----------------------------------------------------------------------


def to_minor_col(amount: Column, currency: Column) -> Column:
    """`amount` (decimal(38,18)) -> minor units as long, half-even, per the currency's exponent.

    Same result as `money.to_minor`. Out of int64 range gives NULL (a plain cast would wrap
    silently), and ingest refuses any NULL. A string with more than 18 fractional digits is
    rounded half-up by Spark's string->decimal cast before this runs; the data has far fewer.
    """
    by_exponent: dict[int, list[str]] = {}
    for code, exp in money.EXPONENTS.items():
        by_exponent.setdefault(exp, []).append(code)
    int64_max = F.lit(Decimal(money.INT64_MAX))
    expr: Column | None = None
    for exp, codes in sorted(by_exponent.items()):
        scaled = F.bround(amount, exp) * F.lit(Decimal(10**exp))
        value = F.when(scaled <= int64_max, scaled.cast("long"))
        expr = (
            F.when(currency.isin(codes), value)
            if expr is None
            else expr.when(currency.isin(codes), value)
        )
    assert expr is not None
    return expr


def usd_cents_col(minor: Column, currency: Column, fx: Mapping[str, Decimal | str]) -> Column:
    """Minor units -> USD cents as long, half-even, exact (integer arithmetic on decimal(38,0)).

    cents = round_half_even(minor * R * 100 / 10^(exp + s)) where rate = R / 10^s, R an integer.
    R*100 and 10^(exp+s) are looked up per currency in literal maps, which keeps the expression
    small (one branch per currency made Spark's code generator give up). A currency without a
    rate, or a result beyond int64, gives NULL.
    """
    numerators: list[Column] = []
    denominators: list[Column] = []
    for code, exp in money.EXPONENTS.items():
        if code not in fx:
            continue
        rate = Decimal(fx[code])
        scale = max(0, -rate.as_tuple().exponent)  # type: ignore[operator]
        numerators += [
            F.lit(code),
            F.lit(Decimal(int(rate.scaleb(scale)) * 100)).cast("decimal(38,0)"),
        ]
        denominators += [F.lit(code), F.lit(Decimal(10 ** (exp + scale))).cast("decimal(38,0)")]
    numerator = minor.cast("decimal(38,0)") * F.create_map(*numerators)[currency]
    denominator = F.create_map(*denominators)[currency]
    remainder = numerator % denominator
    quotient = (numerator - remainder) / denominator  # exact: the difference is divisible
    twice = remainder * F.lit(Decimal(2))
    increment = (
        F.when(twice > denominator, F.lit(1))
        .when(twice == denominator, quotient % F.lit(2))
        .otherwise(F.lit(0))
    )
    cents = quotient + increment
    return F.when(cents <= F.lit(Decimal(money.INT64_MAX)), cents.cast("long"))


# --- owner bank (A/B) --------------------------------------------------------------------------


def owner_bank_id(bank: str) -> str:
    """'A' if the last hex digit of md5(bank) is even, else 'B'. Stable across runs and engines
    (Python's hash() is salted per process, so it must not be used)."""
    digit = int(hashlib.md5(bank.encode()).hexdigest()[-1], 16)  # noqa: S324 - not security
    return "A" if digit % 2 == 0 else "B"


def owner_bank_id_col(bank: Column) -> Column:
    digit = F.conv(F.substring(F.md5(bank), 32, 1), 16, 10).cast("int")
    return F.when(digit % 2 == 0, F.lit("A")).otherwise(F.lit("B"))


# --- exact row index ---------------------------------------------------------------------------

_POSITION_BITS = 33  # monotonically_increasing_id = (partition id << 33) + position in partition


def add_row_index(df: DataFrame, name: str) -> DataFrame:
    """Add `name` = 0-based index of each row in the DataFrame's current row order.

    Equivalent to `rdd.zipWithIndex` but stays on the JVM (a Python round trip over 32M rows is
    far slower). Exactness: per-partition row counts are summed in partition order, and
    `monotonically_increasing_id` supplies the position inside each partition. The input is
    persisted to disk first so both passes see identical partitions. The caller must treat the
    returned frame as persisted.
    """
    df = df.persist(StorageLevel.DISK_ONLY)
    counts = {
        row["pid"]: row["n"]
        for row in df.groupBy(F.spark_partition_id().alias("pid"))
        .agg(F.count("*").alias("n"))
        .collect()
    }
    offsets: dict[int, int] = {}
    running = 0
    for pid in sorted(counts):
        offsets[pid] = running
        running += counts[pid]
    pairs = [x for pid, off in offsets.items() for x in (F.lit(pid), F.lit(off))]
    offset_by_pid = F.create_map(*pairs)
    pid_col = F.spark_partition_id()
    position = F.monotonically_increasing_id() - F.shiftleft(pid_col.cast("long"), _POSITION_BITS)
    return df.withColumn(name, position + offset_by_pid[pid_col].cast("long"))


# --- results files -----------------------------------------------------------------------------


def provenance() -> dict[str, Any]:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout.strip()

    return {
        "git_sha": git("rev-parse", "HEAD"),
        "git_dirty": bool(git("status", "--porcelain")),
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
    }


def write_json(path: Path, doc: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2, sort_keys=True, default=str) + "\n")
