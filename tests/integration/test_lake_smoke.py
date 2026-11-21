"""Three engines, one table: Spark writes an Iceberg table in the JDBC catalog on the compose
Postgres; PyIceberg and DuckDB read the same rows; a Flink container sees the files at the same
absolute path (Iceberg metadata stores absolute paths). Needs `make up`.
"""

import glob
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyspark.sql import SparkSession

pytestmark = [pytest.mark.integration, pytest.mark.slow]

ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE = ROOT / "var" / "warehouse"  # the compose stack mounts this at the same path
TABLE_DIR = WAREHOUSE / "smoke" / "t1"
PG = "localhost:5432/iceberg_catalog"
PG_PASSWORD = os.environ.get("AML_PG_PASSWORD", "postgres")

# Pinned in docs/decisions.md (D-versions). iceberg-spark-runtime-3.5_2.12 = Spark 3.5 / Scala 2.12.
ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0"
POSTGRES_JDBC_PKG = "org.postgresql:postgresql:42.7.14"

# Money is int64 minor units, never float (CLAUDE.md), so the smoke table uses BIGINT cents.
ROWS = [(1, "a", 100), (2, "b", 200), (3, "c", 300)]


@pytest.fixture(scope="module")
def spark_rows() -> Iterator[list[tuple[int, str, int]]]:
    """Create lake.smoke.t1 with Spark, insert ROWS, read them back through Spark."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder.appName("lake-smoke")
        .master("local[2]")
        .config("spark.jars.packages", f"{ICEBERG_PKG},{POSTGRES_JDBC_PKG}")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.lake.uri", f"jdbc:postgresql://{PG}")
        .config("spark.sql.catalog.lake.jdbc.user", "postgres")
        .config("spark.sql.catalog.lake.jdbc.password", PG_PASSWORD)
        .config("spark.sql.catalog.lake.warehouse", f"file://{WAREHOUSE}")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        spark.sql("DROP TABLE IF EXISTS lake.smoke.t1 PURGE")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.smoke")
        spark.sql(
            "CREATE TABLE lake.smoke.t1 (id BIGINT, name STRING, amount_cents BIGINT) USING iceberg"
        )
        values = ",".join(f"({i},'{n}',{a})" for i, n, a in ROWS)
        spark.sql(f"INSERT INTO lake.smoke.t1 VALUES {values}")
        rows = sorted(tuple(r) for r in spark.sql("SELECT * FROM lake.smoke.t1").collect())
        yield rows
    finally:
        try:
            spark.sql("DROP TABLE IF EXISTS lake.smoke.t1 PURGE")
        finally:
            spark.stop()


@pytest.fixture(scope="module")
def pyiceberg_table(spark_rows: list[tuple[int, str, int]]):  # noqa: ANN201
    assert spark_rows  # the table exists only after the Spark fixture has run
    catalog = SqlCatalog(
        "lake",
        uri=f"postgresql+psycopg2://postgres:{PG_PASSWORD}@{PG}",
        warehouse=f"file://{WAREHOUSE}",
    )
    return catalog.load_table("smoke.t1")


def _parquet_files() -> list[str]:
    return sorted(glob.glob(str(TABLE_DIR / "data" / "*.parquet")))


def test_spark_wrote_and_read_three_rows(spark_rows: list[tuple[int, str, int]]) -> None:
    assert spark_rows == ROWS


def test_pyiceberg_reads_same_rows(pyiceberg_table) -> None:  # noqa: ANN001
    arrow = pyiceberg_table.scan().to_arrow()
    got = sorted((r["id"], r["name"], r["amount_cents"]) for r in arrow.to_pylist())
    assert got == ROWS


def test_metadata_paths_are_absolute_under_the_warehouse(pyiceberg_table) -> None:  # noqa: ANN001
    # Iceberg stores absolute paths; this is why containers must mount the warehouse at the same
    # path as the host. `file:/…` and `file:///…` are the same URI.
    location = pyiceberg_table.metadata_location.removeprefix("file://").removeprefix("file:")
    assert location.startswith(str(WAREHOUSE) + "/smoke/t1/metadata/"), location


def test_duckdb_reads_parquet_files_same_rows(spark_rows: list[tuple[int, str, int]]) -> None:
    files = _parquet_files()
    assert files, f"no parquet files under {TABLE_DIR}/data"
    rows = duckdb.connect().execute(
        "SELECT id, name, amount_cents FROM read_parquet(?) ORDER BY id", [files]
    ).fetchall()  # fmt: skip
    assert [tuple(r) for r in rows] == ROWS


def test_duckdb_iceberg_scan_same_rows(pyiceberg_table) -> None:  # noqa: ANN001
    metadata = pyiceberg_table.metadata_location.removeprefix("file://").removeprefix("file:")
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")  # first run downloads the extension
    rows = con.execute(
        "SELECT id, name, amount_cents FROM iceberg_scan(?) ORDER BY id", [metadata]
    ).fetchall()  # fmt: skip
    assert [tuple(r) for r in rows] == ROWS


def test_flink_container_sees_the_table_files_at_the_same_path(
    spark_rows: list[tuple[int, str, int]],
) -> None:
    host_files = _parquet_files()
    assert host_files
    container = subprocess.run(
        ["docker", "ps", "-q", "--filter", "label=com.docker.compose.service=flink-jobmanager"],
        capture_output=True, text=True, check=True,
    ).stdout.split()  # fmt: skip
    assert container, "flink-jobmanager is not running (make up)"
    listing = subprocess.run(
        ["docker", "exec", container[0], "sh", "-c", f"ls {TABLE_DIR}/data/*.parquet"],
        capture_output=True, text=True, check=False,
    )  # fmt: skip
    assert listing.returncode == 0, listing.stderr
    assert sorted(listing.stdout.split()) == host_files
