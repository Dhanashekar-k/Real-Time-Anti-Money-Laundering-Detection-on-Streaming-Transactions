"""The real HI-Small table, checked with DuckDB (an engine other than the one that wrote it).

Skips, with a stated reason, if the SSD or the table is absent: a fresh clone has no data. It reads
the Parquet files directly and ignores the `._*` AppleDouble files exFAT leaves next to them.
Needs no compose stack.
"""

import json
from pathlib import Path

import duckdb
import pytest

from batch.spark import common

pytestmark = [pytest.mark.integration, pytest.mark.slow]

ROOT = Path(__file__).resolve().parents[2]
TABLE_DIR = common.WAREHOUSE / "aml_hi_small" / "raw_transactions" / "data"
INGEST = ROOT / "results" / "M01" / "hi_small" / "ingest.json"
CHECKSUMS = ROOT / "data" / "checksums.txt"


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    if not TABLE_DIR.is_dir():
        pytest.skip(
            f"HI-Small table not found at {TABLE_DIR} (SSD unmounted or `make iceberg` not run)"
        )
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    connection.execute(
        f"CREATE VIEW t AS SELECT * FROM read_parquet('{TABLE_DIR}/*/[0-9]*.parquet')"
    )
    return connection


def _one(con: duckdb.DuckDBPyConnection, sql: str) -> tuple:
    return con.execute(sql).fetchone()


def test_row_count_equals_the_source_file_and_the_recorded_run(con) -> None:  # noqa: ANN001
    lines = next(
        int(line.split()[0])
        for line in CHECKSUMS.read_text().splitlines()
        if line.strip().endswith("HI-Small_Trans.csv") and line.split()[0].isdigit()
    )
    (n,) = _one(con, "SELECT count(*) FROM t")
    assert n == lines - 1  # `wc -l` counts the header line
    assert n == json.loads(INGEST.read_text())["rows"]


def test_laundering_count_equals_the_recorded_run(con) -> None:  # noqa: ANN001
    (n,) = _one(con, "SELECT sum(is_laundering) FROM t")
    assert n == json.loads(INGEST.read_text())["laundering"]


def test_tx_id_is_contiguous_and_unique(con) -> None:  # noqa: ANN001
    n, distinct, lo, hi = _one(
        con, "SELECT count(*), count(DISTINCT tx_id), min(tx_id), max(tx_id) FROM t"
    )
    assert (distinct, lo, hi) == (n, 0, n - 1)


def test_total_order_is_strictly_increasing(con) -> None:  # noqa: ANN001
    (bad,) = _one(
        con,
        """SELECT count(*) FROM (
             SELECT event_ts_ms, src_row,
                    lag(event_ts_ms) OVER w AS pts, lag(src_row) OVER w AS psr
             FROM t WINDOW w AS (ORDER BY tx_id))
           WHERE pts > event_ts_ms OR (pts = event_ts_ms AND psr >= src_row)""",
    )
    assert bad == 0


def test_no_nulls_in_any_column(con) -> None:  # noqa: ANN001
    columns = [r[0] for r in con.execute("DESCRIBE t").fetchall()]
    predicate = " OR ".join(f'"{c}" IS NULL' for c in columns)
    (bad,) = _one(con, f"SELECT count(*) FROM t WHERE {predicate}")
    assert bad == 0


def test_every_split_is_non_empty_and_time_ordered(con) -> None:  # noqa: ANN001
    rows = con.execute(
        "SELECT split, count(*), min(event_ts_ms), max(event_ts_ms) FROM t GROUP BY 1"
    ).fetchall()
    by = {r[0]: r for r in rows}
    assert set(by) == {"train", "val", "test"}
    assert by["train"][3] < by["val"][2] and by["val"][3] < by["test"][2]
    assert sum(r[1] for r in rows) == json.loads(INGEST.read_text())["rows"]


def test_every_data_file_is_in_tx_id_order(con) -> None:  # noqa: ANN001
    files = sorted(TABLE_DIR.glob("*/[0-9]*.parquet"))
    assert files
    for path in files:
        (bad,) = _one(
            con,
            f"""SELECT count(*) FROM (
                  SELECT tx_id, lag(tx_id) OVER (ORDER BY file_row_number) AS prev
                  FROM read_parquet('{path}', file_row_number=true))
                WHERE prev >= tx_id""",
        )
        assert bad == 0, path.name
