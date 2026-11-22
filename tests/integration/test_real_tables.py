"""The real Iceberg tables (HI-Small, HI-Medium), checked with DuckDB (an engine other than the one
that wrote them), against the recorded ingest results.

Each dataset skips, with a stated reason, if the SSD is not mounted or the dataset was never
ingested (no committed ingest.json): a fresh clone has no data. If the SSD is mounted and the
dataset was ingested, a missing table fails. The checks that need no SSD (the results files agree
with each other) are in tests/unit/test_results_consistency.py. DuckDB reads the Parquet files
directly and ignores the `._*` AppleDouble files exFAT leaves next to them. Needs no compose stack.
"""

import json
from pathlib import Path

import duckdb
import pytest

from batch.spark import common

pytestmark = [pytest.mark.integration, pytest.mark.slow]

ROOT = Path(__file__).resolve().parents[2]
CHECKSUMS = ROOT / "data" / "checksums.txt"
DATASETS = {"hi_small": "HI-Small", "hi_medium": "HI-Medium"}  # dataset -> raw file prefix


def _table_dir(ds: str) -> Path:
    return common.WAREHOUSE / f"aml_{ds}" / "raw_transactions" / "data"


def _result(ds: str, name: str) -> dict:
    """A committed results file. A missing one is an error, not a skip."""
    return json.loads((ROOT / "results" / "M01" / ds / f"{name}.json").read_text())


def _source_lines(ds: str) -> int:
    """The `wc -l` count recorded for the dataset's transactions file (includes the header)."""
    name = f"{DATASETS[ds]}_Trans.csv"
    return next(
        int(line.split()[0])
        for line in CHECKSUMS.read_text().splitlines()
        if line.strip().endswith(name) and line.split()[0].isdigit()
    )


@pytest.fixture(scope="module", params=sorted(DATASETS))
def table(request: pytest.FixtureRequest) -> tuple[str, duckdb.DuckDBPyConnection]:
    ds = request.param
    data_dir = _table_dir(ds)
    if not data_dir.is_dir():
        recorded = (ROOT / "results" / "M01" / ds / "ingest.json").is_file()
        if common.DATA_ROOT.is_dir() and recorded:
            pytest.fail(
                f"{ds}: ingest.json is committed and the SSD is mounted, but {data_dir} is gone"
            )
        pytest.skip(f"{ds} table not found at {data_dir} (SSD unmounted or `make iceberg` not run)")
    connection = common.duckdb_connection()
    connection.execute(
        f"CREATE VIEW t AS SELECT * FROM read_parquet('{data_dir}/*/[0-9]*.parquet')"
    )
    return ds, connection


def _one(con: duckdb.DuckDBPyConnection, sql: str) -> tuple:
    return con.execute(sql).fetchone()


# --- the table itself ---------------------------------------------------------------------------


def test_row_count_equals_the_source_file_and_the_recorded_run(table) -> None:  # noqa: ANN001
    ds, con = table
    (n,) = _one(con, "SELECT count(*) FROM t")
    assert n == _source_lines(ds) - 1  # `wc -l` counts the header line
    assert n == _result(ds, "ingest")["rows"]


def test_laundering_count_equals_the_recorded_run(table) -> None:  # noqa: ANN001
    ds, con = table
    (n,) = _one(con, "SELECT sum(is_laundering) FROM t")
    assert n == _result(ds, "ingest")["laundering"]


def test_tx_id_is_contiguous_and_unique(table) -> None:  # noqa: ANN001
    _, con = table
    n, distinct, lo, hi = _one(
        con, "SELECT count(*), count(DISTINCT tx_id), min(tx_id), max(tx_id) FROM t"
    )
    assert (distinct, lo, hi) == (n, 0, n - 1)


def test_total_order_is_strictly_increasing(table) -> None:  # noqa: ANN001
    _, con = table
    (bad,) = _one(
        con,
        """SELECT count(*) FROM (
             SELECT event_ts_ms, src_row,
                    lag(event_ts_ms) OVER w AS pts, lag(src_row) OVER w AS psr
             FROM t WINDOW w AS (ORDER BY tx_id))
           WHERE pts > event_ts_ms OR (pts = event_ts_ms AND psr >= src_row)""",
    )
    assert bad == 0


def test_no_nulls_in_any_column(table) -> None:  # noqa: ANN001
    _, con = table
    columns = [r[0] for r in con.execute("DESCRIBE t").fetchall()]
    predicate = " OR ".join(f'"{c}" IS NULL' for c in columns)
    (bad,) = _one(con, f"SELECT count(*) FROM t WHERE {predicate}")
    assert bad == 0


def test_every_split_is_non_empty_and_time_ordered(table) -> None:  # noqa: ANN001
    ds, con = table
    rows = con.execute(
        "SELECT split, count(*), min(event_ts_ms), max(event_ts_ms) FROM t GROUP BY 1"
    ).fetchall()
    by = {r[0]: r for r in rows}
    assert set(by) == {"train", "val", "test"}
    assert by["train"][3] < by["val"][2] and by["val"][3] < by["test"][2]
    assert sum(r[1] for r in rows) == _result(ds, "ingest")["rows"]


def test_every_data_file_is_in_tx_id_order(table) -> None:  # noqa: ANN001
    ds, con = table
    files = sorted(_table_dir(ds).glob("*/[0-9]*.parquet"))
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
