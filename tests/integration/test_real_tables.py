"""The real Iceberg tables (HI-Small, HI-Medium), checked with DuckDB (an engine other than the one
that wrote them), and the recorded results files checked against each other and the table.

Each dataset skips, with a stated reason, if the SSD or its table is absent: a fresh clone has no
data. DuckDB reads the Parquet files directly and ignores the `._*` AppleDouble files exFAT leaves
next to them. Needs no compose stack.
"""

import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from batch.spark import common
from features import money

pytestmark = [pytest.mark.integration, pytest.mark.slow]

ROOT = Path(__file__).resolve().parents[2]
CHECKSUMS = ROOT / "data" / "checksums.txt"
DATASETS = {"hi_small": "HI-Small", "hi_medium": "HI-Medium"}  # dataset -> raw file prefix


def _table_dir(ds: str) -> Path:
    return common.WAREHOUSE / f"aml_{ds}" / "raw_transactions" / "data"


def _result(ds: str, name: str) -> dict:
    path = ROOT / "results" / "M01" / ds / f"{name}.json"
    if not path.is_file():
        pytest.skip(f"{path} not found (`make` target for it not run yet)")
    return json.loads(path.read_text())


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


# --- the recorded results agree with each other ---------------------------------------------------


@pytest.mark.parametrize("ds", sorted(DATASETS))
def test_every_results_file_counts_the_same_rows(ds: str) -> None:
    ingest = _result(ds, "ingest")
    expected = ingest["rows"]
    assert ingest["source_lines_minus_header"] == expected
    assert _result(ds, "split")["rows"] == expected
    assert _result(ds, "money_parity")["rows_compared"] == expected
    assert _result(ds, "eda")["totals"]["rows"] == expected
    assert _result(ds, "eda")["totals"]["laundering"] == ingest["laundering"]


@pytest.mark.parametrize("ds", sorted(DATASETS))
def test_split_results_agree_with_the_ingest(ds: str) -> None:
    ingest, split = _result(ds, "ingest"), _result(ds, "split")
    assert split["stored_split_mismatches"] == 0
    assert {k: v["rows"] for k, v in split["splits"].items()} == ingest["split_counts"]
    assert (split["t1_ms"], split["t2_ms"]) == (
        ingest["split_boundaries_ms"]["t1"],
        ingest["split_boundaries_ms"]["t2"],
    )
    assert sum(v["laundering"] for v in split["splits"].values()) == ingest["laundering"]


@pytest.mark.parametrize("ds", sorted(DATASETS))
def test_money_parity_has_no_mismatch_and_used_the_committed_fx_table(ds: str) -> None:
    parity = _result(ds, "money_parity")
    assert parity["mismatches"] == {
        "minor_paid": 0,
        "minor_received": 0,
        "cents_paid": 0,
        "cents_received": 0,
    }
    assert parity["config"]["fx_version"] == money.FX_VERSION_REQUIRED
    assert (
        parity["config"]["fx_rates_sha256"]
        == hashlib.sha256(money.FX_PATH.read_bytes()).hexdigest()
    )
    assert _result(ds, "ingest")["fx_version"] == money.FX_VERSION_REQUIRED


@pytest.mark.parametrize("ds", sorted(DATASETS))
def test_ingest_recorded_the_source_file_the_checksums_describe(ds: str) -> None:
    name = f"{DATASETS[ds]}_Trans.csv"
    sha = next(
        line.split()[0]
        for line in CHECKSUMS.read_text().splitlines()
        if line.strip().endswith(name) and len(line.split()[0]) == 64
    )
    assert _result(ds, "ingest")["source"]["sha256"] == sha
