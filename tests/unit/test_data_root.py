"""The big-data root (external SSD, D37): missing SSD fails fast with a clear message."""

import importlib
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_require_data_root_names_the_missing_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AML_DATA_ROOT", "/Volumes/definitely_not_mounted/project_2")
    from batch.spark import common

    common = importlib.reload(common)
    try:
        with pytest.raises(common.DataRootMissing, match="SSD not mounted at /Volumes/definitely"):
            common.require_data_root()
    finally:
        monkeypatch.undo()
        importlib.reload(common)


def test_paths_hang_off_the_data_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AML_DATA_ROOT", str(tmp_path))
    from batch.spark import common

    common = importlib.reload(common)
    try:
        assert common.require_data_root() == tmp_path
        assert common.RAW_DIR == tmp_path / "raw"
        assert common.WAREHOUSE == tmp_path / "warehouse"
        assert common.SPILL_DIR == tmp_path / "spill"
    finally:
        monkeypatch.undo()
        importlib.reload(common)


@pytest.mark.parametrize("target", ["fx", "iceberg", "split", "eda", "money-check"])
def test_make_targets_fail_fast_without_the_ssd(target: str) -> None:
    result = subprocess.run(
        ["make", target, "DS=hi_small", "AML_DATA_ROOT=/Volumes/definitely_not_mounted/project_2"],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )  # fmt: skip
    assert result.returncode != 0
    assert "SSD not mounted at /Volumes/definitely_not_mounted/project_2" in result.stderr


def test_make_up_does_not_need_the_ssd() -> None:
    result = subprocess.run(
        ["make", "-n", "up", "AML_DATA_ROOT=/Volumes/definitely_not_mounted/project_2"],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )  # fmt: skip
    assert result.returncode == 0
    assert "SSD not mounted" not in result.stderr + result.stdout


def test_duckdb_connection_is_capped_and_spills_to_the_given_directory(tmp_path: Path) -> None:
    from batch.spark import common

    spill = tmp_path / "duck"
    con = common.duckdb_connection(spill_dir=spill, memory_limit="1GB")
    assert spill.is_dir()
    assert con.execute("SELECT current_setting('temp_directory')").fetchone()[0] == str(spill)
    # DuckDB reports the limit in its own units; 1GB is 953.6 MiB
    assert "MiB" in con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    assert con.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"


def test_duckdb_connection_needs_the_ssd_unless_given_a_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from batch.spark import common

    monkeypatch.setattr(common, "DATA_ROOT", tmp_path / "not-mounted")
    with pytest.raises(common.DataRootMissing, match="SSD not mounted"):
        common.duckdb_connection()
