"""batch/spark/common.git_dirty: a run is "dirty" when CODE differs from HEAD, never because of the
files the scripts themselves write (results/, the generated EDA docs, config/thresholds.yaml).

Those outputs are excluded for the same reason in all three cases: a result produced by this very
run (or by the previous one) would otherwise make every provenance record say "dirty".
"""

import subprocess
from pathlib import Path

import pytest

from batch.spark import common

pytestmark = pytest.mark.unit


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", *args],
        cwd=repo,
        check=True,
        capture_output=True,
    )


@pytest.fixture()
def repo(tmp_path: Path) -> Path:
    _git(tmp_path, "init", "-q")
    (tmp_path / "code.py").write_text("x = 1\n")
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "r.json").write_text("{}")
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "thresholds.yaml").write_text("a: 1\n")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "eda_hi_small.md").write_text("old\n")
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-q", "-m", "init")
    return tmp_path


def test_a_clean_tree_is_not_dirty(repo: Path) -> None:
    assert common.git_dirty(repo) is False


def test_changed_outputs_do_not_make_it_dirty(repo: Path) -> None:
    (repo / "results" / "r.json").write_text('{"changed": 1}')
    (repo / "results" / "new.png").write_bytes(b"png")  # untracked result
    (repo / "config" / "thresholds.yaml").write_text("a: 2\n")
    (repo / "docs" / "eda_hi_small.md").write_text("new\n")
    assert common.git_dirty(repo) is False


def test_changed_or_new_code_makes_it_dirty(repo: Path) -> None:
    (repo / "code.py").write_text("x = 2\n")
    assert common.git_dirty(repo) is True
    _git(repo, "checkout", "--", "code.py")
    assert common.git_dirty(repo) is False
    (repo / "new_module.py").write_text("y = 1\n")  # untracked code counts
    assert common.git_dirty(repo) is True


def test_other_config_files_are_not_excluded(repo: Path) -> None:
    (repo / "config" / "other.yaml").write_text("b: 1\n")
    assert common.git_dirty(repo) is True  # only thresholds.yaml is a script output


def test_per_dataset_threshold_files_are_outputs_too(repo: Path) -> None:
    (repo / "config" / "thresholds_hi_medium.yaml").write_text("a: 1\n")
    (repo / "config" / "thresholds_hi_small.yaml").write_text("a: 1\n")
    assert common.git_dirty(repo) is False
