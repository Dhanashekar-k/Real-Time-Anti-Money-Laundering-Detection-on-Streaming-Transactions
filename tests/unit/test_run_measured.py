"""eval/run_measured.py: run a command, sample its whole process tree, write what was seen."""

import json
import sys
from pathlib import Path

import pytest

from eval import run_measured as rm

pytestmark = pytest.mark.unit

ALLOCATE = (
    "import time; x = bytearray(300 * 1024 * 1024); "
    "x[::4096] = b'1' * len(x[::4096]); time.sleep(2.5)"
)


def test_a_command_is_measured_and_its_exit_code_returned(tmp_path: Path) -> None:
    out = tmp_path / "run.json"
    code = rm.main(
        [
            "--label",
            "alloc",
            "--out",
            str(out),
            "--interval",
            "0.3",
            "--",
            sys.executable,
            "-c",
            ALLOCATE,
        ]
    )
    assert code == 0
    doc = json.loads(out.read_text())
    assert doc["label"] == "alloc" and doc["returncode"] == 0
    assert doc["peak_process_tree_mb"]["total_mb"] > 300  # the 300 MB buffer was seen
    assert doc["seconds"] >= 2.4 and doc["samples"] >= 3
    assert doc["command"][1] == "-c"


def test_a_failing_command_is_recorded_and_its_code_propagated(tmp_path: Path) -> None:
    out = tmp_path / "bad.json"
    code = rm.main(
        ["--label", "bad", "--out", str(out), "--", sys.executable, "-c", "raise SystemExit(3)"]
    )
    assert code == 3
    assert json.loads(out.read_text())["returncode"] == 3


def test_a_missing_command_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        rm.main(["--label", "x", "--out", str(tmp_path / "x.json")])
