"""eval/memory_gate.py: the parsing and tree helpers that the memory measurement relies on."""

import pytest

from eval import memory_gate as mg

pytestmark = pytest.mark.unit

PS = """\
  100     1   1000 /usr/bin/python
  200   100  50000 /usr/lib/jvm/bin/java
  300   200  20000 /venv/bin/python3.11
  301   300  30000 /venv/bin/python3.11
  400     1   99999 /Applications/Other.app/Contents/MacOS/other
"""


def test_parse_ps_reads_pid_ppid_rss_and_command() -> None:
    rows = mg.parse_ps(PS)
    assert rows[200] == (100, 50_000, "/usr/lib/jvm/bin/java")
    assert len(rows) == 5


def test_descendants_are_the_subtree_and_exclude_unrelated_processes() -> None:
    rows = mg.parse_ps(PS)
    assert mg.descendants(rows, 100) == {100, 200, 300, 301}
    assert mg.descendants(rows, 300) == {300, 301}


def test_tree_snapshot_splits_jvm_and_python_workers_in_mb() -> None:
    snap = mg.tree_snapshot(mg.parse_ps(PS), 100)
    kb = 1024
    assert snap["jvm_mb"] == 50_000 / kb
    assert snap["python_workers_mb"] == (20_000 + 30_000) / kb  # the driver python is not a worker
    assert snap["python_worker_max_mb"] == 30_000 / kb
    assert snap["total_mb"] == (1000 + 50_000 + 20_000 + 30_000) / kb


def test_vm_stat_used_memory() -> None:
    text = (
        "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
        "Pages free:                               1000.\n"
        "Pages active:                             2000.\n"
        "Pages inactive:                           3000.\n"
        "Pages wired down:                         4000.\n"
        "Pages occupied by compressor:             500.\n"
    )
    # used = active + wired + compressor = 6500 pages of 16384 bytes
    assert mg.parse_vm_stat(text) == pytest.approx(6500 * 16384 / 2**20)


def test_peak_summary_takes_the_maximum_of_each_series() -> None:
    samples = [
        {
            "t": 0.0,
            "total_mb": 100.0,
            "jvm_mb": 60.0,
            "python_workers_mb": 30.0,
            "python_worker_max_mb": 20.0,
        },
        {
            "t": 0.5,
            "total_mb": 300.0,
            "jvm_mb": 70.0,
            "python_workers_mb": 200.0,
            "python_worker_max_mb": 120.0,
        },
        {
            "t": 1.0,
            "total_mb": 250.0,
            "jvm_mb": 90.0,
            "python_workers_mb": 150.0,
            "python_worker_max_mb": 100.0,
        },
    ]
    peak = mg.peak_summary(samples)
    assert peak == {
        "total_mb": 300.0,
        "jvm_mb": 90.0,
        "python_workers_mb": 200.0,
        "python_worker_max_mb": 120.0,
    }
