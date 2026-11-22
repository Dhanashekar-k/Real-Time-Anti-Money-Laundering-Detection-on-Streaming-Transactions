"""The committed results files agree with each other, the checksums and the frozen FX table.

Needs no SSD and no stack, so it runs in `make test`: a renamed, deleted or inconsistent results
file fails here instead of being skipped. (The table itself is checked in
tests/integration/test_real_tables.py.)
"""

import hashlib
import json
from pathlib import Path

import pytest

from features import money

ROOT = Path(__file__).resolve().parents[2]
CHECKSUMS = ROOT / "data" / "checksums.txt"
DATASETS = {"hi_small": "HI-Small", "hi_medium": "HI-Medium"}  # dataset -> raw file prefix


def _result(ds: str, name: str) -> dict:
    return json.loads((ROOT / "results" / "M01" / ds / f"{name}.json").read_text())


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
