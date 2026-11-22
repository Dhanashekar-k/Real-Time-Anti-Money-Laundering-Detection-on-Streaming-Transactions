"""Golden feature vectors: the oracle (and, in M4, Flink) must reproduce `tests/golden/s*.json`.

Each file is {transactions: [...], expected: {"<tx_id>": {"<feature>": value}}}. A scenario lists
only the features it is about; every listed value is compared exactly (`==`, no tolerance: Spark
and Flink must match bit for bit, so a tolerance here would hide the difference). JSON null means
"the model sees NaN".

Two states are normal while the owner is working, and neither hides a failure:
- a scenario that still has `TODO(human)` values is SKIPPED, naming how many are left;
- while `_compute_rows` raises NotImplementedError the comparison is XFAIL.
The well-formedness tests always run, so a misspelled feature name or an inconsistent tx_id order
is caught before any value is filled in.
"""

import json
from pathlib import Path

import pytest

from features import expand, spec, vectors

pytestmark = pytest.mark.unit

GOLDEN_DIR = Path(__file__).resolve().parent
TODO = "TODO(human)"
SCENARIOS = sorted(GOLDEN_DIR.glob("s[0-9][0-9]_*.json"))
TX_FIELDS = {
    "tx_id",
    "event_ts_ms",
    "sender_key",
    "receiver_key",
    "amount_paid_usd_cents",
    "amount_received_usd_cents",
    "payment_currency",
    "receiving_currency",
    "payment_format",
}


@pytest.fixture(scope="module")
def s() -> spec.Spec:
    return spec.load_spec()


@pytest.fixture(scope="module")
def names(s: spec.Spec) -> list[str]:
    return expand.feature_names(s)


def _load(path: Path) -> dict:
    return json.loads(path.read_text())


def _todo_count(doc: dict) -> int:
    return sum(v == TODO for feats in doc["expected"].values() for v in feats.values())


def test_the_nine_required_scenarios_exist() -> None:
    prefixes = {p.name[:3] for p in SCENARIOS}
    assert {f"s{i:02d}" for i in range(1, 10)} <= prefixes


@pytest.mark.parametrize("path", SCENARIOS, ids=lambda p: p.stem)
def test_scenario_is_well_formed(path: Path, names: list[str]) -> None:
    doc = _load(path)
    txs = doc["transactions"]
    assert txs, "no transactions"
    for t in txs:
        assert set(t) == TX_FIELDS, f"tx {t.get('tx_id')}: fields differ from the canonical set"
    ids = [t["tx_id"] for t in txs]
    assert len(set(ids)) == len(ids), "duplicate tx_id"
    # tx_id is the rank in (event_ts_ms, source row) order, so ts must not decrease with tx_id
    ordered = sorted(txs, key=lambda t: t["tx_id"])
    stamps = [t["event_ts_ms"] for t in ordered]
    assert stamps == sorted(stamps), "tx_id order contradicts event_ts_ms order"
    for tx_id, feats in doc["expected"].items():
        assert int(tx_id) in ids, f"expected lists tx_id {tx_id}, which is not a transaction"
        assert feats, f"tx_id {tx_id}: no features listed"
        unknown = [f for f in feats if f not in names]
        assert not unknown, f"tx_id {tx_id}: not in feature_names_v1.txt: {unknown}"
    assert not (set(doc) & {"is_laundering", "split", "pattern_type"})


def _same(expected: object, actual: object) -> bool:
    """Exact equality; null only equals null; an int is not a float (counts stay integers)."""
    if expected is None or actual is None:
        return expected is None and actual is None
    return expected == actual and isinstance(expected, float) == isinstance(actual, float)


@pytest.mark.parametrize(
    ("expected", "actual", "same"),
    [
        (1, 1, True),
        (0.99, 0.99, True),
        (None, None, True),
        (1, 1.0, False),  # a count came back as a float
        (0.0, 0, False),  # a ratio came back as an int
        (0.99, 0.9900000000000001, False),  # no tolerance
        (None, 0, False),
        (0, None, False),
    ],
)
def test_comparison_is_exact(expected: object, actual: object, same: bool) -> None:
    assert _same(expected, actual) is same


@pytest.mark.parametrize("path", SCENARIOS, ids=lambda p: p.stem)
def test_oracle_reproduces_scenario(path: Path, s: spec.Spec) -> None:
    doc = _load(path)
    left = _todo_count(doc)
    if left:
        pytest.skip(f"{left} expected values are still {TODO}")
    try:
        got = vectors.compute_vectors(doc["transactions"], s)
    except NotImplementedError:
        pytest.xfail("oracle body not written yet (OWNER: human)")
    wrong = []
    for tx_id, feats in doc["expected"].items():
        for name, expected in feats.items():
            actual = got[int(tx_id)][name]
            if not _same(expected, actual):
                wrong.append(f"tx_id {tx_id} {name}: expected {expected!r}, got {actual!r}")
    assert not wrong, "\n".join(wrong)
