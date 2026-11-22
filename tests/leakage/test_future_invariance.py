"""No feature looks forward: features of tx_id <= s do not change when the future changes.

For a boundary s, the features of every transaction with tx_id <= s are computed on the full input
and again after the rows with tx_id > s have been
- permuted (each column except tx_id and event_ts_ms shuffled independently among the future rows),
- replaced by adversarial rows (huge amounts between accounts that appear in the past),
- dropped.
The features of the past must be bit-identical (`==`, None only equals None, an int is not a float).

The check is itself tested: an engine that looks forward (it adds the number of the sender's later
OUT transactions within the hour to `s_out_count_1h`) must fail it, and the perturbations must
really change the future's own features (otherwise "unchanged past" would prove nothing).

Engines tested: the pure-Python reference (unit), and the Spark job on a synthetic table and on the
first 50,000 rows of the real HI-Small table (integration; needs the SSD and a JVM).
"""

import random
from collections.abc import Callable
from typing import Any

import pytest

from features import spec, synthetic, vectors

Tx = dict[str, Any]
Engine = Callable[[list[Tx]], dict[int, dict[str, Any]]]

KEEP_FIXED = {"tx_id", "event_ts_ms"}


@pytest.fixture(scope="module")
def s() -> spec.Spec:
    return spec.load_spec()


def python_engine(sp: spec.Spec) -> Engine:
    return lambda txs: vectors.compute_vectors(txs, sp)


# --- perturbations of the future ----------------------------------------------------------------


def permute_future(txs: list[Tx], boundary: int, seed: int) -> list[Tx]:
    rnd = random.Random(seed)
    future = [t for t in txs if t["tx_id"] > boundary]
    columns = [c for c in txs[0] if c not in KEEP_FIXED]
    shuffled = {}
    for col in columns:
        values = [t[col] for t in future]
        rnd.shuffle(values)
        shuffled[col] = values
    out = [dict(t) for t in txs if t["tx_id"] <= boundary]
    for i, t in enumerate(future):
        out.append({**t, **{col: shuffled[col][i] for col in columns}})
    return out


def poison_future(txs: list[Tx], boundary: int, seed: int) -> list[Tx]:
    """Future rows that pound the accounts of the past with huge amounts, at the same times."""
    rnd = random.Random(seed)
    past = [t for t in txs if t["tx_id"] <= boundary]
    out = [dict(t) for t in past]
    for t in txs:
        if t["tx_id"] > boundary:
            a, b = rnd.choice(past), rnd.choice(past)
            out.append(
                {
                    **t,
                    "sender_key": a["sender_key"],
                    "sender_bank": a["sender_bank"],
                    "receiver_key": b["receiver_key"],
                    "receiver_bank": b["receiver_bank"],
                    "amount_paid_usd_cents": 10**12,
                    "amount_received_usd_cents": 10**12,
                }
            )
    return out


def drop_future(txs: list[Tx], boundary: int, seed: int) -> list[Tx]:
    return [dict(t) for t in txs if t["tx_id"] <= boundary]


PERTURBATIONS = {"permute": permute_future, "poison": poison_future, "drop": drop_future}


def _same(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return a == b and isinstance(a, float) == isinstance(b, float)


def invariance_report(
    engine: Engine, txs: list[Tx], boundary: int, seed: int = 0
) -> dict[str, Any]:
    """Differences in the past under each perturbation, and how many future vectors changed."""
    base = engine(txs)
    failures: list[tuple[str, int, str, Any, Any]] = []
    changed_future: dict[str, int] = {}
    for name, perturb in PERTURBATIONS.items():
        other = engine(perturb(txs, boundary, seed))
        for tx_id, vec in base.items():
            if tx_id <= boundary:
                for feature, value in vec.items():
                    if not _same(value, other[tx_id][feature]):
                        failures.append((name, tx_id, feature, value, other[tx_id][feature]))
            elif tx_id in other:
                changed_future[name] = changed_future.get(name, 0) + int(
                    any(not _same(v, other[tx_id][f]) for f, v in vec.items())
                )
    return {"failures": failures, "changed_future": changed_future}


# --- engines that look forward (positive controls) -----------------------------------------------


def leaky_engine(sp: spec.Spec, horizon_ms: int | None) -> Engine:
    """Adds the sender's number of LATER transactions (within `horizon_ms`, or all) to a feature.

    A leak bounded by a horizon is visible only for transactions within that horizon of the
    boundary, so it needs dense data to be caught; an unbounded one is visible almost everywhere.
    """
    honest = python_engine(sp)

    def engine(txs: list[Tx]) -> dict[int, dict[str, Any]]:
        out = honest(txs)
        for t in txs:
            later = sum(
                1
                for u in txs
                if u["sender_key"] == t["sender_key"]
                and u["tx_id"] > t["tx_id"]
                and (horizon_ms is None or u["event_ts_ms"] <= t["event_ts_ms"] + horizon_ms)
            )
            out[t["tx_id"]]["s_out_count_1h"] += later  # looks at the future
        return out

    return engine


# --- tests ----------------------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize("fraction", [0.4, 0.75])
def test_python_reference_is_future_invariant(s: spec.Spec, fraction: float) -> None:
    txs = synthetic.random_transactions(2_500, seed=11, n_accounts=25)
    boundary = txs[int(len(txs) * fraction)]["tx_id"]
    report = invariance_report(python_engine(s), txs, boundary)
    assert report["failures"] == [], report["failures"][:5]


@pytest.mark.unit
def test_the_perturbations_really_change_the_future(s: spec.Spec) -> None:
    txs = synthetic.random_transactions(2_500, seed=11, n_accounts=25)
    boundary = txs[1_000]["tx_id"]
    report = invariance_report(python_engine(s), txs, boundary)
    assert report["changed_future"]["permute"] > 100
    assert report["changed_future"]["poison"] > 100


@pytest.mark.unit
def test_an_unbounded_forward_looking_feature_fails_the_check(s: spec.Spec) -> None:
    txs = synthetic.random_transactions(2_500, seed=11, n_accounts=25)
    boundary = txs[1_250]["tx_id"]
    bad = invariance_report(leaky_engine(s, None), txs, boundary)["failures"]
    assert bad, "the leaky engine passed: the check cannot detect a forward-looking feature"
    assert {f for _, _, f, _, _ in bad} == {"s_out_count_1h"}
    assert {name for name, *_ in bad} >= {"poison", "drop"}


@pytest.mark.unit
def test_a_one_hour_forward_look_fails_on_dense_data(s: spec.Spec) -> None:
    # gaps of a few minutes: many transactions of one account fall inside the hour before the
    # boundary, so a leak that reaches one hour ahead changes them when the future changes
    txs = synthetic.random_transactions(
        2_500, seed=13, n_accounts=8, gaps_minutes=synthetic.DENSE_GAPS_MINUTES
    )
    boundary = txs[1_250]["tx_id"]
    assert invariance_report(python_engine(s), txs, boundary)["failures"] == []  # honest: clean
    bad = invariance_report(leaky_engine(s, 3_600_000), txs, boundary)["failures"]
    assert bad, "a 1 h forward look was not detected even on dense data"


@pytest.mark.unit
def test_the_reference_is_invariant_on_dense_data_too(s: spec.Spec) -> None:
    txs = synthetic.random_transactions(
        2_500, seed=14, n_accounts=8, gaps_minutes=synthetic.DENSE_GAPS_MINUTES
    )
    for fraction in (0.3, 0.6, 0.9):
        boundary = txs[int(len(txs) * fraction)]["tx_id"]
        assert invariance_report(python_engine(s), txs, boundary)["failures"] == [], fraction


@pytest.mark.unit
def test_the_checker_is_exact_not_approximate() -> None:
    assert _same(1, 1) and _same(None, None) and _same(0.5, 0.5)
    assert not _same(1, 1.0) and not _same(0.1 + 0.2, 0.3) and not _same(None, 0)


def _spark_engine(session, sp: spec.Spec) -> Engine:
    from batch.spark import features

    def engine(txs: list[Tx]) -> dict[int, dict[str, Any]]:
        df = features.compute_features(features.raw_frame_from_rows(session, txs), sp)
        drop = set(features.KEY_COLUMNS) - {"tx_id"}
        return {
            r["tx_id"]: {k: v for k, v in r.asDict().items() if k not in drop and k != "tx_id"}
            for r in df.collect()
        }

    return engine


@pytest.mark.integration
def test_spark_job_is_future_invariant_on_a_synthetic_table(s: spec.Spec) -> None:
    from batch.spark import features

    session = features.get_session("invariance-synthetic")
    try:
        txs = synthetic.random_transactions(1_500, seed=12, n_accounts=20)
        boundary = txs[700]["tx_id"]
        report = invariance_report(_spark_engine(session, s), txs, boundary)
        assert report["failures"] == [], report["failures"][:5]
        assert report["changed_future"]["poison"] > 50
    finally:
        session.stop()


def _real_slice(ds: str, n: int) -> list[Tx]:
    """The first n transactions of the real table by tx_id; no label column is read."""
    from batch.spark import common

    data_dir = common.WAREHOUSE / f"aml_{ds}" / "raw_transactions" / "data"
    con = common.duckdb_connection()
    try:
        rows = con.execute(
            f"""SELECT tx_id, event_ts_ms, sender_bank, sender_key, receiver_bank, receiver_key,
                       amount_paid_usd_cents, amount_received_usd_cents, payment_currency,
                       receiving_currency, payment_format
                FROM read_parquet('{data_dir}/*/[0-9]*.parquet')
                ORDER BY tx_id LIMIT {n}"""
        ).fetchall()
        names = [d[0] for d in con.description]
    finally:
        con.close()
    return [dict(zip(names, r, strict=True)) for r in rows]


@pytest.mark.integration
@pytest.mark.slow
def test_spark_job_is_future_invariant_on_50k_real_rows(s: spec.Spec) -> None:
    from batch.spark import common, features

    common.require_data_root()
    txs = _real_slice("hi_small", 50_000)
    assert len(txs) == 50_000
    boundary = txs[30_000]["tx_id"]
    session = features.get_session("invariance-real")
    try:
        report = invariance_report(_spark_engine(session, s), txs, boundary)
        assert report["failures"] == [], report["failures"][:5]
        assert report["changed_future"]["poison"] > 1_000
    finally:
        session.stop()
