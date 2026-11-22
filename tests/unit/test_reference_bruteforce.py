"""Independent cross-check of the oracle: a spec-literal O(n^2) implementation, in the tests.

`brute_account` rescans the whole leg list for every leg and applies the definitions of
`features/spec.yaml` word for word: no pointers, no running sums, no multisets. It is slow on
purpose, so the only way it can agree with the oracle is that both read the spec the same way.
It is NOT the oracle: nothing imports it, and if the two disagree the hand-computed golden
vectors decide which one is wrong.

What is compared exactly: every count, sum, distinct count, flag, ratio and share. What is not:
`avg_hold_minutes_*`, compared with a relative tolerance of 1e-9, because the spec defines it as
the mean of float minutes and a running sum (add on append, subtract on evict) and a fresh sum can
differ in the last bits. That is a real parity question for Flink (see docs/decisions.md when the
owner has decided it), not a reason to loosen the other columns.

Amounts here are always >= 1 cent: `s_amount_vs_out_avg_72h` when the window holds only zero-amount
entries is a decision for the oracle's owner (spec null_policy vs the feature's own definition).

Until the oracle body exists the comparison tests report xfail; the self-checks of the brute-force
code against cases worked out by hand run now.
"""

import math
import random
from typing import Any

import pandas as pd
import pytest

from features import reference, spec
from features.vectors import _py

pytestmark = pytest.mark.unit

MIN = 60_000
T0 = 1662033600000
HOLD_COLUMNS = {f"avg_hold_minutes_{w}" for w in ("1h", "24h", "72h")}


@pytest.fixture(scope="module")
def s() -> spec.Spec:
    return spec.load_spec()


# --- the spec-literal implementation ------------------------------------------------------------


def _window(visible: list[dict], t: int, width_ms: int) -> list[dict]:
    """Visible entries with ts > t - W: start exclusive, end inclusive."""
    return [e for e in visible if e["event_ts_ms"] > t - width_ms]


def _hold(rows: list[dict], e: dict) -> float | None:
    """OUT entry: minutes since the latest IN entry with a smaller tx_id (kept forever)."""
    if e["direction"] != "OUT":
        return None
    earlier_in = [x for x in rows if x["direction"] == "IN" and x["tx_id"] < e["tx_id"]]
    if not earlier_in:
        return None
    last = max(earlier_in, key=lambda x: (x["event_ts_ms"], x["tx_id"]))
    return (e["event_ts_ms"] - last["event_ts_ms"]) / 60000


def brute_account(legs: pd.DataFrame, sp: spec.Spec) -> dict[tuple[int, str], dict[str, Any]]:
    """(tx_id, role) -> {column: value} for every leg, straight from the spec text."""
    rows = [
        {**r, "dir_rank": sp.dir_order[r["direction"]]}
        for r in legs.to_dict("records")  # plain dicts: the list is rescanned for every leg
    ]
    out: dict[tuple[int, str], dict[str, Any]] = {}
    for leg in rows:
        t, me = leg["event_ts_ms"], leg["tx_id"]
        visible = [e for e in rows if e["tx_id"] < me]  # spec: visibility

        row: dict[str, Any] = {}
        for w in sp.default_windows:
            win = _window(visible, t, sp.window_ms[w])
            outs = [e for e in win if e["direction"] == "OUT"]
            ins = [e for e in win if e["direction"] == "IN"]
            out_sum = sum(e["amount_usd_cents"] for e in outs)
            in_sum = sum(e["amount_usd_cents"] for e in ins)
            holds = [h for h in (_hold(rows, e) for e in outs) if h is not None]
            cross = [e for e in win if e["payment_currency"] != e["receiving_currency"]]
            row[f"out_count_{w}"] = len(outs)
            row[f"in_count_{w}"] = len(ins)
            row[f"out_sum_usd_cents_{w}"] = out_sum
            row[f"in_sum_usd_cents_{w}"] = in_sum
            row[f"distinct_out_cp_{w}"] = len({e["counterparty_key"] for e in outs})
            row[f"distinct_in_cp_{w}"] = len({e["counterparty_key"] for e in ins})
            row[f"pass_through_ratio_{w}"] = None if in_sum == 0 else out_sum / in_sum
            row[f"avg_hold_minutes_{w}"] = sum(holds) / len(holds) if holds else None
            row[f"cross_currency_share_{w}"] = len(cross) / len(win) if win else None
        win72 = _window(visible, t, sp.window_ms["72h"])
        outs72 = [e for e in win72 if e["direction"] == "OUT"]
        for fmt in sp.payment_formats:
            n = sum(1 for e in outs72 if e["payment_format"] == fmt)
            row[f"format_share_{fmt}_72h"] = n / len(outs72) if outs72 else None
        first = min((e["event_ts_ms"] for e in visible), default=None)
        row["account_age_minutes"] = None if first is None else (t - first) / 60000
        row["log_empty"] = int(not visible)

        cp = leg["counterparty_key"]
        if leg["direction"] == "OUT":
            role = "S"
            row["new_counterparty_72h"] = int(cp not in {e["counterparty_key"] for e in outs72})
            n_out = len(outs72)
            total = sum(e["amount_usd_cents"] for e in outs72)
            avg = total / n_out if n_out else None
            row["amount_vs_out_avg_72h"] = (
                None if not avg else leg["amount_usd_cents"] / avg  # None for 0 or no entries
            )
        else:
            role = "R"
            ins72 = [e for e in win72 if e["direction"] == "IN"]
            row["new_counterparty_72h"] = int(cp not in {e["counterparty_key"] for e in ins72})
            outs24 = [
                e for e in _window(visible, t, sp.window_ms["24h"]) if e["direction"] == "OUT"
            ]
            row["returned_funds_24h"] = int(any(e["counterparty_key"] == cp for e in outs24))
        out[(me, role)] = row
    return out


# --- self-checks: cases worked out by hand, so the brute force itself is checked ----------------


def _frame(rows: list[tuple]) -> pd.DataFrame:
    cols = list(reference.LEG_COLUMNS)
    return pd.DataFrame(rows, columns=cols)


def _leg(tx_id, minute, direction, cp="B", cents=100, pay="USD", recv="USD", fmt="ACH") -> tuple:
    return (tx_id, T0 + minute * MIN, direction, cp, cents, pay, recv, fmt)


def test_selfcheck_window_boundary(s: spec.Spec) -> None:
    # OUT entries at minutes 0, 1, 61, 62, 121; a leg at t sees entries with ts > t - 1h.
    legs = _frame([_leg(i + 1, m, "OUT") for i, m in enumerate([0, 1, 61, 62, 121])])
    got = brute_account(legs, s)
    # by hand: tx3 (minute 61): minute 0 is 61 min old, minute 1 is exactly 60 min old -> both out
    assert got[(3, "S")]["out_count_1h"] == 0
    assert got[(3, "S")]["out_count_24h"] == 2
    # tx4 (minute 62): minute 1 is 61 min old (out), minute 61 is 1 min old (in)
    assert got[(4, "S")]["out_count_1h"] == 1
    # tx5 (minute 121): minute 61 is exactly 60 min old (out), minute 62 is 59 min old (in)
    assert got[(5, "S")]["out_count_1h"] == 1


def test_selfcheck_pass_through_hold_and_ratio(s: spec.Spec) -> None:
    legs = _frame(
        [
            _leg(1, 0, "IN", cp="X", cents=100_000),
            _leg(2, 30, "OUT", cp="Y", cents=99_000),
            _leg(3, 40, "OUT", cp="Z", cents=100),
        ]
    )
    got = brute_account(legs, s)
    assert got[(2, "S")]["pass_through_ratio_1h"] == 0.0  # nothing out yet, 1000.00 in
    assert got[(2, "S")]["avg_hold_minutes_1h"] is None  # no earlier OUT entry
    assert got[(3, "S")]["pass_through_ratio_1h"] == 0.99  # 99000 / 100000
    assert got[(3, "S")]["avg_hold_minutes_1h"] == 30.0  # tx 2 held for 30 min
    assert got[(3, "S")]["amount_vs_out_avg_72h"] == 100 / 99_000


def test_selfcheck_first_seen_and_last_in_survive_eviction(s: spec.Spec) -> None:
    legs = _frame(
        [
            _leg(1, 0, "IN", cp="X"),
            _leg(2, 4800, "OUT", cp="B"),  # 80 h later: the IN entry is outside every window
            _leg(3, 4810, "OUT", cp="C"),
        ]
    )
    got = brute_account(legs, s)
    assert got[(2, "S")]["in_count_72h"] == 0
    assert got[(2, "S")]["account_age_minutes"] == 4800.0
    assert got[(2, "S")]["log_empty"] == 0
    assert got[(3, "S")]["avg_hold_minutes_72h"] == 4800.0  # hold of tx 2, from minute 0


def test_selfcheck_self_transfer_and_empty_history(s: spec.Spec) -> None:
    legs = _frame([_leg(1, 0, "OUT", cp="A"), _leg(1, 0, "IN", cp="A")])  # account A pays itself
    got = brute_account(legs, s)
    for role in ("S", "R"):  # both legs see an empty log
        assert got[(1, role)]["log_empty"] == 1
        assert got[(1, role)]["out_count_72h"] == 0
        assert got[(1, role)]["account_age_minutes"] is None
        assert got[(1, role)]["new_counterparty_72h"] == 1
    assert got[(1, "S")]["amount_vs_out_avg_72h"] is None
    assert got[(1, "R")]["returned_funds_24h"] == 0


# --- random accounts ----------------------------------------------------------------------------


def random_account(seed: int, n_tx: int) -> pd.DataFrame:
    """Legs of one account 'ME': ties, self-transfers, repeated counterparties, gaps over 72 h."""
    rnd = random.Random(seed)
    counterparties = [f"cp{i}" for i in range(rnd.randint(1, 6))]
    currencies = ["USD", "EUR", "JPY"]
    formats = ["ACH", "WIRE", "CASH", "CHEQUE"]
    ts = T0
    rows = []
    for tx_id in range(1, n_tx + 1):
        ts += rnd.choice([0, 0, 1, 5, 30, 60, 90, 600, 1500, 4000, 6000]) * MIN
        kind = rnd.choice(["OUT", "OUT", "IN", "IN", "SELF"])
        pay, recv = rnd.choice(currencies), rnd.choice(currencies)
        fmt = rnd.choice(formats)
        cents = rnd.randint(1, 1_000_000)
        cp = rnd.choice(counterparties)
        if kind in ("OUT", "SELF"):
            rows.append((tx_id, ts, "OUT", "ME" if kind == "SELF" else cp, cents, pay, recv, fmt))
        if kind in ("IN", "SELF"):
            rows.append((tx_id, ts, "IN", "ME" if kind == "SELF" else cp, cents, pay, recv, fmt))
    legs = _frame(rows)
    return legs.sample(frac=1, random_state=seed).reset_index(drop=True)  # arrival order is free


def _compare(oracle_out: pd.DataFrame, expected: dict, sp: spec.Spec) -> list[str]:
    problems = []
    seen = set()
    for rec in oracle_out.to_dict("records"):
        key = (int(rec["tx_id"]), rec["role"])
        seen.add(key)
        for col in reference.role_columns(sp, rec["role"]):
            got, want = _py(rec[col]), expected[key][col]
            if col in HOLD_COLUMNS and got is not None and want is not None:
                same = math.isclose(got, want, rel_tol=1e-9, abs_tol=1e-12)
            else:
                same = (got is None and want is None) or (
                    got is not None
                    and want is not None
                    and got == want
                    and isinstance(got, float) == isinstance(want, float)
                )
            if not same:
                problems.append(f"{key} {col}: oracle {got!r} vs spec-literal {want!r}")
    if seen != set(expected):
        problems.append(
            f"legs differ: oracle-only {sorted(seen - set(expected))[:3]}, "
            f"brute-only {sorted(set(expected) - seen)[:3]}"
        )
    return problems


@pytest.mark.parametrize("seed", range(40))
def test_oracle_equals_the_spec_literal_version(s: spec.Spec, seed: int) -> None:
    legs = random_account(seed, n_tx=random.Random(seed).randint(1, 45))
    try:
        got = reference.compute_account_legs(legs, s)
    except NotImplementedError:
        pytest.xfail("oracle body not written yet (OWNER: human)")
    problems = _compare(got, brute_account(legs, s), s)
    assert not problems, f"seed {seed}:\n" + "\n".join(problems[:10])


def test_the_comparison_can_fail(s: spec.Spec) -> None:
    """Positive control: a one-count error in an otherwise perfect oracle output is reported."""
    legs = random_account(3, n_tx=20)
    expected = brute_account(legs, s)
    rows = []
    for (tx_id, role), cols in sorted(expected.items()):
        row = {
            "tx_id": tx_id,
            "role": role,
            **{c: cols[c] for c in reference.role_columns(s, role)},
        }
        rows.append(row)
    perfect = reference._to_frame(rows, s)
    assert _compare(perfect, expected, s) == []
    broken = perfect.copy()
    broken.loc[0, "out_count_72h"] = broken.loc[0, "out_count_72h"] + 1
    assert any("out_count_72h" in p for p in _compare(broken, expected, s))
