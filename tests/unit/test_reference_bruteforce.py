"""Independent cross-check of the oracle: a spec-literal O(n^2) implementation, in the tests.

`brute_account` rescans the whole leg list for every leg and applies the definitions of
`features/spec.yaml` word for word: no pointers, no running sums, no multisets. It is slow on
purpose, so the only way it can agree with the oracle is that both read the spec the same way.
It is NOT the oracle: nothing imports it, and if the two disagree the hand-computed golden
vectors decide which one is wrong.

Everything is compared exactly, with no tolerance, including `avg_hold_minutes_*` (D40): the hold
of an entry is an int64 number of milliseconds, the window sum is an exact integer, and the feature
is ONE division `total_ms / (count * 60000)`, so a running sum and the fresh sum computed here must
agree to the bit. Zero-amount entries are generated on purpose: `amount_vs_out_avg_72h` is null for
an empty window AND for a zero average (D41).

Until the oracle body exists the comparison tests report xfail; the self-checks of the brute-force
code against cases worked out by hand run now.
"""

import random
from typing import Any

import pandas as pd
import pytest

from features import reference, spec
from features.vectors import _py

pytestmark = pytest.mark.unit

MIN = 60_000
T0 = 1662033600000


@pytest.fixture(scope="module")
def s() -> spec.Spec:
    return spec.load_spec()


# --- the spec-literal implementation ------------------------------------------------------------


def _window(visible: list[dict], t: int, width_ms: int) -> list[dict]:
    """Visible entries with ts > t - W: start exclusive, end inclusive."""
    return [e for e in visible if e["event_ts_ms"] > t - width_ms]


def _hold_ms(rows: list[dict], e: dict) -> int | None:
    """OUT entry: milliseconds since the latest IN entry with a smaller tx_id (kept forever)."""
    if e["direction"] != "OUT":
        return None
    earlier_in = [x for x in rows if x["direction"] == "IN" and x["tx_id"] < e["tx_id"]]
    if not earlier_in:
        return None
    last = max(earlier_in, key=lambda x: (x["event_ts_ms"], x["tx_id"]))
    return e["event_ts_ms"] - last["event_ts_ms"]


def brute_account(legs: pd.DataFrame, sp: spec.Spec) -> dict[tuple[int, str], dict[str, Any]]:
    """(tx_id, role) -> {column: value} for every leg, straight from the spec text."""
    rows = [
        {**r, "dir_rank": sp.dir_order[r["direction"]]}
        for r in legs.to_dict("records")  # plain dicts: the list is rescanned for every leg
    ]
    # D43: a tx_id that has both an OUT and an IN leg in this account's frame is a self-transfer
    self_tx = {r["tx_id"] for r in rows if sum(x["tx_id"] == r["tx_id"] for x in rows) == 2}
    for r in rows:
        r["is_self"] = r["tx_id"] in self_tx
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
            holds = [h for h in (_hold_ms(rows, e) for e in outs) if h is not None]
            cross = [e for e in win if e["payment_currency"] != e["receiving_currency"]]
            row[f"out_count_{w}"] = len(outs)
            row[f"in_count_{w}"] = len(ins)
            row[f"out_sum_usd_cents_{w}"] = out_sum
            row[f"in_sum_usd_cents_{w}"] = in_sum
            row[f"distinct_out_cp_{w}"] = len(
                {e["counterparty_key"] for e in outs if not e["is_self"]}
            )
            row[f"distinct_in_cp_{w}"] = len(
                {e["counterparty_key"] for e in ins if not e["is_self"]}
            )
            row[f"pass_through_ratio_{w}"] = None if in_sum == 0 else out_sum / in_sum
            row[f"avg_hold_minutes_{w}"] = sum(holds) / (len(holds) * 60000) if holds else None
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
            known = {e["counterparty_key"] for e in outs72 if not e["is_self"]}
            row["new_counterparty_72h"] = 0 if leg["is_self"] else int(cp not in known)
            n_out = len(outs72)
            total = sum(e["amount_usd_cents"] for e in outs72)
            # D41: null when the window is empty OR the average is 0
            row["amount_vs_out_avg_72h"] = (
                None if n_out == 0 or total == 0 else leg["amount_usd_cents"] / (total / n_out)
            )
        else:
            role = "R"
            ins72 = [e for e in win72 if e["direction"] == "IN"]
            known = {e["counterparty_key"] for e in ins72 if not e["is_self"]}
            row["new_counterparty_72h"] = 0 if leg["is_self"] else int(cp not in known)
            outs24 = [
                e for e in _window(visible, t, sp.window_ms["24h"]) if e["direction"] == "OUT"
            ]
            row["returned_funds_24h"] = (
                0
                if leg["is_self"]
                else int(any(e["counterparty_key"] == cp for e in outs24 if not e["is_self"]))
            )
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
        assert got[(1, role)]["new_counterparty_72h"] == 0  # D43: no counterparty, so not a new one
    assert got[(1, "S")]["amount_vs_out_avg_72h"] is None
    assert got[(1, "R")]["returned_funds_24h"] == 0


def test_selfcheck_avg_hold_is_one_exact_division(s: spec.Spec) -> None:
    # IN at minute 0; OUT at minutes 10, 20, 50 -> holds 10, 20, 50 minutes
    # = 600,000 + 1,200,000 + 3,000,000 ms = 4,800,000 ms over 3 entries
    legs = _frame(
        [
            _leg(1, 0, "IN", cp="X"),
            _leg(2, 10, "OUT", cp="B"),
            _leg(3, 20, "OUT", cp="C"),
            _leg(4, 50, "OUT", cp="D"),
            _leg(5, 55, "OUT", cp="E"),
        ]
    )
    got = brute_account(legs, s)
    assert got[(5, "S")]["avg_hold_minutes_1h"] == 4_800_000 / (3 * 60_000)  # 26.666...
    assert got[(3, "S")]["avg_hold_minutes_1h"] == 600_000 / 60_000  # only tx 2: 10.0


def test_selfcheck_vs_avg_null_for_empty_window_and_zero_average(s: spec.Spec) -> None:
    legs = _frame(
        [
            _leg(1, 0, "OUT", cp="B", cents=0),
            _leg(2, 10, "OUT", cp="C", cents=500),
            _leg(3, 20, "OUT", cp="D", cents=300),
        ]
    )
    got = brute_account(legs, s)
    assert got[(1, "S")]["amount_vs_out_avg_72h"] is None  # empty window
    assert got[(2, "S")]["amount_vs_out_avg_72h"] is None  # one entry, average 0
    assert got[(3, "S")]["amount_vs_out_avg_72h"] == 300 / (500 / 2)  # entries 0 and 500


def test_selfcheck_self_transfer_is_not_a_counterparty(s: spec.Spec) -> None:
    # tx 1: A pays itself 700; tx 2: A pays B 300; tx 3: A receives from B 100.
    legs = _frame(
        [
            _leg(1, 0, "OUT", cp="A", cents=700),
            _leg(1, 0, "IN", cp="A", cents=700),
            _leg(2, 10, "OUT", cp="B", cents=300),
            _leg(3, 20, "IN", cp="B", cents=100),
        ]
    )
    got = brute_account(legs, s)
    # tx 2 sees the self-transfer in the amount and count windows ...
    assert got[(2, "S")]["out_count_1h"] == 1 and got[(2, "S")]["out_sum_usd_cents_1h"] == 700
    assert got[(2, "S")]["in_count_1h"] == 1 and got[(2, "S")]["in_sum_usd_cents_1h"] == 700
    # ... but not as a counterparty: A is not a distinct counterparty, and B is still new
    assert got[(2, "S")]["distinct_out_cp_1h"] == 0 and got[(2, "S")]["distinct_in_cp_1h"] == 0
    assert got[(2, "S")]["new_counterparty_72h"] == 1
    assert (
        got[(2, "S")]["amount_vs_out_avg_72h"] == 300 / 700
    )  # the OUT self entry is in the average
    # tx 3 (B pays A back 20 minutes after A paid B): a genuine 2-cycle, so returned funds = 1;
    # B was never an IN counterparty, so it is new; the self OUT entry is not a counterparty
    assert got[(3, "R")]["returned_funds_24h"] == 1
    assert got[(3, "R")]["new_counterparty_72h"] == 1
    assert got[(3, "R")]["distinct_out_cp_1h"] == 1  # B only
    # the IN leg of the self-transfer set last_in_ts at minute 0, so tx 2 (minute 10) held 600,000
    # ms; the OUT self entry has no earlier IN, so no hold: 600,000 / (1 * 60,000) = 10.0
    assert got[(3, "R")]["avg_hold_minutes_1h"] == 10.0


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
        cents = 0 if rnd.random() < 0.1 else rnd.randint(1, 1_000_000)  # zero amounts exist (D41)
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
