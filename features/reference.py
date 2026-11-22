"""The feature oracle: what `features/spec.yaml` means, for ONE account's legs (M02 step 2).

Spark calls `compute_account_legs` once per account (`applyInPandas`); Flink must reproduce the
vectors in `tests/golden/`. Nothing else defines the features.

Who wrote what
--------------
Everything in this file except `_compute_rows` is scaffolding (columns, dtypes, validation, null
helpers, the frame builder) and is covered by `tests/unit/test_reference_contract.py`.
`_compute_rows` is the window logic. It is OWNER: human (CLAUDE.md rule 4) and is deliberately
not written: it raises NotImplementedError until you write it.

The contract of `compute_account_legs`
--------------------------------------
Input `legs`: all legs of ONE account, in any order, one row per leg, columns `LEG_COLUMNS`
(`direction` is 'OUT' or 'IN'; an account has at most one OUT and one IN leg per transaction, and
both only for a self-transfer). It must not carry label columns (`spec.forbidden_inputs`).

Output: one row per input leg, sorted by (event_ts_ms, tx_id, OUT before IN), with the columns of
`output_columns(spec)`: `tx_id`, `role` ('S' for an OUT leg, 'R' for an IN leg), the account
features WITHOUT the s_/r_ prefix (the Spark job adds it when it joins the S row and the R row of a
transaction), then the relational features, also unprefixed. A relational feature that belongs to
the other role is null: `s_amount_vs_out_avg_72h` exists only on S rows, `r_returned_funds_24h`
only on R rows, `new_counterparty_72h` on both (it is `s_new_...` on S rows, `r_new_...` on R rows).
Values are "as of just before the transaction": a leg sees only log entries of smaller `tx_id`,
never itself, and for a self-transfer both legs see the same pre-transaction state.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

from features.expand import expand_template
from features.spec import Spec

LEG_COLUMNS: tuple[str, ...] = (
    "tx_id",
    "event_ts_ms",
    "direction",
    "counterparty_key",
    "amount_usd_cents",
    "payment_currency",
    "receiving_currency",
    "payment_format",
)
_INTEGER_LEG_COLUMNS = ("tx_id", "event_ts_ms", "amount_usd_cents")

ROLE_PREFIX: dict[str, str] = {"S": "s_", "R": "r_"}
DIRECTION_ROLE: dict[str, str] = {"OUT": "S", "IN": "R"}

# Columns whose values are ratios, shares or averages (float, null when the denominator is 0).
# Everything else is an integer count, sum or 0/1 flag (spec.yaml null_policy).
_FLOAT_PREFIXES = (
    "pass_through_ratio_",
    "avg_hold_minutes_",
    "cross_currency_share_",
    "format_share_",
    "account_age_minutes",
    "amount_vs_out_avg_",
)


@dataclass(frozen=True)
class Entry:
    """One appended log entry (spec.yaml `legs.entry_fields`).

    `hold_ms` (int64 milliseconds, D40) is set when an OUT entry is appended and is None for IN
    entries and for an OUT entry from an account that has no earlier IN entry.
    """

    tx_id: int
    event_ts_ms: int
    direction: str
    counterparty_key: str
    amount_usd_cents: int
    payment_currency: str
    receiving_currency: str
    payment_format: str
    hold_ms: int | None = None


# --- columns and dtypes ---------------------------------------------------------------------------


def account_columns(spec: Spec) -> list[str]:
    """The account features without a role prefix, in vector order (36 for spec v1)."""
    return [n for t in spec.account_features for n in expand_template(t, spec)]


def _unprefixed(name: str) -> str:
    if name[:2] not in ("s_", "r_"):
        raise ValueError(f"relational feature {name!r} has no s_/r_ prefix")
    return name[2:]


def role_relational(spec: Spec, role: str) -> list[str]:
    """The relational features (unprefixed) that a leg of this role computes."""
    out = []
    for t in spec.relational_features:
        if t.role not in DIRECTION_ROLE:
            raise ValueError(f"relational feature {t.name!r} needs role OUT or IN")
        if DIRECTION_ROLE[t.role] == role:
            out.append(_unprefixed(t.name))
    return out


def relational_columns(spec: Spec) -> list[str]:
    """All relational columns (unprefixed, each once), in first-appearance order."""
    seen: dict[str, None] = {}
    for t in spec.relational_features:
        seen.setdefault(_unprefixed(t.name))
    return list(seen)


def output_columns(spec: Spec) -> list[str]:
    return ["tx_id", "role", *account_columns(spec), *relational_columns(spec)]


def role_columns(spec: Spec, role: str) -> list[str]:
    """The feature columns a row of this role must supply (not `tx_id` and `role`)."""
    if role not in ROLE_PREFIX:
        raise ValueError(f"role must be 'S' or 'R', got {role!r}")
    return [*account_columns(spec), *role_relational(spec, role)]


def output_dtypes(spec: Spec) -> dict[str, str]:
    """pandas dtype name per output column: int64, object (role), float64 or nullable Int64."""
    out = {}
    for col in output_columns(spec):
        if col == "tx_id":
            out[col] = "int64"
        elif col == "role":
            out[col] = "object"
        elif col.startswith(_FLOAT_PREFIXES):
            out[col] = "float64"
        else:
            out[col] = "Int64"
    return out


# --- null helpers (spec.yaml null_policy) -----------------------------------------


def ratio_or_none(numerator: float, denominator: float) -> float | None:
    """numerator / denominator, or None when the denominator is 0 (the model sees NaN)."""
    if denominator == 0:
        return None
    return numerator / denominator


MS_PER_MINUTE = 60_000


def avg_hold_minutes(total_hold_ms: int, count: int) -> float | None:
    """`avg_hold_minutes_*` (D40): ONE division of two integers, `total_ms / (count * 60000)`.

    `total_hold_ms` is the exact int64 sum of `hold_ms` over the OUT entries of the window that
    have a hold, `count` how many there are. None when there are none.
    """
    if count == 0:
        return None
    return total_hold_ms / (count * MS_PER_MINUTE)


def vs_avg_or_none(amount: int, total: int, count: int) -> float | None:
    """`amount / (total / count)` (D41): None for an empty window (count == 0) AND for a zero
    average (total == 0). Evaluated as written: the average is a double, then amount / average."""
    if count == 0 or total == 0:
        return None
    return amount / (total / count)


# --- input validation and the frame builder -----------------------------------


def validate_legs(legs: pd.DataFrame, spec: Spec) -> None:
    """Raise ValueError if `legs` is not a valid single-account leg table."""
    forbidden = sorted(set(legs.columns) & set(spec.forbidden_inputs))
    if forbidden:
        raise ValueError(f"label columns must never reach the oracle: {forbidden}")
    missing = [c for c in LEG_COLUMNS if c not in legs.columns]
    if missing:
        raise ValueError(f"legs is missing columns {missing}")
    if legs.empty:
        return
    for col in _INTEGER_LEG_COLUMNS:
        if not pd.api.types.is_integer_dtype(legs[col]):
            raise ValueError(f"{col} must be an integer column, got {legs[col].dtype}")
    for col in LEG_COLUMNS:
        if legs[col].isna().any():
            raise ValueError(f"null in required column {col}")
    bad = sorted(set(legs["direction"]) - set(spec.dir_order))
    if bad:
        raise ValueError(f"direction must be one of {sorted(spec.dir_order)}, got {bad}")
    if legs.duplicated(["tx_id", "direction"]).any():
        raise ValueError(
            "duplicate (tx_id, direction): an account has one leg per direction per tx"
        )


def _to_frame(rows: Sequence[Mapping[str, Any]], spec: Spec) -> pd.DataFrame:
    """Build the typed output frame. Strict: a row must supply exactly its role's columns.

    A missing column raises (so a forgotten feature cannot silently become null); a column of the
    other role raises too. Columns of the other role's relational features are filled with null.
    None becomes NaN in float columns and <NA> in Int64 columns.
    """
    for row in rows:
        role = row.get("role")
        expected = {"tx_id", "role", *role_columns(spec, role)}  # raises for a bad role
        missing = sorted(expected - set(row))
        extra = sorted(set(row) - expected)
        if missing or extra:
            raise KeyError(
                f"tx_id {row.get('tx_id')} role {role}: missing {missing}, unexpected {extra}"
            )
    dtypes = output_dtypes(spec)
    data = {}
    for col, dtype in dtypes.items():
        values = [row.get(col) for row in rows]
        data[col] = pd.Series(values, dtype="object" if dtype == "object" else dtype)
    return pd.DataFrame(data)


def compute_account_legs(legs: pd.DataFrame, spec: Spec) -> pd.DataFrame:
    """Features for every leg of ONE account, as of just before each transaction.

    See the module docstring for the contract. Raises ValueError on an invalid input.
    """
    validate_legs(legs, spec)
    if legs.empty:
        return _to_frame([], spec)
    return _to_frame(_compute_rows(legs, spec), spec)


# =================================================================================================
# OWNER: human
# The window logic. Written by Claude on 2026-11-22 at the owner's explicit request ("implement it",
# CLAUDE.md rule 4); the owner reviews it and the loop is explained line by line in the handoff.
# =================================================================================================


def _compute_rows(legs: pd.DataFrame, spec: Spec) -> list[dict[str, Any]]:
    """One dict per input leg, in processing order, keyed by the columns `_to_frame` demands.

    Each dict has `tx_id`, `role` ('S' for OUT, 'R' for IN) and exactly
    `role_columns(spec, role)`; None is a null. The single pass of manual M02 §2 with the spec
    decisions D40 to D43. See the comments for the order of operations.
    """
    n = len(legs)
    tx_ids = legs["tx_id"].tolist()
    stamps = legs["event_ts_ms"].tolist()
    outs = [d == "OUT" for d in legs["direction"].tolist()]
    cps = legs["counterparty_key"].tolist()
    amounts = legs["amount_usd_cents"].tolist()
    crosses = [
        a != b for a, b in zip(legs["payment_currency"], legs["receiving_currency"], strict=True)
    ]
    formats = legs["payment_format"].tolist()

    # 1. Total order (event_ts_ms, tx_id, OUT before IN). The OUT leg of a self-transfer must be
    #    processed before its IN leg, because the hold of the OUT leg looks at IN entries that
    #    came BEFORE this transaction.
    dir_rank = [spec.dir_order["OUT" if o else "IN"] for o in outs]
    order = sorted(range(n), key=lambda k: (stamps[k], tx_ids[k], dir_rank[k]))
    sorted_ids = [tx_ids[k] for k in order]
    if any(b < a for a, b in zip(sorted_ids, sorted_ids[1:], strict=False)):
        raise ValueError(
            "tx_id order contradicts (event_ts_ms, tx_id) order: a leg would see a "
            "transaction with a larger tx_id"
        )

    log = _Log()
    fmt_windows = _format_share_windows(spec)
    windows = {
        name: _Window(spec.window_ms[name], log, track_formats=name in fmt_windows)
        for name in spec.default_windows
    }
    for needed in (_NEW_CP_WINDOW, _RETURNED_WINDOW, _AVG_WINDOW):
        if needed not in windows:
            raise ValueError(f"the relational features need the {needed} window in windows.default")
    names = {name: _column_names(name) for name in windows}
    share_names = _format_share_names(spec)

    first_seen: int | None = None  # kept forever: never evicted (account_age, log_empty)
    last_in_ts: int | None = None  # kept forever: the hold of an OUT entry may reach past 72 h
    rows: list[dict[str, Any]] = []

    i = 0
    while i < n:
        # 2. One transaction = the run of legs with the same tx_id: 1 leg, or 2 for a self-transfer
        #    (an account has at most one OUT and one IN leg per tx, checked in validate_legs).
        j = i + 1
        while j < n and sorted_ids[j] == sorted_ids[i]:
            j += 1
        group = order[i:j]
        is_self = len(group) == 2  # D43: both legs in this account's frame = self-transfer

        # 3. Compute every leg of the transaction from the state BEFORE the transaction.
        for k in group:
            t = stamps[k]
            for w in windows.values():
                w.evict_until(t)  # drop entries with ts <= t - W: start exclusive
            row: dict[str, Any] = {"tx_id": tx_ids[k], "role": "S" if outs[k] else "R"}
            for name, w in windows.items():
                c = names[name]
                row[c[0]] = w.n_out
                row[c[1]] = w.n_in
                row[c[2]] = w.sum_out
                row[c[3]] = w.sum_in
                row[c[4]] = len(w.cp_out)  # distinct = number of keys in the multiset
                row[c[5]] = len(w.cp_in)
                row[c[6]] = ratio_or_none(w.sum_out, w.sum_in)
                row[c[7]] = avg_hold_minutes(w.hold_total_ms, w.hold_n)
                row[c[8]] = ratio_or_none(w.n_cross, w.n_out + w.n_in)
            for fmt_window, per_format in share_names.items():
                w = windows[fmt_window]
                for fmt, column in per_format:
                    row[column] = ratio_or_none(w.formats.get(fmt, 0), w.n_out)
            row["account_age_minutes"] = (
                None if first_seen is None else (t - first_seen) / MS_PER_MINUTE
            )
            row["log_empty"] = int(first_seen is None)

            cp = cps[k]
            if outs[k]:
                known = windows[_NEW_CP_WINDOW].cp_out
                row["new_counterparty_72h"] = 0 if is_self else int(cp not in known)
                a = windows[_AVG_WINDOW]
                row["amount_vs_out_avg_72h"] = vs_avg_or_none(amounts[k], a.sum_out, a.n_out)
            else:
                known = windows[_NEW_CP_WINDOW].cp_in
                row["new_counterparty_72h"] = 0 if is_self else int(cp not in known)
                back = windows[_RETURNED_WINDOW].cp_out
                row["returned_funds_24h"] = 0 if is_self else int(cp in back)
            rows.append(row)

        # 4. Only now append the transaction's entries (OUT first). The hold of an OUT entry is
        #    fixed at append time from last_in_ts as it stands BEFORE this transaction's IN leg.
        for k in group:
            t = stamps[k]
            hold_ms = t - last_in_ts if outs[k] and last_in_ts is not None else None
            log.append(t, outs[k], cps[k], amounts[k], crosses[k], formats[k], hold_ms, is_self)
            idx = len(log.ts) - 1
            for w in windows.values():
                w.add(idx)
            if not outs[k]:
                last_in_ts = t
            if first_seen is None:
                first_seen = t
        i = j
    return rows


# --- window state (part of the same OWNER block) ------------------------------------------------

# The relational features read these windows (the names in their spec definitions).
_NEW_CP_WINDOW = "72h"
_RETURNED_WINDOW = "24h"
_AVG_WINDOW = "72h"


class _Log:
    """The appended entries, in order, as parallel lists (cheaper than one object per entry)."""

    __slots__ = ("amount", "cp", "cross", "fmt", "hold_ms", "is_out", "is_self", "ts")

    def __init__(self) -> None:
        self.ts: list[int] = []
        self.is_out: list[bool] = []
        self.cp: list[str] = []
        self.amount: list[int] = []
        self.cross: list[bool] = []
        self.fmt: list[str] = []
        self.hold_ms: list[int | None] = []
        self.is_self: list[bool] = []

    def append(self, ts, is_out, cp, amount, cross, fmt, hold_ms, is_self) -> None:  # noqa: ANN001
        self.ts.append(ts)
        self.is_out.append(is_out)
        self.cp.append(cp)
        self.amount.append(amount)
        self.cross.append(cross)
        self.fmt.append(fmt)
        self.hold_ms.append(hold_ms)
        self.is_self.append(is_self)


def _increment(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _decrement(counts: dict[str, int], key: str) -> None:
    remaining = counts[key] - 1
    if remaining:
        counts[key] = remaining
    else:
        del counts[key]  # drop at zero, so len(counts) is the number of distinct keys


class _Window:
    """Running aggregates over the entries in (t - width, now]: add on append, subtract on evict."""

    __slots__ = (
        "cp_in",
        "cp_out",
        "formats",
        "hold_n",
        "hold_total_ms",
        "log",
        "n_cross",
        "n_in",
        "n_out",
        "start",
        "sum_in",
        "sum_out",
        "width",
    )

    def __init__(self, width_ms: int, log: _Log, track_formats: bool) -> None:
        self.width = width_ms
        self.log = log
        self.start = 0  # index of the oldest entry still inside the window
        self.n_out = self.n_in = self.sum_out = self.sum_in = self.n_cross = 0
        self.hold_total_ms = self.hold_n = 0  # D40: exact int64 sum, never a float
        self.cp_out: dict[str, int] = {}  # counterparty multisets; self-transfers stay out (D43)
        self.cp_in: dict[str, int] = {}
        self.formats: dict[str, int] | None = {} if track_formats else None

    def add(self, i: int) -> None:
        log = self.log
        if log.is_out[i]:
            self.n_out += 1
            self.sum_out += log.amount[i]
            if not log.is_self[i]:
                _increment(self.cp_out, log.cp[i])
            if log.hold_ms[i] is not None:
                self.hold_total_ms += log.hold_ms[i]
                self.hold_n += 1
            if self.formats is not None:
                _increment(self.formats, log.fmt[i])
        else:
            self.n_in += 1
            self.sum_in += log.amount[i]
            if not log.is_self[i]:
                _increment(self.cp_in, log.cp[i])
        if log.cross[i]:
            self.n_cross += 1

    def _remove(self, i: int) -> None:
        log = self.log
        if log.is_out[i]:
            self.n_out -= 1
            self.sum_out -= log.amount[i]
            if not log.is_self[i]:
                _decrement(self.cp_out, log.cp[i])
            if log.hold_ms[i] is not None:
                self.hold_total_ms -= log.hold_ms[i]
                self.hold_n -= 1
            if self.formats is not None:
                _decrement(self.formats, log.fmt[i])
        else:
            self.n_in -= 1
            self.sum_in -= log.amount[i]
            if not log.is_self[i]:
                _decrement(self.cp_in, log.cp[i])
        if log.cross[i]:
            self.n_cross -= 1

    def evict_until(self, t: int) -> None:
        """Remove every entry with ts <= t - width (the window start is exclusive)."""
        cutoff = t - self.width
        ts = self.log.ts
        while self.start < len(ts) and ts[self.start] <= cutoff:
            self._remove(self.start)
            self.start += 1


def _column_names(window: str) -> tuple[str, ...]:
    """The nine windowed account columns, in the order the loop fills them."""
    return tuple(
        f"{base}_{window}"
        for base in (
            "out_count",
            "in_count",
            "out_sum_usd_cents",
            "in_sum_usd_cents",
            "distinct_out_cp",
            "distinct_in_cp",
            "pass_through_ratio",
            "avg_hold_minutes",
            "cross_currency_share",
        )
    )


def _format_share_windows(spec: Spec) -> set[str]:
    return {w for t in spec.account_features if t.for_each == "payment_formats" for w in t.windows}


def _format_share_names(spec: Spec) -> dict[str, list[tuple[str, str]]]:
    """window -> [(payment format, column name)] for the `{fmt}` templates."""
    out: dict[str, list[tuple[str, str]]] = {}
    for t in spec.account_features:
        if t.for_each != "payment_formats":
            continue
        (window,) = t.windows  # the name carries its window literally, so exactly one
        columns = expand_template(t, spec)
        out.setdefault(window, []).extend(zip(spec.payment_formats, columns, strict=True))
    return out
