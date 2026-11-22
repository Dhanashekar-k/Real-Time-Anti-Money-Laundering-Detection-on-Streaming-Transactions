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

    `hold_minutes` is set when an OUT entry is appended and is None for IN entries and for an OUT
    entry from an account that has no earlier IN entry.
    """

    tx_id: int
    event_ts_ms: int
    direction: str
    counterparty_key: str
    amount_usd_cents: int
    payment_currency: str
    receiving_currency: str
    payment_format: str
    hold_minutes: float | None = None


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


def mean_or_none(total: float, count: int) -> float | None:
    """total / count, or None for an empty set."""
    if count == 0:
        return None
    return total / count


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
# The window logic. Not written on purpose (CLAUDE.md rule 4). Everything above this line is
# scaffolding you can rely on; everything you need to return is described here.
# =================================================================================================


def _compute_rows(legs: pd.DataFrame, spec: Spec) -> list[dict[str, Any]]:
    """One dict per input leg, in processing order, keyed by the columns `_to_frame` demands.

    Each dict has `tx_id`, `role` ('S' for OUT, 'R' for IN) and exactly
    `role_columns(spec, role)`; use None for a null value. A forgotten column raises KeyError in
    `_to_frame`, so you cannot leave one out by accident.

    TODO(human): the single pass of manual M02 §2. The pieces, none of them written:
      1. Order: sort by (event_ts_ms, tx_id, spec.dir_order[direction]).
      2. Walk the legs one transaction (same tx_id) at a time. Every leg of the transaction is
         computed from the state BEFORE the transaction; only then are its entries appended
         (a self-transfer has two legs and both must see the same pre-state).
      3. For each window W in spec.default_windows (spec.window_ms[W]): a start pointer into the
         log that advances while log[start].event_ts_ms <= t - W (start exclusive, end inclusive),
         and running counts and sums for OUT and IN, a counterparty multiset per direction (drop a
         key when its count reaches 0: distinct = number of keys), the cross-currency count, the
         hold sum and hold count over OUT entries with a non-null hold. Add on append, subtract on
         evict.
      4. The 72 h window also needs the per-format counts of OUT entries (the format shares).
      5. Outside the window state, kept forever: first_seen (for account_age_minutes and
         log_empty) and last_in_ts (for hold_minutes, which is stored on an OUT entry at append
         time: (entry.event_ts_ms - last_in_ts) / 60000, None if there is no earlier IN entry).
      6. Relational features, from this account's own state: new_counterparty_72h,
         returned_funds_24h (R legs, 24 h OUT multiset contains the sender), and
         amount_vs_out_avg_72h (S legs). See spec.yaml for the exact definitions and
         `ratio_or_none` / `mean_or_none` for the null rules.
      Spec subtleties to decide and write down in the golden vectors: a self-transfer's
      counterparty is the account itself (D36 asks M2 to decide how that counts);
      amount_vs_out_avg_72h divides by an average that is 0 when the window holds only
      zero-amount entries (null_policy says null when the denominator is 0).
    """
    raise NotImplementedError("TODO(human): features/reference.py::_compute_rows")
