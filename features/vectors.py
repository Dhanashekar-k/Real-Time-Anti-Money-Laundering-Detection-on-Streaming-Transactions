"""Transactions -> legs -> full feature vectors, in pure Python (golden tests and small samples).

The Spark job does the same assembly in Spark SQL (`batch/spark/features.py`); this module is the
readable reference for how the pieces fit: each transaction becomes an OUT leg on the sender and an
IN leg on the receiver, each account runs the oracle over its own legs, and the vector of a
transaction is its tx features, the sender's S-role features (`s_` prefix), the receiver's R-role
features (`r_` prefix) and the relational features, in `feature_names_v<N>.txt` order.

No window logic lives here (that is `features/reference.py`, OWNER: human), and the input rows are
read only for the columns named below, so label columns on a transaction never reach a feature.
"""

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from typing import Any

import numpy as np
import pandas as pd

from features import tx_features
from features.reference import (
    DIRECTION_ROLE,
    LEG_COLUMNS,
    account_columns,
    compute_account_legs,
)
from features.spec import Spec

Oracle = Callable[[pd.DataFrame, Spec], pd.DataFrame]


def legs_from_transactions(txs: Iterable[Mapping[str, Any]]) -> dict[str, pd.DataFrame]:
    """One leg table per account key (spec.yaml `legs`). A self-transfer lands both legs on one."""
    rows: dict[str, list[tuple]] = defaultdict(list)
    for tx in txs:
        common = (tx["payment_currency"], tx["receiving_currency"], tx["payment_format"])
        rows[tx["sender_key"]].append(
            (
                tx["tx_id"],
                tx["event_ts_ms"],
                "OUT",
                tx["receiver_key"],
                tx["amount_paid_usd_cents"],
                *common,
            )
        )
        rows[tx["receiver_key"]].append(
            (
                tx["tx_id"],
                tx["event_ts_ms"],
                "IN",
                tx["sender_key"],
                tx["amount_received_usd_cents"],
                *common,
            )
        )
    return {key: pd.DataFrame(r, columns=list(LEG_COLUMNS)) for key, r in rows.items()}


def _py(value: Any) -> Any:
    """numpy / pandas scalar -> plain Python; NaN and <NA> -> None."""
    if value is None or value is pd.NA:
        return None
    if isinstance(value, float | np.floating) and np.isnan(value):
        return None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    return value


def compute_vectors(
    txs: Iterable[Mapping[str, Any]], spec: Spec, oracle: Oracle = compute_account_legs
) -> dict[int, dict[str, float | int | None]]:
    """tx_id -> {feature name -> value}, every key of the vector, in vector order."""
    txs = list(txs)
    by_leg: dict[tuple[str, int, str], dict[str, Any]] = {}
    for account, legs in legs_from_transactions(txs).items():
        for row in oracle(legs, spec).to_dict("records"):
            by_leg[(account, int(row["tx_id"]), row["role"])] = row

    account_cols = account_columns(spec)
    out: dict[int, dict[str, float | int | None]] = {}
    for tx in sorted(txs, key=lambda t: t["tx_id"]):
        tx_id = tx["tx_id"]
        try:
            s_row = by_leg[(tx["sender_key"], tx_id, "S")]
            r_row = by_leg[(tx["receiver_key"], tx_id, "R")]
        except KeyError:
            raise KeyError(
                f"tx_id {tx_id}: the oracle returned no row for one of its legs"
            ) from None
        vec: dict[str, float | int | None] = dict(tx_features.compute(tx, spec))
        for prefix, row in (("s_", s_row), ("r_", r_row)):
            for col in account_cols:
                vec[prefix + col] = _py(row[col])
        for t in spec.relational_features:
            row = s_row if DIRECTION_ROLE[t.role] == "S" else r_row
            vec[t.name] = _py(row[t.name[2:]])
        out[tx_id] = vec
    return out
