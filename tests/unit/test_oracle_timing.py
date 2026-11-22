"""eval/oracle_timing.py: the scaling helper keeps a valid single-account leg table."""

import pandas as pd
import pytest

from eval import oracle_timing
from features import reference, spec

pytestmark = pytest.mark.unit


def _legs() -> pd.DataFrame:
    rows = [
        (1, 1_000, "OUT", "B", 100, "USD", "USD", "ACH"),
        (2, 61_000, "IN", "C", 50, "USD", "USD", "ACH"),
        (3, 121_000, "OUT", "D", 70, "USD", "EUR", "WIRE"),
    ]
    return pd.DataFrame(rows, columns=list(reference.LEG_COLUMNS))


def test_scaled_account_is_valid_and_has_the_requested_size() -> None:
    big = oracle_timing.scale_legs(_legs(), 10)
    assert len(big) == 10
    reference.validate_legs(big, spec.load_spec())  # unique (tx_id, direction), integer columns
    ordered = big.sort_values(["event_ts_ms", "tx_id"])
    assert ordered["tx_id"].is_monotonic_increasing  # tx_id order still agrees with time order
    assert big["tx_id"].is_unique


def test_scaled_account_runs_through_the_oracle() -> None:
    out = reference.compute_account_legs(oracle_timing.scale_legs(_legs(), 25), spec.load_spec())
    assert len(out) == 25
