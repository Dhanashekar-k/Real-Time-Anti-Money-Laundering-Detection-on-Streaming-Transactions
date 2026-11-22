"""avg_hold_minutes divides by the number of hold-bearing OUT entries, not by all OUT entries.

An OUT entry has no hold when the account has not yet received anything (D40: hold_ms is null).
Such an entry counts in `out_count_*` but must not count in the denominator of `avg_hold_minutes_*`.

Account A, minutes from an arbitrary origin (all USD, one counterparty each):
  tx1 OUT  0   no earlier IN          -> no hold
  tx2 IN   10
  tx3 OUT  20  last IN at 10          -> hold 10 min  =   600,000 ms
  tx4 OUT  30  last IN at 10          -> hold 20 min  = 1,200,000 ms
  tx5 OUT  70  last IN at 10          -> hold 60 min  = 3,600,000 ms
  tx6 OUT  85                          (the leg we read, each time before its own entry)

Hand-computed avg_hold_minutes = total_ms / (hold_count * 60000), one division:
  tx3 1h window: OUT = {tx1 (no hold)}            -> count 0                    -> null (not 0.0)
  tx4 1h window: OUT = {tx1, tx3}; holds {tx3}    -> 600,000 / (1 * 60,000)      = 10.0  (not 5.0)
  tx5 24h window: OUT = {tx1, tx3, tx4}, holds {tx3, tx4}
                                                  -> 1,800,000 / (2 * 60,000)    = 15.0  (not 10.0)
  tx5 1h window (ts > 10): OUT = {tx3 (20), tx4 (30)} (tx1 at 0 and IN tx2 at 10 are out)
                                                  -> 1,800,000 / (2 * 60,000)    = 15.0
  tx6 1h window (ts > 25): OUT = {tx4 (30), tx5 (70)}, tx3 evicted
                                                  -> (1,200,000 + 3,600,000) / (2 * 60,000) = 40.0
"""

import pandas as pd
import pytest

from features import reference, spec

pytestmark = pytest.mark.unit

MIN = 60_000


def _legs() -> pd.DataFrame:
    rows = [
        (1, 0, "OUT"),
        (2, 10, "IN"),
        (3, 20, "OUT"),
        (4, 30, "OUT"),
        (5, 70, "OUT"),
        (6, 85, "OUT"),
    ]
    return pd.DataFrame(
        [(i, m * MIN, d, f"cp{i}", 100, "USD", "USD", "ACH") for i, m, d in rows],
        columns=list(reference.LEG_COLUMNS),
    )


@pytest.fixture(scope="module")
def out() -> pd.DataFrame:
    return reference.compute_account_legs(_legs(), spec.load_spec()).set_index("tx_id")


def test_only_no_hold_entries_in_the_window_gives_null_not_zero(out: pd.DataFrame) -> None:
    assert out.loc[3, "out_count_1h"] == 1  # tx1 is an OUT entry in the window ...
    assert pd.isna(out.loc[3, "avg_hold_minutes_1h"])  # ... but it has no hold: no average


def test_denominator_counts_only_hold_bearing_entries(out: pd.DataFrame) -> None:
    assert out.loc[4, "out_count_1h"] == 2  # tx1 (no hold) and tx3 (hold)
    assert out.loc[4, "avg_hold_minutes_1h"] == 10.0  # 600,000 / (1 * 60,000), not / 2 -> 5.0


def test_same_in_a_wider_window(out: pd.DataFrame) -> None:
    assert out.loc[5, "out_count_24h"] == 3
    assert out.loc[5, "avg_hold_minutes_24h"] == 15.0  # (600k + 1.2M) / (2 * 60k), not / 3 -> 10.0


def test_eviction_removes_the_hold_from_both_sum_and_count(out: pd.DataFrame) -> None:
    assert out.loc[5, "avg_hold_minutes_1h"] == 15.0  # tx1 and the IN entry are already out
    assert out.loc[6, "out_count_1h"] == 2  # tx3 evicted: only tx4 and tx5 remain
    assert out.loc[6, "avg_hold_minutes_1h"] == 40.0  # (1.2M + 3.6M) / (2 * 60k)
