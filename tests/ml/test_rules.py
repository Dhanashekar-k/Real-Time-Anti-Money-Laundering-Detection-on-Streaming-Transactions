"""ml/rules_baseline.py: five transparent rules, tuned on train only, plus the documented-zero R1.

Hand-worked values:
- amounts 1..100 in train: np.quantile(q=0.99, method="lower") picks sorted index
  floor((100-1) * 0.99) = floor(98.01) = 98, i.e. the value 99; an alert needs amount > 99, so
  only the amount 100 fires.
"""

import numpy as np
import pandas as pd
import pytest

from ml import rules_baseline as rb

pytestmark = pytest.mark.unit

H = 3_600_000
CUTOFF = 24 * H  # burn-in end for these toy frames


def _frame(**cols) -> pd.DataFrame:
    n = len(next(iter(cols.values())))
    base = {
        "event_ts_ms": np.full(n, CUTOFF + 1),
        "amount_paid_usd_cents": np.full(n, 100),
        "is_cross_currency": np.zeros(n, dtype=int),
        "s_pass_through_ratio_24h": np.full(n, np.nan),
        "s_in_count_24h": np.zeros(n, dtype=int),
        "s_distinct_out_cp_24h": np.zeros(n, dtype=int),
        "r_distinct_in_cp_24h": np.zeros(n, dtype=int),
        "r_returned_funds_24h": np.zeros(n, dtype=int),
        "is_laundering": np.zeros(n, dtype=int),
    }
    base.update({k: np.asarray(v) for k, v in cols.items()})
    return pd.DataFrame(base)


def _rule(rule_id: str) -> rb.Rule:
    return next(r for r in rb.RULES if r.id == rule_id)


def _fitted(rule_id: str, **params) -> rb.FittedRule:
    return rb.FittedRule(rule_id=rule_id, params=params, derived={})


# --- the rules ----------------------------------------------------------------------------------


def test_r1_same_currency_above_the_train_quantile() -> None:
    train = _frame(amount_paid_usd_cents=np.arange(1, 101))
    fitted = rb.fit_one(_rule("R1"), train, burn_in_end_ms=CUTOFF, params={"q": 0.99})
    assert fitted.derived["amount_threshold"] == 99
    df = _frame(
        amount_paid_usd_cents=[99, 100, 100, 5_000],
        is_cross_currency=[0, 0, 1, 1],
    )
    # strictly greater than the threshold, and only for same-currency rows
    assert rb.apply(_rule("R1"), fitted, df).tolist() == [False, True, False, False]


def test_original_r1_is_the_cross_currency_version_and_stays_a_documented_zero() -> None:
    assert rb.ORIGINAL_R1.id == "R1_original"
    train = _frame(amount_paid_usd_cents=np.arange(1, 101))
    fitted = rb.fit_one(rb.ORIGINAL_R1, train, burn_in_end_ms=CUTOFF, params={"q": 0.99})
    df = _frame(
        amount_paid_usd_cents=[100, 100, 5_000],
        is_cross_currency=[0, 1, 1],
        is_laundering=[1, 0, 0],  # every positive is same-currency, as in the real data (D36)
    )
    alert = rb.apply(rb.ORIGINAL_R1, fitted, df)
    assert alert.tolist() == [False, True, True]  # fires on cross-currency rows only
    assert not alert[df["is_laundering"].to_numpy() == 1].any()  # so it never catches a positive


def test_r2_band_is_inclusive_and_nan_never_fires() -> None:
    df = _frame(
        s_pass_through_ratio_24h=[0.9, 1.1, 0.89, 1.11, np.nan, 1.0],
        s_in_count_24h=[2, 2, 2, 2, 2, 1],
    )
    got = rb.apply(_rule("R2"), _fitted("R2", lo=0.9, hi=1.1, min_in=2), df)
    assert got.tolist() == [True, True, False, False, False, False]


def test_r3_and_r4_are_at_least_k() -> None:
    df = _frame(s_distinct_out_cp_24h=[4, 5, 6], r_distinct_in_cp_24h=[6, 5, 4])
    assert rb.apply(_rule("R3"), _fitted("R3", k=5), df).tolist() == [False, True, True]
    assert rb.apply(_rule("R4"), _fitted("R4", k=5), df).tolist() == [True, True, False]


def test_r5_returned_funds_flag() -> None:
    df = _frame(r_returned_funds_24h=[0, 1, 0, 1])
    assert rb.apply(_rule("R5"), _fitted("R5"), df).tolist() == [False, True, False, True]


# --- tuning on train only -----------------------------------------------------------------------


def test_fit_picks_the_grid_point_with_the_best_train_f1() -> None:
    # positives have exactly 5 distinct counterparties; k=5 is perfect, k<5 adds false alerts,
    # k>5 misses every positive
    n_neg, n_pos = 40, 10
    distinct = np.array([3] * n_neg + [5] * n_pos)
    y = np.array([0] * n_neg + [1] * n_pos)
    train = _frame(s_distinct_out_cp_24h=distinct, is_laundering=y)
    fitted = rb.fit_one(_rule("R3"), train, burn_in_end_ms=CUTOFF)
    assert fitted.params == {"k": 5}
    assert fitted.derived["train_f1"] == 1.0


def test_ties_go_to_the_strictest_grid_point() -> None:
    # no positives at all: every k has F1 0, so the strictest (largest) k is kept
    train = _frame(s_distinct_out_cp_24h=[1, 2, 3])
    fitted = rb.fit_one(_rule("R3"), train, burn_in_end_ms=CUTOFF)
    assert fitted.params == {"k": max(rb.K_GRID)}


def test_fit_refuses_rows_that_are_not_train() -> None:
    df = _frame(s_distinct_out_cp_24h=[1, 2])
    df["split"] = ["train", "val"]
    with pytest.raises(ValueError, match="split"):
        rb.fit_one(_rule("R3"), df, burn_in_end_ms=CUTOFF)


def test_fit_refuses_burn_in_rows() -> None:
    df = _frame(s_distinct_out_cp_24h=[1, 2])
    df.loc[0, "event_ts_ms"] = CUTOFF - 1  # inside the first 24 h
    with pytest.raises(ValueError, match="burn"):
        rb.fit_one(_rule("R3"), df, burn_in_end_ms=CUTOFF)


def test_amount_threshold_comes_from_train_not_from_the_frame_it_is_applied_to() -> None:
    train = _frame(amount_paid_usd_cents=np.arange(1, 101))
    fitted = rb.fit_one(_rule("R1"), train, burn_in_end_ms=CUTOFF, params={"q": 0.99})
    huge = _frame(amount_paid_usd_cents=np.full(1000, 10**9))
    again = rb.apply(_rule("R1"), fitted, huge)
    assert again.all()  # threshold is still 99, not recomputed from `huge`
    assert fitted.derived["amount_threshold"] == 99


def test_fit_all_covers_the_five_rules_and_the_original() -> None:
    train = _frame(amount_paid_usd_cents=np.arange(1, 101))
    fits = rb.fit_all(train, burn_in_end_ms=CUTOFF)
    assert [f.rule_id for f in fits.rules] == ["R1", "R2", "R3", "R4", "R5"]
    assert fits.original_r1.rule_id == "R1_original"
    assert fits.original_r1.params == {"q": 0.99}  # fixed, not tuned


# --- combining and choosing ---------------------------------------------------------------------


def test_rule_set_is_the_or_of_the_five_and_excludes_the_original() -> None:
    train = _frame(amount_paid_usd_cents=np.arange(1, 101))
    fits = rb.fit_all(train, burn_in_end_ms=CUTOFF)
    df = _frame(
        r_returned_funds_24h=[1, 0, 0],
        s_distinct_out_cp_24h=[0, 100, 0],
        is_cross_currency=[0, 0, 1],
        amount_paid_usd_cents=[1, 1, 10**9],  # row 3 is cross-currency and huge: original R1 only
    )
    assert rb.rule_set_alert(fits, df).tolist() == [True, True, False]


def test_choose_baseline_takes_the_higher_val_f1_and_ties_go_to_the_rule_set() -> None:
    val = {"rule_set": 0.30, "R1": 0.10, "R2": 0.35, "R3": 0.05, "R4": 0.0, "R5": 0.2}
    assert rb.choose_baseline(val) == "R2"
    val["R2"] = 0.30
    assert rb.choose_baseline(val) == "rule_set"  # tie
    val["rule_set"] = 0.5
    assert rb.choose_baseline(val) == "rule_set"


# --- the printout the owner reviews before `make rules` -------------------------------------------


def test_definitions_text_lists_all_rules_grids_and_the_swap() -> None:
    text = rb.definitions_text()
    for needle in ("R1 ", "R2 ", "R3 ", "R4 ", "R5 ", "R1_original"):
        assert needle in text, needle
    assert "same-currency" in text and "cross-currency" in text
    assert "train" in text and "val" in text
    for q in rb.Q_GRID:
        assert str(q) in text
    for k in rb.K_GRID:
        assert str(k) in text
    assert "is_laundering" not in text  # the printout names no label column


def test_rules_read_only_feature_columns_and_the_amount() -> None:
    needed = set(rb.REQUIRED_COLUMNS)
    assert needed == {
        "amount_paid_usd_cents",
        "is_cross_currency",
        "s_pass_through_ratio_24h",
        "s_in_count_24h",
        "s_distinct_out_cp_24h",
        "r_distinct_in_cp_24h",
        "r_returned_funds_24h",
    }
    # every rule must give the same answer when label-like columns are dropped
    train = _frame(amount_paid_usd_cents=np.arange(1, 101))
    fits = rb.fit_all(train, burn_in_end_ms=CUTOFF)
    df = _frame(r_returned_funds_24h=[1, 0], s_distinct_out_cp_24h=[10, 2])
    with_label = rb.rule_set_alert(fits, df)
    without = rb.rule_set_alert(fits, df.drop(columns=["is_laundering"]))
    assert with_label.tolist() == without.tolist()
