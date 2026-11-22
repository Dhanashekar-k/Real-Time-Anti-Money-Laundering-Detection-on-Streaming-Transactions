"""ml/metrics.py: confusion counts, F1, alerts per day, tail days from eda.json, paired bootstrap.

Hand-worked values:
- y = [1,1,0,0,1], alert = [1,0,1,0,1]: tp 2, fp 1, fn 1, tn 1 -> precision 2/3, recall 2/3,
  F1 = 2*tp / (2*tp + fp + fn) = 4/6.
- 2022-09-11 is 10 days after 2022-09-01 (day number 19236), so day number 19246.
"""

import numpy as np
import pytest

from ml import metrics

pytestmark = pytest.mark.unit

DAY = 86_400_000


def test_confusion_and_prf1_by_hand() -> None:
    y = np.array([1, 1, 0, 0, 1])
    alert = np.array([1, 0, 1, 0, 1])
    assert metrics.confusion(y, alert) == (2, 1, 1, 1)
    p, r, f1 = metrics.prf1(2, 1, 1)
    assert (p, r) == (2 / 3, 2 / 3)
    assert f1 == 4 / 6


def test_zero_division_is_zero_not_nan() -> None:
    assert metrics.prf1(0, 0, 0) == (0.0, 0.0, 0.0)  # no alerts, no positives
    assert metrics.prf1(0, 3, 0) == (0.0, 0.0, 0.0)  # alerts but no positives
    assert metrics.prf1(0, 0, 4) == (0.0, 0.0, 0.0)  # positives but no alerts


def test_tail_days_come_from_eda_json() -> None:
    eda = {"sparse_days": {"leading": [], "trailing": ["2022-09-11", "2022-09-12"]}}
    assert metrics.tail_day_numbers(eda) == {19246, 19247}
    both = {"sparse_days": {"leading": ["2022-09-01"], "trailing": ["2022-09-11"]}}
    assert metrics.tail_day_numbers(both) == {19236, 19246}


def test_tail_days_from_the_real_eda_files() -> None:
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    for ds, n in (("hi_small", 8), ("hi_medium", 12)):
        eda = json.loads((root / "results" / "M01" / ds / "eda.json").read_text())
        days = metrics.tail_day_numbers(eda)
        assert len(days) == n
        assert max(days) - min(days) == n - 1  # one contiguous run


def test_without_tail_mask() -> None:
    ts = np.array([19245 * DAY + 5, 19246 * DAY, 19246 * DAY + 7, 19247 * DAY + 1])
    keep = metrics.without_days(ts, {19246, 19247})
    assert keep.tolist() == [True, False, False, False]


def test_alerts_per_day() -> None:
    ts = np.array([0, 10, 20, DAY + 5, DAY + 6])  # two UTC days
    alert = np.array([1, 1, 0, 1, 0])
    assert metrics.alerts_per_day(ts, alert) == 1.5  # 3 alerts over 2 days
    assert metrics.alerts_per_day(np.array([], dtype=np.int64), np.array([], dtype=int)) == 0.0


# --- paired bootstrap ---------------------------------------------------------------------------


def _toy(n: int = 400, seed: int = 0):
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < 0.15).astype(int)
    good = np.where(rng.random(n) < 0.85, y, 1 - y)  # right 85% of the time
    poor = np.where(rng.random(n) < 0.60, y, 1 - y)
    return y, good, poor


def test_point_estimates_are_the_plain_f1s() -> None:
    y, a, b = _toy()
    out = metrics.paired_bootstrap_f1(y, a, b, n_resamples=50, seed=1)
    assert out["f1_a"]["point"] == metrics.prf1(*metrics.confusion(y, a)[:3])[2]
    assert out["f1_b"]["point"] == metrics.prf1(*metrics.confusion(y, b)[:3])[2]
    assert out["diff"]["point"] == out["f1_a"]["point"] - out["f1_b"]["point"]
    assert out["n_rows"] == len(y) and out["n_resamples"] == 50 and out["seed"] == 1


def test_default_is_1000_resamples_95_percent() -> None:
    y, a, b = _toy()
    out = metrics.paired_bootstrap_f1(y, a, b)
    assert (out["n_resamples"], out["level"]) == (1000, 0.95)


def test_intervals_contain_their_point_estimates() -> None:
    y, a, b = _toy()
    out = metrics.paired_bootstrap_f1(y, a, b, seed=3)
    for key in ("f1_a", "f1_b", "diff"):
        lo, hi = out[key]["ci"]
        assert lo <= out[key]["point"] <= hi, key


def test_identical_predictors_have_a_zero_width_difference_interval() -> None:
    y, a, _ = _toy()
    out = metrics.paired_bootstrap_f1(y, a, a.copy(), seed=2)
    assert out["diff"]["point"] == 0.0
    assert out["diff"]["ci"] == (0.0, 0.0)  # paired: the same rows are drawn for both
    assert out["f1_a"]["ci"] == out["f1_b"]["ci"]


def test_clearly_better_predictor_has_a_positive_difference_interval() -> None:
    y, good, poor = _toy(n=2000, seed=5)
    out = metrics.paired_bootstrap_f1(y, good, poor, seed=4)
    assert out["diff"]["ci"][0] > 0


def test_same_seed_same_result_different_seed_different_draws() -> None:
    y, a, b = _toy()
    r1 = metrics.paired_bootstrap_f1(y, a, b, seed=7)
    r2 = metrics.paired_bootstrap_f1(y, a, b, seed=7)
    r3 = metrics.paired_bootstrap_f1(y, a, b, seed=8)
    assert r1 == r2
    assert r1["diff"]["ci"] != r3["diff"]["ci"]


def test_paired_interval_is_narrower_than_the_difference_of_independent_ones() -> None:
    y, a, _ = _toy(n=1500, seed=11)
    b = a.copy()
    b[:40] = 1 - b[:40]  # nearly the same predictions
    out = metrics.paired_bootstrap_f1(y, a, b, seed=1)
    width = out["diff"]["ci"][1] - out["diff"]["ci"][0]
    marg = [out[k]["ci"][1] - out[k]["ci"][0] for k in ("f1_a", "f1_b")]
    assert width < min(marg)


def test_cell_counting_matches_resampling_rows() -> None:
    """The multinomial shortcut gives the same intervals as drawing row indices with replacement."""
    y, a, b = _toy(n=300, seed=2)
    out = metrics.paired_bootstrap_f1(y, a, b, n_resamples=4000, seed=1)
    rng = np.random.default_rng(99)
    diffs = []
    for _ in range(4000):
        idx = rng.integers(0, len(y), len(y))
        fa = metrics.prf1(*metrics.confusion(y[idx], a[idx])[:3])[2]
        fb = metrics.prf1(*metrics.confusion(y[idx], b[idx])[:3])[2]
        diffs.append(fa - fb)
    lo, hi = np.percentile(diffs, [2.5, 97.5])
    assert out["diff"]["ci"][0] == pytest.approx(lo, abs=0.02)
    assert out["diff"]["ci"][1] == pytest.approx(hi, abs=0.02)


def test_rejects_bad_input() -> None:
    with pytest.raises(ValueError, match="same length"):
        metrics.paired_bootstrap_f1(np.array([1, 0]), np.array([1]), np.array([1, 0]))
    with pytest.raises(ValueError, match="empty"):
        metrics.paired_bootstrap_f1(np.array([]), np.array([]), np.array([]))
    with pytest.raises(ValueError, match="0/1"):
        metrics.paired_bootstrap_f1(np.array([2, 0]), np.array([1, 0]), np.array([1, 0]))
