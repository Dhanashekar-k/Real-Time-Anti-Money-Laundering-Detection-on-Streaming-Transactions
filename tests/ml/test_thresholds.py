"""ml/thresholds.py: F1-optimal and alert-budget thresholds, per bank, from validation scores.

Hand-worked F1 case: scores [0.9, 0.8, 0.7, 0.6, 0.5], labels [1, 1, 0, 1, 0], P = 3 positives.
F1 at a cut = 2*tp / (alerts + P):
  >=0.9: tp 1, alerts 1 -> 2/4  = 0.5
  >=0.8: tp 2, alerts 2 -> 4/5  = 0.8
  >=0.7: tp 2, alerts 3 -> 4/6  = 0.667
  >=0.6: tp 3, alerts 4 -> 6/7  = 0.857   <- best
  >=0.5: tp 3, alerts 5 -> 6/8  = 0.75
"""

import numpy as np
import pandas as pd
import pytest

from ml import thresholds

pytestmark = pytest.mark.unit

DAY = 86_400_000


def test_f1_optimal_by_hand() -> None:
    y = np.array([1, 1, 0, 1, 0])
    s = np.array([0.9, 0.8, 0.7, 0.6, 0.5])
    got = thresholds.f1_optimal(y, s)
    assert got["threshold"] == 0.6
    assert got["f1"] == 6 / 7
    assert (got["precision"], got["recall"], got["alerts"]) == (3 / 4, 1.0, 4)


def test_f1_optimal_does_not_split_tied_scores() -> None:
    # the 0.5 group holds a positive and a negative: a cut must take both or neither
    y = np.array([1, 0, 1])
    s = np.array([0.9, 0.5, 0.5])
    got = thresholds.f1_optimal(y, s)
    assert got["alerts"] in (1, 3)
    assert got["threshold"] in (0.9, 0.5)


def test_equal_f1_cuts_go_to_the_higher_threshold() -> None:
    # labels in descending-score order, P = 3 positives at ranks 1, 5 and 12 (n = 12).
    # F1 = 2*tp / (alerts + 3): 1 alert -> 2/4 = 0.5; 5 alerts (tp 2) -> 4/8 = 0.5 (a tie);
    # every other cut is lower (2 to 4 alerts: <= 0.4; 6 to 11: 4/9 or less; 12: 6/15 = 0.4).
    y = np.array([1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1])
    s = np.linspace(0.99, 0.0, 12)
    got = thresholds.f1_optimal(y, s)
    assert got["f1"] == 0.5
    assert got["alerts"] == 1  # the stricter of the two tied cuts
    assert got["threshold"] == s[0]


def test_f1_without_positives_is_an_error() -> None:
    with pytest.raises(ValueError, match="positive"):
        thresholds.f1_optimal(np.zeros(5, dtype=int), np.linspace(0, 1, 5))


def test_budget_threshold_k_per_day() -> None:
    # two days, K = 2 -> at most 4 alerts in total
    ts = np.array([0, 1, 2, 3, DAY, DAY + 1, DAY + 2])
    s = np.array([0.95, 0.90, 0.85, 0.20, 0.80, 0.75, 0.10])
    got = thresholds.budget(s, ts, k_per_day=2)
    assert got["n_days"] == 2 and got["target_alerts"] == 4
    assert got["threshold"] == 0.80  # scores >= 0.80: 0.95, 0.90, 0.85, 0.80 -> 4 alerts
    assert got["alerts"] == 4 and got["alerts_per_day"] == 2.0


def test_budget_never_exceeds_the_target_when_scores_tie_at_the_cut() -> None:
    ts = np.arange(6)
    s = np.array([0.9, 0.9, 0.9, 0.9, 0.1, 0.1])  # tie group of 4 straddles a target of 3
    got = thresholds.budget(s, ts, k_per_day=3)  # one day
    assert got["alerts"] <= 3
    assert got["alerts"] == 0  # the whole top group would exceed it: nothing can alert
    assert got["threshold"] > 0.9
    assert "tie" in got["note"]


def test_budget_when_fewer_rows_than_the_budget_alerts_on_everything() -> None:
    ts = np.array([0, 1, 2])
    s = np.array([0.3, 0.2, 0.1])
    got = thresholds.budget(s, ts, k_per_day=200)
    assert got["alerts"] == 3 and got["threshold"] == 0.1


def test_per_bank_uses_each_banks_own_scores_and_days() -> None:
    rows = []
    rng = np.random.default_rng(0)
    for bank, n, pos in (("A", 400, 40), ("B", 300, 15)):
        y = np.zeros(n, dtype=int)
        y[:pos] = 1
        score = np.where(y == 1, rng.uniform(0.5, 1.0, n), rng.uniform(0.0, 0.7, n))
        ts = np.repeat([0, DAY], n // 2)
        rows.append(pd.DataFrame({"bank": bank, "y": y, "score": score, "ts": ts}))
    frame = pd.concat(rows, ignore_index=True)
    out = thresholds.per_bank(frame, k_per_day=20)
    assert set(out) == {"A", "B"}
    for bank in ("A", "B"):
        assert out[bank]["budget"]["alerts"] <= 40  # 20 per day x 2 days
        assert 0 < out[bank]["f1_optimal"]["threshold"] < 1
    assert out["A"]["f1_optimal"]["threshold"] != out["B"]["f1_optimal"]["threshold"]


def test_yaml_document_has_what_m5_reads() -> None:
    out = {
        "A": {
            "f1_optimal": {"threshold": 0.4, "f1": 0.5},
            "budget": {"threshold": 0.7, "alerts_per_day": 199.5},
        }
    }
    doc = thresholds.to_yaml_doc(out, dataset="hi_small", model="aml-xgb", version="3", k=200)
    assert doc["banks"]["A"]["budget"]["threshold"] == 0.7
    assert doc["meta"]["k_alerts_per_day"] == 200
    assert doc["meta"]["tuned_on"] == "val"
    assert "test" not in str(doc["meta"]["tuned_on"]).replace("tuned", "")
