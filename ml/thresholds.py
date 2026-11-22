"""Decision thresholds per bank, tuned on validation scores only (M02 step 7).

Two thresholds per bank, both "alert when score >= threshold":
- `f1_optimal`: the cut with the best F1 on val. Equal-F1 cuts go to the higher threshold (fewer
  alerts). Tied scores are never split: a cut takes a whole group of equal scores or none of it.
- `budget`: the cut that gives at most K alerts per replay-day on average over the val days of
  that bank (the analyst capacity). M5 serves with this one.

Both are chosen once, from the champion model's validation scores (D38). These functions take a
frame of validation rows; the caller must not pass test rows.
"""

from typing import Any

import numpy as np
import pandas as pd

from ml import metrics

DAY_MS = metrics.DAY_MS


def _cuts(score: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Order rows by descending score; return (order, valid cut positions).

    A cut at position i alerts on rows 0..i. It is valid only where the next score differs, so a
    group of tied scores is never split.
    """
    order = np.argsort(-score, kind="stable")
    s = score[order]
    valid = np.flatnonzero(np.append(s[1:] != s[:-1], True))
    return order, valid


def f1_optimal(y: np.ndarray, score: np.ndarray) -> dict[str, Any]:
    y, score = np.asarray(y).astype(int), np.asarray(score, dtype=float)
    positives = int(y.sum())
    if positives == 0:
        raise ValueError("no positive rows: an F1-optimal threshold is undefined")
    order, valid = _cuts(score)
    tp = np.cumsum(y[order])[valid]
    alerts = valid + 1
    f1 = 2 * tp / (alerts + positives)
    best = int(np.argmax(f1))  # argmax returns the first: the highest threshold among equal F1s
    return {
        "threshold": float(score[order][valid[best]]),
        "f1": float(f1[best]),
        "precision": float(tp[best] / alerts[best]),
        "recall": float(tp[best] / positives),
        "alerts": int(alerts[best]),
    }


def budget(score: np.ndarray, event_ts_ms: np.ndarray, k_per_day: int) -> dict[str, Any]:
    score = np.asarray(score, dtype=float)
    n_days = int(np.unique(np.asarray(event_ts_ms) // DAY_MS).size)
    target = int(k_per_day * n_days)
    order, valid = _cuts(score)
    counts = valid + 1
    ok = np.flatnonzero(counts <= target)
    note = ""
    if ok.size == 0:  # the top group of tied scores alone is over the budget
        threshold = float(np.nextafter(score.max(), np.inf))
        alerts = 0
        note = "the top group of tied scores exceeds the budget, so nothing alerts"
    else:
        pick = ok[-1]  # the largest allowed alert count
        threshold = float(score[order][valid[pick]])
        alerts = int(counts[pick])
        if alerts < target and pick + 1 < valid.size:
            note = "the next group of tied scores would exceed the budget"
    return {
        "threshold": threshold,
        "alerts": alerts,
        "n_days": n_days,
        "target_alerts": target,
        "alerts_per_day": alerts / n_days if n_days else 0.0,
        "note": note,
    }


def per_bank(frame: pd.DataFrame, k_per_day: int = 200) -> dict[str, dict[str, Any]]:
    """frame columns: bank, y, score, ts (validation rows only)."""
    out = {}
    for bank, g in frame.groupby("bank", sort=True):
        y, s, ts = g["y"].to_numpy(), g["score"].to_numpy(), g["ts"].to_numpy()
        out[str(bank)] = {
            "f1_optimal": {
                **f1_optimal(y, s),
                "val_alerts_per_day": float(
                    metrics.alerts_per_day(ts, s >= f1_optimal(y, s)["threshold"])
                ),
            },
            "budget": budget(s, ts, k_per_day),
        }
    return out


def to_yaml_doc(
    result: dict[str, dict[str, Any]], dataset: str, model: str, version: str, k: int
) -> dict[str, Any]:
    """The content of config/thresholds.yaml."""
    return {
        "meta": {
            "dataset": dataset,
            "model": model,
            "model_version": str(version),
            "tuned_on": "val",
            "k_alerts_per_day": k,
            "rule": "alert when score >= threshold",
        },
        "banks": result,
    }
