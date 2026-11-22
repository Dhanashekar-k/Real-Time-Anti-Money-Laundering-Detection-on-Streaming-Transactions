"""Shared evaluation arithmetic: confusion counts, F1, alerts per day, sparse-tail days, bootstrap.

Used by the rules baseline, thresholds and the final evaluation, so the two systems being compared
are always scored by the same code.

The bootstrap is paired. For two predictors A and B scored on the same rows, every resample draws
one set of rows and scores both on it, so the interval on F1(A) - F1(B) reflects that the two are
evaluated on the same data. Resampling n rows with replacement gives a multinomial over the cells
(label, A alert, B alert); F1 depends only on those 8 counts, so the resample is drawn as one
multinomial instead of an index array of millions of rows. The two are the same distribution
(checked in tests/ml/test_metrics.py).
"""

from datetime import date
from typing import Any

import numpy as np

DAY_MS = 86_400_000
_EPOCH = date(1970, 1, 1)


def confusion(y: np.ndarray, alert: np.ndarray) -> tuple[int, int, int, int]:
    """(tp, fp, fn, tn) for 0/1 labels and 0/1 alerts."""
    y = np.asarray(y).astype(bool)
    alert = np.asarray(alert).astype(bool)
    tp = int(np.sum(y & alert))
    fp = int(np.sum(~y & alert))
    fn = int(np.sum(y & ~alert))
    tn = int(np.sum(~y & ~alert))
    return tp, fp, fn, tn


def prf1(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    """(precision, recall, F1). A zero denominator gives 0.0 (read it with the alert count)."""
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * tp / (2 * tp + fp + fn) if tp else 0.0
    return precision, recall, f1


def _day_number(iso_day: str) -> int:
    return (date.fromisoformat(iso_day) - _EPOCH).days


def tail_day_numbers(eda: dict[str, Any]) -> set[int]:
    """UTC day numbers of the sparse leading and trailing days that `eda.json` flags (D36).

    Read from the file, never typed in: the rule that finds them lives in batch/spark/eda.py.
    """
    sparse = eda["sparse_days"]
    return {_day_number(d) for d in [*sparse["leading"], *sparse["trailing"]]}


def without_days(event_ts_ms: np.ndarray, days: set[int]) -> np.ndarray:
    """Boolean mask: True for rows whose UTC day is NOT in `days`."""
    day = np.asarray(event_ts_ms) // DAY_MS
    return ~np.isin(day, list(days))


def alerts_per_day(event_ts_ms: np.ndarray, alert: np.ndarray) -> float:
    """Alerts divided by the number of distinct UTC days that have at least one row."""
    ts = np.asarray(event_ts_ms)
    if ts.size == 0:
        return 0.0
    days = np.unique(ts // DAY_MS).size
    return float(np.sum(np.asarray(alert).astype(bool))) / days


def _f1_from_cells(cells: np.ndarray, which: str) -> np.ndarray:
    """F1 of predictor `a` or `b` from cell counts of shape (..., 8); cell = y*4 + a*2 + b."""
    c = cells.reshape(*cells.shape[:-1], 2, 2, 2)  # [y, a, b]
    if which == "a":
        tp, fp, fn = c[..., 1, 1, :].sum(-1), c[..., 0, 1, :].sum(-1), c[..., 1, 0, :].sum(-1)
    else:
        tp, fp, fn = c[..., 1, :, 1].sum(-1), c[..., 0, :, 1].sum(-1), c[..., 1, :, 0].sum(-1)
    denom = 2 * tp + fp + fn
    return np.where(denom > 0, 2 * tp / np.maximum(denom, 1), 0.0)


def paired_bootstrap_f1(
    y: np.ndarray,
    alert_a: np.ndarray,
    alert_b: np.ndarray,
    n_resamples: int = 1000,
    seed: int = 0,
    level: float = 0.95,
) -> dict[str, Any]:
    """Point estimates and percentile intervals for F1(A), F1(B) and F1(A) - F1(B), paired.

    Every resample is one draw of n rows with replacement, scored for both predictors.
    """
    y, a, b = (np.asarray(v) for v in (y, alert_a, alert_b))
    if not (len(y) == len(a) == len(b)):
        raise ValueError("y, alert_a and alert_b must have the same length")
    if len(y) == 0:
        raise ValueError("cannot bootstrap an empty set")
    for name, v in (("y", y), ("alert_a", a), ("alert_b", b)):
        if not np.isin(v, (0, 1)).all():
            raise ValueError(f"{name} must hold only 0/1")
    n = len(y)
    cell = y.astype(int) * 4 + a.astype(int) * 2 + b.astype(int)
    cells = np.bincount(cell, minlength=8)

    rng = np.random.default_rng(seed)
    draws = rng.multinomial(n, cells / n, size=n_resamples)
    f1_a, f1_b = _f1_from_cells(draws, "a"), _f1_from_cells(draws, "b")
    diff = f1_a - f1_b

    point_a = float(_f1_from_cells(cells, "a"))
    point_b = float(_f1_from_cells(cells, "b"))
    q = [(1 - level) / 2 * 100, (1 + level) / 2 * 100]

    def block(point: float, samples: np.ndarray) -> dict[str, Any]:
        lo, hi = np.percentile(samples, q)
        return {"point": point, "ci": (float(lo), float(hi))}

    return {
        "f1_a": block(point_a, f1_a),
        "f1_b": block(point_b, f1_b),
        "diff": block(point_a - point_b, diff),
        "n_rows": n,
        "n_resamples": n_resamples,
        "seed": seed,
        "level": level,
        "method": "paired multinomial bootstrap over (label, alert_a, alert_b) cells",
    }
