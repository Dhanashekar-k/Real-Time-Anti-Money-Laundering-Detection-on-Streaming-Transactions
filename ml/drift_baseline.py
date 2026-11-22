"""The drift baseline saved with every model version (M02 v2; used by the M08 drift monitor).

For each feature: the decile edges of the TRAINING window (after the burn-in), the share of
training rows in each bin, and the share that is null (its own bin). For the score: deciles of the
VALIDATION scores (training scores are overconfident). Plus alerts per day per bank on val.

Duplicate edges are dropped (most counts are 0 for most rows), so a feature can have fewer than
10 bins. Binning rule, shared with the monitor through `bin_index`: bin 0 is x <= edges[0], bin i
is edges[i-1] < x <= edges[i], the last bin is x > edges[-1]. PSI against this baseline is the
monitor's job; this module only builds the baseline.

Pure: the caller gives frames and scores; writing the JSON and attaching it to the MLflow model
version happens in ml/train.py. The result holds no timestamp or SHA, so the same data gives the
same file; provenance goes in a sibling file written by the caller.
"""

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

FORBIDDEN_COLUMNS = ("is_laundering", "split", "pattern_type", "attempt_id")
DECILES = np.linspace(0.1, 0.9, 9)


def bin_index(edges: Sequence[float], values: np.ndarray) -> np.ndarray:
    """Bin of each value: edges[i-1] < x <= edges[i]; values are non-null."""
    return np.searchsorted(
        np.asarray(edges, dtype=float), np.asarray(values, dtype=float), side="left"
    )


def _edges(values: np.ndarray) -> list[float]:
    """Strictly increasing decile edges of non-null values (duplicates dropped)."""
    if values.size == 0:
        return []
    raw = np.quantile(values, DECILES, method="lower")
    return [float(x) for x in np.unique(raw)]


def _proportions(edges: list[float], values: np.ndarray, n_total: int) -> list[float]:
    counts = np.bincount(bin_index(edges, values), minlength=len(edges) + 1)
    return [float(c) / n_total for c in counts]


def feature_distributions(x: np.ndarray, names: Sequence[str]) -> dict[str, Any]:
    """Per-feature edges, bin shares and null share of a matrix (rows x features, NaN = null).

    Works on one column at a time, so a float32 matrix is never copied whole. Use it before the
    matrix is freed; `assemble` adds the score part once the champion's scores exist.
    """
    features: dict[str, Any] = {}
    for j, col in enumerate(names):
        values_all = np.asarray(x[:, j], dtype=float)
        null = np.isnan(values_all)
        values = values_all[~null]
        edges = _edges(values)
        features[col] = {
            "edges": edges,
            "props": _proportions(edges, values, len(values_all)),
            "null_prop": float(null.sum()) / len(values_all),
        }
    return features


def assemble(
    features: Mapping[str, Any],
    val_scores: np.ndarray,
    alerts_per_day: Mapping[str, float],
    meta: Mapping[str, Any],
    window: Mapping[str, str],
) -> dict[str, Any]:
    """The content of baseline_dist.json from the feature part and the validation scores."""
    scores = np.asarray(val_scores, dtype=float)
    score_edges = _edges(scores)
    return {
        **dict(meta),
        "window": dict(window),
        "features": dict(features),
        "score": {"edges": score_edges, "props": _proportions(score_edges, scores, len(scores))},
        "alerts_per_day": dict(alerts_per_day),
    }


def build_baseline(
    train: pd.DataFrame,
    val_scores: np.ndarray,
    alerts_per_day: Mapping[str, float],
    meta: Mapping[str, Any],
    window: Mapping[str, str],
    feature_columns: Sequence[str] | None = None,
) -> dict[str, Any]:
    """The content of baseline_dist.json. `train` holds feature columns for the training window."""
    banned = [c for c in FORBIDDEN_COLUMNS if c in train.columns]
    if banned:
        raise ValueError(f"label-like columns must not be baselined: {banned}")
    columns = list(feature_columns) if feature_columns is not None else list(train.columns)
    x = train[columns].to_numpy(dtype=float)
    return assemble(feature_distributions(x, columns), val_scores, alerts_per_day, meta, window)
