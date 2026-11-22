"""ml/drift_baseline.py: per-feature decile baselines of the training window (M02 v2).

Rules from the manual: bins are deciles of the TRAINING window; duplicate edges are dropped (many
features are 0 for most rows, so a feature can end with fewer than 10 bins); nulls get their own
share; the score baseline uses VALIDATION scores; proportions sum to 1 (+- 1e-9); edges strictly
increase; recomputing on the same data gives the same file.

Bin rule used here and exported for the monitor: bin i holds values x with
edges[i-1] < x <= edges[i]; bin 0 is x <= edges[0]; the last bin is x > edges[-1].
"""

import json

import numpy as np
import pandas as pd
import pytest

from ml import drift_baseline as db

pytestmark = pytest.mark.unit


def _train(n: int = 1000, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    mostly_zero = np.where(rng.random(n) < 0.95, 0.0, rng.exponential(5.0, n))
    return pd.DataFrame(
        {
            "smooth": rng.normal(size=n),
            "mostly_zero": mostly_zero,
            "with_nulls": np.where(rng.random(n) < 0.2, np.nan, rng.uniform(0, 10, n)),
            "constant": np.full(n, 3.0),
        }
    )


def _build(train: pd.DataFrame, **kw) -> dict:
    scores = np.random.default_rng(1).uniform(size=500)
    return db.build_baseline(
        train,
        val_scores=scores,
        alerts_per_day={"bank_a": 140.2, "bank_b": 96.5},
        meta={"ds": "hi_small", "model_version": "3", "spec_version": 1},
        window={"from": "2022-09-02", "to": "2022-09-06"},
        **kw,
    )


def test_bin_index_rule() -> None:
    edges = [1.0, 2.0, 3.0]
    x = np.array([0.5, 1.0, 1.0001, 2.0, 3.0, 3.1])
    assert db.bin_index(edges, x).tolist() == [0, 0, 1, 1, 2, 3]


def test_proportions_sum_to_one_with_the_null_share() -> None:
    base = _build(_train())
    for name, f in base["features"].items():
        assert sum(f["props"]) + f["null_prop"] == pytest.approx(1.0, abs=1e-9), name
        assert len(f["props"]) == len(f["edges"]) + 1, name
        assert all(p >= 0 for p in f["props"]), name


def test_edges_strictly_increase_and_duplicates_are_dropped() -> None:
    base = _build(_train())
    for name, f in base["features"].items():
        e = f["edges"]
        assert all(b > a for a, b in zip(e, e[1:], strict=False)), name
    assert len(base["features"]["smooth"]["edges"]) == 9  # 10 deciles -> 9 inner edges
    assert len(base["features"]["mostly_zero"]["edges"]) < 9  # 95% zeros: the deciles collapse
    assert base["features"]["constant"]["edges"] == [3.0]  # one distinct value -> one edge


def test_nulls_have_their_own_share_and_are_not_in_the_deciles() -> None:
    base = _build(_train(n=2000, seed=4))
    f = base["features"]["with_nulls"]
    expected_null = _train(n=2000, seed=4)["with_nulls"].isna().mean()
    assert f["null_prop"] == pytest.approx(expected_null, abs=1e-12)
    assert len(f["edges"]) == 9  # deciles of the non-null values


def test_each_bin_of_a_smooth_feature_holds_about_a_tenth() -> None:
    f = _build(_train(n=10_000))["features"]["smooth"]
    assert all(abs(p - 0.1) < 0.01 for p in f["props"])


def test_the_training_data_reproduces_its_own_proportions() -> None:
    train = _train()
    f = _build(train)["features"]["with_nulls"]
    x = train["with_nulls"].to_numpy()
    ok = ~np.isnan(x)
    counts = np.bincount(db.bin_index(f["edges"], x[ok]), minlength=len(f["props"]))
    assert (counts / len(x)).tolist() == pytest.approx(f["props"], abs=1e-12)


def test_score_baseline_uses_the_validation_scores_given() -> None:
    train = _train()
    low = db.build_baseline(
        train,
        val_scores=np.linspace(0.0, 0.1, 1000),
        alerts_per_day={},
        meta={},
        window={},
    )
    high = db.build_baseline(
        train,
        val_scores=np.linspace(0.9, 1.0, 1000),
        alerts_per_day={},
        meta={},
        window={},
    )
    assert max(low["score"]["edges"]) <= 0.1
    assert min(high["score"]["edges"]) >= 0.9
    assert sum(low["score"]["props"]) == pytest.approx(1.0, abs=1e-9)


def test_same_data_gives_the_same_file() -> None:
    a = json.dumps(_build(_train()), sort_keys=True)
    b = json.dumps(_build(_train()), sort_keys=True)
    assert a == b


def test_shape_matches_the_manual() -> None:
    base = _build(_train())
    assert set(base) == {
        "ds",
        "model_version",
        "spec_version",
        "window",
        "features",
        "score",
        "alerts_per_day",
    }
    assert base["alerts_per_day"] == {"bank_a": 140.2, "bank_b": 96.5}
    assert set(base["features"]["smooth"]) == {"edges", "props", "null_prop"}


def test_label_like_columns_are_refused() -> None:
    train = _train()
    train["is_laundering"] = 0
    with pytest.raises(ValueError, match="is_laundering"):
        _build(train)
