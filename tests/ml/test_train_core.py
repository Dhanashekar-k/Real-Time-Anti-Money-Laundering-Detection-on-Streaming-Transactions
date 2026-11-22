"""ml/train.py: champion selection and one real (small) XGBoost fit.

D38 (decided by the user): the champion is the MEDIAN seed by validation PR-AUC of the winning
variant, never chosen by test and never by F1. The variant is the one with the higher mean
validation PR-AUC (Claude's proposal in D38, ties to the first in VARIANTS).
"""

import numpy as np
import pytest

from ml import train

pytestmark = pytest.mark.unit


def _r(variant: str, seed: int, pr: float) -> train.SeedResult:
    return train.SeedResult(
        variant, seed, best_iteration=10, val_pr_auc=pr, val_roc_auc=0.9, fit_seconds=1.0
    )


def test_champion_is_the_median_seed_by_val_pr_auc() -> None:
    results = {
        "weighted": [_r("weighted", s, p) for s, p in enumerate([0.10, 0.50, 0.30, 0.20, 0.40])],
        "unweighted": [_r("unweighted", s, p) for s, p in enumerate([0.01] * 5)],
    }
    variant, champion = train.select_champion(results)
    assert variant == "weighted"
    # sorted by PR-AUC: 0.10, 0.20, 0.30, 0.40, 0.50 -> the median is 0.30, seed 2
    assert (champion.seed, champion.val_pr_auc) == (2, 0.30)


def test_the_champion_is_not_the_best_seed() -> None:
    results = {"weighted": [_r("weighted", s, p) for s, p in enumerate([0.9, 0.1, 0.5, 0.2, 0.3])]}
    _, champion = train.select_champion(results)
    assert champion.val_pr_auc == 0.3  # not 0.9


def test_variant_with_the_higher_mean_wins_even_with_a_lower_best_seed() -> None:
    results = {
        "weighted": [_r("weighted", s, p) for s, p in enumerate([0.9, 0.0, 0.0, 0.0, 0.0])],  # 0.18
        "unweighted": [_r("unweighted", s, 0.3) for s in range(5)],  # mean 0.30
    }
    assert train.select_champion(results)[0] == "unweighted"


def test_ties_between_variants_go_to_the_first() -> None:
    results = {v: [_r(v, s, 0.2) for s in range(5)] for v in train.VARIANTS}
    assert train.select_champion(results)[0] == train.VARIANTS[0]


def test_ties_between_seeds_are_broken_by_seed_number() -> None:
    results = {"weighted": [_r("weighted", s, 0.2) for s in range(5)]}
    assert train.select_champion(results)[1].seed == 2


def test_even_number_of_seeds_takes_the_lower_median() -> None:
    results = {"weighted": [_r("weighted", s, p) for s, p in enumerate([0.1, 0.2, 0.3, 0.4])]}
    assert train.select_champion(results)[1].val_pr_auc == 0.3  # index len // 2 of the sorted list


def test_mean_std_uses_the_sample_standard_deviation() -> None:
    mean, std = train.mean_std([1.0, 2.0, 3.0, 4.0, 5.0])
    assert mean == 3.0
    assert std == pytest.approx(np.sqrt(2.5))  # ddof = 1
    assert train.mean_std([2.0]) == (2.0, 0.0)


def _toy(n: int, seed: int):
    rng = np.random.default_rng(seed)
    x = rng.normal(size=(n, 5)).astype("float32")
    x[rng.random(x.shape) < 0.1] = np.nan  # nulls are NaN
    y = (x[:, 0] + 0.5 * np.nan_to_num(x[:, 1]) + rng.normal(0, 0.3, n) > 1.2).astype(int)
    return x, y


def test_a_real_fit_early_stops_ranks_well_and_is_reproducible() -> None:
    names = [f"f{i}" for i in range(5)]
    xt, yt = _toy(4000, 0)
    xv, yv = _toy(2000, 1)
    dtrain, dval = train.make_matrices(xt, yt, xv, yv, names)
    spw = float((yt == 0).sum() / (yt == 1).sum())
    a = train.fit_one(dtrain, dval, yv, "weighted", seed=3, scale_pos_weight=spw, max_rounds=200)
    b = train.fit_one(dtrain, dval, yv, "weighted", seed=3, scale_pos_weight=spw, max_rounds=200)
    res, scores = a.result, a.val_scores
    assert res.val_pr_auc > 0.8 and res.val_roc_auc > 0.9
    assert 0 < res.best_iteration < 200  # early stopping chose a round
    assert scores.shape == (2000,) and ((0 <= scores) & (scores <= 1)).all()
    assert (a.val_scores == b.val_scores).all()  # same seed, same model
    assert set(a.booster.get_score(importance_type="gain")) <= set(names)


def test_unweighted_variant_sets_no_class_weight() -> None:
    assert "scale_pos_weight" not in train.params_for("unweighted", seed=0, scale_pos_weight=99.0)
    assert train.params_for("weighted", seed=0, scale_pos_weight=99.0)["scale_pos_weight"] == 99.0
    p = train.params_for("weighted", seed=7, scale_pos_weight=1.0)
    assert (p["seed"], p["tree_method"], p["eval_metric"]) == (7, "hist", "aucpr")
