"""Model training (M02 step 7): 2 imbalance variants x 5 seeds of XGBoost, MLflow, champion.

    uv run python -m ml.train --ds hi_small      (or: make train DS=hi_small)

Protocol (D38, fixed before any test row is read):
- training rows: split = train and event_ts >= data_start + 24 h (D04); validation rows:
  split = val; the test split is never read here (`ml.data` refuses it);
- `tree_method=hist`, early stopping on validation `aucpr` (50 rounds, at most 1000);
- variants: `weighted` (scale_pos_weight = negatives / positives of the training rows) and
  `unweighted`; seeds 0 to 4 (they change the row and column subsampling);
- the variant with the higher MEAN validation PR-AUC wins; the champion is the MEDIAN seed of that
  variant by validation PR-AUC (not the best, never by test or by F1);
- thresholds are then tuned once, on the champion's validation scores (`ml.thresholds`), and the
  drift baseline is built from the training window and those validation scores.

Writes results/M02/<ds>/{train,thresholds,baseline_dist}.json, config/thresholds.yaml and the two
figures, logs every fit to MLflow, registers the champion as `aml-xgb` with the alias `champion`
and attaches the baseline to that model version.
"""

import argparse
import gc
import json
import os
import statistics
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xgboost as xgb
import yaml
from sklearn.metrics import average_precision_score, roc_auc_score

from ml import data, drift_baseline, metrics, thresholds

VARIANTS = ("weighted", "unweighted")
SEEDS = (0, 1, 2, 3, 4)
MAX_ROUNDS = 1000
EARLY_STOPPING_ROUNDS = 50
K_ALERTS_PER_DAY = 200
MODEL_NAME = "aml-xgb"
BASE_PARAMS: dict[str, Any] = {
    "objective": "binary:logistic",
    "eval_metric": "aucpr",
    "tree_method": "hist",
    "max_depth": 6,
    "eta": 0.1,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 1,
}


@dataclass(frozen=True)
class SeedResult:
    variant: str
    seed: int
    best_iteration: int
    val_pr_auc: float
    val_roc_auc: float
    fit_seconds: float


@dataclass
class Fit:
    result: SeedResult
    booster: xgb.Booster
    val_scores: np.ndarray


def mean_std(values: Sequence[float]) -> tuple[float, float]:
    """Mean and sample standard deviation (ddof = 1; 0.0 for a single value)."""
    mean = float(statistics.fmean(values))
    return mean, float(statistics.stdev(values)) if len(values) > 1 else 0.0


def select_champion(results: dict[str, list[SeedResult]]) -> tuple[str, SeedResult]:
    """Variant with the higher mean val PR-AUC (ties: first in VARIANTS); its median seed."""
    best = max(
        (v for v in VARIANTS if v in results),
        key=lambda v: (statistics.fmean(r.val_pr_auc for r in results[v]), -VARIANTS.index(v)),
    )
    ranked = sorted(results[best], key=lambda r: (r.val_pr_auc, r.seed))
    return best, ranked[len(ranked) // 2]


def params_for(variant: str, seed: int, scale_pos_weight: float) -> dict[str, Any]:
    params = {**BASE_PARAMS, "seed": seed}
    if variant == "weighted":
        params["scale_pos_weight"] = scale_pos_weight
    elif variant != "unweighted":
        raise ValueError(f"unknown variant {variant!r}")
    return params


def make_matrices(
    x_train: np.ndarray, y_train: np.ndarray, x_val: np.ndarray, y_val: np.ndarray, names: list[str]
) -> tuple[xgb.QuantileDMatrix, xgb.QuantileDMatrix]:
    """Quantized matrices (memory: the training set is binned once and reused by every fit)."""
    dtrain = xgb.QuantileDMatrix(x_train, y_train, feature_names=names)
    dval = xgb.QuantileDMatrix(x_val, y_val, feature_names=names, ref=dtrain)
    return dtrain, dval


def fit_one(
    dtrain: xgb.QuantileDMatrix,
    dval: xgb.QuantileDMatrix,
    y_val: np.ndarray,
    variant: str,
    seed: int,
    scale_pos_weight: float,
    max_rounds: int = MAX_ROUNDS,
) -> Fit:
    started = time.perf_counter()
    booster = xgb.train(
        params_for(variant, seed, scale_pos_weight),
        dtrain,
        num_boost_round=max_rounds,
        evals=[(dval, "val")],
        early_stopping_rounds=EARLY_STOPPING_ROUNDS,
        verbose_eval=False,
    )
    best = int(booster.best_iteration)
    scores = booster.predict(dval, iteration_range=(0, best + 1))
    result = SeedResult(
        variant=variant,
        seed=seed,
        best_iteration=best,
        val_pr_auc=float(average_precision_score(y_val, scores)),
        val_roc_auc=float(roc_auc_score(y_val, scores)),
        fit_seconds=round(time.perf_counter() - started, 1),
    )
    return Fit(result, booster, scores)


# --- figures --------------------------------------------------------------------------------


def importance_figure(booster: xgb.Booster, caption: str, path: Path, top: int = 20) -> None:
    import textwrap

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    gain = booster.get_score(importance_type="gain")
    items = sorted(gain.items(), key=lambda kv: kv[1])[-top:]
    fig, ax = plt.subplots(figsize=(8, 7))
    ax.barh([k for k, _ in items], [v for _, v in items])
    ax.set_xlabel("mean gain per split")
    ax.set_title(f"Feature importance (gain), top {len(items)}")
    fig.text(0.01, 0.005, textwrap.fill(caption, 105), fontsize=7, va="bottom")
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=110)
    plt.close(fig)


def pr_figure(y: np.ndarray, score: np.ndarray, path: Path, title: str) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from sklearn.metrics import precision_recall_curve

    precision, recall, _ = precision_recall_curve(y, score)
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot(recall, precision)
    ax.set_xlabel("recall")
    ax.set_ylabel("precision")
    ax.set_title(title)
    ax.set_ylim(0, 1.02)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=110)
    plt.close(fig)


# --- the run --------------------------------------------------------------------------------


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).date().isoformat()


def _table_snapshot(ds: str, table: str) -> int | None:
    from pyiceberg.catalog.sql import SqlCatalog

    from batch.spark import common

    catalog = SqlCatalog("lake", uri=common.pyiceberg_uri(), warehouse=f"file://{common.WAREHOUSE}")
    snap = catalog.load_table(f"aml_{ds}.{table}").current_snapshot()
    return snap.snapshot_id if snap else None


def run(ds: str, results_dir: Path | None = None, config_dir: Path | None = None) -> dict[str, Any]:
    import mlflow
    from mlflow import MlflowClient

    from batch.spark import common
    from eval.model_eval import importance_caption
    from features.spec import load_spec

    spec = load_spec()
    results_dir = (results_dir or common.ROOT / "results" / "M02") / ds
    names = data.feature_names()
    started = time.perf_counter()

    # Memory (HI-Medium's training set does not fit twice in 24 GB): the float32 arrays are read in
    # chunks, everything that needs the training array is done while it exists, then it is freed
    # before the validation array is read.
    tr = data.read_matrix(ds, "train", burn_in=True)
    train = tr.meta
    y_train = train["y"].to_numpy()
    n_pos, n_neg = int(y_train.sum()), int((y_train == 0).sum())
    spw = n_neg / n_pos
    is_cross = tr.column("is_cross_currency") == 1
    cross_rows, cross_laundering = int(is_cross.sum()), int(y_train[is_cross].sum())
    feature_part = drift_baseline.feature_distributions(tr.x, names)  # training-window deciles
    dtrain = xgb.QuantileDMatrix(tr.x, y_train, feature_names=names)
    del tr, is_cross
    gc.collect()
    va = data.read_matrix(ds, "val")
    val = va.meta
    y_val = val["y"].to_numpy()
    dval = xgb.QuantileDMatrix(va.x, y_val, feature_names=names, ref=dtrain)
    del va
    gc.collect()

    fits: dict[str, list[Fit]] = {v: [] for v in VARIANTS}
    for variant in VARIANTS:
        for seed in SEEDS:
            fit = fit_one(dtrain, dval, y_val, variant, seed, spw)
            fits[variant].append(fit)
            r = fit.result
            print(
                f"{variant:10s} seed {seed}: val PR-AUC {r.val_pr_auc:.4f} "
                f"ROC-AUC {r.val_roc_auc:.4f}  rounds {r.best_iteration + 1}  {r.fit_seconds}s"
            )
    variant, champion_result = select_champion(
        {v: [f.result for f in fs] for v, fs in fits.items()}
    )
    champion = next(f for f in fits[variant] if f.result.seed == champion_result.seed)

    # thresholds: once, on the champion's validation scores, per bank (D38)
    val_frame = pd.DataFrame(
        {"bank": val["bank"], "y": y_val, "score": champion.val_scores, "ts": val["event_ts_ms"]}
    )
    per_bank = thresholds.per_bank(val_frame, K_ALERTS_PER_DAY)

    # drift baseline: training window deciles + validation score deciles (M02 v2)
    alerts_per_day = {}
    for bank, g in val_frame.groupby("bank", sort=True):
        thr = per_bank[str(bank)]["budget"]["threshold"]
        alerts_per_day[f"bank_{str(bank).lower()}"] = metrics.alerts_per_day(
            g["ts"].to_numpy(), g["score"].to_numpy() >= thr
        )
    baseline = drift_baseline.assemble(
        feature_part,
        val_scores=champion.val_scores,
        alerts_per_day=alerts_per_day,
        meta={"ds": ds, "spec_version": spec.spec_version},
        window={
            "from": _iso(int(train["event_ts_ms"].min())),
            "to": _iso(int(train["event_ts_ms"].max())),
        },
    )

    # figures: the importance caption states the cross-currency artifact with the training counts
    caption = importance_caption(cross_rows, cross_laundering)
    importance_figure(champion.booster, caption, results_dir / "importance_gain.png")
    pr_figure(
        y_val,
        champion.val_scores,
        results_dir / "pr_curve_val.png",
        f"{ds} champion, validation PR curve (PR-AUC {champion_result.val_pr_auc:.3f})",
    )

    # MLflow
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001"))
    mlflow.set_experiment(f"aml-{ds}")
    client = MlflowClient()
    snapshot_features = _table_snapshot(ds, "features_tx")
    common_params = {
        **{k: v for k, v in BASE_PARAMS.items()},
        "max_rounds": MAX_ROUNDS,
        "early_stopping_rounds": EARLY_STOPPING_ROUNDS,
        "spec_version": spec.spec_version,
        "fx_version": spec.fx_version_required,
        "dataset": ds,
        "features_tx_snapshot_id": snapshot_features,
        "train_rows": len(train),
        "train_positives": n_pos,
        "val_rows": len(val),
        "val_positives": int(y_val.sum()),
    }
    champion_run_id = None
    for v, fs in fits.items():
        for fit in fs:
            r = fit.result
            is_champion = r is champion_result
            with mlflow.start_run(run_name=f"{v}-seed{r.seed}") as run_:
                mlflow.log_params(
                    {
                        **common_params,
                        "variant": v,
                        "seed": r.seed,
                        "scale_pos_weight": spw if v == "weighted" else 1.0,
                    }
                )
                mlflow.log_metrics(
                    {
                        "val_pr_auc": r.val_pr_auc,
                        "val_roc_auc": r.val_roc_auc,
                        "best_iteration": r.best_iteration,
                    }
                )
                mlflow.set_tag("champion", str(is_champion).lower())
                if is_champion:
                    champion_run_id = run_.info.run_id
                    mlflow.log_artifact(str(results_dir / "importance_gain.png"))
                    mlflow.log_artifact(str(results_dir / "pr_curve_val.png"))
                    base_path = results_dir / "baseline_dist.json"
                    base_doc = {**baseline, "model_version": "pending"}
                    base_path.write_text(json.dumps(base_doc, indent=2, sort_keys=True) + "\n")
                    mlflow.log_artifact(str(base_path), artifact_path="drift")
                    import mlflow.xgboost

                    info = mlflow.xgboost.log_model(
                        fit.booster, name="model", registered_model_name=MODEL_NAME
                    )
                    version = str(info.registered_model_version)
    client.set_registered_model_alias(MODEL_NAME, "champion", version)
    client.set_model_version_tag(
        MODEL_NAME, version, "baseline_dist", f"runs:/{champion_run_id}/drift/baseline_dist.json"
    )

    # final files: the baseline now names the model version it belongs to
    baseline["model_version"] = version
    common.write_json(results_dir / "baseline_dist.json", baseline)
    thr_doc = thresholds.to_yaml_doc(per_bank, ds, MODEL_NAME, version, K_ALERTS_PER_DAY)
    cfg = (config_dir or common.ROOT / "config") / "thresholds.yaml"
    cfg.parent.mkdir(parents=True, exist_ok=True)
    cfg.write_text(yaml.safe_dump(thr_doc, sort_keys=True))
    common.write_json(
        results_dir / "thresholds.json", {"dataset": ds, **common.provenance(), **thr_doc}
    )

    summary = {
        v: {
            "val_pr_auc_mean_std": mean_std([f.result.val_pr_auc for f in fs]),
            "val_roc_auc_mean_std": mean_std([f.result.val_roc_auc for f in fs]),
            "seeds": [asdict(f.result) for f in fs],
        }
        for v, fs in fits.items()
    }
    doc = {
        "dataset": ds,
        **common.provenance(),
        "config": {
            **common_params,
            "seeds": list(SEEDS),
            "variants": list(VARIANTS),
            "k_alerts_per_day": K_ALERTS_PER_DAY,
            "burn_in_end_ms": data.burn_in_end_ms(ds),
        },
        "data": {
            "train_rows": len(train),
            "train_positives": n_pos,
            "val_rows": len(val),
            "val_positives": int(y_val.sum()),
            "scale_pos_weight": spw,
            "features_tx_snapshot_id": snapshot_features,
            "cross_currency_rows_train": cross_rows,
            "cross_currency_laundering_train": cross_laundering,
        },
        "variants": summary,
        "selection": {
            "variant": variant,
            "rule": "higher mean val PR-AUC; champion = median seed by val PR-AUC",
            "champion": asdict(champion_result),
            "model_version": version,
            "mlflow_run_id": champion_run_id,
            "test_rows_read": 0,
        },
        "importance_caption": caption,
        "seconds_total": round(time.perf_counter() - started, 1),
    }
    common.write_json(results_dir / "train.json", doc)
    return doc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", required=True, choices=["hi_small", "hi_medium"])
    parser.add_argument("--results-dir", type=Path, default=None, help="default: results/M02")
    parser.add_argument("--config-dir", type=Path, default=None, help="default: config/")
    args = parser.parse_args(argv)
    doc = run(args.ds, args.results_dir, args.config_dir)
    sel = doc["selection"]
    print(
        f"\n{args.ds}: variant {sel['variant']}, champion seed {sel['champion']['seed']} "
        f"(val PR-AUC {sel['champion']['val_pr_auc']:.4f}), model version {sel['model_version']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
