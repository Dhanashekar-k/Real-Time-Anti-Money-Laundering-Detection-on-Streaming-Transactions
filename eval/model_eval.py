"""Final evaluation (M02 step 8): the only code allowed to read the test split.

    uv run python -m eval.model_eval --ds hi_small --final

`--final` is required. Without it the process exits non-zero before anything is read. With it,
`open_test_gate` prints a warning and appends a line to `results/M02/<ds>/test_access_log.txt`
(time, git SHA, dataset, reason) right before the test rows are read, so every access leaves a
trace.

What is computed (all from the rows given, nothing typed):
- per slice, `all_test` and `without_tail` (the sparse trailing days of eda.json, D36): F1,
  precision, recall, alerts per day, PR-AUC, ROC-AUC for the model, F1/precision/recall for the
  baseline, precision@K per (bank, day), recall per pattern type, and a PAIRED bootstrap of
  F1(model), F1(baseline) and F1(model) - F1(baseline) over the same resampled rows;
- the T2 delta (F1 points above the baseline) from that bootstrap;
- a `headline` that states, with counts taken from the rows, the two artifacts of this dataset
  that a reader must not miss: every laundering row is same-currency, and the sparse tail.

The functions are pure (a DataFrame in, a dict out); `run` loads the validation and test rows and
the registered champion, and refuses to read the test split without `--final` on a clean tree.
"""

import argparse
import os
import subprocess
import sys
import warnings
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

from ml import metrics

ROOT = Path(__file__).resolve().parents[1]
RESULTS_ROOT = ROOT / "results" / "M02"
DAY_MS = metrics.DAY_MS
T2_POINTS = 0.20  # the manual's T2 trigger: F1 above the rules by 20 points


class FinalRequired(RuntimeError):
    """The test split was requested without --final."""


class DirtyTree(RuntimeError):
    """The test split was requested while the code differs from the commit it is recorded as."""


def _is_dirty() -> bool:
    """Code differs from HEAD (script outputs such as results/ do not count)."""
    from batch.spark import common

    return common.git_dirty(ROOT)


def _git_state() -> str:
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True
        ).stdout.strip()
        return (sha or "unknown") + ("+dirty" if _is_dirty() else "")
    except OSError:
        return "unknown"


def open_test_gate(final: bool, ds: str, results_dir: Path, reason: str) -> None:
    """Call immediately before reading test rows. Raises unless `final`; logs every access."""
    if not final:
        raise FinalRequired("the test split is read only with --final (see M02 §8)")
    if _is_dirty():
        raise DirtyTree(
            "the working tree has uncommitted code changes: commit first, so the result can be "
            "reproduced from its git SHA (there is no override)"
        )
    warnings.warn(
        f"reading the test split of {ds}: it is touched once per model family",
        UserWarning,
        stacklevel=2,
    )
    results_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).isoformat(timespec="seconds")
    line = f"{stamp} git={_git_state()} ds={ds} reason={reason}\n"
    with open(results_dir / "test_access_log.txt", "a") as fh:
        fh.write(line)


# --- pieces --------------------------------------------------------------------------------


def model_alert(frame: pd.DataFrame, bank_thresholds: Mapping[str, float]) -> np.ndarray:
    """score >= the threshold of the row's own bank. A bank without a threshold is an error."""
    banks = set(frame["bank"].unique())
    missing = sorted(banks - set(bank_thresholds))
    if missing:
        raise KeyError(f"no threshold for bank(s) {missing}")
    thr = frame["bank"].map(bank_thresholds).to_numpy(dtype=float)
    return frame["score"].to_numpy(dtype=float) >= thr


def _system(y: np.ndarray, alert: np.ndarray, ts: np.ndarray, score: np.ndarray | None) -> dict:
    tp, fp, fn, _ = metrics.confusion(y, alert)
    p, r, f1 = metrics.prf1(tp, fp, fn)
    out: dict[str, Any] = {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": p,
        "recall": r,
        "f1": f1,
        "alerts": int(alert.sum()),
        "alerts_per_day": metrics.alerts_per_day(ts, alert),
    }
    if score is not None:
        both = len(np.unique(y)) == 2
        out["pr_auc"] = float(average_precision_score(y, score)) if both else None
        out["roc_auc"] = float(roc_auc_score(y, score)) if both else None
    return out


def precision_at_k(frame: pd.DataFrame, k: int) -> dict[str, Any]:
    """Top-K scores per (bank, UTC day), pooled: hits / alerts."""
    day = frame["ts"].to_numpy() // DAY_MS
    work = frame.assign(_day=day).sort_values("score", ascending=False, kind="stable")
    top = work.groupby(["bank", "_day"], sort=False).head(k)
    alerts, hits = len(top), int(top["y"].sum())
    return {
        "k": k,
        "cells": int(work.groupby(["bank", "_day"]).ngroups),
        "alerts": alerts,
        "hits": hits,
        "precision": hits / alerts if alerts else 0.0,
    }


def per_pattern_recall(frame: pd.DataFrame, alert: np.ndarray) -> dict[str, Any]:
    """Recall by pattern type over the laundering rows, plus the rows without a pattern label.

    Not every laundering row is in a pattern file (coverage is returned), so a pattern's recall is
    over the labelled rows only.
    """
    pos = frame["y"].to_numpy() == 1
    labelled = frame["pattern_type"].notna().to_numpy()
    out: dict[str, Any] = {"patterns": {}}
    for name in sorted(frame.loc[pos & labelled, "pattern_type"].unique()):
        sel = pos & (frame["pattern_type"] == name).to_numpy()
        n, caught = int(sel.sum()), int((sel & alert).sum())
        out["patterns"][str(name)] = {"positives": n, "caught": caught, "recall": caught / n}
    sel = pos & ~labelled
    n, caught = int(sel.sum()), int((sel & alert).sum())
    out["without_pattern_label"] = {
        "positives": n,
        "caught": caught,
        "recall": caught / n if n else None,
    }
    out["pattern_coverage"] = float((pos & labelled).sum() / pos.sum()) if pos.any() else None
    return out


def importance_caption(cross_currency_rows: int, cross_currency_laundering: int) -> str:
    """Caption for the feature-importance plot (the artifact must travel with the figure)."""
    counts = f"{cross_currency_laundering:,} of {cross_currency_rows:,} cross-currency rows"
    if cross_currency_laundering == 0:
        return (
            f"Gain importance. {counts} are laundering, so any feature built on the cross-currency "
            "flag (is_cross_currency and the s_/r_ cross_currency_share_* features) can separate "
            "the classes in this data for a reason that is a generator artifact (D36). It is not "
            "evidence about real laundering."
        )
    return f"Gain importance. {counts} are laundering."


# --- the evaluation -----------------------------------------------------------------------------


def evaluate(
    test: pd.DataFrame,
    bank_thresholds: Mapping[str, float],
    baseline_alert: np.ndarray,
    baseline_name: str,
    tail_days: set[int],
    k_per_day: int,
    n_resamples: int = 1000,
    seed: int = 0,
) -> dict[str, Any]:
    """`test` columns: ts (event_ts_ms), bank, y, score, is_cross_currency, pattern_type."""
    m_alert = model_alert(test, bank_thresholds)
    b_alert = np.asarray(baseline_alert, dtype=bool)
    keep_masks = {
        "all_test": np.ones(len(test), dtype=bool),
        "without_tail": metrics.without_days(test["ts"].to_numpy(), tail_days),
    }
    slices: dict[str, Any] = {}
    for name, keep in keep_masks.items():
        f = test[keep]
        y, ts = f["y"].to_numpy(), f["ts"].to_numpy()
        ma, ba = m_alert[keep], b_alert[keep]
        slices[name] = {
            "n_rows": int(len(f)),
            "n_positive": int(y.sum()),
            "model": _system(y, ma, ts, f["score"].to_numpy(dtype=float)),
            "baseline": _system(y, ba, ts, None),
            "bootstrap": metrics.paired_bootstrap_f1(y, ma, ba, n_resamples, seed),
            "precision_at_k_per_bank_day": precision_at_k(f, k_per_day),
            "per_pattern_recall": {
                "model": per_pattern_recall(f, ma),
                "baseline": per_pattern_recall(f, ba),
            },
        }

    t2: dict[str, Any] = {
        "baseline": baseline_name,
        "definition": f"F1(model) - F1(baseline) >= {T2_POINTS} (20 points), paired bootstrap",
    }
    for name, s in slices.items():
        diff = s["bootstrap"]["diff"]
        t2[name] = {
            "delta_f1": diff["point"],
            "ci": list(diff["ci"]),
            "meets_20_points": bool(diff["point"] >= T2_POINTS),
        }

    return {
        "slices": slices,
        "t2": t2,
        "headline": _headline(test, slices, t2, tail_days, baseline_name),
        "bootstrap": {"n_resamples": n_resamples, "seed": seed},
        "k_per_day": k_per_day,
    }


def _headline(
    test: pd.DataFrame, slices: dict, t2: dict, tail_days: set[int], baseline_name: str
) -> list[str]:
    lines = []
    for name, label in (("all_test", "all test rows"), ("without_tail", "test without the tail")):
        t = t2[name]
        lines.append(
            f"T2 vs {baseline_name}, {label}: F1(model) - F1(baseline) = {t['delta_f1']:+.4f}, "
            f"paired 95% CI [{t['ci'][0]:+.4f}, {t['ci'][1]:+.4f}]; "
            f"{'meets' if t['meets_20_points'] else 'does not meet'} the 20-point trigger."
        )
    cross = test["is_cross_currency"].to_numpy() == 1
    n_cross, pos_cross = int(cross.sum()), int(test.loc[cross, "y"].sum())
    if pos_cross == 0:
        lines.append(
            f"Generator artifact: 0 of {n_cross:,} cross-currency rows in this test split are "
            "laundering. Every laundering row is same-currency (D36), so any feature built on the "
            "cross-currency flag (is_cross_currency and the s_/r_ cross_currency_share_* "
            "features) can separate the classes here for a reason that is a property of the "
            "generator. It says nothing about real laundering, and a high importance for those "
            "features must not be read as a finding."
        )
    else:
        lines.append(
            f"{pos_cross:,} of {n_cross:,} cross-currency rows in this test split are laundering."
        )
    tail = ~metrics.without_days(test["ts"].to_numpy(), tail_days)
    n_tail, pos_tail = int(tail.sum()), int(test.loc[tail, "y"].sum())
    lines.append(
        f"Sparse tail: {n_tail:,} rows ({pos_tail:,} laundering) of {len(test):,} test rows, a "
        "generator artifact, not a temporal trend (D36). Every metric is given with and without it."
    )
    return lines


GOLDEN_DIR = ROOT / "tests" / "golden"


def thresholds_path(ds: str) -> Path:
    """Each dataset has its own tuned thresholds; a model is evaluated with its own."""
    return ROOT / "config" / f"thresholds_{ds}.yaml"


def golden_status(directory: Path = GOLDEN_DIR) -> dict[str, Any]:
    """How many golden expected values are still `TODO(human)`, counted from the files.

    Every result built on features that the golden vectors have not yet checked is provisional.
    """
    import json

    files = sorted(directory.glob("s[0-9][0-9]_*.json"))
    per_file = {
        f.name: sum(
            v == "TODO(human)"
            for feats in json.loads(f.read_text())["expected"].values()
            for v in feats.values()
        )
        for f in files
    }
    todo = sum(per_file.values())
    return {
        "scenarios": len(files),
        "scenarios_with_todo": sum(1 for n in per_file.values() if n),
        "todo_values": todo,
        "provisional": todo > 0,
    }


def provisional_headline(status: dict[str, Any]) -> list[str]:
    if not status["provisional"]:
        return []
    return [
        f"PROVISIONAL: {status['todo_values']} golden expected values in "
        f"{status['scenarios_with_todo']} scenarios are still TODO(human), so `make golden` has "
        "not yet checked the features these results were computed from."
    ]


def load_thresholds(path: Path, kind: str, dataset: str | None = None) -> dict[str, float]:
    import yaml

    doc = yaml.safe_load(path.read_text())
    if dataset is not None and doc["meta"]["dataset"] != dataset:
        raise ValueError(
            f"{path} holds thresholds tuned for {doc['meta']['dataset']}, not for {dataset}"
        )
    banks = doc["banks"]
    return {bank: float(cfg[kind]["threshold"]) for bank, cfg in banks.items()}


def run(ds: str, final: bool, results_root: Path | None = None) -> dict[str, Any]:
    """Inputs that need no test row first; then the gate; then the one read of the test split."""
    import json

    import mlflow
    import xgboost as xgb

    from batch.spark import common
    from ml import data, rules_baseline

    out_dir = (results_root or RESULTS_ROOT) / ds
    if not final:
        raise FinalRequired("the test split is read only with --final (see M02 §8)")

    # 1. inputs that do not touch the test split
    rules = json.loads((out_dir / "rules.json").read_text())
    fits = rules_baseline.fits_from_json(rules["fits"])
    thr_file = thresholds_path(ds)
    f1_thr = load_thresholds(thr_file, "f1_optimal", dataset=ds)
    budget_thr = load_thresholds(thr_file, "budget", dataset=ds)
    eda = json.loads((common.ROOT / "results" / "M01" / ds / "eda.json").read_text())
    tail_days = metrics.tail_day_numbers(eda)
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001"))
    booster = mlflow.xgboost.load_model("models:/aml-xgb@champion")
    names = data.feature_names()
    best = int(booster.best_iteration)
    version = mlflow.MlflowClient().get_model_version_by_alias("aml-xgb", "champion").version

    # the registered model must be the champion that train.py selected: same validation PR-AUC
    from sklearn.metrics import average_precision_score

    val = data.read_matrix(ds, "val")
    val_scores = booster.predict(
        xgb.DMatrix(val.x, feature_names=names), iteration_range=(0, best + 1)
    )
    train_doc = json.loads((out_dir / "train.json").read_text())
    expected_pr = train_doc["selection"]["champion"]["val_pr_auc"]
    got_pr = float(average_precision_score(val.meta["y"].to_numpy(), val_scores))
    if abs(got_pr - expected_pr) > 1e-9:
        raise RuntimeError(f"registered champion val PR-AUC {got_pr} != train.json {expected_pr}")
    if str(version) != str(train_doc["selection"]["model_version"]):
        raise RuntimeError(f"champion alias points at version {version}, train.json says otherwise")

    # 2. the gate, immediately before the one read of the test split
    open_test_gate(final, ds, out_dir, reason=f"final evaluation of aml-xgb v{version}")
    test = data.read_matrix(
        ds,
        "test",
        allow_test=True,
        patterns=True,
        extra_raw_columns=("amount_paid_usd_cents",),
        exact_columns=rules_baseline.RULE_FEATURES,  # float64: the rules compare against thresholds
    )
    score = booster.predict(xgb.DMatrix(test.x, feature_names=names), iteration_range=(0, best + 1))
    frame = pd.DataFrame(
        {
            "ts": test.meta["event_ts_ms"].to_numpy(),
            "bank": test.meta["bank"].to_numpy(),
            "y": test.meta["y"].to_numpy(),
            "score": score,
            "is_cross_currency": test.column("is_cross_currency"),
            "pattern_type": test.meta["pattern_type"].to_numpy(),
        }
    )
    rule_frame = test.exact.assign(amount_paid_usd_cents=test.meta["amount_paid_usd_cents"])
    baseline_name = rules["baseline"]
    baseline = rules_baseline.baseline_alert(fits, baseline_name, rule_frame)

    kw = dict(
        baseline_alert=baseline, baseline_name=baseline_name, tail_days=tail_days,
        k_per_day=200, n_resamples=1000, seed=0,
    )  # fmt: skip
    primary = evaluate(frame, f1_thr, **kw)
    at_budget = evaluate(frame, budget_thr, **kw)
    golden = golden_status()
    headline = [*provisional_headline(golden), *primary["headline"]]
    doc = {
        "dataset": ds,
        **common.provenance(),
        "provisional": golden["provisional"],
        "golden_status": golden,
        "headline": headline,
        "config": {
            "model": f"aml-xgb v{version} (alias champion)",
            "best_iteration": best,
            "baseline": baseline_name,
            "thresholds_file": str(thr_file.relative_to(ROOT)),
            "primary_operating_point": "per-bank F1-optimal threshold chosen on val",
            "bootstrap": primary["bootstrap"],
            "tail_days": sorted(tail_days),
            "test_rows": int(len(frame)),
        },
        "t2": primary["t2"],
        "at_f1_optimal_threshold": primary["slices"],
        "at_budget_threshold": at_budget["slices"],
        "t2_at_budget_threshold": at_budget["t2"],
        "thresholds": {"f1_optimal": f1_thr, "budget": budget_thr},
    }
    common.write_json(out_dir / "metrics.json", doc)
    _pr_figure(frame, baseline, out_dir / "pr_curve_test.png", tail_days)
    return doc


def _pr_figure(frame: pd.DataFrame, baseline: np.ndarray, path: Path, tail_days: set[int]) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from sklearn.metrics import precision_recall_curve

    fig, ax = plt.subplots(figsize=(6.5, 5))
    slices = {
        "all test rows": np.ones(len(frame), dtype=bool),
        "without the sparse tail": metrics.without_days(frame["ts"].to_numpy(), tail_days),
    }
    for label, keep in slices.items():
        f = frame[keep]
        p, r, _ = precision_recall_curve(f["y"], f["score"])
        ax.plot(r, p, label=label)
        tp, fp, fn, _ = metrics.confusion(f["y"].to_numpy(), baseline[keep])
        pr, rc, _ = metrics.prf1(tp, fp, fn)
        ax.scatter([rc], [pr], marker="x", label=f"baseline rules, {label}")
    ax.set_xlabel("recall")
    ax.set_ylabel("precision")
    ax.set_ylim(0, 1.02)
    ax.set_title("Test PR curve (model) and the rules baseline")
    ax.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", required=True, choices=["hi_small", "hi_medium"])
    parser.add_argument("--final", action="store_true", help="required: reads the test split")
    args = parser.parse_args(argv)
    if not args.final:
        print(
            "refusing to run: the test split is read only with --final "
            f"(results would go to {RESULTS_ROOT / args.ds})",
            file=sys.stderr,
        )
        return 2
    doc = run(args.ds, final=True)
    print("\n".join(doc["headline"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
