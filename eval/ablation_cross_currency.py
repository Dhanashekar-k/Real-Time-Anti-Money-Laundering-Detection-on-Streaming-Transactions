"""Validation-only ablation: how much does the cross-currency family contribute? (exploratory)

    uv run python -m eval.ablation_cross_currency --ds hi_small

Every laundering row of the data is same-currency (D36), and several features are built on the
cross-currency flag. This retrains the champion's variant (unweighted, the same parameters and
seeds as ml/train.py) on the training rows with groups of features removed, and reports mean and
standard deviation of the VALIDATION PR-AUC. It reads no test row, selects nothing and changes
neither the champion nor any threshold: it only helps to read the results.

Groups:
- `all_features`: the 85 features (reproduces ml/train.py's unweighted variant);
- `without_cross_currency_flag_family`: drops `is_cross_currency` and every
  `*_cross_currency_share_*`;
- `...and_currency_codes`: also drops `payment_currency_code` and `receiving_currency_code`,
  because two trees on them can rebuild the flag (equal or different currency).
"""

import argparse
import time
from collections.abc import Sequence
from typing import Any

from batch.spark import common
from ml import data, train

GROUPS: dict[str, tuple[str, ...]] = {
    "all_features": (),
    "without_cross_currency_flag_family": ("is_cross_currency", "cross_currency_share"),
    "without_flag_family_and_currency_codes": (
        "is_cross_currency",
        "cross_currency_share",
        "payment_currency_code",
        "receiving_currency_code",
    ),
}


def dropped(names: Sequence[str], patterns: Sequence[str]) -> list[str]:
    return [n for n in names if any(p in n for p in patterns)]


def run(ds: str) -> dict[str, Any]:
    names_all = data.feature_names()
    tr = data.read_matrix(ds, "train", burn_in=True)
    va = data.read_matrix(ds, "val")
    y_train, y_val = tr.meta["y"].to_numpy(), va.meta["y"].to_numpy()
    out: dict[str, Any] = {}
    for group, patterns in GROUPS.items():
        drop = dropped(names_all, patterns)
        keep = [n for n in names_all if n not in drop]
        cols = [names_all.index(n) for n in keep]
        dtrain, dval = train.make_matrices(tr.x[:, cols], y_train, va.x[:, cols], y_val, keep)
        results = []
        started = time.perf_counter()
        for seed in train.SEEDS:
            fit = train.fit_one(dtrain, dval, y_val, "unweighted", seed, scale_pos_weight=1.0)
            results.append(fit.result)
        pr = [r.val_pr_auc for r in results]
        roc = [r.val_roc_auc for r in results]
        out[group] = {
            "features_used": len(keep),
            "features_dropped": drop,
            "val_pr_auc_mean_std": train.mean_std(pr),
            "val_roc_auc_mean_std": train.mean_std(roc),
            "val_pr_auc_by_seed": pr,
            "seconds": round(time.perf_counter() - started, 1),
        }
        m, s = out[group]["val_pr_auc_mean_std"]
        print(f"{group}: {len(keep)} features, val PR-AUC {m:.4f} ± {s:.4f}")
    return out


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", required=True, choices=["hi_small", "hi_medium"])
    args = parser.parse_args(argv)
    groups = run(args.ds)
    base = groups["all_features"]["val_pr_auc_mean_std"][0]
    for g in groups.values():
        g["delta_val_pr_auc_vs_all_features"] = g["val_pr_auc_mean_std"][0] - base
    doc = {
        "dataset": args.ds,
        **common.provenance(),
        "status": "exploratory; validation only; selects nothing; no test row read",
        "variant": "unweighted",
        "seeds": list(train.SEEDS),
        "groups": groups,
    }
    common.write_json(
        common.ROOT / "results" / "M02" / args.ds / "ablation_cross_currency.json", doc
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
