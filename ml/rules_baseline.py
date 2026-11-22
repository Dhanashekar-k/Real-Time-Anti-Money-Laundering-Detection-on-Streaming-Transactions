"""The rules baseline: five transparent rules, tuned on train only (M02 step 6).

    uv run python -m ml.rules_baseline --definitions    # print rules and protocol, run nothing

Protocol (printed by `definitions_text`, reviewed by the owner before any run):
- Thresholds are chosen by grid search on TRAIN rows (burn-in excluded), one rule at a time, to
  maximise that rule's F1 on train. Ties go to the strictest grid point.
- The rule set is the OR of R1..R5. The baseline that the model must beat (T2) is the better of
  the rule set and the best single rule, chosen by validation F1; ties go to the rule set.
- Test is touched once, in eval/model_eval.py, both ways (all rows, and without the sparse tail).
- R1 differs from the manual (D38): the manual's "cross-currency AND amount above p99" can never
  fire on a laundering row in this dataset (every laundering row is same-currency, D36), so R1 is
  "same-currency AND ...". The manual's version is kept as `R1_original`, fitted with the same p99
  and reported, but is not part of the rule set.

The functions here are pure: they take frames that the caller has already filtered to one split.
Labels are read only to score a threshold (`is_laundering`), never as an input to a rule.
"""

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import product
from typing import Any

import numpy as np
import pandas as pd

from ml import metrics

LABEL = "is_laundering"
REQUIRED_COLUMNS = (
    "amount_paid_usd_cents",
    "is_cross_currency",
    "s_pass_through_ratio_24h",
    "s_in_count_24h",
    "s_distinct_out_cp_24h",
    "r_distinct_in_cp_24h",
    "r_returned_funds_24h",
)

# Grids, strictest first (ties in train F1 go to the first).
Q_GRID = (0.999, 0.995, 0.99, 0.95, 0.9)
BANDS = ((0.95, 1.05), (0.9, 1.1), (0.8, 1.2), (0.5, 1.5))
MIN_IN_GRID = (5, 3, 2, 1)
K_GRID = (20, 15, 10, 8, 5, 4, 3)

Predicate = Callable[[pd.DataFrame, Mapping[str, Any], Mapping[str, Any]], np.ndarray]


@dataclass(frozen=True)
class Rule:
    id: str
    title: str
    text: str
    grid: tuple[dict[str, Any], ...]
    predicate: Predicate
    amount_quantile: bool = False  # the rule needs a train quantile of the amount (derived)


@dataclass(frozen=True)
class FittedRule:
    rule_id: str
    params: dict[str, Any]
    derived: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FitSet:
    rules: list[FittedRule]  # R1..R5, in order
    original_r1: FittedRule


# --- the rules --------------------------------------------------------------------------------


def _r1(df: pd.DataFrame, p: Mapping[str, Any], d: Mapping[str, Any]) -> np.ndarray:
    big = df["amount_paid_usd_cents"].to_numpy() > d["amount_threshold"]
    return big & (df["is_cross_currency"].to_numpy() == 0)


def _r1_original(df: pd.DataFrame, p: Mapping[str, Any], d: Mapping[str, Any]) -> np.ndarray:
    big = df["amount_paid_usd_cents"].to_numpy() > d["amount_threshold"]
    return big & (df["is_cross_currency"].to_numpy() == 1)


def _r2(df: pd.DataFrame, p: Mapping[str, Any], d: Mapping[str, Any]) -> np.ndarray:
    ratio = df["s_pass_through_ratio_24h"].to_numpy(dtype=float)  # NaN compares False
    in_band = (ratio >= p["lo"]) & (ratio <= p["hi"])
    return in_band & (df["s_in_count_24h"].to_numpy() >= p["min_in"])


def _r3(df: pd.DataFrame, p: Mapping[str, Any], d: Mapping[str, Any]) -> np.ndarray:
    return df["s_distinct_out_cp_24h"].to_numpy() >= p["k"]


def _r4(df: pd.DataFrame, p: Mapping[str, Any], d: Mapping[str, Any]) -> np.ndarray:
    return df["r_distinct_in_cp_24h"].to_numpy() >= p["k"]


def _r5(df: pd.DataFrame, p: Mapping[str, Any], d: Mapping[str, Any]) -> np.ndarray:
    return df["r_returned_funds_24h"].to_numpy() == 1


RULES: tuple[Rule, ...] = (
    Rule(
        "R1",
        "large same-currency payment",
        "same-currency AND amount_paid_usd_cents > train quantile q of amount_paid_usd_cents",
        tuple({"q": q} for q in Q_GRID),
        _r1,
        amount_quantile=True,
    ),
    Rule(
        "R2",
        "pass-through account",
        "s_pass_through_ratio_24h in [lo, hi] AND s_in_count_24h >= min_in",
        tuple({"lo": lo, "hi": hi, "min_in": m} for (lo, hi), m in product(BANDS, MIN_IN_GRID)),
        _r2,
    ),
    Rule(
        "R3",
        "fan-out",
        "s_distinct_out_cp_24h >= k",
        tuple({"k": k} for k in K_GRID),
        _r3,
    ),
    Rule(
        "R4",
        "fan-in",
        "r_distinct_in_cp_24h >= k",
        tuple({"k": k} for k in K_GRID),
        _r4,
    ),
    Rule(
        "R5",
        "returned funds",
        "r_returned_funds_24h == 1 (no parameter)",
        ({},),
        _r5,
    ),
)
RULES_BY_ID = {r.id: r for r in RULES}

ORIGINAL_R1 = Rule(
    "R1_original",
    "large cross-currency payment (the manual's R1)",
    "cross-currency AND amount_paid_usd_cents > train p99 of amount_paid_usd_cents (q fixed)",
    ({"q": 0.99},),
    _r1_original,
    amount_quantile=True,
)


# --- fitting --------------------------------------------------------------------------------------


def _check_train_frame(train: pd.DataFrame, burn_in_end_ms: int) -> None:
    if "split" in train.columns and not (train["split"] == "train").all():
        raise ValueError(
            "fit needs train rows only: the frame has a split column with other values"
        )
    if len(train) and train["event_ts_ms"].min() < burn_in_end_ms:
        raise ValueError("fit needs rows after the 24 h burn-in (D04): found earlier rows")
    missing = [c for c in (*REQUIRED_COLUMNS, LABEL, "event_ts_ms") if c not in train.columns]
    if missing:
        raise ValueError(f"train frame is missing columns {missing}")


def _derive(rule: Rule, params: Mapping[str, Any], train: pd.DataFrame) -> dict[str, Any]:
    if not rule.amount_quantile:
        return {}
    amounts = train["amount_paid_usd_cents"].to_numpy()
    return {"amount_threshold": int(np.quantile(amounts, params["q"], method="lower"))}


def fit_one(
    rule: Rule,
    train: pd.DataFrame,
    burn_in_end_ms: int,
    params: Mapping[str, Any] | None = None,
) -> FittedRule:
    """Tune one rule on train (or take `params` as given, for the fixed original R1)."""
    _check_train_frame(train, burn_in_end_ms)
    y = train[LABEL].to_numpy()
    candidates = [dict(params)] if params is not None else [dict(g) for g in rule.grid]
    best: tuple[float, dict[str, Any], dict[str, Any]] | None = None
    for cand in candidates:
        derived = _derive(rule, cand, train)
        alert = rule.predicate(train, cand, derived)
        f1 = metrics.prf1(*metrics.confusion(y, alert)[:3])[2]
        if best is None or f1 > best[0]:  # strict: the first (strictest) of equal F1s wins
            best = (f1, cand, derived)
    assert best is not None
    f1, cand, derived = best
    return FittedRule(rule.id, cand, {**derived, "train_f1": f1})


def fit_all(train: pd.DataFrame, burn_in_end_ms: int) -> FitSet:
    rules = [fit_one(r, train, burn_in_end_ms) for r in RULES]
    original = fit_one(ORIGINAL_R1, train, burn_in_end_ms, params={"q": 0.99})
    return FitSet(rules, original)


# --- applying -------------------------------------------------------------------------------------


def apply(rule: Rule, fitted: FittedRule, df: pd.DataFrame) -> np.ndarray:
    """Boolean alert per row. Reads only feature columns, never the label."""
    return np.asarray(rule.predicate(df, fitted.params, fitted.derived), dtype=bool)


def rule_set_alert(fits: FitSet, df: pd.DataFrame) -> np.ndarray:
    """OR of R1..R5 (the original R1 is not part of the rule set)."""
    alert = np.zeros(len(df), dtype=bool)
    for f in fits.rules:
        alert |= apply(RULES_BY_ID[f.rule_id], f, df)
    return alert


def choose_baseline(val_f1: Mapping[str, float]) -> str:
    """'rule_set' or the id of the best single rule, by validation F1; ties go to the rule set."""
    singles: Sequence[str] = [r.id for r in RULES]
    best_single = max(singles, key=lambda rid: val_f1[rid])  # first of equal values
    return "rule_set" if val_f1["rule_set"] >= val_f1[best_single] else best_single


# --- the printout the owner reviews ---------------------------------------------------------------


def definitions_text() -> str:
    lines = [
        "RULES BASELINE: definitions and protocol (nothing has been run)",
        "",
        "Protocol",
        "  - thresholds: grid search on TRAIN rows (after the 24 h burn-in), one rule at a time,",
        "    maximising that rule's F1 on train; ties go to the strictest grid point (first below)",
        "  - rule set = OR of R1..R5; R1_original is reported but is NOT in the rule set",
        "  - baseline for T2 = the better of the rule set and the best single rule on VAL F1",
        "    (tie -> rule set); the paired bootstrap compares the model with that baseline",
        "  - test is read once, in eval/model_eval.py --final, and reported both ways:",
        "    all rows, and without the sparse tail days listed in eda.json",
        "  - labels score a threshold; no rule reads them",
        "",
        "Rules",
    ]
    for r in (*RULES, ORIGINAL_R1):
        lines.append(f"  {r.id} ({r.title})")
        lines.append(f"      {r.text}")
        lines.append(f"      grid, strictest first: {_grid_text(r)}")
    lines += [
        "",
        "R1 vs the manual: the manual's R1 is cross-currency; every laundering row in HI-Small and",
        "HI-Medium is same-currency (0 of 72,170 and 0 of 485,144 cross-currency rows, D36), so it",
        "would score recall 0 by construction. R1 is same-currency; the manual's version is kept",
        "as R1_original with q = 0.99 and reported as a documented zero. D38 records the swap.",
    ]
    return "\n".join(lines)


def _grid_text(rule: Rule) -> str:
    if rule.id in ("R1", "R1_original"):
        return "q in {" + ", ".join(str(g["q"]) for g in rule.grid) + "}"
    if rule.id == "R2":
        bands = ", ".join(f"[{lo}, {hi}]" for lo, hi in BANDS)
        mins = ", ".join(str(m) for m in MIN_IN_GRID)
        return f"band in {{{bands}}} x min_in in {{{mins}}}"
    if rule.id in ("R3", "R4"):
        return "k in {" + ", ".join(str(g["k"]) for g in rule.grid) + "}"
    return "none"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--definitions", action="store_true", help="print the rules and exit")
    args = parser.parse_args(argv)
    if args.definitions:
        print(definitions_text())
        return 0
    raise SystemExit(
        "ml.rules_baseline: running needs features_tx (M02 step 8, not built yet). "
        "Review the definitions with --definitions first."
    )


if __name__ == "__main__":
    raise SystemExit(main())
