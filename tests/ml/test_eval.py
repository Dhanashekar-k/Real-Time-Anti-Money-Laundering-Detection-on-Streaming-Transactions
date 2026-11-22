"""eval/model_eval.py: the evaluation core and the gate that guards the test split.

Hand-worked values for the per-pattern and precision@K checks are in the test bodies.
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from eval import model_eval as ev
from ml import metrics

pytestmark = pytest.mark.unit

DAY = 86_400_000
ROOT = Path(__file__).resolve().parents[2]


# --- the gate --------------------------------------------------------------------------------


def test_no_final_flag_no_access_and_no_log(tmp_path: Path) -> None:
    with pytest.raises(ev.FinalRequired):
        ev.open_test_gate(False, "hi_small", tmp_path, reason="unit test")
    assert not (tmp_path / "test_access_log.txt").exists()


def test_final_flag_warns_and_appends_one_log_line_per_access(tmp_path: Path) -> None:
    with pytest.warns(UserWarning, match="test split"):
        ev.open_test_gate(True, "hi_small", tmp_path, reason="first read")
    with pytest.warns(UserWarning):
        ev.open_test_gate(True, "hi_small", tmp_path, reason="second read")
    lines = (tmp_path / "test_access_log.txt").read_text().splitlines()
    assert len(lines) == 2
    assert "hi_small" in lines[0] and "first read" in lines[0]
    assert "second read" in lines[1]
    assert re.match(r"\d{4}-\d\d-\d\dT", lines[0])  # a timestamp
    assert "git=" in lines[0]


def test_main_without_final_exits_nonzero_and_writes_nothing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(ev, "RESULTS_ROOT", tmp_path)
    assert ev.main(["--ds", "hi_small"]) != 0
    assert list(tmp_path.iterdir()) == []


def test_nothing_else_reads_the_test_split() -> None:
    """Only eval/model_eval.py may filter on the test split (a static check of the sources)."""
    pattern = re.compile(
        r"""split\W{0,12}(==|=|in)\W{0,4}['"]test['"]|['"]test['"]\s*\)?\s*==\s*\w*split"""
    )
    offenders = []
    for folder in ("ml", "batch", "features", "services", "ops"):
        for path in (ROOT / folder).rglob("*.py"):
            if path == ROOT / "ml" / "data.py":
                continue  # the refusal itself: `if split == "test" and not allow_test: raise`
            if pattern.search(path.read_text()):
                offenders.append(str(path.relative_to(ROOT)))
    assert offenders == []


# --- the evaluation -----------------------------------------------------------------------------


def _frame() -> pd.DataFrame:
    """Two banks, two days, one sparse-tail day. Positives are all same-currency."""
    rng = np.random.default_rng(0)
    rows = []
    for day, n in ((0, 300), (1, 300), (2, 20)):  # day 2 stands for the sparse tail
        for i in range(n):
            bank = "A" if i % 2 == 0 else "B"
            pos = int(rng.random() < (0.5 if day == 2 else 0.05))
            cross = int(rng.random() < 0.3 and not pos)  # no cross-currency positive
            score = float(np.clip(rng.normal(0.7 if pos else 0.3, 0.2), 0, 1))
            pattern = rng.choice(["CYCLE", "FAN_OUT", None]) if pos else None
            rows.append((day * DAY + i, bank, pos, score, cross, pattern))
    return pd.DataFrame(
        rows, columns=["ts", "bank", "y", "score", "is_cross_currency", "pattern_type"]
    )


def _run(frame: pd.DataFrame, **kw) -> dict:
    baseline = (
        frame["score"].to_numpy() + np.random.default_rng(1).normal(0, 0.3, len(frame))
    ) > 0.8
    return ev.evaluate(
        frame,
        bank_thresholds={"A": 0.5, "B": 0.6},
        baseline_alert=baseline,
        baseline_name="rule_set",
        tail_days={2},
        k_per_day=20,
        n_resamples=200,
        seed=3,
        **kw,
    )


def test_both_slices_are_reported_and_the_tail_is_excluded_from_one() -> None:
    f = _frame()
    out = _run(f)
    assert set(out["slices"]) == {"all_test", "without_tail"}
    assert out["slices"]["all_test"]["n_rows"] == len(f)
    assert out["slices"]["without_tail"]["n_rows"] == int((f["ts"] // DAY != 2).sum())
    assert out["slices"]["all_test"]["n_positive"] > out["slices"]["without_tail"]["n_positive"]


def test_model_alert_uses_the_threshold_of_the_rows_own_bank() -> None:
    f = _frame()
    alert = ev.model_alert(f, {"A": 0.5, "B": 0.6})
    expected = np.where(f["bank"] == "A", f["score"] >= 0.5, f["score"] >= 0.6)
    assert alert.tolist() == expected.tolist()
    with pytest.raises(KeyError, match="C"):
        ev.model_alert(f.assign(bank="C"), {"A": 0.5})


def test_metrics_match_the_shared_arithmetic() -> None:
    f = _frame()
    out = _run(f)["slices"]["all_test"]
    alert = ev.model_alert(f, {"A": 0.5, "B": 0.6})
    tp, fp, fn, _ = metrics.confusion(f["y"].to_numpy(), alert)
    p, r, f1 = metrics.prf1(tp, fp, fn)
    assert (out["model"]["precision"], out["model"]["recall"], out["model"]["f1"]) == (p, r, f1)
    assert out["model"]["alerts"] == int(alert.sum())
    assert 0 < out["model"]["pr_auc"] <= 1 and 0 < out["model"]["roc_auc"] <= 1


def test_paired_bootstrap_is_reported_with_marginals_and_the_difference() -> None:
    boot = _run(_frame())["slices"]["all_test"]["bootstrap"]
    assert boot["n_resamples"] == 200 and boot["seed"] == 3
    for key in ("f1_a", "f1_b", "diff"):
        lo, hi = boot[key]["ci"]
        assert lo <= boot[key]["point"] <= hi
    assert boot["diff"]["point"] == pytest.approx(boot["f1_a"]["point"] - boot["f1_b"]["point"])


def test_t2_delta_is_computed_not_typed() -> None:
    out = _run(_frame())
    s = out["slices"]["all_test"]
    assert out["t2"]["all_test"]["delta_f1"] == s["bootstrap"]["diff"]["point"]
    assert out["t2"]["baseline"] == "rule_set"
    assert "points" in out["t2"]["definition"]


def test_per_pattern_recall_by_hand() -> None:
    # 4 CYCLE positives (3 alerted), 2 FAN_OUT (0 alerted), 1 positive with no pattern (alerted)
    f = pd.DataFrame(
        {
            "ts": np.arange(8),
            "bank": "A",
            "y": [1, 1, 1, 1, 1, 1, 1, 0],
            "score": [0.9, 0.9, 0.9, 0.1, 0.1, 0.1, 0.9, 0.5],
            "is_cross_currency": 0,
            "pattern_type": ["CYCLE"] * 4 + ["FAN_OUT"] * 2 + [None, None],
        }
    )
    got = ev.per_pattern_recall(f, ev.model_alert(f, {"A": 0.5}))
    assert got["patterns"]["CYCLE"] == {"positives": 4, "caught": 3, "recall": 0.75}
    assert got["patterns"]["FAN_OUT"] == {"positives": 2, "caught": 0, "recall": 0.0}
    assert got["without_pattern_label"] == {"positives": 1, "caught": 1, "recall": 1.0}
    assert got["pattern_coverage"] == 6 / 7  # laundering rows that carry a pattern label


def test_precision_at_k_per_bank_day_by_hand() -> None:
    # one bank, two days, K = 2. day 0 top-2 scores: 0.9 (pos), 0.8 (neg) -> 1 hit of 2
    # day 1 top-2: 0.7 (pos), 0.6 (pos) -> 2 hits of 2; pooled: 3 / 4
    f = pd.DataFrame(
        {
            "ts": [0, 1, 2, DAY, DAY + 1, DAY + 2],
            "bank": "A",
            "y": [1, 0, 1, 1, 1, 0],
            "score": [0.9, 0.8, 0.1, 0.7, 0.6, 0.5],
        }
    )
    got = ev.precision_at_k(f, k=2)
    assert got == {"k": 2, "cells": 2, "alerts": 4, "hits": 3, "precision": 0.75}


def test_headline_reports_the_cross_currency_artifact_with_counts_from_the_frame() -> None:
    f = _frame()
    out = _run(f)
    n_cross = int(f["is_cross_currency"].sum())
    text = "\n".join(out["headline"])
    assert f"0 of {n_cross} cross-currency rows" in text
    assert "generator artifact" in text
    assert "same-currency" in text
    # the artifact is not only the flag: the windowed shares are built on it
    assert "is_cross_currency" in text and "cross_currency_share" in text


def test_headline_does_not_claim_the_artifact_if_a_cross_currency_row_is_laundering() -> None:
    f = _frame()
    f.loc[f.index[f["is_cross_currency"] == 1][0], "y"] = 1
    text = "\n".join(_run(f)["headline"])
    assert "perfect negative signal" not in text
    assert "cross-currency rows" in text  # the counts are still stated


def test_headline_reports_the_tail_with_its_counts() -> None:
    f = _frame()
    out = _run(f)
    tail = f[f["ts"] // DAY == 2]
    text = "\n".join(out["headline"])
    assert f"{len(tail)} rows" in text and f"{int(tail['y'].sum())} laundering" in text
    assert "not a temporal trend" in text


def test_importance_caption_carries_the_artifact() -> None:
    cap = ev.importance_caption(cross_currency_rows=485_144, cross_currency_laundering=0)
    assert "is_cross_currency" in cap and "generator artifact" in cap
    assert "cross_currency_share" in cap  # the windowed shares derive from the same flag
    assert "0 of 485,144" in cap


def test_importance_caption_makes_no_artifact_claim_when_it_is_not_true() -> None:
    cap = ev.importance_caption(cross_currency_rows=1_000, cross_currency_laundering=3)
    assert "generator artifact" not in cap and "perfectly" not in cap
    assert "3 of 1,000" in cap


# --- the runner's helpers -----------------------------------------------------------------------


def test_golden_status_counts_the_todo_values(tmp_path: Path) -> None:
    (tmp_path / "s01_x.json").write_text(
        '{"expected": {"1": {"a": "TODO(human)", "b": 3}, "2": {"c": "TODO(human)"}}}'
    )
    (tmp_path / "s02_y.json").write_text('{"expected": {"1": {"a": 1.0}}}')
    status = ev.golden_status(tmp_path)
    assert (status["scenarios"], status["todo_values"], status["scenarios_with_todo"]) == (2, 2, 1)
    assert status["provisional"] is True
    (tmp_path / "s01_x.json").write_text('{"expected": {"1": {"a": 2}}}')
    assert ev.golden_status(tmp_path)["provisional"] is False


def test_the_real_golden_directory_is_read_not_typed() -> None:
    status = ev.golden_status()
    assert status["scenarios"] >= 10
    assert status["todo_values"] == status["todo_values"]  # a measured count, whatever it is today


def test_thresholds_yaml_is_read_per_bank(tmp_path: Path) -> None:
    f = tmp_path / "thresholds.yaml"
    f.write_text(
        "meta: {model_version: '4'}\n"
        "banks:\n  A: {f1_optimal: {threshold: 0.4}, budget: {threshold: 0.7}}\n"
        "  B: {f1_optimal: {threshold: 0.3}, budget: {threshold: 0.8}}\n"
    )
    assert ev.load_thresholds(f, "f1_optimal") == {"A": 0.4, "B": 0.3}
    assert ev.load_thresholds(f, "budget") == {"A": 0.7, "B": 0.8}
    with pytest.raises(KeyError):
        ev.load_thresholds(f, "nope")


def test_provisional_headline_is_added_when_golden_is_open() -> None:
    lines = ev.provisional_headline(
        {"provisional": True, "todo_values": 150, "scenarios_with_todo": 10}
    )
    assert "PROVISIONAL" in lines[0] and "150" in lines[0]
    assert ev.provisional_headline({"provisional": False, "todo_values": 0}) == []
