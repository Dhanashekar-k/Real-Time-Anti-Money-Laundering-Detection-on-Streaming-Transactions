"""Laundering rows with a pattern label vs without: why the model's recall differs so much.

    uv run python -m eval.pattern_gap --ds hi_small

TRAIN AND VALIDATION ONLY. The test split is not read, not even to count account legs: the leg
counts and cluster sizes below are computed over train and val rows. "Patterned" means the
transaction was assigned to a line of the IBM pattern file (`patterns` table, `tx_id` not null);
"no_pattern" is every other laundering row (D36: coverage is partial).

For each group it reports how many rows there are and how they differ in amount (USD), payment
format, cross-currency, self-transfers, how many legs the sender and the receiver account have in
train+val, how many OTHER laundering rows the same sender (or receiver) has, and the medians of
the features the model sees. With `--with-model` it also gives the champion's recall on the
validation rows of each group at the per-bank F1-optimal thresholds (validation, so no test row).

This describes the data and the model's behaviour. It does not claim a mechanism for why the
generator leaves some laundering rows out of the pattern file; that is not measured here.
"""

import argparse
import json
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from batch.spark import common

DEFAULT_FEATURES = (
    "s_out_count_24h",
    "s_in_count_24h",
    "r_in_count_24h",
    "r_out_count_24h",
    "s_distinct_out_cp_72h",
    "r_distinct_in_cp_72h",
    "s_account_age_minutes",
    "r_account_age_minutes",
    "s_pass_through_ratio_24h",
    "s_new_counterparty_72h",
    "r_new_counterparty_72h",
    "s_log_empty",
    "r_log_empty",
    "r_returned_funds_24h",
)
_GROUPS = {"patterned": "patterned", "no_pattern": "NOT patterned"}


def _dist(con: Any, expr: str, where: str) -> dict[str, float | None]:
    row = con.execute(
        f"""SELECT min({expr}), quantile_cont({expr}, 0.25), quantile_cont({expr}, 0.5),
                   quantile_cont({expr}, 0.75), quantile_cont({expr}, 0.9),
                   quantile_cont({expr}, 0.99), max({expr}), avg({expr})
            FROM laund WHERE {where}"""
    ).fetchall()[0]
    keys = ("min", "p25", "median", "p75", "p90", "p99", "max", "mean")
    return {k: (None if v is None else float(v)) for k, v in zip(keys, row, strict=True)}


def run(
    ds: str,
    warehouse: Path | None = None,
    with_model: bool = True,
    features: Sequence[str] = DEFAULT_FEATURES,
) -> dict[str, Any]:
    wh = warehouse or common.WAREHOUSE
    raw = f"read_parquet('{wh}/aml_{ds}/raw_transactions/data/*/[0-9]*.parquet')"
    feats = f"read_parquet('{wh}/aml_{ds}/features_tx/data/[0-9]*.parquet')"
    pats = f"read_parquet('{wh}/aml_{ds}/patterns/data/[0-9]*.parquet')"
    con = common.duckdb_connection()
    try:
        con.execute(
            f"""CREATE TEMP TABLE legs_cnt AS
                SELECT k, count(*) AS n FROM (
                    SELECT sender_key AS k FROM {raw} WHERE split IN ('train', 'val')
                    UNION ALL
                    SELECT receiver_key FROM {raw} WHERE split IN ('train', 'val')) GROUP BY k"""
        )
        con.execute(
            f"""CREATE TEMP TABLE laund AS
                SELECT tx_id, split, sender_key, receiver_key, payment_format,
                       amount_paid_usd_cents / 100.0 AS usd,
                       payment_currency <> receiving_currency AS cross_ccy,
                       sender_key = receiver_key AS self_tx,
                       split_part(sender_key, ':', 1)
                           = split_part(receiver_key, ':', 1) AS same_bank,
                       (pattern_type IS NOT NULL) AS patterned, pattern_type,
                       sc.n AS sender_legs, rc.n AS receiver_legs,
                       count(*) OVER (PARTITION BY sender_key) - 1 AS other_same_sender,
                       count(*) OVER (PARTITION BY receiver_key) - 1 AS other_same_receiver
                FROM (SELECT r.*, p.pattern_type FROM {raw} r
                      LEFT JOIN (SELECT DISTINCT tx_id, pattern_type FROM {pats}
                                 WHERE tx_id IS NOT NULL) p USING (tx_id)
                      WHERE r.split IN ('train', 'val') AND r.is_laundering = 1) t
                LEFT JOIN legs_cnt sc ON sc.k = t.sender_key
                LEFT JOIN legs_cnt rc ON rc.k = t.receiver_key"""
        )
        total = con.execute("SELECT count(*) FROM laund").fetchall()[0][0]
        by_split = dict(con.execute("SELECT split, count(*) FROM laund GROUP BY split").fetchall())
        groups: dict[str, Any] = {}
        for name, flag in (("patterned", "patterned"), ("no_pattern", "NOT patterned")):
            n = con.execute(f"SELECT count(*) FROM laund WHERE {flag}").fetchall()[0][0]
            fmt = dict(
                con.execute(
                    f"SELECT payment_format, count(*) FROM laund WHERE {flag} "
                    "GROUP BY 1 ORDER BY 2 DESC"
                ).fetchall()
            )
            flags = con.execute(
                f"""SELECT sum(cross_ccy::INT), sum(self_tx::INT), avg(same_bank::INT),
                           sum((split = 'val')::INT) FROM laund WHERE {flag}"""
            ).fetchall()[0]
            feat_stats = {}
            for f in features:
                r = con.execute(
                    f"""SELECT quantile_cont(x.{f}, 0.5), avg(x.{f}), avg((x.{f} IS NULL)::INT),
                               avg((x.{f} IS NOT NULL AND x.{f} <> 0)::INT)
                        FROM laund l JOIN {feats} x USING (tx_id) WHERE {flag}"""
                ).fetchall()[0]
                feat_stats[f] = {
                    "median": None if r[0] is None else float(r[0]),
                    "mean": None if r[1] is None else float(r[1]),
                    "null_share": float(r[2]),
                    "nonzero_share": float(r[3]),
                }
            groups[name] = {
                "rows": int(n),
                "share_of_laundering": n / total if total else None,
                "rows_in_val": int(flags[3] or 0),
                "amount_usd": _dist(con, "usd", flag),
                "zero_amount_rows": int(
                    con.execute(f"SELECT count(*) FROM laund WHERE {flag} AND usd = 0").fetchall()[
                        0
                    ][0]
                ),
                "payment_format": {k: int(v) for k, v in fmt.items()},
                "cross_currency_rows": int(flags[0] or 0),
                "self_transfer_rows": int(flags[1] or 0),
                "same_bank_share": None if flags[2] is None else float(flags[2]),
                "sender_legs": _dist(con, "sender_legs", flag),
                "receiver_legs": _dist(con, "receiver_legs", flag),
                "other_laundering_rows_same_sender": _dist(con, "other_same_sender", flag),
                "other_laundering_rows_same_receiver": _dist(con, "other_same_receiver", flag),
                "share_above_1m_usd": float(
                    con.execute(
                        f"SELECT avg((usd > 1000000)::INT) FROM laund WHERE {flag}"
                    ).fetchall()[0][0]
                ),
                "top_senders": [
                    {
                        "sender": r[0],
                        "rows": int(r[1]),
                        "share_of_group": float(r[1]) / n,
                        "median_usd": float(r[2]),
                        "payment_formats": sorted(r[3]),
                    }
                    for r in con.execute(
                        f"""SELECT sender_key, count(*) AS n, median(usd),
                                   list(DISTINCT payment_format)
                            FROM laund WHERE {flag} GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 6"""
                    ).fetchall()
                ],
                "rows_from_senders_with_at_least_50_laundering_rows": int(
                    con.execute(
                        f"""SELECT coalesce(sum(n), 0) FROM (
                                SELECT count(*) AS n FROM laund WHERE {flag}
                                GROUP BY sender_key HAVING count(*) >= 50)"""
                    ).fetchall()[0][0]
                ),
                "features": feat_stats,
            }
        pattern_types = dict(
            con.execute(
                "SELECT pattern_type, count(*) FROM laund WHERE patterned "
                "GROUP BY 1 ORDER BY 2 DESC"
            ).fetchall()
        )
    finally:
        con.close()
    out: dict[str, Any] = {
        "dataset": ds,
        "scope": "train and val rows only; the test split is not read",
        "laundering_rows_train_val": int(total),
        "rows_by_split": {k: int(v) for k, v in by_split.items()},
        "pattern_types_patterned": {k: int(v) for k, v in pattern_types.items()},
        "groups": groups,
    }
    if with_model:
        out["model_val_recall"] = _model_val_recall(ds, wh)
    return out


def _model_val_recall(ds: str, warehouse: Path) -> dict[str, Any]:
    """Champion recall on validation laundering rows, per group, at the F1-optimal thresholds."""
    import mlflow
    import numpy as np
    import xgboost as xgb
    import yaml

    from eval.model_eval import load_thresholds, thresholds_path
    from ml import data, registry

    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001"))
    thr_file = thresholds_path(ds)
    version = registry.resolve_version(mlflow.MlflowClient(), ds)
    registry.check_matches(
        ds, version, yaml.safe_load(thr_file.read_text())["meta"]["model_version"], thr_file.name
    )
    booster = mlflow.xgboost.load_model(f"models:/{registry.MODEL_NAME}/{version}")
    thr = load_thresholds(thr_file, "f1_optimal", dataset=ds)
    val = data.read_matrix(ds, "val", patterns=True, warehouse=warehouse)
    best = int(booster.best_iteration)
    score = booster.predict(
        xgb.DMatrix(val.x, feature_names=val.names), iteration_range=(0, best + 1)
    )
    alert = score >= val.meta["bank"].map(thr).to_numpy(dtype=float)
    pos = val.meta["y"].to_numpy() == 1
    patterned = val.meta["pattern_type"].notna().to_numpy()
    out: dict[str, Any] = {"model_version": version}
    for name, sel in (("patterned", pos & patterned), ("no_pattern", pos & ~patterned)):
        out[name] = {
            "positives": int(sel.sum()),
            "caught": int((sel & alert).sum()),
            "recall": float((sel & alert).sum() / sel.sum()) if sel.any() else None,
            "mean_score": float(np.mean(score[sel])) if sel.any() else None,
            "median_score": float(np.median(score[sel])) if sel.any() else None,
        }
    return out


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", required=True, choices=["hi_small", "hi_medium"])
    parser.add_argument("--no-model", action="store_true", help="skip the champion's val recall")
    args = parser.parse_args(argv)
    out = run(args.ds, with_model=not args.no_model)
    doc = {"dataset": args.ds, **common.provenance(), **out}
    path = common.ROOT / "results" / "M02" / args.ds / "pattern_gap.json"
    common.write_json(path, doc)
    print(json.dumps({k: out[k] for k in ("laundering_rows_train_val", "rows_by_split")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
