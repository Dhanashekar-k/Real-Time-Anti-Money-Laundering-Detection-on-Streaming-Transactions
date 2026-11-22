"""Wall-clock time of the oracle on a hot account (Risk 1 of M02).

    uv run python -m eval.oracle_timing --ds hi_small

Reads the legs of the dataset's busiest account (named in results/M01/<ds>/eda.json) from the raw
table, runs `compute_account_legs` on them, and also on a copy of that account scaled up to the
other dataset's hot-account size (the real legs repeated end to end with shifted tx_id and time, so
the density and the counterparty mix are those of a real hot account). Writes
results/M02/<ds>/oracle_timing.json. Only feature columns are read; no label.

`rss_peak_mb` is the whole process peak (DuckDB and pandas included), so it is an upper bound for
the oracle alone.
"""

import argparse
import json
import resource
import statistics
import sys
import time
from typing import Any

import pandas as pd

from batch.spark import common
from features import reference
from features.spec import Spec, load_spec

SCALE_TARGETS = {"hi_small": 1_083_821}  # HI-Medium's hot account (eda.json of hi_medium)


def hot_account(ds: str) -> tuple[str, int]:
    eda = json.loads((common.ROOT / "results" / "M01" / ds / "eda.json").read_text())
    tpa = eda["accounts"]["tx_per_account"]
    return tpa["max_account"], int(eda["accounts"]["legs_per_account"]["max"])


def read_legs(ds: str, account: str) -> pd.DataFrame:
    data_dir = common.WAREHOUSE / f"aml_{ds}" / "raw_transactions" / "data"
    con = common.duckdb_connection()
    try:
        src = f"read_parquet('{data_dir}/*/[0-9]*.parquet')"
        return con.execute(
            f"""
            SELECT tx_id, event_ts_ms, 'OUT' AS direction, receiver_key AS counterparty_key,
                   amount_paid_usd_cents AS amount_usd_cents, payment_currency,
                   receiving_currency, payment_format
            FROM {src} WHERE sender_key = ?
            UNION ALL
            SELECT tx_id, event_ts_ms, 'IN', sender_key, amount_received_usd_cents,
                   payment_currency, receiving_currency, payment_format
            FROM {src} WHERE receiver_key = ?
            """,
            [account, account],
        ).fetchdf()
    finally:
        con.close()


def scale_legs(legs: pd.DataFrame, target: int) -> pd.DataFrame:
    """Repeat the account end to end (tx_id and time shifted) until it has `target` legs."""
    base = legs.sort_values(["event_ts_ms", "tx_id"]).reset_index(drop=True)
    id_span = int(base["tx_id"].max()) + 1
    ts_span = int(base["event_ts_ms"].max() - base["event_ts_ms"].min()) + 60_000
    parts = []
    for i in range(-(-target // len(base))):
        part = base.copy()
        part["tx_id"] = part["tx_id"] + i * id_span
        part["event_ts_ms"] = part["event_ts_ms"] + i * ts_span
        parts.append(part)
    return pd.concat(parts, ignore_index=True).iloc[:target]


def _rss_mb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 1e6 if sys.platform == "darwin" else peak / 1e3  # macOS: bytes, Linux: KiB


def time_oracle(legs: pd.DataFrame, spec: Spec, repeats: int = 3) -> dict[str, Any]:
    runs, loop_only = [], []
    for _ in range(repeats):
        t0 = time.perf_counter()
        out = reference.compute_account_legs(legs, spec)
        runs.append(time.perf_counter() - t0)
        assert len(out) == len(legs)
    for _ in range(repeats):
        t0 = time.perf_counter()
        reference._compute_columns(legs, spec)
        loop_only.append(time.perf_counter() - t0)
    return {
        "legs": len(legs),
        "seconds_full": [round(x, 3) for x in runs],
        "seconds_full_median": round(statistics.median(runs), 3),
        "seconds_loop_only": [round(x, 3) for x in loop_only],
        "microseconds_per_leg_full": round(statistics.median(runs) / len(legs) * 1e6, 2),
        "rss_peak_mb": round(_rss_mb(), 1),
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", default="hi_small", choices=["hi_small"])
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args(argv)
    common.require_data_root()
    spec = load_spec()
    account, expected = hot_account(args.ds)
    legs = read_legs(args.ds, account)
    if len(legs) != expected:
        raise SystemExit(f"hot account has {len(legs)} legs, eda.json says {expected}")
    real = time_oracle(legs, spec, args.repeats)
    scaled = time_oracle(scale_legs(legs, SCALE_TARGETS[args.ds]), spec, args.repeats)
    doc = {
        "dataset": args.ds,
        **common.provenance(),
        "config": {"account": account, "repeats": args.repeats, "python": sys.version.split()[0]},
        "hot_account": real,
        "scaled_to_other_dataset_hot_account": {
            **scaled,
            "note": "the real legs repeated end to end; HI-Medium's own hot account has 1,083,821",
        },
    }
    common.write_json(common.ROOT / "results" / "M02" / args.ds / "oracle_timing.json", doc)
    print(
        json.dumps(
            {k: doc[k] for k in ("hot_account", "scaled_to_other_dataset_hot_account")}, indent=2
        )
    )


if __name__ == "__main__":
    main()
