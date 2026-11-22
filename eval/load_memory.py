"""Memory of loading and fitting the training set (the 24 GB question for HI-Medium).

    uv run python -m eval.load_memory --ds hi_small

Measures, each in a FRESH process (a peak RSS is only meaningful for one process):
- `scale`: the training rows read with `ml.data.read_matrix`, replicated k = 1, 2, 4 times (a
  stand-in for a larger training set: the rows are the same HI-Small rows, NOT HI-Medium data),
  then a QuantileDMatrix, then 10 boosting rounds; peak RSS after each stage;
- `train_run`: the real `ml.train` for the dataset, into a scratch results and config directory and
  a throwaway MLflow database, run under `/usr/bin/time -l` (so nothing real is overwritten).
It also fits a straight line through the two largest scale points and extrapolates the DMatrix-build
peak to the row counts of `--target-rows`; that is an extrapolation from two measured points, not a
measurement, and the file says so. Writes results/M02/<ds>/load_memory.json.
"""

import argparse
import json
import resource
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from batch.spark import common


def parse_time_l(text: str) -> dict[str, float]:
    """`/usr/bin/time -l` (macOS): seconds, max RSS and peak footprint in MB."""
    out: dict[str, float] = {}
    for line in text.splitlines():
        parts = line.split()
        if "real" in parts and "user" in parts:
            out["seconds"] = float(parts[0])
        elif line.strip().endswith("maximum resident set size"):
            out["max_rss_mb"] = float(parts[0]) / 1e6
        elif line.strip().endswith("peak memory footprint"):
            out["peak_footprint_mb"] = float(parts[0]) / 1e6
    return out


def fit_line(points: Sequence[tuple[float, float]]) -> tuple[float, float]:
    """(slope, intercept) of the straight line through the first and last point."""
    if len(points) < 2 or points[0][0] == points[-1][0]:
        raise ValueError("need two points with different x")
    (x0, y0), (x1, y1) = points[0], points[-1]
    slope = (y1 - y0) / (x1 - x0)
    return slope, y0 - slope * x0


def extrapolate(points: Sequence[tuple[float, float]], x: float) -> float:
    slope, intercept = fit_line(points)
    return slope * x + intercept


def _peak_mb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 1e6 if sys.platform == "darwin" else peak / 1e3


def worker(ds: str, k: int) -> dict[str, Any]:
    """Runs in its own process: load, replicate k times, DMatrix, 10 rounds; peak RSS per stage."""
    import gc
    import time

    import numpy as np
    import xgboost as xgb

    from ml import data, train

    start = _peak_mb()
    t0 = time.perf_counter()
    m = data.read_matrix(ds, "train", burn_in=True)
    y, names = m.meta["y"].to_numpy(), m.names
    x = m.x if k == 1 else np.concatenate([m.x] * k)
    y = np.tile(y, k)
    del m
    gc.collect()
    rows, x_mb, loaded = len(x), x.nbytes / 1e6, _peak_mb()
    t1 = time.perf_counter()
    d = xgb.QuantileDMatrix(x, y, feature_names=names)
    dmatrix = _peak_mb()
    t2 = time.perf_counter()
    del x
    gc.collect()
    xgb.train(train.params_for("unweighted", 0, 1.0), d, num_boost_round=10, verbose_eval=False)
    return {
        "factor": k,
        "rows": rows,
        "x_float32_mb": round(x_mb),
        "peak_rss_mb": {
            "after_imports": round(start),
            "after_load": round(loaded),
            "after_dmatrix": round(dmatrix),
            "after_10_rounds": round(_peak_mb()),
        },
        "seconds": {
            "load": round(t1 - t0, 1),
            "dmatrix": round(t2 - t1, 1),
            "ten_rounds": round(time.perf_counter() - t2, 1),
        },
    }


def worker_iterator(ds: str, k: int) -> dict[str, Any]:
    """Like `worker`, but the matrix is a file read in slices and XGBoost is fed by an iterator.

    The training rows are spilled once, then written k times into a bigger file (the same rows
    repeated: a stand-in for a bigger training set, not HI-Medium data), so the full matrix is
    never in memory at any stage.
    """
    import gc
    import os
    import time

    import numpy as np
    import xgboost as xgb

    from ml import data, train

    start = _peak_mb()
    t0 = time.perf_counter()
    scratch = common.DATA_ROOT / "tmp" / f"load_memory_{k}"
    scratch.mkdir(parents=True, exist_ok=True)
    m = data.read_matrix(ds, "train", burn_in=True, spill_to=scratch / "base.npy")
    n, cols = len(m.x), m.x.shape[1]
    y = np.tile(m.meta["y"].to_numpy(), k)
    names = m.names
    big = scratch / "big.npy"
    probe = np.lib.format.open_memmap(big, mode="w+", dtype=np.float32, shape=(n * k, cols))
    offset = int(probe.offset)
    del probe
    fd = os.open(big, os.O_WRONLY)
    chunk = 250_000
    for rep_i in range(k):
        for a in range(0, n, chunk):
            block = m.x.rows(a, a + chunk)
            os.pwrite(fd, memoryview(block).cast("B"), offset + (rep_i * n + a) * cols * 4)
    os.close(fd)
    m.close()
    big_rows = data.RowFile(big)
    rows, x_mb, loaded = n * k, n * k * cols * 4 / 1e6, _peak_mb()
    t1 = time.perf_counter()
    d = xgb.QuantileDMatrix(train.ChunkIter(big_rows, y, names, train.ITER_BATCH_ROWS))
    dmatrix = _peak_mb()
    t2 = time.perf_counter()
    big_rows.close()
    gc.collect()
    xgb.train(train.params_for("unweighted", 0, 1.0), d, num_boost_round=10, verbose_eval=False)
    import shutil

    shutil.rmtree(scratch, ignore_errors=True)
    return {
        "factor": k,
        "rows": rows,
        "x_float32_mb_on_disk": round(x_mb),
        "peak_rss_mb": {
            "after_imports": round(start),
            "after_spill": round(loaded),
            "after_dmatrix": round(dmatrix),
            "after_10_rounds": round(_peak_mb()),
        },
        "seconds": {
            "spill": round(t1 - t0, 1),
            "dmatrix": round(t2 - t1, 1),
            "ten_rounds": round(time.perf_counter() - t2, 1),
        },
    }


def train_run(ds: str, scratch: Path) -> dict[str, Any]:
    """The real ml.train, into scratch directories, under /usr/bin/time -l."""
    env = {
        **__import__("os").environ,
        "MLFLOW_TRACKING_URI": f"sqlite:///{scratch}/mlflow.db",
        "PYTHONPATH": str(common.ROOT),
    }
    cmd = [
        "/usr/bin/time",
        "-l",
        sys.executable,
        "-m",
        "ml.train",
        "--ds",
        ds,
        "--results-dir",
        str(scratch / "results"),
        "--config-dir",
        str(scratch / "config"),
    ]
    done = subprocess.run(cmd, cwd=scratch, env=env, capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(done.stderr[-2000:])
    return parse_time_l(done.stderr)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", default="hi_small", choices=["hi_small"])
    parser.add_argument("--factors", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--target-rows", type=int, default=0, help="rows to extrapolate to")
    parser.add_argument("--target-basis", default="", help="where target-rows comes from")
    parser.add_argument("--skip-train-run", action="store_true")
    parser.add_argument("--worker", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--mode", choices=["memory", "iterator"], default="memory")
    parser.add_argument("--out-name", default="load_memory.json")
    args = parser.parse_args(argv)
    if args.worker:
        run_worker = worker_iterator if args.mode == "iterator" else worker
        print(json.dumps(run_worker(args.ds, args.worker)))
        return 0

    common.require_data_root()
    scale = []
    for k in args.factors:
        done = subprocess.run(
            [
                sys.executable,
                "-m",
                "eval.load_memory",
                "--ds",
                args.ds,
                "--worker",
                str(k),
                "--mode",
                args.mode,
            ],
            cwd=common.ROOT,
            capture_output=True,
            text=True,
            env={**__import__("os").environ, "PYTHONPATH": str(common.ROOT)},
        )
        if done.returncode != 0:
            raise RuntimeError(done.stderr[-2000:])
        scale.append(json.loads(done.stdout.strip().splitlines()[-1]))
        print(f"k={k}: {scale[-1]['peak_rss_mb']}")
    doc: dict[str, Any] = {
        "dataset": args.ds,
        **common.provenance(),
        "mode": args.mode,
        "note": "scale rows are HI-Small training rows repeated k times, not HI-Medium data",
        "scale": scale,
    }
    pts = [(s["rows"], s["peak_rss_mb"]["after_dmatrix"]) for s in scale[-2:]]
    if args.target_rows and len(pts) == 2:
        doc["extrapolation"] = {
            "method": "straight line through the last two scale points (peak RSS after the DMatrix "
            "build against rows); an extrapolation, not a measurement",
            "points_rows_mb": pts,
            "target_rows": args.target_rows,
            "target_rows_basis": args.target_basis,
            "peak_rss_mb_at_target": round(extrapolate(pts, args.target_rows)),
        }
    if not args.skip_train_run:
        with tempfile.TemporaryDirectory(prefix="aml_train_run_") as tmp:
            doc["train_run"] = {
                "what": "the real ml.train into scratch results/config and a throwaway MLflow db",
                **train_run(args.ds, Path(tmp)),
            }
    common.write_json(common.ROOT / "results" / "M02" / args.ds / args.out_name, doc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
