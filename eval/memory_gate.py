"""Measured memory of the hot-account stage at HI-Medium scale (the 24 GB gate before HI-Medium).

    uv run python -m eval.memory_gate

What it runs: the real legs of HI-Medium's three largest accounts (1,083,821 / 683,233 / 210,234
legs, from results/M01/hi_medium/eda.json), plus every 20th other transaction as background load,
through the real `account_features_df(legs_df(...))` of `batch/spark/features.py`, on local Spark
with the same settings as `make features` (all cores, 10g driver heap), and writes the result to a
no-op sink. The shuffle partition count is chosen so that the three hot accounts fall in three
different tasks that all start at once: the worst case for memory. It does not run the HI-Medium
job; it reads three accounts' transactions and a 5% sample from the lake.

What it measures: the whole process tree (driver, JVM, Python workers) sampled with `ps` every
0.5 s, and system memory and swap with `vm_stat` / `sysctl`. RSS counts pages shared by forked
workers in each worker, so the sum of worker RSS can overstate the true total; the system-wide
`vm_stat` figure does not, but includes whatever else the Mac is doing, so both are reported.

What it does NOT measure: the JVM's own use while shuffling and joining all 64M legs of HI-Medium
(its heap is capped by the driver memory setting; off-heap and spill are not bounded here), and the
join, UDF and Iceberg-write stages. Those are named as unmeasured in the output.
"""

import argparse
import json
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from batch.spark import common, features
from features.spec import load_spec

SCRATCH_DEFAULT = Path("/tmp/aml_memory_gate")  # overridden by --scratch


# --- sampling helpers (unit-tested) -------------------------------------------------------------


def parse_ps(text: str) -> dict[int, tuple[int, int, str]]:
    """`ps -axo pid=,ppid=,rss=,comm=` -> {pid: (ppid, rss_kb, command)}."""
    rows = {}
    for line in text.splitlines():
        parts = line.split(None, 3)
        if len(parts) == 4:
            rows[int(parts[0])] = (int(parts[1]), int(parts[2]), parts[3])
    return rows


def descendants(rows: dict[int, tuple[int, int, str]], root: int) -> set[int]:
    children: dict[int, list[int]] = {}
    for pid, (ppid, _, _) in rows.items():
        children.setdefault(ppid, []).append(pid)
    out, stack = set(), [root]
    while stack:
        pid = stack.pop()
        if pid in rows and pid not in out:
            out.add(pid)
            stack.extend(children.get(pid, []))
    return out


def tree_snapshot(rows: dict[int, tuple[int, int, str]], root: int) -> dict[str, float]:
    kb = 1024.0
    pids = descendants(rows, root)
    jvm = sum(rows[p][1] for p in pids if p != root and "java" in rows[p][2])
    workers = [rows[p][1] for p in pids if p != root and "java" not in rows[p][2]]
    return {
        "total_mb": sum(rows[p][1] for p in pids) / kb,
        "jvm_mb": jvm / kb,
        "python_workers_mb": sum(workers) / kb,
        "python_worker_max_mb": (max(workers) if workers else 0) / kb,
    }


def parse_vm_stat(text: str) -> float:
    """Used memory in MB: active + wired + compressor pages."""
    size = 16384
    pages: dict[str, int] = {}
    for line in text.splitlines():
        if "page size of" in line:
            size = int(line.split("page size of")[1].split("bytes")[0])
        elif ":" in line:
            key, value = line.split(":", 1)
            pages[key.strip()] = int(value.strip().rstrip("."))
    used = pages["Pages active"] + pages["Pages wired down"] + pages["Pages occupied by compressor"]
    return used * size / 2**20


def peak_summary(samples: list[dict[str, float]]) -> dict[str, float]:
    keys = ("total_mb", "jvm_mb", "python_workers_mb", "python_worker_max_mb")
    return {k: max(s[k] for s in samples) for k in keys}


def _swap_used_mb() -> float:
    out = subprocess.run(["sysctl", "-n", "vm.swapusage"], capture_output=True, text=True).stdout
    return float(out.split("used =")[1].split("M")[0])


class Sampler(threading.Thread):
    def __init__(self, root_pid: int, interval: float = 0.5) -> None:
        super().__init__(daemon=True)
        self.root, self.interval = root_pid, interval
        self.stop_flag = threading.Event()
        self.samples: list[dict[str, float]] = []

    def run(self) -> None:
        t0 = time.perf_counter()
        while not self.stop_flag.is_set():
            ps = subprocess.run(
                ["ps", "-axo", "pid=,ppid=,rss=,comm="], capture_output=True, text=True
            ).stdout
            snap = tree_snapshot(parse_ps(ps), self.root)
            vm = subprocess.run(["vm_stat"], capture_output=True, text=True).stdout
            snap["system_used_mb"] = parse_vm_stat(vm)
            snap["swap_used_mb"] = _swap_used_mb()
            snap["t"] = round(time.perf_counter() - t0, 2)
            self.samples.append(snap)
            self.stop_flag.wait(self.interval)


# --- the measurement ----------------------------------------------------------------------------


def top_accounts(ds: str, n: int = 3) -> list[dict[str, Any]]:
    eda = json.loads((common.ROOT / "results" / "M01" / ds / "eda.json").read_text())
    return eda["accounts"]["tx_per_account"]["top"][:n]


def extract_subset(ds: str, accounts: list[str], out: Path, every: int = 20) -> int:
    """Transactions of the hot accounts plus every `every`-th other one, as a Parquet file."""
    data_dir = common.WAREHOUSE / f"aml_{ds}" / "raw_transactions" / "data"
    listed = ", ".join(f"'{a}'" for a in accounts)
    cols = ", ".join(features.RAW_COLUMNS)
    con = common.duckdb_connection()
    try:
        con.execute(
            f"""COPY (SELECT {cols} FROM read_parquet('{data_dir}/*/[0-9]*.parquet')
                      WHERE sender_key IN ({listed}) OR receiver_key IN ({listed})
                         OR tx_id % {every} = 0)
                TO '{out}' (FORMAT PARQUET)"""
        )
        return int(con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0])
    finally:
        con.close()


def choose_partitions(spark: Any, keys: list[str], max_n: int) -> int:
    """Largest n <= max_n for which the hot keys land in n-distinct tasks (pmod(hash(key), n))."""
    for n in range(max_n, 2, -1):
        got = spark.sql(
            "SELECT " + ", ".join(f"pmod(hash('{k}'), {n}) AS p{i}" for i, k in enumerate(keys))
        ).first()
        if len(set(got)) == len(keys):
            return n
    raise RuntimeError("no partition count separates the hot accounts")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", default="hi_medium", choices=["hi_medium"])
    parser.add_argument("--scratch", type=Path, default=SCRATCH_DEFAULT)
    parser.add_argument("--every", type=int, default=20)
    args = parser.parse_args(argv)
    common.require_data_root()
    args.scratch.mkdir(parents=True, exist_ok=True)
    top = top_accounts(args.ds)
    keys = [t["account"] for t in top]
    subset = args.scratch / "subset.parquet"
    n_tx = extract_subset(args.ds, keys, subset, args.every)

    import os

    spec = load_spec()
    sampler = Sampler(os.getpid())
    spark = features.get_session("memory-gate")
    try:
        cores = spark.sparkContext.defaultParallelism
        n_parts = choose_partitions(spark, keys, cores)
        spark.conf.set("spark.sql.shuffle.partitions", str(n_parts))
        raw = spark.read.parquet(str(subset)).select(*features.RAW_COLUMNS)
        legs = features.legs_df(raw)
        stage = features.account_features_df(legs, spec)
        swap_before, used_before = _swap_used_mb(), None
        sampler.start()
        time.sleep(1.5)  # idle JVM baseline
        started = time.perf_counter()
        stage.write.format("noop").mode("overwrite").save()
        seconds = time.perf_counter() - started
        time.sleep(1.0)
    finally:
        sampler.stop_flag.set()
        sampler.join()
        spark.stop()

    samples = sampler.samples
    used_before = samples[0]["system_used_mb"]
    doc = {
        "dataset": args.ds,
        **common.provenance(),
        "machine": {
            "cores_used": cores,
            "ram_gb": int(
                subprocess.run(
                    ["sysctl", "-n", "hw.memsize"], capture_output=True, text=True
                ).stdout
            )
            / 2**30,
            "python": sys.version.split()[0],
        },
        "config": {
            "hot_accounts": [{"account": t["account"], "legs": t["legs"]} for t in top],
            "background": f"every {args.every}th transaction of the dataset",
            "subset_transactions": n_tx,
            "shuffle_partitions": n_parts,
            "driver_memory": "see AML_SPARK_DRIVER_MEMORY (the settings of make features)",
            "stage": "legs -> groupBy(account_key).applyInPandas(oracle) -> noop sink",
        },
        "stage_seconds": round(seconds, 1),
        "peak_process_tree_mb": peak_summary(samples),
        "system": {
            "used_mb_before": round(used_before, 1),
            "used_mb_peak": round(max(s["system_used_mb"] for s in samples), 1),
            "used_mb_peak_minus_before": round(
                max(s["system_used_mb"] for s in samples) - used_before, 1
            ),
            "swap_used_mb_before": swap_before,
            "swap_used_mb_peak": max(s["swap_used_mb"] for s in samples),
        },
        "not_measured": [
            "the JVM while shuffling and joining all 64M legs (heap capped by the driver setting)",
            "the tx-feature, join, UDF and Iceberg-write stages",
        ],
        "timeline": [
            {
                k: round(v, 1)
                for k, v in s.items()
                if k in ("t", "total_mb", "python_workers_mb", "jvm_mb")
            }
            for s in samples[:: max(1, len(samples) // 60)]
        ],
    }
    common.write_json(common.ROOT / "results" / "M02" / args.ds / "memory_gate.json", doc)
    print(
        json.dumps(
            {k: doc[k] for k in ("stage_seconds", "peak_process_tree_mb", "system")}, indent=2
        )
    )


if __name__ == "__main__":
    # run through the importable module: worker functions are pickled by reference
    from eval import memory_gate as _job

    _job.main()
