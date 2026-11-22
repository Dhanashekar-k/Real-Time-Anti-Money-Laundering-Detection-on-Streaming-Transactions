"""Run a command and record its wall time and the memory of its WHOLE process tree.

    uv run python -m eval.run_measured --label features \
        --out results/M02/hi_medium/run_features.json -- make features DS=hi_medium

`/usr/bin/time -l` reports the largest single process; a Spark job is a JVM plus several Python
workers, so the figure that matters for a 24 GB machine is the sum over the tree, sampled with `ps`
(same sampler as eval/memory_gate.py). RSS counts pages shared by forked workers once per worker,
so the sum can overstate the true total; the system-wide used memory and the swap use, sampled with
`vm_stat` and `sysctl`, are recorded next to it and include whatever else the Mac is doing.
The command's exit code is returned.
"""

import argparse
import json
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path

from batch.spark import common
from eval.memory_gate import Sampler, peak_summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--label", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("give the command after --")

    started = time.perf_counter()
    proc = subprocess.Popen(command, cwd=common.ROOT)
    sampler = Sampler(proc.pid, interval=args.interval)
    sampler.start()
    try:
        code = proc.wait()
    finally:
        sampler.stop_flag.set()
        sampler.join()
    seconds = time.perf_counter() - started
    samples = sampler.samples
    doc = {
        "label": args.label,
        **common.provenance(),
        "command": command,
        "returncode": code,
        "seconds": round(seconds, 1),
        "samples": len(samples),
        "peak_process_tree_mb": {k: round(v, 1) for k, v in peak_summary(samples).items()},
        "system": {
            "used_mb_before": round(samples[0]["system_used_mb"], 1),
            "used_mb_peak": round(max(s["system_used_mb"] for s in samples), 1),
            "swap_used_mb_before": samples[0]["swap_used_mb"],
            "swap_used_mb_peak": max(s["swap_used_mb"] for s in samples),
        },
        "note": "RSS summed over the process tree; shared pages are counted once per process",
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps({k: doc[k] for k in ("label", "returncode", "seconds", "peak_process_tree_mb")})
    )
    return code


if __name__ == "__main__":
    sys.exit(main())
