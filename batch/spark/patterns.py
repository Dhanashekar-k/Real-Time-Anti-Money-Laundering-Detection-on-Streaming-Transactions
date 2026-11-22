"""IBM patterns file -> Iceberg `lake.aml_<ds>.patterns` (M01 step 7).

The file holds blocks `BEGIN LAUNDERING ATTEMPT - <TYPE>[: <detail>]` ... `END LAUNDERING ATTEMPT -
<TYPE>`; each line inside has the same 11 columns as the transactions file. Types are read from the
file, not assumed. Each pattern line is matched to a transaction on every column (timestamp,
sender, receiver, both amounts and currencies, format, label).

Ambiguity: the transactions file can contain exact duplicate rows. A pattern line whose key matches
several transactions cannot be tied to one of them. Policy: the k-th pattern line with a key takes
the k-th transaction (by tx_id) with that key, so a transaction is used at most once, and the line
is flagged `ambiguous`. Coverage is reported two ways (assigned only, and an upper bound that
counts every candidate of an ambiguous key). Lines with no transaction left keep `tx_id` NULL.

Usage: python -m batch.spark.patterns --ds hi_small   (after csv_to_iceberg)
"""

import argparse
import re
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from batch.spark import common, csv_to_iceberg
from features import money

ROOT = common.ROOT

_BEGIN = re.compile(r"^BEGIN LAUNDERING ATTEMPT - (?P<type>[^:]*?)\s*(?::\s*(?P<detail>.*?))?\s*$")
_END = re.compile(r"^END LAUNDERING ATTEMPT - (?P<type>.*?)\s*$")

# Every column of the file, expressed in the typed raw_transactions columns.
MATCH_KEY = [
    "event_ts_ms", "sender_key", "receiver_key", "amount_paid_minor", "payment_currency",
    "amount_received_minor", "receiving_currency", "payment_format", "is_laundering",
]  # fmt: skip


def parse_patterns(path: Path) -> list[dict[str, Any]]:
    """One dict per data line: attempt_id, line_in_attempt, pattern_type, pattern_detail and the
    11 transaction fields as strings under `raw_<name>` (same names as csv_to_iceberg)."""
    rows: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    attempt_id = -1
    line_in_attempt = 0
    for number, text in enumerate(path.read_text().splitlines(), start=1):
        line = text.strip()
        if not line:
            continue
        begin = _BEGIN.match(line)
        if begin:
            if current is not None:
                raise ValueError(
                    f"{path}:{number}: BEGIN inside an unclosed block ({current['type']})"
                )
            attempt_id += 1
            line_in_attempt = 0
            current = {"type": begin["type"], "detail": begin["detail"] or ""}
            continue
        end = _END.match(line)
        if end:
            if current is None:
                raise ValueError(f"{path}:{number}: END without a BEGIN")
            if end["type"] != current["type"]:
                raise ValueError(
                    f"{path}:{number}: END type {end['type']!r} does not match "
                    f"BEGIN {current['type']!r}"
                )
            current = None
            continue
        if current is None:
            raise ValueError(f"{path}:{number}: data line outside a block: {line!r}")
        fields = [f.strip() for f in line.split(",")]
        if len(fields) != csv_to_iceberg.N_FIELDS:
            raise ValueError(
                f"{path}:{number}: expected {csv_to_iceberg.N_FIELDS} fields, got {len(fields)}"
            )
        rows.append(
            {
                "attempt_id": attempt_id,
                "line_in_attempt": line_in_attempt,
                "pattern_type": current["type"],
                "pattern_detail": current["detail"],
                **{f"raw_{n}": v for n, v in zip(csv_to_iceberg.RAW_COLUMNS, fields, strict=True)},
            }
        )
        line_in_attempt += 1
    if current is not None:
        raise ValueError(f"{path}: unclosed block at end of file ({current['type']})")
    return rows


def build_patterns(
    spark: SparkSession,
    *,
    ds: str,
    patterns_path: Path,
    namespace: str,
    fx: Mapping[str, Decimal | str],
    results_dir: Path,
) -> dict[str, Any]:
    """Write `lake.<namespace>.patterns` and `<results_dir>/<ds>/patterns.json`."""
    parsed = parse_patterns(patterns_path)
    if not parsed:
        raise ValueError(f"{patterns_path}: no pattern lines found")

    schema = (
        "src_row long, attempt_id long, line_in_attempt long, pattern_type string, "
        "pattern_detail string, " + ", ".join(f"raw_{n} string" for n in csv_to_iceberg.RAW_COLUMNS)
    )
    lines = spark.createDataFrame([{"src_row": i, **row} for i, row in enumerate(parsed)], schema)
    typed = csv_to_iceberg.build_typed(lines, fx)  # the same parsing as the transactions file
    csv_to_iceberg.check_no_nulls(typed, lines)
    pat = typed.select("src_row", *MATCH_KEY).join(
        lines.select("src_row", "attempt_id", "line_in_attempt", "pattern_type", "pattern_detail"),
        "src_row",
    )
    pat = pat.withColumn(
        "ord",
        F.row_number().over(
            Window.partitionBy(*MATCH_KEY).orderBy("attempt_id", "line_in_attempt")
        ),
    ).withColumn("n_lines", F.count("*").over(Window.partitionBy(*MATCH_KEY)))

    raw_tx = spark.table(f"lake.{namespace}.raw_transactions")
    candidates = raw_tx.join(F.broadcast(pat.select(*MATCH_KEY).distinct()), MATCH_KEY, "left_semi")
    candidates = candidates.select("tx_id", *MATCH_KEY).withColumn(
        "ord", F.row_number().over(Window.partitionBy(*MATCH_KEY).orderBy("tx_id"))
    ).withColumn("n_candidates", F.count("*").over(Window.partitionBy(*MATCH_KEY)))  # fmt: skip
    # The candidate count per key must not depend on a line having a partner at its own `ord`:
    # a line beyond the last candidate has no tx_id but its key still has candidates.
    per_key = candidates.select(*MATCH_KEY, "n_candidates").distinct()

    matched = pat.join(candidates.select("tx_id", "ord", *MATCH_KEY), [*MATCH_KEY, "ord"], "left")
    matched = matched.join(per_key, MATCH_KEY, "left")
    n_candidates = F.coalesce(F.col("n_candidates"), F.lit(0))
    # ambiguous: the key does not identify exactly one transaction for this line, i.e. several
    # candidates, or more pattern lines with this key than transactions. A line whose key matches
    # no transaction at all stays unmatched and not ambiguous.
    ambiguous = (n_candidates > 1) | ((n_candidates >= 1) & (F.col("n_lines") > n_candidates))
    out = matched.select(
        "attempt_id",
        "pattern_type",
        "pattern_detail",
        "line_in_attempt",
        "tx_id",
        ambiguous.alias("ambiguous"),
    )
    table = f"lake.{namespace}.patterns"
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    out.writeTo(table).createOrReplace()

    written = spark.table(table)
    total = written.count()
    if total != len(parsed):
        raise RuntimeError(f"patterns rows {total} != pattern lines in the file {len(parsed)}")
    unmatched = written.filter("tx_id IS NULL").count()
    ambiguous = written.filter("ambiguous").count()

    laundering_total = raw_tx.filter("is_laundering = 1").count()
    assigned = (
        written.filter("tx_id IS NOT NULL").select("tx_id").distinct()
        .join(raw_tx.filter("is_laundering = 1").select("tx_id"), "tx_id").count()
    )  # fmt: skip
    upper_bound = candidates.filter("is_laundering = 1").select("tx_id").distinct().count()

    by_type: dict[str, dict[str, int]] = {}
    for row in parsed:
        entry = by_type.setdefault(row["pattern_type"], {"attempts": 0, "lines": 0})
        entry["lines"] += 1
    for kind in by_type:
        by_type[kind]["attempts"] = len(
            {r["attempt_id"] for r in parsed if r["pattern_type"] == kind}
        )

    summary: dict[str, Any] = {
        **common.provenance(),
        "dataset": ds,
        "config": {"namespace": namespace, "table": table},
        "source": {"path": str(patterns_path)},
        "attempts": len({r["attempt_id"] for r in parsed}),
        "lines": len(parsed),
        "by_type": by_type,
        "unmatched_lines": unmatched,
        "ambiguous_lines": ambiguous,
        "pattern_lines_not_labelled_laundering": sum(r["raw_is_laundering"] != "1" for r in parsed),
        "laundering_total": laundering_total,
        "laundering_in_pattern_assigned": assigned,
        "laundering_in_pattern_upper_bound": upper_bound,
        "laundering_outside_any_pattern_lower_bound": laundering_total - upper_bound,
        "laundering_outside_any_pattern_upper_bound": laundering_total - assigned,
        "coverage_assigned": assigned / laundering_total if laundering_total else None,
        "coverage_upper_bound": upper_bound / laundering_total if laundering_total else None,
    }
    common.write_json(results_dir / ds / "patterns.json", summary)
    return summary


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", required=True, choices=sorted(csv_to_iceberg.DATASET_FILES))
    parser.add_argument("--patterns", type=Path, help="override the patterns file path")
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results" / "M01")
    args = parser.parse_args(argv)
    common.require_data_root()
    path = args.patterns or common.RAW_DIR / csv_to_iceberg.DATASET_FILES[args.ds][1]
    spark = common.get_spark(f"patterns-{args.ds}")
    try:
        summary = build_patterns(
            spark, ds=args.ds, patterns_path=path, namespace=f"aml_{args.ds}",
            fx=money.load_fx(), results_dir=args.results_dir,
        )  # fmt: skip
    finally:
        spark.stop()
    print(f"{args.ds}: {summary['attempts']} attempts, {summary['lines']} lines, "
          f"coverage {summary['coverage_assigned']}")  # fmt: skip


if __name__ == "__main__":
    main()
