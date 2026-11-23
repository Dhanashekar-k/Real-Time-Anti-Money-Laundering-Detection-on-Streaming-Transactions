"""Onboarding dry run: map a sample file with a bank's YAML, no Kafka, and say what is wrong.

    make onboard-check BANK=b SAMPLE=tests/fixtures/bank_b_sample.jsonl
    uv run python -m stream.connectors.onboard_check --bank b --sample FILE [--json OUT]

One record per line (CSV lines or JSON objects; a UTF-8 byte-order mark and CRLF are fine; a blank
line in the middle is a malformed record). Prints:

- how many records mapped, and the DLQ count and rate by error code;
- field coverage: for each canonical field, how many records it parsed on its own (a field is
  judged independently of the others). Only the field step is judged: what a bank file checks in
  `validate` (Bank B's unknown currency, negative amount) costs a DLQ record but not coverage, while
  what it checks inside a field (Bank A's currency names and amounts) lowers that field's coverage;
- unmapped vocabulary: every value the bank sent that no map or currency list knows, with counts;
- unused source columns (CSV);
- 3 example canonical records and the first 3 failures with their line numbers.

Exit code 1 if any record would go to the DLQ, so a script (and the M10 timing run) can loop until
the sample is clean.
"""

from __future__ import annotations

import argparse
import codecs
import json
from collections import Counter, defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from stream.connectors.engine import BankConfig, MappingError, diagnose, load_config, map_record

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = 3


def read_sample(path: Path) -> list[bytes]:
    """The sample's records as bytes, one per line. No decoding, so bad bytes stay bad bytes."""
    data = Path(path).read_bytes()
    if data.startswith(codecs.BOM_UTF8):
        data = data[len(codecs.BOM_UTF8) :]
    return data.splitlines()


def check_sample(cfg: BankConfig, lines: Sequence[bytes]) -> dict[str, Any]:
    total = len(lines)
    mapped = 0
    by_code: Counter[str] = Counter()
    unmapped: dict[str, Counter[str]] = defaultdict(Counter)
    ok: Counter[str] = Counter()
    canonical: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for number, line in enumerate(lines, start=1):
        result = map_record(line, cfg)
        if isinstance(result, MappingError):
            by_code[result.code] += 1
            if result.code == "UNKNOWN_VALUE" and result.field:
                unmapped[result.field][result.value or "(empty)"] += 1
            if len(failed) < EXAMPLES:
                failed.append(
                    {
                        "line": number,
                        "code": result.code,
                        "field": result.field,
                        "detail": result.detail,
                        "value": result.value,
                    }
                )
        else:
            mapped += 1
            if len(canonical) < EXAMPLES:
                canonical.append(result)
        fields = diagnose(line, cfg)
        if not isinstance(fields, MappingError):
            for name, code in fields.items():
                if code is None:
                    ok[name] += 1
    dropped = total - mapped
    unused = [c for c in cfg.columns if c not in cfg.used_columns and c not in cfg.ignored]
    return {
        "bank": cfg.bank_id,
        "mapping_version": cfg.mapping_version,
        "records": total,
        "mapped": mapped,
        "mapped_pct": 100 * mapped / total if total else None,
        "dlq": {
            "count": dropped,
            "rate": dropped / total if total else None,
            "by_code": dict(by_code),
        },
        "field_coverage": {
            name: {"ok": ok[name], "of": total, "pct": 100 * ok[name] / total if total else None}
            for name, _ in cfg.steps
        },
        "unmapped_values": {f: dict(c) for f, c in unmapped.items()},
        "unused_source_columns": unused if cfg.fmt == "csv" else [],
        "examples": {"canonical": canonical, "failed": failed},
    }


def format_report(report: dict[str, Any]) -> str:
    out: list[str] = []
    dlq = report["dlq"]
    rate = None if dlq["rate"] is None else 100 * dlq["rate"]
    out.append(
        f"Bank {report['bank']} (mapping {report['mapping_version']}): "
        f"{report['records']} records, {report['mapped']} mapped ({_pct(report['mapped_pct'])}), "
        f"{dlq['count']} would go to the DLQ ({_pct(rate)})"
    )
    if dlq["by_code"]:
        out.append("\nDLQ by error code:")
        out += [f"  {code:<24} {n}" for code, n in sorted(dlq["by_code"].items())]
    out.append(
        "\nField coverage (each field parsed on its own; validation failures are not shown here):"
    )
    for name, c in report["field_coverage"].items():
        out.append(f"  {name:<24} {c['ok']:>7} / {c['of']:<7} {_pct(c['pct'])}")
    if report["unmapped_values"]:
        out.append("\nUnmapped vocabulary (add to value_maps, or ask the bank):")
        for field, values in report["unmapped_values"].items():
            out += [f"  {field}: {value!r} x {n}" for value, n in sorted(values.items())]
    if report["unused_source_columns"]:
        out.append(
            f"\nSource columns that nothing reads: {', '.join(report['unused_source_columns'])}"
        )
    out.append(f"\nFirst {len(report['examples']['canonical'])} canonical records:")
    out += ["  " + json.dumps(r, sort_keys=True) for r in report["examples"]["canonical"]]
    if report["examples"]["failed"]:
        out.append("\nFirst failures:")
        for f in report["examples"]["failed"]:
            where = f" [{f['field']}]" if f["field"] else ""
            out.append(f"  line {f['line']}: {f['code']}{where} {f['detail']}")
    return "\n".join(out)


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}%"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--bank", required=True, help="a, b, ... (reads connectors/banks/bank_<x>.yaml)"
    )
    parser.add_argument("--sample", required=True, type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--json", type=Path, help="also write the report here")
    args = parser.parse_args(argv)
    path = args.config or ROOT / "connectors" / "banks" / f"bank_{args.bank.lower()}.yaml"
    report = check_sample(load_config(path), read_sample(args.sample))
    print(format_report(report))
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 1 if report["dlq"]["count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
