"""Recompute every row's money with `features/money.py` and compare with the Iceberg table.

Not Spark: DuckDB reads the table's Parquet files and the source CSV (a second engine, and the
Python implementation), so it checks the whole pipeline end to end rather than itself. For every
row it checks (a) `amount_*_minor` against `money.to_minor` of the source text, matched by
`src_row`, and (b) `amount_*_usd_cents` against `money.minor_to_usd_cents` with the frozen FX
table. Writes results/M01/<ds>/money_parity.json (the number D36 quotes).
"""

import argparse
import hashlib
from pathlib import Path
from typing import Any

from batch.spark import common, csv_to_iceberg
from features import money

MAX_EXAMPLES = 5
_CHUNK = 200_000


def check_money(
    table_data_dir: Path, trans_csv: Path, fx: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Compare the table under `table_data_dir` with `trans_csv`; returns counts and examples."""
    fx = money.load_fx() if fx is None else fx
    con = common.duckdb_connection()
    # `[0-9]*.parquet` skips the `._*` AppleDouble files exFAT leaves next to the data files
    con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{table_data_dir}/*/[0-9]*.parquet')")
    con.execute(
        "CREATE TABLE s AS SELECT row_number() OVER () - 1 AS src_row, paid, recv "
        f"FROM read_csv('{trans_csv}', header=false, skip=1, all_varchar=true, parallel=false, "
        "names=['ts','fb','fa','tb','ta','recv','rccy','paid','pccy','fmt','l'])"
    )
    cursor = con.execute(
        "SELECT t.tx_id, t.src_row, s.paid, t.payment_currency, t.amount_paid_minor, "
        "t.amount_paid_usd_cents, s.recv, t.receiving_currency, t.amount_received_minor, "
        "t.amount_received_usd_cents FROM t JOIN s USING (src_row)"
    )
    counts = {"rows": 0, "minor_paid": 0, "minor_received": 0, "cents_paid": 0, "cents_received": 0}
    examples: list[dict[str, Any]] = []
    while chunk := cursor.fetchmany(_CHUNK):
        for tx, src, paid, pccy, pminor, pcents, recv, rccy, rminor, rcents in chunk:
            counts["rows"] += 1
            wrong = []
            if money.to_minor(paid, pccy) != pminor:
                counts["minor_paid"] += 1
                wrong.append("minor_paid")
            if money.to_minor(recv, rccy) != rminor:
                counts["minor_received"] += 1
                wrong.append("minor_received")
            if money.minor_to_usd_cents(pminor, pccy, fx=fx) != pcents:
                counts["cents_paid"] += 1
                wrong.append("cents_paid")
            if money.minor_to_usd_cents(rminor, rccy, fx=fx) != rcents:
                counts["cents_received"] += 1
                wrong.append("cents_received")
            if wrong and len(examples) < MAX_EXAMPLES:
                examples.append({"tx_id": tx, "src_row": src, "wrong": wrong, "paid": paid,
                                 "payment_currency": pccy, "recv": recv,
                                 "receiving_currency": rccy})  # fmt: skip
    return {"rows_compared": counts.pop("rows"), "mismatches": counts, "examples": examples}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", required=True, choices=sorted(csv_to_iceberg.DATASET_FILES))
    parser.add_argument("--results-dir", type=Path, default=common.ROOT / "results" / "M01")
    args = parser.parse_args(argv)
    common.require_data_root()
    data_dir = common.WAREHOUSE / f"aml_{args.ds}" / "raw_transactions" / "data"
    trans = common.RAW_DIR / csv_to_iceberg.DATASET_FILES[args.ds][0]
    result = check_money(data_dir, trans)
    doc = {
        **common.provenance(),
        "dataset": args.ds,
        "config": {
            "table_data_dir": str(data_dir),
            "source": str(trans),
            "fx_rates_sha256": hashlib.sha256(money.FX_PATH.read_bytes()).hexdigest(),
            "fx_version": money.FX_VERSION_REQUIRED,
        },
        **result,
    }
    common.write_json(args.results_dir / args.ds / "money_parity.json", doc)
    total = sum(result["mismatches"].values())
    print(f"{args.ds}: compared {result['rows_compared']:,} rows, mismatches {total}")
    if total:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
