"""EDA of `aml_<ds>.raw_transactions` and `.patterns`: results/M01/<ds>/eda.json, docs/eda_<ds>.md.

Every number in the markdown comes from the JSON, which a script wrote (CLAUDE.md rule 1); the
prose around the tables states only how a number was defined.

Definitions that matter downstream (they size Spark `applyInPandas` and Flink state):
 - leg: one transaction contributes an OUT leg to the sender and an IN leg to the receiver, so a
   self-transfer is two legs on the same account.
 - transactions per account: distinct `tx_id` over both roles (a self-transfer counts once).
 - max entries in the retention window: the largest number of legs one account has inside any
   window of `windows.state_retention_ms` (spec.yaml), `e.ts > t - W` and `e.ts <= t`; legs with
   the same timestamp as `t` are all counted, so it is an upper bound on Flink's per-key state.
 - sparse days: a leading or trailing run of days whose row count is below `SPARSE_FRACTION` of the
   reference daily row count, the median of the busier half of the days (a plain median breaks
   once half the days or more are sparse; the manual asks whether a sparse tail of extra days
   exists).
"""

import argparse
import math
import statistics
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from batch.spark import common, csv_to_iceberg
from features import money

SPARSE_FRACTION = 0.1
TOP_ACCOUNTS = 5
PERCENTILES = (0.5, 0.9, 0.99, 0.999)
_SPEC_PATH = common.ROOT / "features" / "spec.yaml"


# --- pure helpers (unit-tested without Spark) -----------------------------------------------------


def nearest_rank(sorted_values: list[int], p: float) -> int:
    """Nearest-rank percentile of an ascending list: the value at rank ceil(p * n)."""
    if not sorted_values:
        raise ValueError("no values")
    rank = max(1, math.ceil(p * len(sorted_values)))
    return sorted_values[rank - 1]


def nearest_rank_hist(histogram: list[tuple[int, int]], p: float) -> int:
    """`nearest_rank` on a histogram of (value, frequency) pairs, without expanding it.

    The result equals nearest_rank(sorted(expanded values), p): the value at rank ceil(p * n),
    where n is the sum of the frequencies. Lets the driver hold one row per distinct count instead
    of one per account.
    """
    n = sum(freq for _, freq in histogram)
    if n == 0:
        raise ValueError("no values")
    rank = max(1, math.ceil(p * n))
    seen = 0
    for value, freq in sorted(histogram):
        seen += freq
        if seen >= rank:
            return value
    raise AssertionError("unreachable: rank <= n")


def find_sparse_days(days: list[tuple[str, int]], fraction: float = SPARSE_FRACTION) -> dict:
    """Leading and trailing runs of days (ascending) with rows < fraction * reference daily rows."""
    if not days:
        raise ValueError("no days")
    counts = sorted(count for _, count in days)
    reference = statistics.median(counts[len(counts) // 2 :])  # median of the busier half
    threshold = reference * fraction
    leading: list[str] = []
    for day, count in days:
        if count >= threshold:
            break
        leading.append(day)
    trailing: list[str] = []
    for day, count in reversed(days[len(leading) :]):
        if count >= threshold:
            break
        trailing.append(day)
    return {
        "fraction": fraction,
        "reference_rows_per_day": reference,
        "threshold_rows": threshold,
        "leading": leading,
        "trailing": list(reversed(trailing)),
    }


def sparse_day_totals(sparse: dict, per_day: list[dict[str, Any]]) -> dict[str, Any]:
    """Rows and laundering rows inside the sparse leading and trailing runs, and their rate."""
    by_day = {d["day"]: d for d in per_day}
    out: dict[str, Any] = {}
    for run in ("leading", "trailing"):
        rows = sum(by_day[day]["rows"] for day in sparse[run])
        laundering = sum(by_day[day]["laundering"] for day in sparse[run])
        out[run] = {"days": len(sparse[run]), "rows": rows, "laundering": laundering,
                    "laundering_rate": _rate(laundering, rows)}  # fmt: skip
    return out


def _rate(laundering: int, rows: int) -> float:
    return laundering / rows if rows else 0.0


def _group(df: DataFrame, column: str) -> list[dict[str, Any]]:
    rows = (
        df.groupBy(column)
        .agg(F.count("*").alias("rows"), F.sum("is_laundering").alias("laundering"))
        .collect()
    )
    out = [
        {
            "key": r[column],
            "rows": r["rows"],
            "laundering": int(r["laundering"]),
            "laundering_rate": _rate(int(r["laundering"]), r["rows"]),
        }
        for r in rows
    ]
    return sorted(out, key=lambda d: (-d["rows"], str(d["key"])))


# --- sections -------------------------------------------------------------------------------------


def _totals(df: DataFrame) -> dict[str, Any]:
    row = df.agg(
        F.count("*").alias("rows"),
        F.sum("is_laundering").alias("laundering"),
        F.min("event_ts_ms").alias("first_ts_ms"),
        F.max("event_ts_ms").alias("last_ts_ms"),
        F.countDistinct("sender_bank").alias("sender_banks"),
        F.countDistinct("receiver_bank").alias("receiver_banks"),
    ).first()
    rows, laundering = row["rows"], int(row["laundering"])
    return {
        "rows": rows,
        "laundering": laundering,
        "laundering_rate": _rate(laundering, rows),
        "one_laundering_in": (rows / laundering) if laundering else None,
        "first_ts_ms": row["first_ts_ms"],
        "last_ts_ms": row["last_ts_ms"],
        "sender_banks": row["sender_banks"],
        "receiver_banks": row["receiver_banks"],
    }


def _per_day(df: DataFrame) -> list[dict[str, Any]]:
    rows = (
        df.groupBy(F.date_format("event_ts", "yyyy-MM-dd").alias("day"))
        .agg(
            F.count("*").alias("rows"),
            F.sum("is_laundering").alias("laundering"),
            F.min("event_ts_ms").alias("first_ts_ms"),
            F.max("event_ts_ms").alias("last_ts_ms"),
        )
        .orderBy("day")
        .collect()
    )
    return [
        {
            "day": r["day"],
            "rows": r["rows"],
            "laundering": int(r["laundering"]),
            "first_ts_ms": r["first_ts_ms"],
            "last_ts_ms": r["last_ts_ms"],
        }
        for r in rows
    ]


def _legs(df: DataFrame) -> DataFrame:
    out = df.select(
        F.col("sender_key").alias("account"), "tx_id", "event_ts_ms", F.lit("OUT").alias("role")
    )
    inn = df.select(
        F.col("receiver_key").alias("account"), "tx_id", "event_ts_ms", F.lit("IN").alias("role")
    )
    return out.unionByName(inn)


def window_entries(legs: DataFrame, window_ms: int) -> DataFrame:
    """Per leg: how many legs the same account has in (t - window_ms, t], itself included.

    Start-exclusive, like the spec's `e.event_ts_ms > t - W`; legs with the same timestamp as `t`
    are all counted (an upper bound on the entries a keyed state holds at that moment).
    """
    return legs.withColumn(
        "entries",
        F.count("*").over(
            Window.partitionBy("account").orderBy("event_ts_ms").rangeBetween(-(window_ms - 1), 0)
        ),
    )


def _accounts(df: DataFrame) -> dict[str, Any]:
    legs = _legs(df)
    per_account = (
        legs.groupBy("account")
        .agg(F.countDistinct("tx_id").alias("tx"), F.count("*").alias("legs"))
        .persist()
    )
    try:
        distinct = per_account.count()
        tx_hist = [
            (r["tx"], r["n"])
            for r in per_account.groupBy("tx").agg(F.count("*").alias("n")).collect()
        ]
        leg_hist = [
            (r["legs"], r["n"])
            for r in per_account.groupBy("legs").agg(F.count("*").alias("n")).collect()
        ]
        top = per_account.orderBy(F.col("tx").desc(), "account").limit(TOP_ACCOUNTS).collect()
    finally:
        per_account.unpersist()
    window_ms = int(yaml.safe_load(_SPEC_PATH.read_text())["windows"]["state_retention_ms"])
    worst = (
        window_entries(legs, window_ms)
        .groupBy("account")
        .agg(F.max("entries").alias("entries"))
        .orderBy(F.col("entries").desc(), "account")
        .first()
    )
    return {
        "distinct": distinct,
        "distinct_senders": df.select("sender_key").distinct().count(),
        "distinct_receivers": df.select("receiver_key").distinct().count(),
        "tx_per_account": {
            "percentile_method": "nearest rank",
            **{f"p{p * 100:g}": nearest_rank_hist(tx_hist, p) for p in PERCENTILES},
            "max": max(v for v, _ in tx_hist),
            "max_account": top[0]["account"],
            "top": [{"account": r["account"], "tx": r["tx"], "legs": r["legs"]} for r in top],
        },
        "legs_per_account": {
            **{f"p{p * 100:g}": nearest_rank_hist(leg_hist, p) for p in PERCENTILES},
            "max": max(v for v, _ in leg_hist),
        },
        "max_entries_in_retention_window": {
            "window_ms": window_ms,
            "max": worst["entries"],
            "account": worst["account"],
        },
    }


def _self_transfers(df: DataFrame, rows: int) -> dict[str, Any]:
    selfs = df.filter(F.col("sender_key") == F.col("receiver_key"))
    agg = selfs.agg(F.count("*").alias("rows"), F.sum("is_laundering").alias("laundering")).first()
    n = agg["rows"]
    return {
        "rows": n,
        "share": _rate(n, rows),
        "laundering": int(agg["laundering"] or 0),
        "by_payment_format": _group(selfs, "payment_format"),
    }


def _cross_currency(df: DataFrame) -> dict[str, Any]:
    flagged = df.withColumn(
        "cross_currency", F.col("payment_currency") != F.col("receiving_currency")
    )
    return {str(r["key"]).lower(): r for r in _group(flagged, "cross_currency")}


def _patterns(spark: SparkSession, df: DataFrame, namespace: str) -> dict[str, Any]:
    patterns = spark.table(f"lake.{namespace}.patterns")
    labelled = patterns.join(
        df.select("tx_id", "is_laundering"), on="tx_id", how="left"
    ).withColumn("is_laundering", F.coalesce("is_laundering", F.lit(0)))
    by_type = []
    for r in (
        labelled.groupBy("pattern_type")
        .agg(
            F.countDistinct("attempt_id").alias("attempts"),
            F.count("*").alias("lines"),
            F.sum(F.col("tx_id").isNotNull().cast("int")).alias("lines_assigned"),
            F.sum(F.col("ambiguous").cast("int")).alias("ambiguous_lines"),
            F.countDistinct("tx_id").alias("distinct_tx"),
            F.countDistinct(F.when(F.col("is_laundering") == 1, F.col("tx_id"))).alias(
                "distinct_laundering_tx"
            ),
        )
        .collect()
    ):
        by_type.append({k: r[k] for k in r.asDict()})
    by_type.sort(key=lambda d: (-d["lines"], d["pattern_type"]))
    covered = (
        labelled.filter(F.col("tx_id").isNotNull() & (F.col("is_laundering") == 1))
        .select("tx_id")
        .distinct()
        .count()
    )
    total = df.filter("is_laundering = 1").count()
    return {
        "by_type": by_type,
        "attempts": patterns.select("attempt_id").distinct().count(),
        "lines": patterns.count(),
        "laundering_tx_in_some_pattern": covered,
        "laundering_tx_total": total,
        "laundering_tx_in_no_pattern": total - covered,
        "coverage": _rate(covered, total),
    }


def _vocabularies(df: DataFrame) -> dict[str, Any]:
    currencies = {
        r[0]
        for c in ("payment_currency", "receiving_currency")
        for r in df.select(c).distinct().collect()
    }
    formats = {r[0] for r in df.select("payment_format").distinct().collect()}
    spec = yaml.safe_load(_SPEC_PATH.read_text())["vocabularies"]
    return {
        "currencies_seen": sorted(currencies),
        "currencies_not_in_spec": sorted(currencies - set(spec["currencies"])),
        "currencies_in_spec_not_seen": sorted(set(spec["currencies"]) - currencies),
        "formats_seen": sorted(formats),
        "formats_not_in_spec": sorted(formats - set(spec["payment_formats"])),
        "formats_in_spec_not_seen": sorted(set(spec["payment_formats"]) - formats),
    }


def _amounts(df: DataFrame) -> dict[str, Any]:
    def zero_by(minor: str, ccy: str) -> dict[str, int]:
        rows = df.filter(F.col(minor) == 0).groupBy(ccy).count().collect()
        return {r[ccy]: r["count"] for r in sorted(rows, key=lambda r: r[ccy])}

    top = df.agg(
        F.max("amount_paid_usd_cents").alias("paid"),
        F.max("amount_received_usd_cents").alias("recv"),
    ).first()
    return {
        "zero_paid_minor_by_currency": zero_by("amount_paid_minor", "payment_currency"),
        "zero_received_minor_by_currency": zero_by("amount_received_minor", "receiving_currency"),
        "max_paid_usd_cents": top["paid"],
        "max_received_usd_cents": top["recv"],
    }


def source_precision(spark: SparkSession, trans_path: Path) -> dict[str, Any]:
    """Source rows whose amount has more non-zero decimals than the currency exponent allows
    (so converting to minor units rounds), per currency, for the paid and received amounts.
    Read straight from the CSV text: the table only holds the rounded result."""
    names, _ = csv_to_iceberg.value_maps()
    exponents = {c: str(e) for c, e in money.EXPONENTS.items()}
    fields = F.split(F.col("value"), ",")
    lines = spark.read.text(str(trans_path))
    out: dict[str, Any] = {}
    for label, amount_idx, ccy_idx in (("paid", 7, 8), ("received", 5, 6)):
        amount = F.trim(fields[amount_idx])
        decimals = F.regexp_replace(F.regexp_extract(amount, r"^\d+\.(\d+)$", 1), "0+$", "")
        code = csv_to_iceberg.lookup(F.trim(fields[ccy_idx]), names)
        exponent = csv_to_iceberg.lookup(code, exponents).cast("int")
        rows = (
            lines.filter(amount.rlike(r"^\d+(\.\d+)?$"))
            .filter(F.length(decimals) > exponent)
            .groupBy(code.alias("ccy"))
            .count()
            .collect()
        )
        out[f"{label}_over_precision_by_currency"] = {
            r["ccy"]: r["count"] for r in sorted(rows, key=lambda r: r["ccy"])
        }
    return out


# --- driver ---------------------------------------------------------------------------------------


def run_eda(
    spark: SparkSession, *, ds: str, namespace: str, trans_path: Path | None
) -> dict[str, Any]:
    df = spark.table(f"lake.{namespace}.raw_transactions").persist()
    try:
        totals = _totals(df)
        per_day = _per_day(df)
        owners = _group(df, "owner_bank_id")
        for o in owners:
            o["share"] = _rate(o["rows"], totals["rows"])
        sparse = find_sparse_days([(d["day"], d["rows"]) for d in per_day])
        sparse["totals"] = sparse_day_totals(sparse, per_day)
        in_sparse = sparse["leading"] + sparse["trailing"]
        sparse["by_split"] = (
            _group(
                df.filter(F.date_format("event_ts", "yyyy-MM-dd").isin(in_sparse)),
                "split",
            )
            if in_sparse
            else []
        )
        # per-split rows and laundering with the sparse days left out: the base rates the split
        # shows are distorted by them (the tail is all laundering-heavy and all in one split)
        sparse["by_split_without_sparse_days"] = _group(
            df.filter(~F.date_format("event_ts", "yyyy-MM-dd").isin(in_sparse)), "split"
        )
        sparse["by_split_all_days"] = _group(df, "split")
        return {
            **common.provenance(),
            "dataset": ds,
            "config": {
                "table": f"lake.{namespace}.raw_transactions",
                "sparse_fraction": SPARSE_FRACTION,
                "timezone": "UTC",
            },
            "totals": totals,
            "per_day": per_day,
            "sparse_days": sparse,
            "accounts": _accounts(df),
            "self_transfers": _self_transfers(df, totals["rows"]),
            "by_payment_format": _group(df, "payment_format"),
            "by_payment_currency": _group(df, "payment_currency"),
            "by_receiving_currency": _group(df, "receiving_currency"),
            "cross_currency": _cross_currency(df),
            "owner_bank": owners,
            "patterns": _patterns(spark, df, namespace),
            "vocabularies": _vocabularies(df),
            "amounts": _amounts(df),
            "source_precision": source_precision(spark, trans_path) if trans_path else None,
        }
    finally:
        df.unpersist()


# --- markdown (generated from the JSON; no number is typed here) -------------------------------


def _pct(x: float) -> str:
    return f"{x * 100:.3f}%"


def _table(header: list[str], rows: list[list[Any]]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def _rate_table(title: str, key_header: str, rows: list[dict[str, Any]]) -> str:
    body = [
        [r["key"], f"{r['rows']:,}", f"{r['laundering']:,}", _pct(r["laundering_rate"])]
        for r in rows
    ]
    return f"{title}\n\n" + _table([key_header, "rows", "laundering", "rate"], body)


def _paired(all_days: list[dict], kept: list[dict]) -> list[tuple[dict, dict]]:
    """Split rows in train, val, test order: all days beside the days left after the sparse ones."""
    order = {"train": 0, "val": 1, "test": 2}
    by_kept = {r["key"]: r for r in kept}
    empty = {"rows": 0, "laundering": 0, "laundering_rate": 0.0}
    return [
        (a, by_kept.get(a["key"], empty)) for a in sorted(all_days, key=lambda r: order[r["key"]])
    ]


def render_markdown(doc: dict[str, Any]) -> str:
    t, acc, sp = doc["totals"], doc["accounts"], doc["self_transfers"]
    tpa, win = acc["tx_per_account"], acc["max_entries_in_retention_window"]
    sparse = doc["sparse_days"]
    pat = doc["patterns"]
    voc = doc["vocabularies"]
    parts = [
        f"# EDA: {doc['dataset']}",
        f"Generated by `make eda DS={doc['dataset']}` from `results/M01/{doc['dataset']}/eda.json` "
        f"(git {doc['git_sha'][:7]}, {doc['timestamp']}). Do not edit by hand: every number below "
        "comes from that file.",
        "## Totals",
        _table(
            ["", ""],
            [
                ["rows", f"{t['rows']:,}"],
                ["laundering rows", f"{t['laundering']:,}"],
                ["laundering rate", _pct(t["laundering_rate"])],
                ["one laundering row in", f"{t['one_laundering_in']:,.1f}"],
                [
                    "sender banks / receiver banks",
                    f"{t['sender_banks']:,} / {t['receiver_banks']:,}",
                ],
                ["first / last event (ms, UTC)", f"{t['first_ts_ms']} / {t['last_ts_ms']}"],
            ],
        ),
        "## Transactions per day (UTC)",
        _table(
            ["day", "rows", "laundering", "first event (ms)", "last event (ms)"],
            [
                [
                    d["day"],
                    f"{d['rows']:,}",
                    f"{d['laundering']:,}",
                    d["first_ts_ms"],
                    d["last_ts_ms"],
                ]
                for d in doc["per_day"]
            ],
        ),
        f"Sparse-day rule: a leading or trailing run of days below {sparse['fraction']:g} x the "
        f"reference daily row count ({sparse['reference_rows_per_day']:,.1f}, the median of the "
        f"busier half of the days), i.e. fewer than "
        f"{sparse['threshold_rows']:,.1f} rows. Leading sparse days: "
        f"{', '.join(sparse['leading']) or 'none'}. Trailing sparse days: "
        f"{', '.join(sparse['trailing']) or 'none'}.",
        _table(
            ["sparse run", "days", "rows", "laundering", "rate"],
            [
                [
                    run,
                    t["days"],
                    f"{t['rows']:,}",
                    f"{t['laundering']:,}",
                    _pct(t["laundering_rate"]),
                ]
                for run, t in sparse["totals"].items()
            ],
        ),
        "Sparse-day rows by split:",
        _table(
            ["split", "rows", "laundering", "rate"],
            [
                [r["key"], f"{r['rows']:,}", f"{r['laundering']:,}", _pct(r["laundering_rate"])]
                for r in sparse["by_split"]
            ]
            or [["none", 0, 0, _pct(0.0)]],
        ),
        "Rows and laundering per split, all days and without the sparse days:",
        _table(
            [
                "split",
                "rows (all)",
                "laundering (all)",
                "rate (all)",
                "rows (no sparse days)",
                "laundering (no sparse days)",
                "rate (no sparse days)",
            ],
            [
                [
                    a["key"],
                    f"{a['rows']:,}",
                    f"{a['laundering']:,}",
                    _pct(a["laundering_rate"]),
                    f"{b['rows']:,}",
                    f"{b['laundering']:,}",
                    _pct(b["laundering_rate"]),
                ]
                for a, b in _paired(
                    sparse["by_split_all_days"], sparse["by_split_without_sparse_days"]
                )
            ],
        ),
        "## Accounts",
        _table(
            ["", ""],
            [
                ["distinct accounts (bank:account)", f"{acc['distinct']:,}"],
                [
                    "distinct senders / receivers",
                    f"{acc['distinct_senders']:,} / {acc['distinct_receivers']:,}",
                ],
                *[
                    [f"transactions per account, {k}", f"{v:,}"]
                    for k, v in tpa.items()
                    if k.startswith("p") and k != "percentile_method"
                ],
                ["transactions per account, max", f"{tpa['max']:,} ({tpa['max_account']})"],
                *[[f"legs per account, {k}", f"{v:,}"] for k, v in acc["legs_per_account"].items()],
                [
                    f"max entries in one account's {win['window_ms'] // 3_600_000} h window",
                    f"{win['max']:,} ({win['account']})",
                ],
            ],
        ),
        f"Percentiles use the {tpa['percentile_method']} method. A leg is one side of a "
        "transaction (a self-transfer is two legs on one account); transactions per account count "
        "distinct `tx_id` over both roles. The window figure is an upper bound on Flink state per "
        "key (legs with the same timestamp are all counted).",
        "Busiest accounts by transactions:",
        _table(
            ["account", "transactions", "legs"],
            [[a["account"], f"{a['tx']:,}", f"{a['legs']:,}"] for a in tpa["top"]],
        ),
        "## Self-transfers (same bank and account on both sides)",
        _table(
            ["", ""],
            [
                ["rows", f"{sp['rows']:,}"],
                ["share of all rows", _pct(sp["share"])],
                ["laundering rows among them", f"{sp['laundering']:,}"],
            ],
        ),
        _rate_table("Self-transfers by payment format", "format", sp["by_payment_format"]),
        "## Laundering rate by group",
        _rate_table("Rate by payment format", "format", doc["by_payment_format"]),
        _rate_table("Rate by payment currency", "currency", doc["by_payment_currency"]),
        _rate_table("Rate by receiving currency", "currency", doc["by_receiving_currency"]),
        _rate_table(
            "Rate by cross-currency (true = paid and received currency differ)",
            "cross-currency",
            list(doc["cross_currency"].values()),
        ),
        "## Owner bank (A/B, stable hash of the sender bank)",
        _table(
            ["owner", "rows", "share", "laundering", "rate"],
            [
                [
                    o["key"],
                    f"{o['rows']:,}",
                    _pct(o["share"]),
                    f"{o['laundering']:,}",
                    _pct(o["laundering_rate"]),
                ]
                for o in doc["owner_bank"]
            ],
        ),
        "## Patterns",
        f"{pat['attempts']:,} attempts, {pat['lines']:,} pattern lines. Laundering transactions in "
        f"some pattern: {pat['laundering_tx_in_some_pattern']:,} of {pat['laundering_tx_total']:,} "
        f"({_pct(pat['coverage'])}); in no pattern: {pat['laundering_tx_in_no_pattern']:,}.",
        _table(
            [
                "type",
                "attempts",
                "lines",
                "lines assigned",
                "ambiguous lines",
                "distinct tx",
                "of which laundering",
            ],
            [
                [
                    p["pattern_type"],
                    p["attempts"],
                    f"{p['lines']:,}",
                    f"{p['lines_assigned']:,}",
                    p["ambiguous_lines"],
                    f"{p['distinct_tx']:,}",
                    f"{p['distinct_laundering_tx']:,}",
                ]
                for p in pat["by_type"]
            ],
        ),
        "## Vocabularies against `features/spec.yaml`",
        _table(
            ["", "currencies", "payment formats"],
            [
                ["seen", ", ".join(voc["currencies_seen"]), ", ".join(voc["formats_seen"])],
                [
                    "seen, not in spec",
                    ", ".join(voc["currencies_not_in_spec"]) or "none",
                    ", ".join(voc["formats_not_in_spec"]) or "none",
                ],
                [
                    "in spec, not seen",
                    ", ".join(voc["currencies_in_spec_not_seen"]) or "none",
                    ", ".join(voc["formats_in_spec_not_seen"]) or "none",
                ],
            ],
        ),
        "## Amounts",
        _table(
            ["", ""],
            [
                [
                    "rows paid 0 minor units, by currency",
                    json_inline(doc["amounts"]["zero_paid_minor_by_currency"]),
                ],
                [
                    "rows received 0 minor units, by currency",
                    json_inline(doc["amounts"]["zero_received_minor_by_currency"]),
                ],
                ["largest paid amount (USD cents)", f"{doc['amounts']['max_paid_usd_cents']:,}"],
                [
                    "largest received amount (USD cents)",
                    f"{doc['amounts']['max_received_usd_cents']:,}",
                ],
            ],
        ),
    ]
    prec = doc.get("source_precision")
    if prec:
        parts += [
            "Source rows with more non-zero decimals than the currency exponent allows (converting "
            "to minor units rounds them, half-even):",
            _table(
                ["", "by currency"],
                [
                    ["paid amount", json_inline(prec["paid_over_precision_by_currency"])],
                    ["received amount", json_inline(prec["received_over_precision_by_currency"])],
                ],
            ),
        ]
    return "\n\n".join(parts) + "\n"


def json_inline(mapping: dict[str, int]) -> str:
    return ", ".join(f"{k}: {v:,}" for k, v in mapping.items()) or "none"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", required=True, choices=sorted(csv_to_iceberg.DATASET_FILES))
    parser.add_argument("--trans", type=Path, help="override the transactions CSV path")
    parser.add_argument("--results-dir", type=Path, default=common.ROOT / "results" / "M01")
    parser.add_argument("--docs-dir", type=Path, default=common.ROOT / "docs")
    args = parser.parse_args(argv)
    common.require_data_root()
    trans = args.trans or common.RAW_DIR / csv_to_iceberg.DATASET_FILES[args.ds][0]
    spark = common.get_spark(f"eda-{args.ds}")
    try:
        doc = run_eda(spark, ds=args.ds, namespace=f"aml_{args.ds}", trans_path=trans)
    finally:
        spark.stop()
    common.write_json(args.results_dir / args.ds / "eda.json", doc)
    args.docs_dir.mkdir(parents=True, exist_ok=True)
    (args.docs_dir / f"eda_{args.ds}.md").write_text(render_markdown(doc), encoding="utf-8")
    t, tpa = doc["totals"], doc["accounts"]["tx_per_account"]
    print(
        f"{args.ds}: rows={t['rows']:,} laundering={t['laundering']:,} "
        f"accounts={doc['accounts']['distinct']:,} max_tx_per_account={tpa['max']:,}"
    )


if __name__ == "__main__":
    main()
