"""Write features/fx_rates.yaml from a measured results/M01/<ds>/fx_variance.json (M01 step 6).

Pure Python, no Spark. A person reviews the variance table first; this script only turns the
reviewed numbers into the frozen table, so nothing in the YAML is typed by hand.

Method (approved 2026-11-22):
 - The generator uses one constant rate per currency, with USD as the hub. For each fiat currency X
   the amount-weighted rate of the USD>X pair is rounded to the fewest significant digits (at most
   7) that stays within 1e-8 of it: the "recovered round constant" (EUR 0.8534, JPY 105.4, ...).
   If no such constant exists the script stops instead of guessing.
 - BTC has no clean constant (8 decimals against cent-rounded USD), so its constant is the USD>BTC
   amount-weighted rate on rows with paid >= 100 units, rounded to 6 significant digits. The
   all-amounts rate must agree within 1e-6.
 - The table stores the reciprocal (USD per one unit), 15 significant digits, half-even, because
   `features/money.py` multiplies by it.
"""

import argparse
import json
import math
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
FX_PATH = ROOT / "features" / "fx_rates.yaml"

FX_VERSION = 1
RECOVER_TOLERANCE = Decimal("1e-8")
RECOVER_MAX_DIGITS = 7
BTC_DIGITS = 6
BTC_CONSISTENCY = Decimal("1e-6")
RATE_DIGITS = 15

FIAT_BASIS = "recovered round constant"
BTC_BASIS = (
    "estimated from the paid≥100 subset, last digit uncertain: rel_stderr (cv/sqrt(n) of that "
    "subset) is larger than the rounding of the constant; BTC USD cents are exact relative to "
    "this table only"
)


def round_sig(value: Decimal, digits: int) -> Decimal:
    """Round to `digits` significant digits, half-even."""
    with localcontext() as ctx:
        ctx.prec = 60
        return value.quantize(Decimal(1).scaleb(value.adjusted() - digits + 1), ROUND_HALF_EVEN)


def plain(value: Decimal) -> str:
    """No exponent, no trailing zeros: Decimal('0.8534000') -> '0.8534'."""
    return format(value.normalize(), "f")


def recover_constant(weighted: Decimal) -> Decimal | None:
    """The shortest round number within RECOVER_TOLERANCE (relative) of `weighted`, else None."""
    with localcontext() as ctx:
        ctx.prec = 60
        for digits in range(1, RECOVER_MAX_DIGITS + 1):
            candidate = round_sig(weighted, digits)
            if abs(candidate - weighted) / weighted <= RECOVER_TOLERANCE:
                return candidate
    return None


def reciprocal(per_usd: Decimal) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 60
        return round_sig(1 / per_usd, RATE_DIGITS)


def _rel(measured: Decimal, constant: Decimal) -> str:
    with localcontext() as ctx:
        ctx.prec = 60
        return f"{float((measured - constant) / constant):.1e}"


def _btc_uncertainty(pair: dict[str, Any], doc: dict[str, Any], constant: Decimal) -> str:
    """Extra evidence fields for BTC: the standard error of the subset's mean rate (cv/sqrt(n))
    and the independent BTC>USD pair, whose implied BTC-per-USD is 1/weighted_rate."""
    rel_stderr = pair["cv_paid_ge_100"] / math.sqrt(pair["n_paid_ge_100"])
    reverse = next(
        (
            p
            for p in doc["pairs"]
            if p["paid_currency"] == "BTC" and p["received_currency"] == "USD"
        ),
        None,
    )
    out = f', rel_stderr: "{rel_stderr:.1e}"'
    if reverse is not None and reverse["weighted_rate"]:
        with localcontext() as ctx:
            ctx.prec = 60
            implied = 1 / Decimal(reverse["weighted_rate"])  # BTC per USD from BTC>USD rows
            out += (
                f", cross_check_btc_to_usd_rows: {reverse['n']}, "
                f'cross_check_rel_dev: "{float((implied - constant) / constant):.1e}"'
            )
    return out


def build(doc: dict[str, Any], currencies: list[str]) -> str:
    """The YAML text for a fx_variance document. Raises if a currency cannot be frozen honestly."""
    usd_pairs = {
        p["received_currency"]: p for p in doc["pairs"] if p["paid_currency"] == "USD"
    }  # pair USD>X: the rate is "X per USD"
    rates: dict[str, str] = {"USD": "1"}
    evidence: dict[str, str] = {}
    for ccy in currencies:
        if ccy == "USD":
            continue
        pair = usd_pairs.get(ccy)
        if pair is None:
            raise ValueError(f"no USD>{ccy} pair in the variance file")
        weighted = Decimal(pair["weighted_rate"])
        if ccy == "BTC":
            subset = pair["weighted_rate_paid_ge_100"]
            if subset is None:
                raise ValueError("USD>BTC has no rows with paid >= 100 units")
            constant = round_sig(Decimal(subset), BTC_DIGITS)
            if abs(weighted - constant) / constant > BTC_CONSISTENCY:
                raise ValueError(f"USD>BTC all-amounts rate {weighted} disagrees with {constant}")
            basis, measured, rows = BTC_BASIS, Decimal(subset), pair["n_paid_ge_100"]
            extra = _btc_uncertainty(pair, doc, constant)
        else:
            extra = ""
            recovered = recover_constant(weighted)
            if recovered is None:
                raise ValueError(f"USD>{ccy}: no round constant within 1e-8 of {weighted}")
            constant, basis, measured, rows = recovered, FIAT_BASIS, weighted, pair["n"]
        rates[ccy] = plain(reciprocal(constant))
        evidence[ccy] = (
            f'{{basis: "{basis}", per_usd: "{plain(constant)}", '
            f'measured_per_usd: "{plain(round_sig(measured, 12))}", '
            f'rel_dev: "{_rel(measured, constant)}", rows: {rows}, '
            f'cv_paid_ge_100: "{pair["cv_paid_ge_100"]:.1e}"{extra}}}'
        )
    rate_lines = "\n".join(f'  {c}: "{rates[c]}"' for c in currencies)
    evidence_lines = "\n".join(f"  {c}: {evidence[c]}" for c in currencies if c != "USD")
    ds, source = doc["dataset"], f"results/M01/{doc['dataset']}/fx_variance.json"
    return (
        f"# GENERATED by `make fx-freeze DS={ds}` from {source}. Do not edit by hand.\n"
        "# Rates are USD per one unit of the currency, quoted so no digit is lost.\n"
        f"fx_version: {FX_VERSION}\n"
        "method: >-\n"
        "  USD is the hub. Fiat: the USD>X amount-weighted rate recovered to a round constant\n"
        "  (within 1e-8). BTC: USD>BTC amount-weighted rate on rows with paid >= 100, 6 sig.\n"
        f"  digits. rates_to_usd is the reciprocal of per_usd to {RATE_DIGITS} significant\n"
        "  digits, half-even.\n"
        "source:\n"
        f"  dataset: {ds}\n"
        f"  variance_file: {source}\n"
        f"  variance_git_sha: {doc['git_sha']}\n"
        f'  variance_timestamp: "{doc["timestamp"]}"\n'
        f"  cross_currency_rows: {doc['cross_currency_rows']}\n"
        f"rates_to_usd:\n{rate_lines}\n"
        f"evidence:\n{evidence_lines}\n"
    )


BTC_CHECK_SIGMAS = 3


def check(doc: dict[str, Any], frozen: dict[str, Any], currencies: list[str]) -> list[str]:
    """Compare a measured fx_variance document with the frozen table; never writes anything.

    Returns one line per difference (empty list = the measurement agrees with the frozen table).
    Each currency is compared on its own, so one problem does not hide another. Fiat: the constant
    recovered from the measurement must be identical to the frozen one (and recoverable at all).
    BTC: the measured paid>=100 rate must lie within BTC_CHECK_SIGMAS standard errors of the frozen
    constant, where the standard error is that of the difference of two independent estimates,
    sqrt(se_frozen^2 + se_measured^2). `build()`'s own invariant (BTC all-amounts rate within 1e-6
    of its subset constant) belongs to freezing and is deliberately not applied here.
    """
    usd_pairs = {p["received_currency"]: p for p in doc["pairs"] if p["paid_currency"] == "USD"}
    problems: list[str] = []
    for ccy in currencies:
        if ccy == "USD":
            continue
        pair, old = usd_pairs.get(ccy), frozen["evidence"][ccy]
        if pair is None:
            problems.append(f"{ccy}: no USD>{ccy} pair in the measurement")
        elif ccy != "BTC":
            recovered = recover_constant(Decimal(pair["weighted_rate"]))
            if recovered is None:
                problems.append(f"{ccy}: no round constant within 1e-8 of {pair['weighted_rate']}")
            elif plain(recovered) != old["per_usd"]:
                problems.append(
                    f"{ccy}: measured constant {plain(recovered)} != frozen {old['per_usd']}"
                )
        else:
            problems += _check_btc(pair, old)
    return problems


def _check_btc(pair: dict[str, Any], old: dict[str, Any]) -> list[str]:
    subset = pair["weighted_rate_paid_ge_100"]
    if subset is None or not pair["n_paid_ge_100"] or pair["cv_paid_ge_100"] is None:
        return ["BTC: too few rows with paid >= 100 units to compare"]
    with localcontext() as ctx:
        ctx.prec = 60
        rel = float((Decimal(subset) - Decimal(old["per_usd"])) / Decimal(old["per_usd"]))
    se = math.hypot(
        pair["cv_paid_ge_100"] / math.sqrt(pair["n_paid_ge_100"]), float(old["rel_stderr"])
    )
    if abs(rel) > BTC_CHECK_SIGMAS * se:
        return [
            f"BTC: measured {plain(round_sig(Decimal(subset), 12))} differs from frozen "
            f"{old['per_usd']} by {rel:.1e}, more than {BTC_CHECK_SIGMAS} standard errors "
            f"({se:.1e})"
        ]
    return []


def format_check(doc: dict[str, Any], frozen: dict[str, Any], currencies: list[str]) -> str:
    """Per currency: measured vs frozen constant, for the person reading the check."""
    lines = [f"{'ccy':<5}{'measured per_usd':>22}{'frozen per_usd':>20}  rows(measured)"]
    for p in doc["pairs"]:
        ccy = p["received_currency"]
        if p["paid_currency"] != "USD" or ccy not in currencies or ccy == "USD":
            continue
        weighted = p["weighted_rate_paid_ge_100"] if ccy == "BTC" else p["weighted_rate"]
        lines.append(
            f"{ccy:<5}{plain(round_sig(Decimal(weighted), 12)):>22}"
            f"{frozen['evidence'][ccy]['per_usd']:>20}  {p['n']}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    from features import money

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", required=True)
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results" / "M01")
    parser.add_argument("--out", type=Path, default=FX_PATH)
    parser.add_argument(
        "--check",
        action="store_true",
        help="compare the measurement with the frozen table at --out and exit 1 on a difference; "
        "writes nothing",
    )
    args = parser.parse_args(argv)
    doc = json.loads((args.results_dir / args.ds / "fx_variance.json").read_text())
    if args.check:
        frozen = yaml.safe_load(args.out.read_text(encoding="utf-8"))
        print(format_check(doc, frozen, money.CURRENCIES))
        problems = check(doc, frozen, money.CURRENCIES)
        for line in problems:
            print(f"DIFFERENT: {line}")
        print(
            f"{args.ds}: {'DIFFERS from' if problems else 'agrees with'} the frozen table "
            f"(fx_version {frozen['fx_version']}, from {frozen['source']['dataset']}); "
            "nothing written"
        )
        raise SystemExit(1 if problems else 0)
    text = build(doc, money.CURRENCIES)
    args.out.write_text(text, encoding="utf-8")
    print(f"wrote {args.out}")
    print(text)


if __name__ == "__main__":
    main()
