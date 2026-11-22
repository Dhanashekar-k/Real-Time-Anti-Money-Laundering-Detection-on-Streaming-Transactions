"""Money: int64 minor units and int64 USD cents, via Decimal ROUND_HALF_EVEN (D05).

This is the only Python implementation (batch jobs and, from M3, the connectors import it).
Flink never converts money; it reads USD cents from `tx.canonical`. The Spark-native version in
`batch/spark/common.py` must give identical integers (tested on random rows).

The exponents come from `features/spec.yaml` so there is one place to change them. There are no
floats anywhere in this module: amounts arrive as strings, because a float has already lost digits
the source file held.
"""

from collections.abc import Mapping
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext
from pathlib import Path

import yaml

_FEATURES_DIR = Path(__file__).resolve().parent
SPEC_PATH = _FEATURES_DIR / "spec.yaml"
FX_PATH = _FEATURES_DIR / "fx_rates.yaml"

INT64_MAX = 2**63 - 1  # the spec says int64: larger values are errors, never silently wrapped

# BTC amounts up to ~1e12 with 8 decimals, times an FX rate, times 100, need well under 60 digits.
# The default context (28 digits) would make `quantize` raise on large exact values.
_PRECISION = 60


def _load_spec() -> dict:
    return yaml.safe_load(SPEC_PATH.read_text())


_SPEC = _load_spec()
EXPONENTS: dict[str, int] = dict(_SPEC["money"]["exponents"])
CURRENCIES: list[str] = list(_SPEC["vocabularies"]["currencies"])
FX_VERSION_REQUIRED: int = int(_SPEC["fx_version_required"])


def _exponent(currency: str) -> int:
    try:
        return EXPONENTS[currency]
    except KeyError:
        raise ValueError(f"unknown currency {currency!r} (not in spec money.exponents)") from None


def to_minor(amount: str, currency: str) -> int:
    """Amount string -> integer minor units: quantize to the currency exponent, half-even.

    Raises TypeError for anything but a str, ValueError for a negative, non-finite or malformed
    amount or an unknown currency.
    """
    if not isinstance(amount, str):
        raise TypeError(f"amount must be a str, got {type(amount).__name__}")
    exp = _exponent(currency)
    with localcontext() as ctx:
        ctx.prec = _PRECISION
        try:
            value = Decimal(amount)
        except InvalidOperation:
            raise ValueError(f"not a decimal amount: {amount!r}") from None
        if not value.is_finite():
            raise ValueError(f"amount must be finite: {amount!r}")
        if value < 0:
            raise ValueError(f"amount must not be negative: {amount!r}")
        try:
            quantized = value.quantize(Decimal(1).scaleb(-exp), rounding=ROUND_HALF_EVEN)
        except InvalidOperation:
            raise ValueError(f"amount out of range: {amount!r}") from None
        minor = int(quantized.scaleb(exp))
    if minor > INT64_MAX:
        raise ValueError(f"amount exceeds int64 minor units: {amount!r} {currency}")
    return minor


def minor_to_usd_cents(
    minor: int, currency: str, fx: Mapping[str, Decimal | str] | None = None
) -> int:
    """Integer minor units -> integer USD cents: minor / 10^exp * rate * 100, half-even.

    `fx` maps currency -> USD per one unit of the currency; default is the frozen table.
    """
    exp = _exponent(currency)
    if minor < 0 or minor > INT64_MAX:
        raise ValueError(f"minor units out of int64 range: {minor}")
    rates = load_fx() if fx is None else fx
    if currency not in rates:
        raise ValueError(f"no FX rate for currency {currency!r}")
    with localcontext() as ctx:
        ctx.prec = _PRECISION
        usd = Decimal(minor).scaleb(-exp) * Decimal(rates[currency]) * 100
        cents = int(usd.quantize(Decimal(1), rounding=ROUND_HALF_EVEN))
    if cents > INT64_MAX:
        raise ValueError(f"USD cents exceed int64: {minor} {currency}")
    return cents


def load_fx(path: Path = FX_PATH) -> dict[str, Decimal]:
    """Read the frozen FX table. Rates must be quoted decimal strings (YAML floats lose digits)."""
    if not Path(path).exists():
        raise FileNotFoundError(
            f"{path} does not exist. The FX table is frozen in M01 after a person reviews the "
            "variance table: run `make fx DS=hi_small`, review results/M01/hi_small/"
            "fx_variance.json, then freeze it."
        )
    doc = yaml.safe_load(Path(path).read_text())
    if doc.get("fx_version") != FX_VERSION_REQUIRED:
        raise ValueError(
            f"fx_version {doc.get('fx_version')!r} does not match the spec's "
            f"fx_version_required {FX_VERSION_REQUIRED}"
        )
    raw = doc["rates_to_usd"]
    missing = [c for c in CURRENCIES if c not in raw]
    if missing:
        raise ValueError(f"fx table is missing currencies: {', '.join(missing)}")
    rates: dict[str, Decimal] = {}
    for currency, value in raw.items():
        if not isinstance(value, str):
            raise ValueError(f"rate for {currency} must be a quoted decimal string, got {value!r}")
        try:
            rates[currency] = Decimal(value)
        except InvalidOperation:
            raise ValueError(f"rate for {currency} is not a decimal string: {value!r}") from None
    return rates
