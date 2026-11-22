"""The Spark-native money conversion must give integers identical to features/money.py.

Needs a JVM, not the compose stack (no catalog). 1M random rows, with forced half-way ties.
"""

import random
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from batch.spark import common
from features import money

pytestmark = [pytest.mark.integration, pytest.mark.slow]

N = 1_000_000
FX = {
    "USD": "1", "EUR": "1.0850", "GBP": "1.2500", "CHF": "1.1000", "JPY": "0.0067",
    "CNY": "0.14", "INR": "0.012", "CAD": "0.73", "AUD": "0.65", "MXN": "0.05", "BRL": "0.2",
    "RUB": "0.011", "SAR": "0.2667", "ILS": "0.27", "BTC": "50000.00",
}  # fmt: skip


def _random_amount(rng: random.Random, currency: str) -> str:
    exp = money.EXPONENTS[currency]
    # Stay inside int64 for both minor units and USD cents (BTC: 1e8 satoshi per coin, times a
    # 50000 rate, times 100). Out-of-range values have their own test below.
    max_whole = 10**7 if currency == "BTC" else 10**11
    whole = int(max_whole * rng.random() ** rng.choice([1, 2, 4, 8]))
    if rng.random() < 0.3:
        # Force a half-way tie one digit past the currency's precision: x.xx5, x.5, ...
        frac = "".join(rng.choice("0123456789") for _ in range(exp)) + "5"
        return f"{whole}.{frac}"
    digits = rng.randrange(0, exp + 4)
    frac = "".join(rng.choice("0123456789") for _ in range(digits))
    return f"{whole}.{frac}" if frac else str(whole)


@pytest.fixture(scope="module")
def rows() -> list[tuple[int, str, str]]:
    rng = random.Random(20261121)  # fixed seed: a failure is reproducible
    currencies = list(money.EXPONENTS)
    out = []
    for i in range(N):
        currency = rng.choice(currencies)
        out.append((i, currency, _random_amount(rng, currency)))
    return out


@pytest.mark.parametrize("table", ["synthetic", "frozen"])
def test_spark_and_python_agree_on_one_million_rows(
    rows: list[tuple[int, str, str]], table: str
) -> None:
    # "frozen" is features/fx_rates.yaml: 15-digit rates, the widest the integer arithmetic sees.
    fx = {k: Decimal(v) for k, v in FX.items()} if table == "synthetic" else money.load_fx()
    spark = common.get_spark("money-parity", catalog=False)
    try:
        df = spark.createDataFrame(rows, "id long, currency string, amount string")
        amount = F.col("amount").cast("decimal(38,18)")
        minor = common.to_minor_col(amount, F.col("currency"))
        cents = common.usd_cents_col(minor, F.col("currency"), fx)
        out = {
            r["id"]: (r["minor"], r["cents"])
            for r in df.select("id", minor.alias("minor"), cents.alias("cents")).collect()
        }
    finally:
        spark.stop()

    assert len(out) == N
    mismatches = []
    for i, currency, text in rows:
        expected_minor = money.to_minor(text, currency)
        expected_cents = money.minor_to_usd_cents(expected_minor, currency, fx=fx)
        if out[i] != (expected_minor, expected_cents):
            mismatches.append((i, currency, text, out[i], (expected_minor, expected_cents)))
    assert not mismatches, f"{len(mismatches)} mismatches, first: {mismatches[:3]}"


def test_out_of_range_values_become_null_in_spark_and_raise_in_python() -> None:
    fx = {"BTC": Decimal("50000.00"), "USD": Decimal("2")}
    cases = [
        ("BTC", "92233720368.54775807"),  # exactly int64 max satoshi: minor ok, cents ~4.6e17 ok
        ("BTC", "92233720368.54775808"),  # one satoshi more: minor out of range
        ("USD", "92233720368547758.07"),  # int64 max cents; rate 2 doubles it: cents out of range
        ("USD", "1.00"),
        ("USD", "-0.125"),  # money.to_minor raises on a negative amount; Spark must not keep it
        ("JPY", "-3"),
    ]
    spark = common.get_spark("money-overflow", catalog=False)
    try:
        df = spark.createDataFrame(cases, "currency string, amount string")
        minor = common.to_minor_col(F.col("amount").cast("decimal(38,18)"), F.col("currency"))
        cents = common.usd_cents_col(minor, F.col("currency"), fx)
        rows = df.select("currency", "amount", minor.alias("minor"), cents.alias("cents")).collect()
    finally:
        spark.stop()
    got = {(r["currency"], r["amount"]): (r["minor"], r["cents"]) for r in rows}
    assert got[cases[0]] == (money.INT64_MAX, 461168601842738790)
    assert got[cases[1]] == (None, None)
    assert got[cases[2]] == (money.INT64_MAX, None)
    assert got[cases[3]] == (100, 200)
    assert got[cases[4]] == (None, None)
    assert got[cases[5]] == (None, None)
    # The Python implementation refuses the same inputs instead of returning NULL.
    assert money.minor_to_usd_cents(money.INT64_MAX, "BTC", fx=fx) == 461168601842738790
    with pytest.raises(ValueError, match="int64"):
        money.to_minor(cases[1][1], "BTC")
    with pytest.raises(ValueError, match="negative"):
        money.to_minor(cases[4][1], "USD")
    with pytest.raises(ValueError, match="int64"):
        money.minor_to_usd_cents(money.INT64_MAX, "USD", fx=fx)
