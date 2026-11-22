"""Money is int64 minor units and int64 USD cents, via Decimal ROUND_HALF_EVEN (D05).

`features/money.py` is the only Python implementation; exponents come from features/spec.yaml.
"""

import ast
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from features import money

ROOT = Path(__file__).resolve().parents[2]
SPEC = yaml.safe_load((ROOT / "features" / "spec.yaml").read_text())


# --- to_minor ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("amount", "currency", "expected"),
    [
        ("100.00", "USD", 10000),
        ("0.125", "USD", 12),  # 12.5 cents: ties go to the even neighbour (12)
        ("0.135", "USD", 14),  # 13.5 -> 14
        ("0.145", "USD", 14),  # 14.5 -> 14
        ("99.995", "USD", 10000),  # 9999.5 -> 10000
        ("2.5", "JPY", 2),  # JPY has no minor unit
        ("3.5", "JPY", 4),
        ("1000000.00", "JPY", 1000000),
        ("0.00123456", "BTC", 123456),
        ("0.123456785", "BTC", 12345678),  # ...78.5 -> 78
        ("0.123456775", "BTC", 12345678),  # ...77.5 -> 78
        ("123456789012.34", "USD", 12345678901234),
        ("0", "USD", 0),
    ],
)
def test_to_minor_half_even(amount: str, currency: str, expected: int) -> None:
    assert money.to_minor(amount, currency) == expected


@pytest.mark.parametrize("bad", ["-1", "-0.01", "abc", "", "NaN", "Infinity", "1e999999999"])
def test_to_minor_rejects_bad_amounts(bad: str) -> None:
    with pytest.raises(ValueError):
        money.to_minor(bad, "USD")


@pytest.mark.parametrize("not_a_string", [1.5, 2, Decimal("1.00"), None])
def test_to_minor_requires_a_string(not_a_string: object) -> None:
    # A float has already lost the value the source file held; refuse it rather than round it.
    with pytest.raises(TypeError):
        money.to_minor(not_a_string, "USD")  # type: ignore[arg-type]


def test_to_minor_rejects_unknown_currency() -> None:
    with pytest.raises(ValueError, match="currency"):
        money.to_minor("1.00", "XXX")


# --- exponents come from the spec -------------------------------------------------------------


def test_exponents_match_spec() -> None:
    assert money.EXPONENTS == SPEC["money"]["exponents"]


def test_every_spec_currency_has_an_exponent() -> None:
    assert set(SPEC["vocabularies"]["currencies"]) <= set(money.EXPONENTS)


# --- minor_to_usd_cents -----------------------------------------------------------------------

FX = {"USD": "1", "EUR": "1.0850", "JPY": "0.0067", "BTC": "50000.00", "GBP": "0.5"}


@pytest.mark.parametrize(
    ("minor", "currency", "expected"),
    [
        (1250, "USD", 1250),
        (1000, "EUR", 1085),  # 10.00 EUR * 1.0850 = 10.85 USD
        (1000, "JPY", 670),  # 1000 JPY * 0.0067 = 6.70 USD
        (100_000_000, "BTC", 5_000_000),  # 1 BTC * 50000
        (5, "GBP", 2),  # 0.05 GBP * 0.5 = 2.5 cents -> 2
        (7, "GBP", 4),  # 3.5 cents -> 4
        (9, "GBP", 4),  # 4.5 cents -> 4
        (0, "EUR", 0),
    ],
)
def test_minor_to_usd_cents(minor: int, currency: str, expected: int) -> None:
    assert money.minor_to_usd_cents(minor, currency, fx=FX) == expected


def test_minor_to_usd_cents_is_exact_for_large_amounts() -> None:
    # 1 BTC at 50000.00 is 1/20 of a cent per satoshi... so this input maps to the odd integer
    # 2**53 + 1, which a float cannot represent (it would round to 2**53).
    target = 2**53 + 1
    got = money.minor_to_usd_cents(target * 20, "BTC", fx={"BTC": "50000.00"})
    assert got == target
    assert float(target) != target  # the control: a float really would have lost it


def test_minor_to_usd_cents_unknown_currency() -> None:
    with pytest.raises(ValueError, match="currency"):
        money.minor_to_usd_cents(100, "XXX", fx=FX)


def test_minor_to_usd_cents_missing_rate() -> None:
    with pytest.raises(ValueError, match="rate"):
        money.minor_to_usd_cents(100, "CHF", fx=FX)


def test_results_are_python_ints() -> None:
    assert type(money.to_minor("1.00", "USD")) is int
    assert type(money.minor_to_usd_cents(100, "USD", fx=FX)) is int


# --- no floats --------------------------------------------------------------------------------


def test_module_uses_no_float() -> None:
    tree = ast.parse(Path(money.__file__).read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, float):
            pytest.fail(f"float literal {node.value!r} at line {node.lineno}")
        if isinstance(node, ast.Name) and node.id == "float" and isinstance(node.ctx, ast.Load):
            # allowed only inside a type annotation that rejects floats, never as a call
            parent_calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and n.func is node]
            assert not parent_calls, f"float() call at line {node.lineno}"


# --- the FX table file ------------------------------------------------------------------------


def _write_fx(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "fx.yaml"
    path.write_text(body)
    return path


def _full_table(version: int = 1) -> str:
    rates = "\n".join(f'  {c}: "1.5"' for c in SPEC["vocabularies"]["currencies"])
    return f"fx_version: {version}\nmethod: test\nrates_to_usd:\n{rates}\n"


def test_load_fx_accepts_a_complete_table(tmp_path: Path) -> None:
    fx = money.load_fx(_write_fx(tmp_path, _full_table()))
    assert set(fx) == set(SPEC["vocabularies"]["currencies"])
    assert all(isinstance(v, Decimal) for v in fx.values())


def test_load_fx_rejects_a_wrong_version(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="fx_version"):
        money.load_fx(_write_fx(tmp_path, _full_table(version=2)))


def test_load_fx_rejects_a_missing_currency(tmp_path: Path) -> None:
    body = _full_table().replace('  BTC: "1.5"\n', "")
    with pytest.raises(ValueError, match="BTC"):
        money.load_fx(_write_fx(tmp_path, body))


def test_load_fx_rejects_unquoted_numbers(tmp_path: Path) -> None:
    # YAML reads 1.5 as a float; rates must be quoted decimal strings so no digit is lost.
    body = _full_table().replace('EUR: "1.5"', "EUR: 1.5")
    with pytest.raises(ValueError, match="decimal string"):
        money.load_fx(_write_fx(tmp_path, body))


@pytest.mark.xfail(
    strict=True,
    reason="features/fx_rates.yaml is frozen in M01 step 6, after the user approves the variance "
    "table. strict=True: when the file lands, this must be turned into a plain test.",
)
def test_frozen_fx_table_is_complete() -> None:
    fx = money.load_fx()
    assert set(fx) == set(SPEC["vocabularies"]["currencies"])
