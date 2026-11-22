"""The frozen FX table (features/fx_rates.yaml) and the script that writes it.

The script is pure Python: it turns a reviewed fx_variance.json into the table, so the numbers
are reproducible. The committed table must equal what the script produces from the committed
variance file, and every rate must be the reciprocal of its stated constant to 15 digits.
"""

import json
from decimal import Decimal, localcontext
from pathlib import Path

import pytest
import yaml

from batch.spark import fx_freeze
from features import money

ROOT = Path(__file__).resolve().parents[2]
VARIANCE = ROOT / "results" / "M01" / "hi_small" / "fx_variance.json"
FROZEN = yaml.safe_load((ROOT / "features" / "fx_rates.yaml").read_text(encoding="utf-8"))


# --- the frozen file ---------------------------------------------------------------------------


def test_every_rate_is_the_reciprocal_of_its_constant_to_15_significant_digits() -> None:
    assert set(FROZEN["evidence"]) == set(money.CURRENCIES) - {"USD"}
    for ccy, ev in FROZEN["evidence"].items():
        with localcontext() as ctx:
            ctx.prec = 60
            expected = fx_freeze.round_sig(1 / Decimal(ev["per_usd"]), 15)
            stored = Decimal(FROZEN["rates_to_usd"][ccy])
            assert stored == expected, ccy
            assert len(stored.as_tuple().digits) <= 15
            assert abs(stored * Decimal(ev["per_usd"]) - 1) < Decimal("1e-14"), ccy


def test_fx_version_matches_the_spec() -> None:
    assert FROZEN["fx_version"] == money.FX_VERSION_REQUIRED


def test_basis_is_stated_for_every_currency() -> None:
    for ccy, ev in FROZEN["evidence"].items():
        if ccy == "BTC":
            assert ev["basis"] == (
                "estimated from the paid≥100 subset, last digit uncertain (~±4e-7), "
                "immaterial to cents"
            )
        else:
            assert ev["basis"] == "recovered round constant", ccy


def test_every_fiat_constant_is_within_1e8_of_what_was_measured() -> None:
    for ccy, ev in FROZEN["evidence"].items():
        if ccy != "BTC":
            assert abs(float(ev["rel_dev"])) <= 1e-8, ccy


def test_btc_uncertainty_is_what_the_basis_text_says() -> None:
    assert abs(float(FROZEN["evidence"]["BTC"]["rel_dev"])) <= 4e-7


def test_committed_table_is_what_the_script_produces_from_the_variance_file() -> None:
    doc = json.loads(VARIANCE.read_text())
    expected = fx_freeze.build(doc, money.CURRENCIES)
    assert (ROOT / "features" / "fx_rates.yaml").read_text(encoding="utf-8") == expected


# --- the script, on synthetic input ------------------------------------------------------------


def _pair(received: str, weighted: str, **extra: object) -> dict:
    base = {
        "paid_currency": "USD", "received_currency": received, "weighted_rate": weighted,
        "weighted_rate_paid_ge_100": weighted, "n": 100, "n_paid_ge_100": 50,
        "cv_paid_ge_100": 1e-6,
    }  # fmt: skip
    return base | extra


def _doc(**overrides: dict) -> dict:
    pairs = {c: _pair(c, "2.0") for c in money.CURRENCIES if c not in ("USD",)} | overrides
    return {
        "dataset": "t", "git_sha": "abc", "timestamp": "2026-01-01T00:00:00+00:00",
        "cross_currency_rows": 1, "pairs": list(pairs.values()),
    }  # fmt: skip


@pytest.mark.parametrize(
    ("weighted", "expected"),
    [("0.853399999584", "0.8534"), ("105.400000010", "105.4"), ("3.75109998772", "3.7511"),
     ("21.1431000006", "21.1431"), ("2.0", "2")],
)  # fmt: skip
def test_recover_constant(weighted: str, expected: str) -> None:
    assert fx_freeze.plain(fx_freeze.recover_constant(Decimal(weighted))) == expected


def test_recover_constant_refuses_a_number_that_is_not_round() -> None:
    assert fx_freeze.recover_constant(Decimal("1.234567891234")) is None


def test_build_stops_instead_of_guessing() -> None:
    doc = _doc(EUR=_pair("EUR", "0.85341234567"))
    with pytest.raises(ValueError, match="EUR"):
        fx_freeze.build(doc, money.CURRENCIES)


def test_build_requires_every_usd_pair() -> None:
    doc = _doc()
    doc["pairs"] = [p for p in doc["pairs"] if p["received_currency"] != "JPY"]
    with pytest.raises(ValueError, match="JPY"):
        fx_freeze.build(doc, money.CURRENCIES)


def test_btc_uses_the_paid_ge_100_subset_and_checks_the_all_amounts_rate() -> None:
    good = _pair("BTC", "0.0000841607176199", weighted_rate_paid_ge_100="0.0000841606685855")
    table = yaml.safe_load(fx_freeze.build(_doc(BTC=good), money.CURRENCIES))
    assert table["evidence"]["BTC"]["per_usd"] == "0.0000841607"
    assert table["rates_to_usd"]["BTC"] == "11882.0304488912"
    bad = _pair("BTC", "0.0000900000", weighted_rate_paid_ge_100="0.0000841606685855")
    with pytest.raises(ValueError, match="disagrees"):
        fx_freeze.build(_doc(BTC=bad), money.CURRENCIES)


def test_built_table_loads_in_money(tmp_path: Path) -> None:
    path = tmp_path / "fx.yaml"
    path.write_text(fx_freeze.build(_doc(), money.CURRENCIES), encoding="utf-8")
    fx = money.load_fx(path)
    assert fx["USD"] == 1 and fx["EUR"] == Decimal("0.5")
