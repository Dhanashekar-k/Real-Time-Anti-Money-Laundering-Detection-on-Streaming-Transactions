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
            assert ev["basis"] == fx_freeze.BTC_BASIS
            assert "immaterial" not in ev["basis"]  # not supported: see the stderr test below
        else:
            assert ev["basis"] == "recovered round constant", ccy


def test_every_fiat_constant_is_within_1e8_of_what_was_measured() -> None:
    for ccy, ev in FROZEN["evidence"].items():
        if ccy != "BTC":
            assert abs(float(ev["rel_dev"])) <= 1e-8, ccy


def test_btc_uncertainty_is_stated_from_the_sample_not_from_the_rounding() -> None:
    ev = FROZEN["evidence"]["BTC"]
    # The 6-digit constant is within ~4e-7 of the subset mean, but the mean itself rests on a few
    # rows: its standard error is what the last digit really depends on.
    assert float(ev["rel_stderr"]) > 10 * abs(float(ev["rel_dev"]))
    assert float(ev["rel_stderr"]) == pytest.approx(
        float(ev["cv_paid_ge_100"]) / ev["rows"] ** 0.5, rel=0.05
    )
    # the independent BTC>USD pair must agree within a few standard errors
    assert abs(float(ev["cross_check_rel_dev"])) <= 3 * float(ev["rel_stderr"])


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
    assert "rel_stderr" in table["evidence"]["BTC"]
    assert table["rates_to_usd"]["BTC"] == "11882.0304488912"
    bad = _pair("BTC", "0.0000900000", weighted_rate_paid_ge_100="0.0000841606685855")
    with pytest.raises(ValueError, match="disagrees"):
        fx_freeze.build(_doc(BTC=bad), money.CURRENCIES)


def test_built_table_loads_in_money(tmp_path: Path) -> None:
    path = tmp_path / "fx.yaml"
    path.write_text(fx_freeze.build(_doc(), money.CURRENCIES), encoding="utf-8")
    fx = money.load_fx(path)
    assert fx["USD"] == 1 and fx["EUR"] == Decimal("0.5")


# --- fx-check: compare a measurement with the frozen table, never write ---------------------------


def _measured(**changes: str) -> dict:
    """The committed HI-Small measurement with some pairs' weighted rates replaced."""
    doc = json.loads(VARIANCE.read_text())
    for pair in doc["pairs"]:
        if pair["paid_currency"] == "USD" and pair["received_currency"] in changes:
            rate = changes[pair["received_currency"]]
            pair["weighted_rate"] = rate
            if pair["received_currency"] == "BTC":
                pair["weighted_rate_paid_ge_100"] = rate
    return doc


def test_check_agrees_when_the_measurement_is_the_one_the_table_came_from() -> None:
    assert fx_freeze.check(_measured(), FROZEN, money.CURRENCIES) == []


def test_check_reports_a_changed_fiat_constant() -> None:
    problems = fx_freeze.check(_measured(EUR="0.8535"), FROZEN, money.CURRENCIES)
    assert len(problems) == 1 and problems[0].startswith("EUR:")


def test_check_reports_a_fiat_rate_that_is_not_a_round_constant() -> None:
    problems = fx_freeze.check(_measured(EUR="0.85341234567"), FROZEN, money.CURRENCIES)
    assert len(problems) == 1 and "EUR" in problems[0]


@pytest.mark.parametrize(("drift", "agrees"), [("1e-5", True), ("-1e-5", True), ("1e-3", False)])
def test_check_allows_btc_to_move_within_three_standard_errors(drift: str, agrees: bool) -> None:
    frozen_constant = Decimal(FROZEN["evidence"]["BTC"]["per_usd"])
    moved = str(frozen_constant * (1 + Decimal(drift)))
    problems = fx_freeze.check(_measured(BTC=moved), FROZEN, money.CURRENCIES)
    assert (problems == []) is agrees
    if not agrees:
        assert problems[0].startswith("BTC:") and "standard errors" in problems[0]


def test_check_cli_writes_nothing_and_sets_the_exit_code(tmp_path: Path) -> None:
    results = tmp_path / "results"
    (results / "t").mkdir(parents=True)
    table = tmp_path / "fx_rates.yaml"
    table.write_text((ROOT / "features" / "fx_rates.yaml").read_text(encoding="utf-8"))
    before = table.read_bytes()
    (results / "t" / "fx_variance.json").write_text(json.dumps(_measured()))
    argv = ["--ds", "t", "--results-dir", str(results), "--out", str(table), "--check"]
    with pytest.raises(SystemExit) as same:
        fx_freeze.main(argv)
    assert same.value.code == 0
    (results / "t" / "fx_variance.json").write_text(json.dumps(_measured(JPY="110.5")))
    with pytest.raises(SystemExit) as different:
        fx_freeze.main(argv)
    assert different.value.code == 1
    assert table.read_bytes() == before  # the frozen table is never touched by --check


def test_check_accepts_btc_all_amounts_gap_that_build_refuses() -> None:
    # HI-Medium: the all-amounts BTC rate is ~6e-6 from its own 6-digit subset constant. That
    # breaks the freezing invariant (1e-6) but is small-BTC rounding, not drift: check() must
    # judge the subset rate against the frozen constant only, while build() still refuses.
    frozen_constant = Decimal(FROZEN["evidence"]["BTC"]["per_usd"])
    doc = _measured()
    btc = next(
        p for p in doc["pairs"] if p["paid_currency"] == "USD" and p["received_currency"] == "BTC"
    )
    btc["weighted_rate_paid_ge_100"] = str(frozen_constant * (1 + Decimal("5e-6")))
    btc["weighted_rate"] = str(frozen_constant * (1 - Decimal("1e-6")))
    with pytest.raises(ValueError, match="disagrees"):
        fx_freeze.build(doc, money.CURRENCIES)
    assert fx_freeze.check(doc, FROZEN, money.CURRENCIES) == []


def test_check_reports_every_currency_that_differs_not_just_the_first() -> None:
    frozen_constant = Decimal(FROZEN["evidence"]["BTC"]["per_usd"])
    doc = _measured(EUR="0.8535", JPY="110.5", BTC=str(frozen_constant * Decimal("1.01")))
    problems = fx_freeze.check(doc, FROZEN, money.CURRENCIES)
    assert sorted(line.split(":")[0] for line in problems) == ["BTC", "EUR", "JPY"]


# --- fx-check edge cases and pinned results ------------------------------------------------------


def _btc_pair(doc: dict) -> dict:
    return next(
        p for p in doc["pairs"] if p["paid_currency"] == "USD" and p["received_currency"] == "BTC"
    )


@pytest.mark.parametrize(
    "change",
    [
        {"n_paid_ge_100": 0},
        {"n_paid_ge_100": fx_freeze.MIN_BTC_ROWS - 1},
        {"weighted_rate_paid_ge_100": None},
        {"cv_paid_ge_100": None},
    ],
)
def test_check_says_so_when_there_are_too_few_btc_rows(change: dict) -> None:
    doc = _measured()
    _btc_pair(doc).update(change)
    problems = fx_freeze.check(doc, FROZEN, money.CURRENCIES)
    assert len(problems) == 1 and problems[0].startswith("BTC:") and "need" in problems[0]
    fx_freeze.format_check(doc, FROZEN, money.CURRENCIES)  # must not raise on the missing rate


@pytest.mark.parametrize(("factor", "agrees"), [(0.9, True), (1.1, False)])
def test_check_btc_band_edges(factor: float, agrees: bool) -> None:
    import math

    doc = _measured()
    pair = _btc_pair(doc)
    se = math.hypot(
        pair["cv_paid_ge_100"] / math.sqrt(pair["n_paid_ge_100"]),
        float(FROZEN["evidence"]["BTC"]["rel_stderr"]),
    )
    drift = Decimal(repr(fx_freeze.BTC_CHECK_SIGMAS * se * factor))
    pair["weighted_rate_paid_ge_100"] = str(
        Decimal(FROZEN["evidence"]["BTC"]["per_usd"]) * (1 + drift)
    )
    assert (fx_freeze.check(doc, FROZEN, money.CURRENCIES) == []) is agrees


def test_the_committed_hi_medium_measurement_agrees_with_the_frozen_table() -> None:
    # What `make fx-check DS=hi_medium` printed, pinned so the claim in D36 is guarded.
    path = ROOT / "results" / "M01" / "hi_medium" / "fx_variance.json"
    assert fx_freeze.check(json.loads(path.read_text()), FROZEN, money.CURRENCIES) == []
