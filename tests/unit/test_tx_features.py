"""features/tx_features.py: the nine per-transaction features of spec.yaml `tx_features`.

Expected values are worked out from the spec text, not by running the function:
- 2022-09-01T12:00:00Z = 1662033600000 ms, a Thursday (Monday=0 -> 3)
- 2022-09-05T00:00:00Z = 1662336000000 ms, a Monday
- log10(1 + cents/100): 0 cents -> log10(1) = 0; 900 cents -> log10(10) = 1; 99,900 cents ->
  log10(1000) = 3; 10,000 cents -> log10(101) = 2.00432137378264...
"""

import pytest

from features import spec, tx_features

pytestmark = pytest.mark.unit

THU_NOON = 1662033600000
MON_MIDNIGHT = 1662336000000


@pytest.fixture(scope="module")
def s() -> spec.Spec:
    return spec.load_spec()


def _tx(**over) -> dict:
    base = {
        "tx_id": 1,
        "event_ts_ms": THU_NOON,
        "sender_key": "001:AAA",
        "receiver_key": "002:BBB",
        "amount_paid_usd_cents": 10_000,
        "amount_received_usd_cents": 10_000,
        "payment_currency": "USD",
        "receiving_currency": "USD",
        "payment_format": "ACH",
    }
    base.update(over)
    return base


def test_names_are_the_first_block_of_the_vector(s: spec.Spec) -> None:
    names = [line for line in open(spec.SPEC_PATH.parent / "feature_names_v1.txt").read().split()]
    assert list(tx_features.compute(_tx(), s)) == names[:9]


@pytest.mark.parametrize(
    ("cents", "expected"),
    [(0, 0.0), (900, 1.0), (99_900, 3.0)],
)
def test_log10_exact_at_powers_of_ten(s: spec.Spec, cents: int, expected: float) -> None:
    assert tx_features.compute(_tx(amount_paid_usd_cents=cents), s)["log10_amount_paid_usd"] == (
        expected
    )


def test_log10_of_101(s: spec.Spec) -> None:
    got = tx_features.compute(_tx(amount_paid_usd_cents=10_000), s)["log10_amount_paid_usd"]
    assert got == pytest.approx(2.00432137378264, rel=1e-13)


def test_codes_follow_vocabulary_order(s: spec.Spec) -> None:
    v = tx_features.compute(
        _tx(payment_format="WIRE", payment_currency="BTC", receiving_currency="EUR"), s
    )
    assert v["payment_format_code"] == 6  # ACH BITCOIN CASH CHEQUE CREDIT_CARD REINVESTMENT WIRE
    assert v["payment_currency_code"] == 14  # BTC is the 15th currency
    assert v["receiving_currency_code"] == 1  # EUR
    assert tx_features.compute(_tx(payment_format="ACH"), s)["payment_format_code"] == 0


def test_cross_currency(s: spec.Spec) -> None:
    assert tx_features.compute(_tx(), s)["is_cross_currency"] == 0
    assert tx_features.compute(_tx(receiving_currency="EUR"), s)["is_cross_currency"] == 1


def test_utc_hour_and_weekday_monday_is_zero(s: spec.Spec) -> None:
    thu = tx_features.compute(_tx(event_ts_ms=THU_NOON), s)
    assert (thu["hour_of_day"], thu["day_of_week"]) == (12, 3)
    mon = tx_features.compute(_tx(event_ts_ms=MON_MIDNIGHT), s)
    assert (mon["hour_of_day"], mon["day_of_week"]) == (0, 0)
    last_minute = tx_features.compute(_tx(event_ts_ms=MON_MIDNIGHT - 60_000), s)  # Sunday 23:59
    assert (last_minute["hour_of_day"], last_minute["day_of_week"]) == (23, 6)


def test_same_bank_and_self_transfer(s: spec.Spec) -> None:
    other_bank = tx_features.compute(_tx(), s)
    assert (other_bank["same_bank"], other_bank["self_transfer"]) == (0, 0)
    same_bank = tx_features.compute(_tx(receiver_key="001:ZZZ"), s)
    assert (same_bank["same_bank"], same_bank["self_transfer"]) == (1, 0)
    selfie = tx_features.compute(_tx(receiver_key="001:AAA"), s)
    assert (selfie["same_bank"], selfie["self_transfer"]) == (1, 1)


def test_account_ids_are_only_unique_within_a_bank(s: spec.Spec) -> None:
    # same account id at two banks is two accounts, not a self-transfer
    v = tx_features.compute(_tx(sender_key="001:AAA", receiver_key="002:AAA"), s)
    assert (v["same_bank"], v["self_transfer"]) == (0, 0)


def test_unknown_vocabulary_value_is_an_error(s: spec.Spec) -> None:
    with pytest.raises(ValueError, match="XYZ"):
        tx_features.compute(_tx(payment_currency="XYZ"), s)
    with pytest.raises(ValueError, match="CRYPTO"):
        tx_features.compute(_tx(payment_format="CRYPTO"), s)


def test_label_columns_are_never_read(s: spec.Spec) -> None:
    # a row that carries label-like columns must give the same answer as one that does not
    clean = tx_features.compute(_tx(), s)
    dirty = tx_features.compute(_tx(is_laundering=1, split="test", pattern_type="CYCLE"), s)
    assert clean == dirty
