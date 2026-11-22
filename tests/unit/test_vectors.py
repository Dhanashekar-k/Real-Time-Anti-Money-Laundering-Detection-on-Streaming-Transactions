"""features/vectors.py: transactions -> legs -> full feature vectors (everything except the window
logic, which is injected here as a fake oracle so these tests do not need `_compute_rows`)."""

import pandas as pd
import pytest

from features import expand, reference, spec, vectors

pytestmark = pytest.mark.unit

T0 = 1662033600000


@pytest.fixture(scope="module")
def s() -> spec.Spec:
    return spec.load_spec()


def _tx(tx_id: int, sender: str, receiver: str, paid: int = 100, received: int | None = None, **kw):
    base = {
        "tx_id": tx_id,
        "event_ts_ms": T0 + tx_id * 60_000,
        "sender_key": sender,
        "receiver_key": receiver,
        "amount_paid_usd_cents": paid,
        "amount_received_usd_cents": paid if received is None else received,
        "payment_currency": "USD",
        "receiving_currency": "USD",
        "payment_format": "ACH",
    }
    base.update(kw)
    return base


def test_legs_from_transactions_maps_directions_and_amounts() -> None:
    txs = [_tx(1, "001:A", "002:B", paid=500, received=450, receiving_currency="EUR")]
    legs = vectors.legs_from_transactions(txs)
    assert set(legs) == {"001:A", "002:B"}
    out = legs["001:A"].iloc[0]
    assert (out["direction"], out["counterparty_key"], out["amount_usd_cents"]) == (
        "OUT",
        "002:B",
        500,
    )
    inn = legs["002:B"].iloc[0]
    assert (inn["direction"], inn["counterparty_key"], inn["amount_usd_cents"]) == (
        "IN",
        "001:A",
        450,
    )
    for leg in (out, inn):  # both legs carry the transaction's currencies and format
        assert (leg["payment_currency"], leg["receiving_currency"], leg["payment_format"]) == (
            "USD",
            "EUR",
            "ACH",
        )


def test_self_transfer_puts_both_legs_on_one_account() -> None:
    legs = vectors.legs_from_transactions([_tx(1, "001:A", "001:A", paid=700)])
    assert set(legs) == {"001:A"}
    assert sorted(legs["001:A"]["direction"]) == ["IN", "OUT"]


def test_legs_carry_no_label_columns() -> None:
    tx = _tx(1, "001:A", "002:B", is_laundering=1, split="test", pattern_type="CYCLE")
    for frame in vectors.legs_from_transactions([tx]).values():
        assert set(frame.columns) == set(reference.LEG_COLUMNS)


def _fake_oracle(s: spec.Spec):
    """Stands in for compute_account_legs: encodes who was asked and which role in the values."""

    def oracle(legs: pd.DataFrame, spec_: spec.Spec) -> pd.DataFrame:
        rows = []
        ordered = legs.sort_values(["event_ts_ms", "tx_id", "direction"], ascending=[1, 1, False])
        for n_prior, (_, leg) in enumerate(ordered.iterrows()):
            role = reference.DIRECTION_ROLE[leg["direction"]]
            row = {"tx_id": int(leg["tx_id"]), "role": role}
            for col in reference.role_columns(spec_, role):
                dtype = reference.output_dtypes(spec_)[col]
                row[col] = None if dtype == "float64" else 0
            row["out_count_1h"] = len(legs)  # a value that tells the two accounts apart
            row["in_count_1h"] = n_prior
            rows.append(row)
        return reference._to_frame(rows, spec_)

    return oracle


def test_vectors_have_all_85_names_in_order(s: spec.Spec) -> None:
    txs = [_tx(1, "001:A", "002:B"), _tx(2, "002:B", "001:A")]
    out = vectors.compute_vectors(txs, s, oracle=_fake_oracle(s))
    names = expand.feature_names(s)
    assert list(out) == [1, 2]
    assert list(out[1]) == names
    assert len(names) == 85


def test_sender_gets_s_features_and_receiver_gets_r_features(s: spec.Spec) -> None:
    # A has 3 legs (OUT, IN, OUT), B has 2 (IN, OUT): the fake oracle writes len(legs) into
    # out_count_1h, so the prefix tells us whose state each side came from.
    txs = [_tx(1, "001:A", "002:B"), _tx(2, "002:B", "001:A"), _tx(3, "001:A", "003:C")]
    out = vectors.compute_vectors(txs, s, oracle=_fake_oracle(s))
    assert out[1]["s_out_count_1h"] == 3  # sender A
    assert out[1]["r_out_count_1h"] == 2  # receiver B
    assert out[2]["s_out_count_1h"] == 2  # sender B
    assert out[2]["r_out_count_1h"] == 3  # receiver A
    assert out[3]["r_out_count_1h"] == 1  # receiver C has one leg


def test_nulls_become_none_and_ints_stay_ints(s: spec.Spec) -> None:
    out = vectors.compute_vectors([_tx(1, "001:A", "002:B")], s, oracle=_fake_oracle(s))[1]
    assert out["s_pass_through_ratio_1h"] is None
    assert out["s_log_empty"] == 0 and isinstance(out["s_log_empty"], int)
    assert isinstance(out["log10_amount_paid_usd"], float)


def test_relational_features_come_from_the_owning_role(s: spec.Spec) -> None:
    out = vectors.compute_vectors([_tx(1, "001:A", "002:B")], s, oracle=_fake_oracle(s))[1]
    for name in (
        "s_new_counterparty_72h",
        "r_new_counterparty_72h",
        "r_returned_funds_24h",
    ):
        assert out[name] == 0  # fake oracle: 0 for every int column of that role
    assert out["s_amount_vs_out_avg_72h"] is None  # float column, fake oracle: None


def test_transaction_without_both_legs_is_an_error(s: spec.Spec) -> None:
    def broken(legs: pd.DataFrame, spec_: spec.Spec) -> pd.DataFrame:
        return reference._to_frame([], spec_)  # returns nothing for any account

    with pytest.raises(KeyError, match="tx_id 1"):
        vectors.compute_vectors([_tx(1, "001:A", "002:B")], s, oracle=broken)
