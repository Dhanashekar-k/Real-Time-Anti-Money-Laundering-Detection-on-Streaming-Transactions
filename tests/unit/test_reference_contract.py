"""features/reference.py: the contract around the oracle (M02 step 2), not the window logic.

Everything here must pass with the oracle body still unwritten, except the one test that needs the
real loop: it reports `xfail` while `_compute_rows` raises NotImplementedError and becomes a real
test the moment the body exists. The feature values themselves are checked by tests/golden/ and by
test_reference_bruteforce.py.
"""

import math

import pandas as pd
import pytest

from features import expand, reference, spec

pytestmark = pytest.mark.unit

N_ACCOUNT = 36
N_FLOAT = 18  # ratios, averages and shares: 3+3+3, 7 format shares, age, amount_vs_out_avg
N_INT = 21  # 18 counts/sums/distincts + log_empty + new_counterparty + returned_funds


@pytest.fixture(scope="module")
def s() -> spec.Spec:
    return spec.load_spec()


@pytest.fixture(scope="module")
def names(s: spec.Spec) -> list[str]:
    return expand.feature_names(s)


def _legs(rows: list[tuple]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=[
            "tx_id",
            "event_ts_ms",
            "direction",
            "counterparty_key",
            "amount_usd_cents",
            "payment_currency",
            "receiving_currency",
            "payment_format",
        ],
    )


def _leg(tx_id: int, ts: int, direction: str = "OUT", cp: str = "001:B", cents: int = 100) -> tuple:
    return (tx_id, ts, direction, cp, cents, "USD", "USD", "ACH")


# --- columns ------------------------------------------------------------------------------------


def test_output_columns_derive_from_the_name_list(s: spec.Spec, names: list[str]) -> None:
    cols = reference.output_columns(s)
    assert cols[:2] == ["tx_id", "role"]
    assert len(cols) == 2 + N_ACCOUNT + 3  # 3 relational columns, shared by the two roles
    account = cols[2 : 2 + N_ACCOUNT]
    assert ["s_" + c for c in account] == names[9 : 9 + N_ACCOUNT]
    assert ["r_" + c for c in account] == names[9 + N_ACCOUNT : 9 + 2 * N_ACCOUNT]
    assert cols[2 + N_ACCOUNT :] == [
        "new_counterparty_72h",
        "returned_funds_24h",
        "amount_vs_out_avg_72h",
    ]


def test_each_role_fills_exactly_its_relational_features(s: spec.Spec, names: list[str]) -> None:
    relational = set(names[9 + 2 * N_ACCOUNT :])
    assert reference.role_relational(s, "S") == ["new_counterparty_72h", "amount_vs_out_avg_72h"]
    assert reference.role_relational(s, "R") == ["new_counterparty_72h", "returned_funds_24h"]
    for role in ("S", "R"):
        prefix = reference.ROLE_PREFIX[role]
        filled = {prefix + c for c in reference.role_relational(s, role)}
        assert filled <= relational
    # together they are the four relational names of the vector, nothing more
    assert (
        {reference.ROLE_PREFIX["S"] + c for c in reference.role_relational(s, "S")}
        | {reference.ROLE_PREFIX["R"] + c for c in reference.role_relational(s, "R")}
    ) == relational


def test_dtypes(s: spec.Spec) -> None:
    dtypes = reference.output_dtypes(s)
    assert list(dtypes) == reference.output_columns(s)
    assert dtypes["tx_id"] == "int64"
    assert dtypes["role"] == "object"
    floats = [c for c, d in dtypes.items() if d == "float64"]
    ints = [c for c, d in dtypes.items() if d == "Int64"]
    assert (len(floats), len(ints)) == (N_FLOAT, N_INT)
    for col in (
        "pass_through_ratio_24h",
        "avg_hold_minutes_1h",
        "cross_currency_share_72h",
        "format_share_WIRE_72h",
        "account_age_minutes",
        "amount_vs_out_avg_72h",
    ):
        assert dtypes[col] == "float64", col
    for col in (
        "out_count_1h",
        "in_sum_usd_cents_72h",
        "distinct_out_cp_24h",
        "log_empty",
        "new_counterparty_72h",
        "returned_funds_24h",
    ):
        assert dtypes[col] == "Int64", col


# --- null helpers (spec null_policy) --------------------------------------------------------------


def test_null_helpers() -> None:
    assert reference.ratio_or_none(990, 1000) == 0.99
    assert reference.ratio_or_none(5, 0) is None
    assert reference.ratio_or_none(0, 7) == 0.0


def test_avg_hold_is_one_division_of_two_integers() -> None:
    # D40: total_ms / (count * 60000). 3 entries holding 30, 60 and 90 minutes: 10,800,000 ms
    assert reference.avg_hold_minutes(10_800_000, 3) == 60.0
    assert reference.avg_hold_minutes(0, 2) == 0.0  # holds of 0 ms are real, not null
    assert reference.avg_hold_minutes(0, 0) is None  # no OUT entry with a hold
    assert reference.avg_hold_minutes(1, 3) == 1 / 180_000  # one correctly rounded division
    assert reference.MS_PER_MINUTE == 60_000


def test_vs_avg_is_null_for_an_empty_window_and_for_a_zero_average() -> None:
    # D41: amount / (total / count); null if count == 0 OR total == 0
    assert reference.vs_avg_or_none(100, 99_000, 1) == 100 / (99_000 / 1)
    assert reference.vs_avg_or_none(100, 0, 0) is None  # empty window
    assert reference.vs_avg_or_none(100, 0, 3) is None  # entries exist, but their average is 0
    assert reference.vs_avg_or_none(0, 500, 2) == 0.0  # a zero amount over a real average


# --- input validation -----------------------------------------------------------------------------


def test_missing_column_is_an_error(s: spec.Spec) -> None:
    bad = _legs([_leg(1, 0)]).drop(columns=["payment_format"])
    with pytest.raises(ValueError, match="payment_format"):
        reference.compute_account_legs(bad, s)


def test_bad_direction_is_an_error(s: spec.Spec) -> None:
    with pytest.raises(ValueError, match="direction"):
        reference.compute_account_legs(_legs([_leg(1, 0, direction="SIDEWAYS")]), s)


def test_null_in_a_required_column_is_an_error(s: spec.Spec) -> None:
    legs = _legs([_leg(1, 0)])
    legs.loc[0, "counterparty_key"] = None
    with pytest.raises(ValueError, match="counterparty_key"):
        reference.compute_account_legs(legs, s)


def test_duplicate_leg_is_an_error(s: spec.Spec) -> None:
    # one account has at most one OUT and one IN leg per transaction
    with pytest.raises(ValueError, match="duplicate"):
        reference.compute_account_legs(_legs([_leg(1, 0), _leg(1, 0)]), s)


def test_self_transfer_pair_is_not_a_duplicate(s: spec.Spec) -> None:
    legs = _legs([_leg(1, 0, "OUT"), _leg(1, 0, "IN")])
    reference.validate_legs(legs, s)  # must not raise


def test_non_integer_ids_are_an_error(s: spec.Spec) -> None:
    legs = _legs([_leg(1, 0)])
    legs["tx_id"] = legs["tx_id"].astype(float)
    with pytest.raises(ValueError, match="tx_id"):
        reference.compute_account_legs(legs, s)


def test_label_columns_in_the_input_are_refused(s: spec.Spec) -> None:
    legs = _legs([_leg(1, 0)])
    legs["is_laundering"] = 1
    with pytest.raises(ValueError, match="is_laundering"):
        reference.compute_account_legs(legs, s)


def test_empty_input_gives_an_empty_frame_with_the_right_columns(s: spec.Spec) -> None:
    out = reference.compute_account_legs(_legs([]), s)
    assert list(out.columns) == reference.output_columns(s)
    assert len(out) == 0
    assert {c: str(d) for c, d in out.dtypes.items()} == reference.output_dtypes(s)


# --- the frame builder (scaffold) -----------------------------------------------------------------


def _full_row(s: spec.Spec, role: str, tx_id: int, **over) -> dict:
    """A row with every column the role must supply, all zero, plus overrides."""
    row = {"tx_id": tx_id, "role": role}
    for col in reference.role_columns(s, role):
        row[col] = 0.0 if reference.output_dtypes(s)[col] == "float64" else 0
    row.update(over)
    return row


def test_to_frame_types_and_nulls(s: spec.Spec) -> None:
    rows = [
        _full_row(s, "S", 1, pass_through_ratio_1h=None, amount_vs_out_avg_72h=2.5),
        _full_row(s, "R", 2, returned_funds_24h=1),
    ]
    out = reference._to_frame(rows, s)
    assert {c: str(d) for c, d in out.dtypes.items()} == reference.output_dtypes(s)
    assert math.isnan(out.loc[0, "pass_through_ratio_1h"])
    assert out.loc[0, "amount_vs_out_avg_72h"] == 2.5
    assert pd.isna(out.loc[0, "returned_funds_24h"])  # not an S-role feature: null, not 0
    assert pd.isna(out.loc[1, "amount_vs_out_avg_72h"])
    assert out.loc[1, "returned_funds_24h"] == 1


def test_to_frame_refuses_a_missing_or_unknown_column(s: spec.Spec) -> None:
    row = _full_row(s, "S", 1)
    del row["out_count_1h"]
    with pytest.raises(KeyError, match="out_count_1h"):
        reference._to_frame([row], s)
    row = _full_row(s, "S", 1, not_a_feature=3)
    with pytest.raises(KeyError, match="not_a_feature"):
        reference._to_frame([row], s)
    row = _full_row(s, "S", 1, returned_funds_24h=1)  # an R-only feature on an S row
    with pytest.raises(KeyError, match="returned_funds_24h"):
        reference._to_frame([row], s)


# --- needs the real loop (xfail until it is written) ----------------------------------------------


def test_one_row_per_leg_in_processing_order(s: spec.Spec) -> None:
    """Rows come back sorted by (event_ts_ms, tx_id, OUT before IN), whatever the input order."""
    legs = _legs(
        [
            _leg(7, 5_000, "IN"),
            _leg(3, 1_000, "OUT"),
            _leg(5, 5_000, "OUT"),  # same minute as tx 7, smaller tx_id
            _leg(9, 9_000, "OUT"),
            _leg(9, 9_000, "IN"),  # self-transfer: OUT first
        ]
    )
    try:
        out = reference.compute_account_legs(legs, s)
    except NotImplementedError:
        pytest.xfail("oracle body not written yet (OWNER: human)")
    assert list(out.columns) == reference.output_columns(s)
    assert list(zip(out["tx_id"], out["role"], strict=True)) == [
        (3, "S"),
        (5, "S"),
        (7, "R"),
        (9, "S"),
        (9, "R"),
    ]
    shuffled = reference.compute_account_legs(legs.sample(frac=1, random_state=1), s)
    pd.testing.assert_frame_equal(out, shuffled)
