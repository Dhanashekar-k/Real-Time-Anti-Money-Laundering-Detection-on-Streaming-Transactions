"""features/expand.py: spec.yaml -> the ordered feature name list (M02 step 1).

The committed `features/feature_names_v1.txt` is what Flink copies in M4 and what the scorer reads,
so any change to the spec (or to the expansion rules) has to show up as a diff in that file.

Order (spec.yaml `vector_order.rule`): tx features; then `s_` account features; then `r_` account
features; then relational features. Inside an account block the templates keep their listed order
and each template expands over its windows in `windows.default` order (feature-major: all three
windows of `out_count` before `in_count`). That reading of "listed order; windows in
windows.default order" is the one the tests pin; the manual's wording does not say more.
"""

import copy

import pytest

from features import expand, spec

pytestmark = pytest.mark.unit

N_TX = 9
N_ACCOUNT = 36  # 9 windowed templates x 3 windows + 7 format shares + account_age + log_empty
N_RELATIONAL = 4
N_TOTAL = N_TX + 2 * N_ACCOUNT + N_RELATIONAL  # 85


@pytest.fixture(scope="module")
def loaded() -> spec.Spec:
    return spec.load_spec()


@pytest.fixture(scope="module")
def names(loaded: spec.Spec) -> list[str]:
    return expand.feature_names(loaded)


def test_generated_equals_committed(loaded: spec.Spec) -> None:
    committed = expand.names_path(loaded)
    assert committed.name == "feature_names_v1.txt"
    assert committed.is_file(), "run `uv run python -m features.expand` and commit the file"
    assert committed.read_text() == expand.render(loaded)


def test_count(names: list[str]) -> None:
    assert len(names) == N_TOTAL == 85


def test_no_duplicates(names: list[str]) -> None:
    assert len(set(names)) == len(names)


def test_block_order(names: list[str]) -> None:
    tx = names[:N_TX]
    s_block = names[N_TX : N_TX + N_ACCOUNT]
    r_block = names[N_TX + N_ACCOUNT : N_TX + 2 * N_ACCOUNT]
    relational = names[N_TX + 2 * N_ACCOUNT :]
    assert tx == [
        "log10_amount_paid_usd",
        "is_cross_currency",
        "payment_format_code",
        "payment_currency_code",
        "receiving_currency_code",
        "hour_of_day",
        "day_of_week",
        "same_bank",
        "self_transfer",
    ]
    assert all(n.startswith("s_") for n in s_block)
    assert all(n.startswith("r_") for n in r_block)
    # the two account blocks differ only in the prefix
    assert [n[2:] for n in s_block] == [n[2:] for n in r_block]
    assert relational == [
        "s_new_counterparty_72h",
        "r_new_counterparty_72h",
        "r_returned_funds_24h",
        "s_amount_vs_out_avg_72h",
    ]


def test_account_block_order(names: list[str]) -> None:
    s_block = names[N_TX : N_TX + N_ACCOUNT]
    # templates in listed order, windows 1h, 24h, 72h inside each template
    assert s_block[:6] == [
        "s_out_count_1h",
        "s_out_count_24h",
        "s_out_count_72h",
        "s_in_count_1h",
        "s_in_count_24h",
        "s_in_count_72h",
    ]
    templates = [
        "out_count",
        "in_count",
        "out_sum_usd_cents",
        "in_sum_usd_cents",
        "distinct_out_cp",
        "distinct_in_cp",
        "pass_through_ratio",
        "avg_hold_minutes",
        "cross_currency_share",
    ]
    windowed = s_block[:27]
    expected = [f"s_{t}_{w}" for t in templates for w in ("1h", "24h", "72h")]
    assert windowed == expected
    # format shares: 72h only, in vocabulary order, then the two window-less features
    assert s_block[27:] == [
        "s_format_share_ACH_72h",
        "s_format_share_BITCOIN_72h",
        "s_format_share_CASH_72h",
        "s_format_share_CHEQUE_72h",
        "s_format_share_CREDIT_CARD_72h",
        "s_format_share_REINVESTMENT_72h",
        "s_format_share_WIRE_72h",
        "s_account_age_minutes",
        "s_log_empty",
    ]


def test_no_forbidden_word(loaded: spec.Spec, names: list[str]) -> None:
    for bad in ("is_laundering", "pattern_type", "attempt_id", "split", "label", "launder"):
        assert not [n for n in names if bad in n.lower()], bad


# --- the expansion rules themselves, on small hand-made documents ------------------------------


def _doc() -> dict:
    """A fresh copy of the real spec document, to mutate."""
    return copy.deepcopy(spec.load_raw())


def test_template_without_windows_key_gets_all_default_windows() -> None:
    doc = _doc()
    doc["windows"]["default"] = ["1h", "24h"]
    doc["account_features"] = [{"name": "out_count_{w}", "def": "x"}]
    doc["relational_features"] = []
    doc["tx_features"] = []
    out = expand.feature_names(spec.parse_spec(doc))
    assert out == ["s_out_count_1h", "s_out_count_24h", "r_out_count_1h", "r_out_count_24h"]


def test_windows_none_means_one_unwindowed_name() -> None:
    doc = _doc()
    doc["account_features"] = [{"name": "log_empty", "windows": "none", "def": "x"}]
    doc["relational_features"] = []
    doc["tx_features"] = []
    assert expand.feature_names(spec.parse_spec(doc)) == ["s_log_empty", "r_log_empty"]


def test_explicit_window_list_is_respected() -> None:
    doc = _doc()
    doc["account_features"] = [{"name": "x_{w}", "windows": ["72h"], "def": "x"}]
    doc["relational_features"] = []
    doc["tx_features"] = []
    assert expand.feature_names(spec.parse_spec(doc)) == ["s_x_72h", "r_x_72h"]


def test_unknown_window_is_an_error() -> None:
    doc = _doc()
    doc["account_features"] = [{"name": "x_{w}", "windows": ["5m"], "def": "x"}]
    with pytest.raises(ValueError, match="5m"):
        spec.parse_spec(doc)


def test_placeholder_without_a_source_is_an_error() -> None:
    doc = _doc()
    doc["account_features"] = [{"name": "x_{w}", "windows": "none", "def": "x"}]
    with pytest.raises(ValueError, match=r"\{w\}"):
        spec.parse_spec(doc)
    doc = _doc()
    doc["account_features"] = [{"name": "x_{fmt}", "def": "x", "windows": "none"}]
    with pytest.raises(ValueError, match=r"\{fmt\}"):
        spec.parse_spec(doc)


def test_duplicate_names_are_an_error() -> None:
    doc = _doc()
    doc["tx_features"] = doc["tx_features"] + [doc["tx_features"][0]]
    with pytest.raises(ValueError, match="duplicate"):
        expand.feature_names(spec.parse_spec(doc))


def test_cli_check_fails_on_a_stale_file(tmp_path, loaded: spec.Spec) -> None:
    stale = tmp_path / "feature_names_v1.txt"
    stale.write_text("not the list\n")
    assert expand.main(["--check", "--out", str(stale)]) == 1
    stale.write_text(expand.render(loaded))
    assert expand.main(["--check", "--out", str(stale)]) == 0
