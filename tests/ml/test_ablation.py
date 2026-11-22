"""eval/ablation_cross_currency.py: which features each ablation group removes."""

import pytest

from eval import ablation_cross_currency as ab
from ml import data

pytestmark = pytest.mark.unit

NAMES = data.feature_names()


def test_flag_family_is_the_flag_and_the_six_windowed_shares() -> None:
    got = ab.dropped(NAMES, ab.GROUPS["without_cross_currency_flag_family"])
    assert got == [
        "is_cross_currency",
        *[f"s_cross_currency_share_{w}" for w in ("1h", "24h", "72h")],
        *[f"r_cross_currency_share_{w}" for w in ("1h", "24h", "72h")],
    ]


def test_the_wider_group_adds_exactly_the_two_currency_codes() -> None:
    narrow = set(ab.dropped(NAMES, ab.GROUPS["without_cross_currency_flag_family"]))
    wide = set(ab.dropped(NAMES, ab.GROUPS["without_flag_family_and_currency_codes"]))
    assert wide - narrow == {"payment_currency_code", "receiving_currency_code"}


def test_the_baseline_group_drops_nothing() -> None:
    assert ab.dropped(NAMES, ab.GROUPS["all_features"]) == []
