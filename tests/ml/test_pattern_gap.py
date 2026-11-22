"""eval/pattern_gap.py: laundering rows with a pattern label vs without, train and val only.

Fixture (all laundering rows listed): tx 0..9, A = sender key, B/C = receivers.
  train: tx 0 patterned (CYCLE), tx 1 no pattern, tx 2 not laundering
  val:   tx 3 patterned (FAN-OUT), tx 4 no pattern, tx 5 no pattern
  test:  tx 6, 7 laundering (one patterned): must not appear anywhere in the output
"""

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from eval import pattern_gap

pytestmark = pytest.mark.unit

FEATS = ("s_out_count_24h",)  # the fixture's features_tx has only three columns


def _write(base: Path, table: str, frame: pd.DataFrame, sub: str = "") -> None:
    d = base / "aml_hi_small" / table / "data" / sub
    d.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.register("f", frame)
    con.execute(f"COPY f TO '{d}/00000-1-x-0-00001.parquet' (FORMAT PARQUET)")


@pytest.fixture()
def warehouse(tmp_path: Path) -> Path:
    rows = [
        # tx, split, sender, receiver, cents, format, pay, recv, laundering
        (0, "train", "1:A", "1:B", 100_000, "WIRE", "USD", "USD", 1),
        (1, "train", "1:A", "2:C", 5_000, "ACH", "USD", "USD", 1),
        (2, "train", "1:A", "2:B", 700, "ACH", "USD", "EUR", 0),
        (3, "val", "1:A", "1:C", 40_000, "WIRE", "USD", "USD", 1),
        (4, "val", "1:D", "1:E", 900, "CHEQUE", "USD", "USD", 1),
        (5, "val", "1:D", "1:D", 900, "REINVESTMENT", "USD", "USD", 1),
        (6, "test", "1:A", "1:B", 123_456_789, "BITCOIN", "BTC", "BTC", 1),
        (7, "test", "1:A", "1:B", 123_456_789, "BITCOIN", "BTC", "BTC", 1),
    ]
    raw = pd.DataFrame(
        rows,
        columns=[
            "tx_id", "split", "sender_key", "receiver_key", "amount_paid_usd_cents",
            "payment_format", "payment_currency", "receiving_currency", "is_laundering",
        ],
    )  # fmt: skip
    feats = raw[["tx_id"]].assign(s_out_count_24h=raw["tx_id"], r_in_count_24h=0, s_log_empty=0)
    pat = pd.DataFrame(
        {"tx_id": [0, 3, 6, None], "pattern_type": ["CYCLE", "FAN-OUT", "CYCLE", "CYCLE"]}
    )
    _write(tmp_path, "raw_transactions", raw, "part0")
    _write(tmp_path, "features_tx", feats)
    _write(tmp_path, "patterns", pat)
    return tmp_path


def test_groups_and_counts_exclude_the_test_split(warehouse: Path) -> None:
    out = pattern_gap.run("hi_small", warehouse=warehouse, with_model=False, features=FEATS)
    g = out["groups"]
    assert g["patterned"]["rows"] == 2 and g["no_pattern"]["rows"] == 3  # tx 0,3 / tx 1,4,5
    assert out["laundering_rows_train_val"] == 5
    assert out["rows_by_split"] == {"train": 2, "val": 3}  # no test row, not even in a count


def test_amounts_are_in_usd_and_split_by_group(warehouse: Path) -> None:
    g = pattern_gap.run("hi_small", warehouse=warehouse, with_model=False, features=FEATS)["groups"]
    assert g["patterned"]["amount_usd"]["median"] == 700.0  # (1000.00 + 400.00) / 2
    assert g["no_pattern"]["amount_usd"]["median"] == 9.0  # 50.00, 9.00, 9.00
    assert g["no_pattern"]["amount_usd"]["max"] == 50.0


def test_format_shares_and_cross_currency_and_self_transfers(warehouse: Path) -> None:
    g = pattern_gap.run("hi_small", warehouse=warehouse, with_model=False, features=FEATS)["groups"]
    assert g["patterned"]["payment_format"] == {"WIRE": 2}
    assert g["no_pattern"]["payment_format"] == {"ACH": 1, "CHEQUE": 1, "REINVESTMENT": 1}
    assert (
        g["patterned"]["cross_currency_rows"] == 0 and g["no_pattern"]["cross_currency_rows"] == 0
    )
    assert g["no_pattern"]["self_transfer_rows"] == 1  # tx 5: D pays D
    assert g["patterned"]["self_transfer_rows"] == 0


def test_leg_counts_use_train_and_val_rows_only(warehouse: Path) -> None:
    g = pattern_gap.run("hi_small", warehouse=warehouse, with_model=False, features=FEATS)["groups"]
    # account 1:A has legs in tx 0,1,2,3 (train/val) = 4; tx 6 and 7 (test) must not count
    assert g["patterned"]["sender_legs"]["max"] == 4
    assert g["no_pattern"]["sender_legs"]["max"] == 4  # tx 1 is also from 1:A


def test_cluster_sizes_count_other_laundering_rows_of_the_same_sender(warehouse: Path) -> None:
    out = pattern_gap.run("hi_small", warehouse=warehouse, with_model=False, features=FEATS)
    # laundering senders in train+val: 1:A (tx 0, 1, 3) and 1:D (tx 4, 5):
    # other laundering rows of the same sender, per row: 2, 2, 2, 1, 1
    assert out["groups"]["patterned"]["other_laundering_rows_same_sender"]["max"] == 2
    assert out["groups"]["no_pattern"]["other_laundering_rows_same_sender"]["min"] == 1


def test_feature_medians_are_reported_for_the_features_asked_for(warehouse: Path) -> None:
    g = pattern_gap.run(
        "hi_small", warehouse=warehouse, with_model=False, features=("s_out_count_24h",)
    )
    assert set(g["groups"]["patterned"]["features"]) == {"s_out_count_24h"}
    assert g["groups"]["patterned"]["features"]["s_out_count_24h"]["median"] == 1.5  # tx 0 and 3


def test_concentration_of_the_no_pattern_rows_by_sender(warehouse: Path) -> None:
    out = pattern_gap.run("hi_small", warehouse=warehouse, with_model=False, features=FEATS)
    top = out["groups"]["no_pattern"]["top_senders"]
    # no-pattern rows: tx 1 (1:A), tx 4 and tx 5 (1:D)
    assert [(t["sender"], t["rows"]) for t in top] == [("1:D", 2), ("1:A", 1)]
    assert top[0]["share_of_group"] == pytest.approx(2 / 3)
    assert top[0]["payment_formats"] == ["CHEQUE", "REINVESTMENT"]
    assert out["groups"]["no_pattern"]["rows_from_senders_with_at_least_50_laundering_rows"] == 0
    # patterned rows: tx 0 and tx 3, both sent by 1:A
    assert out["groups"]["patterned"]["top_senders"] == [
        {
            "sender": "1:A",
            "rows": 2,
            "share_of_group": 1.0,
            "median_usd": 700.0,
            "payment_formats": ["WIRE"],
        }
    ]


def test_share_of_rows_above_one_million_dollars(warehouse: Path) -> None:
    g = pattern_gap.run("hi_small", warehouse=warehouse, with_model=False, features=FEATS)["groups"]
    assert (
        g["patterned"]["share_above_1m_usd"] == 0.0 and g["no_pattern"]["share_above_1m_usd"] == 0.0
    )
