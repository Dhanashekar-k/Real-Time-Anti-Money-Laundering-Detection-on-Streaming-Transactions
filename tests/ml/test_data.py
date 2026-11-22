"""ml/data.py: the one place that reads features, labels and splits for training and evaluation.

Fixture: tiny parquet tables laid out like the real warehouse
(`aml_<ds>/raw_transactions/data/<part>/*.parquet`, `features_tx/data/*.parquet`,
`patterns/data/*.parquet`).
"""

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from features import expand, spec
from ml import data

pytestmark = pytest.mark.unit

DAY = 86_400_000
START = 1_000 * DAY  # arbitrary data_start for the fixture
S = spec.load_spec()
NAMES = expand.feature_names(S)


@pytest.fixture()
def warehouse(tmp_path: Path) -> Path:
    """12 transactions: ids 0..11, two per phase, in order train(burn-in), train, val, test."""
    rows = []
    plan = [
        ("train", 0.2 * DAY),  # inside the 24 h burn-in
        ("train", 0.5 * DAY),
        ("train", 2 * DAY),
        ("train", 2.5 * DAY),
        ("train", 3 * DAY),
        ("val", 4 * DAY),
        ("val", 4.5 * DAY),
        ("val", 5 * DAY),
        ("test", 6 * DAY),
        ("test", 6.5 * DAY),
        ("test", 7 * DAY),
        ("test", 7.5 * DAY),
    ]
    for i, (split, off) in enumerate(plan):
        rows.append(
            {
                "tx_id": i,
                "event_ts_ms": int(START + off),
                "owner_bank_id": "A" if i % 2 == 0 else "B",
                "is_laundering": int(i in (2, 6, 9, 10)),
                "split": split,
                "amount_paid_usd_cents": 1000 + i,
            }
        )
    raw = pd.DataFrame(rows)
    feats = raw[["tx_id", "event_ts_ms", "owner_bank_id"]].copy()
    for j, name in enumerate(NAMES):
        feats[name] = float(j) + feats["tx_id"] / 100.0
    feats.loc[feats.tx_id == 3, "s_pass_through_ratio_24h"] = None  # a null feature
    pat = pd.DataFrame(
        {
            "tx_id": [2, 9, None],
            "pattern_type": ["CYCLE", "FAN-OUT", "CYCLE"],
            "ambiguous": [False, False, False],
        }
    )
    base = tmp_path / "aml_hi_small"
    for table, frame, sub in (
        ("raw_transactions", raw, "part0"),
        ("features_tx", feats, None),
        ("patterns", pat, None),
    ):
        d = base / table / "data" / (sub or "")
        d.mkdir(parents=True)
        duckdb.connect().register("f", frame).execute(
            f"COPY f TO '{d}/00000-1-x-0-00001.parquet' (FORMAT PARQUET)"
        )
    return tmp_path


def read(warehouse: Path, split: str, **kw) -> pd.DataFrame:
    return data.read_split("hi_small", split, warehouse=warehouse, **kw)


def test_train_excludes_the_burn_in_and_keeps_only_train_rows(warehouse: Path) -> None:
    df = read(warehouse, "train", burn_in=True)
    assert df["tx_id"].tolist() == [2, 3, 4]  # tx 0 and 1 are inside the first 24 h
    assert (df["event_ts_ms"] >= START + DAY).all()


def test_burn_in_is_data_start_plus_24_hours_taken_from_the_table(warehouse: Path) -> None:
    assert data.data_start_ms("hi_small", warehouse=warehouse) == int(START + 0.2 * DAY)
    # data_start is tx 0 at 0.2 days, so the cut is 1.2 days: tx 1 (0.5 days) is still burn-in
    df = read(warehouse, "train", burn_in=True)
    assert df["tx_id"].min() == 2


def test_val_rows_only_and_no_burn_in(warehouse: Path) -> None:
    df = read(warehouse, "val")
    assert df["tx_id"].tolist() == [5, 6, 7]


def test_the_test_split_is_refused_without_the_explicit_flag(warehouse: Path) -> None:
    with pytest.raises(PermissionError, match="test"):
        read(warehouse, "test")
    assert read(warehouse, "test", allow_test=True)["tx_id"].tolist() == [8, 9, 10, 11]


def test_unknown_split_is_an_error(warehouse: Path) -> None:
    with pytest.raises(ValueError, match="split"):
        read(warehouse, "holdout")


def test_columns_are_ids_label_bank_features_in_vector_order(warehouse: Path) -> None:
    df = read(warehouse, "val")
    assert list(df.columns) == ["tx_id", "event_ts_ms", "bank", "y", *NAMES]
    assert df["bank"].tolist() == ["B", "A", "B"]
    assert df["y"].tolist() == [0, 1, 0]  # tx 6 is laundering


def test_no_split_or_pattern_column_leaks_into_the_feature_matrix(warehouse: Path) -> None:
    df = read(warehouse, "val", patterns=True)
    assert "split" not in df.columns
    assert data.feature_matrix(df).shape == (3, 85)
    assert list(data.feature_matrix(df).columns) == NAMES  # exactly the 85, label and pattern out


def test_nulls_stay_null_in_the_matrix(warehouse: Path) -> None:
    df = read(warehouse, "train", burn_in=True)
    x = data.feature_matrix(df)
    assert x.loc[df["tx_id"] == 3, "s_pass_through_ratio_24h"].isna().all()
    assert str(x.dtypes.unique().tolist()) == "[dtype('float32')]"


def test_patterns_are_joined_only_when_asked_and_unassigned_lines_dropped(warehouse: Path) -> None:
    assert "pattern_type" not in read(warehouse, "val").columns
    df = read(warehouse, "val", patterns=True)
    assert (
        df.loc[df["tx_id"] == 6, "pattern_type"].isna().all()
    )  # laundering without a pattern line
    t = read(warehouse, "test", allow_test=True, patterns=True)
    assert t.set_index("tx_id").loc[9, "pattern_type"] == "FAN-OUT"
    assert len(t) == 4  # the pattern join must not duplicate or drop transactions


def test_extra_raw_columns_for_the_rules(warehouse: Path) -> None:
    df = read(warehouse, "val", extra_raw_columns=("amount_paid_usd_cents",))
    assert df["amount_paid_usd_cents"].tolist() == [1005, 1006, 1007]
    with pytest.raises(ValueError, match="column"):
        read(warehouse, "val", extra_raw_columns=("is_laundering; DROP",))


def test_only_the_evaluation_module_may_ask_for_the_test_split() -> None:
    root = Path(__file__).resolve().parents[2]
    offenders = []
    for folder in ("ml", "batch", "features", "services", "ops", "eval"):
        for path in (root / folder).rglob("*.py"):
            if "allow_test=True" in path.read_text() and path.name != "model_eval.py":
                offenders.append(str(path.relative_to(root)))
    assert offenders == []
