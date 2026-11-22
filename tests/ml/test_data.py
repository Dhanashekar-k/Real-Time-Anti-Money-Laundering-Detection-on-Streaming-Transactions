"""ml/data.py: the one place that reads features, labels and splits for training and evaluation.

Fixture: tiny parquet tables laid out like the real warehouse
(`aml_<ds>/raw_transactions/data/<part>/*.parquet`, `features_tx/data/*.parquet`,
`patterns/data/*.parquet`).
"""

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

from features import expand, spec
from ml import data

pytestmark = pytest.mark.unit

DAY = 86_400_000
START = 1_000 * DAY  # arbitrary data_start for the fixture
S = spec.load_spec()
NAMES = expand.feature_names(S)


def _tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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
    # integer features hold integers (Spark writes them as bigint); one is huge on purpose
    feats["s_out_count_1h"] = feats["tx_id"].astype("int64")
    feats["s_out_sum_usd_cents_1h"] = feats["tx_id"].astype("int64") * 10**12 + 7
    pat = pd.DataFrame(
        {
            "tx_id": [2, 9, None],
            "pattern_type": ["CYCLE", "FAN-OUT", "CYCLE"],
            "ambiguous": [False, False, False],
        }
    )
    return raw, feats, pat


def _write(
    base: Path, table: str, frame: pd.DataFrame, parts: list[pd.DataFrame], sub: str = ""
) -> None:
    d = base / "aml_hi_small" / table / "data" / sub
    d.mkdir(parents=True, exist_ok=True)
    for i, part in enumerate(parts):
        con = duckdb.connect()
        con.register("f", part)
        con.execute(f"COPY f TO '{d}/0000{i}-1-x-0-00001.parquet' (FORMAT PARQUET)")


@pytest.fixture()
def warehouse(tmp_path: Path) -> Path:
    """12 transactions: ids 0..11, two per phase, in order train(burn-in), train, val, test."""
    raw, feats, pat = _tables()
    _write(tmp_path, "raw_transactions", raw, [raw], sub="part0")
    _write(tmp_path, "features_tx", feats, [feats])
    _write(tmp_path, "patterns", pat, [pat])
    return tmp_path


@pytest.fixture()
def shuffled_warehouse(tmp_path: Path) -> Path:
    """Same data, but features_tx rows are in reverse order spread over three files (as Spark
    leaves them): the loader must not depend on the order rows come out of the files."""
    raw, feats, pat = _tables()
    rev = feats.iloc[::-1].reset_index(drop=True)
    _write(tmp_path, "raw_transactions", raw, [raw], sub="part0")
    _write(tmp_path, "features_tx", feats, [rev.iloc[:5], rev.iloc[5:9], rev.iloc[9:]])
    _write(tmp_path, "patterns", pat, [pat])
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


# --- the chunked float32 loader -----------------------------------------------------------------


def matrix(wh: Path, split: str, **kw) -> data.SplitMatrix:
    kw.setdefault("batch_rows", 3)  # tiny batches: every code path crosses a batch boundary
    return data.read_matrix("hi_small", split, warehouse=wh, **kw)


@pytest.mark.parametrize("which", ["warehouse", "shuffled_warehouse"])
def test_matrix_equals_the_dataframe_path_exactly(which: str, request) -> None:
    wh = request.getfixturevalue(which)
    m = matrix(wh, "val")
    df = read(wh, "val")
    expected = data.feature_matrix(df).to_numpy()
    assert m.x.dtype == np.float32 and m.x.shape == (3, 85)
    np.testing.assert_array_equal(m.x, expected)  # NaN positions included
    assert m.names == NAMES
    assert m.meta["tx_id"].tolist() == df["tx_id"].tolist() == [5, 6, 7]
    assert m.meta["bank"].tolist() == df["bank"].tolist()
    assert m.meta["y"].tolist() == df["y"].tolist()


def test_rows_come_back_in_tx_id_order_whatever_order_the_files_hold(
    shuffled_warehouse: Path,
) -> None:
    m = matrix(shuffled_warehouse, "train", burn_in=True)
    assert m.meta["tx_id"].tolist() == [2, 3, 4]
    plain = matrix(shuffled_warehouse, "test", allow_test=True)
    assert plain.meta["tx_id"].tolist() == [8, 9, 10, 11]
    # a feature that encodes the row (tx_id / 100 + column index) proves rows were not mixed up
    j = NAMES.index("log10_amount_paid_usd")
    np.testing.assert_allclose(plain.x[:, j], [j + t / 100 for t in (8, 9, 10, 11)], rtol=1e-6)


def test_nulls_are_nan_and_big_integers_round_like_a_float32_cast(warehouse: Path) -> None:
    m = matrix(warehouse, "train", burn_in=True)
    j = NAMES.index("s_pass_through_ratio_24h")
    assert np.isnan(m.x[m.meta["tx_id"].to_numpy() == 3, j]).all()
    k = NAMES.index("s_out_sum_usd_cents_1h")
    exact = np.array([t * 10**12 + 7 for t in (2, 3, 4)], dtype="int64")
    np.testing.assert_array_equal(m.x[:, k], exact.astype("float32"))  # int64 -> float32, no error


def test_burn_in_and_split_filters_and_the_test_guard(warehouse: Path) -> None:
    assert matrix(warehouse, "train", burn_in=True).meta["tx_id"].tolist() == [2, 3, 4]
    assert matrix(warehouse, "val").meta["tx_id"].tolist() == [5, 6, 7]
    with pytest.raises(PermissionError, match="test"):
        matrix(warehouse, "test")
    with pytest.raises(ValueError, match="split"):
        matrix(warehouse, "holdout")


def test_exact_columns_come_back_as_float64_without_the_float32_rounding(warehouse: Path) -> None:
    cols = ("s_out_sum_usd_cents_1h", "s_pass_through_ratio_24h")
    m = matrix(warehouse, "train", burn_in=True, exact_columns=cols)
    assert list(m.exact.columns) == list(cols) and m.exact.dtypes.eq("float64").all()
    assert m.exact["s_out_sum_usd_cents_1h"].tolist() == [t * 10**12 + 7 for t in (2, 3, 4)]
    assert m.exact["s_pass_through_ratio_24h"].isna().tolist() == [False, True, False]
    assert list(matrix(warehouse, "val").exact.columns) == []  # none asked for, none returned


def test_extras_and_patterns_are_in_the_meta_frame(warehouse: Path) -> None:
    m = matrix(
        warehouse,
        "test",
        allow_test=True,
        patterns=True,
        extra_raw_columns=("amount_paid_usd_cents",),
    )
    assert m.meta["amount_paid_usd_cents"].tolist() == [1008, 1009, 1010, 1011]
    assert m.meta.set_index("tx_id").loc[9, "pattern_type"] == "FAN-OUT"
    assert m.meta["pattern_type"].isna().sum() == 3
    assert len(m.meta) == len(m.x) == 4  # the pattern join neither drops nor duplicates rows


def test_a_gap_in_the_transaction_ids_falls_back_to_a_sorted_read(tmp_path: Path) -> None:
    """Ids that are not contiguous give no position from tx_id: the loader sorts instead."""
    raw, feats, pat = _tables()
    feats = feats[feats.tx_id != 6].iloc[::-1]  # tx 6 has no features; rows reversed
    _write(tmp_path, "raw_transactions", raw, [raw], sub="part0")
    _write(tmp_path, "features_tx", feats, [feats.iloc[:4], feats.iloc[4:]])
    _write(tmp_path, "patterns", pat, [pat])
    m = matrix(tmp_path, "val")
    assert m.meta["tx_id"].tolist() == [5, 7]
    j = NAMES.index("log10_amount_paid_usd")
    np.testing.assert_allclose(m.x[:, j], [j + 0.05, j + 0.07], rtol=1e-6)


def test_batch_size_does_not_change_the_result(warehouse: Path) -> None:
    a = matrix(warehouse, "train", burn_in=True, batch_rows=1)
    b = matrix(warehouse, "train", burn_in=True, batch_rows=1000)
    np.testing.assert_array_equal(a.x, b.x)
    assert a.meta.equals(b.meta)


def test_an_empty_selection_is_an_error_not_an_empty_matrix(tmp_path: Path) -> None:
    raw, feats, pat = _tables()
    feats = feats[feats.tx_id < 5]  # no validation features at all
    _write(tmp_path, "raw_transactions", raw, [raw], sub="part0")
    _write(tmp_path, "features_tx", feats, [feats])
    _write(tmp_path, "patterns", pat, [pat])
    with pytest.raises(ValueError, match="no rows"):
        matrix(tmp_path, "val")


def test_the_feature_matrix_never_holds_a_label_or_split_column(warehouse: Path) -> None:
    m = matrix(warehouse, "val", patterns=True)
    assert m.x.shape[1] == 85 and m.names == NAMES
    assert not ({"y", "split", "is_laundering", "pattern_type"} & set(m.names))


def test_read_split_can_ask_for_only_some_features(warehouse: Path) -> None:
    cols = ("s_in_count_24h", "r_returned_funds_24h")
    df = read(warehouse, "val", feature_columns=cols)
    assert list(df.columns) == ["tx_id", "event_ts_ms", "bank", "y", *cols]
    with pytest.raises(ValueError, match="not features"):
        read(warehouse, "val", feature_columns=("nope",))
