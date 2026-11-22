"""Training and evaluation data: features_tx joined with the label and the split (M02).

The only module that reads the label and the split together with the features, and only for
training and evaluation (CLAUDE.md rule 2 keeps them out of feature lists, topics and prompts).

- `read_split(ds, "train" | "val")`: rows of one split. `burn_in=True` drops rows in the first 24 h
  of the data (D04), as the training set must. The test split is refused unless the caller passes
  the explicit test flag; the only caller that sets it is `eval/model_eval.py`, after its
  `--final` gate (a test scans the sources for that).
- Columns: `tx_id`, `event_ts_ms`, `bank` (owner_bank_id), `y` (the label), the 85 features in
  `feature_names_v1.txt` order, then any requested extras (`pattern_type`, raw columns).
- `feature_matrix(df)`: the 85 feature columns as float32, nulls as NaN. Nothing else.

Reads the Parquet files of the current snapshot by glob. That equals the table while each table
has one snapshot, which is how features_tx is written (drop, then create; D36's caveat).
"""

import re
from pathlib import Path

import pandas as pd

from batch.spark import common
from features import expand
from features.spec import load_spec

SPLITS = ("train", "val", "test")
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


def feature_names() -> list[str]:
    spec = load_spec()
    path = expand.names_path(spec)
    if path.read_text() != expand.render(spec):
        raise RuntimeError(f"{path} is out of date: run `uv run python -m features.expand`")
    return path.read_text().split()


def _glob(warehouse: Path, ds: str, table: str, partitioned: bool) -> str:
    sub = "*/" if partitioned else ""
    return f"{warehouse}/aml_{ds}/{table}/data/{sub}[0-9]*.parquet"


def data_start_ms(ds: str, warehouse: Path | None = None) -> int:
    """Timestamp of the first transaction of the dataset (the origin of the 24 h burn-in)."""
    wh = warehouse or common.WAREHOUSE
    con = common.duckdb_connection()
    try:
        src = f"read_parquet('{_glob(wh, ds, 'raw_transactions', True)}')"
        return int(con.execute(f"SELECT min(event_ts_ms) FROM {src}").fetchone()[0])
    finally:
        con.close()


def burn_in_end_ms(ds: str, warehouse: Path | None = None) -> int:
    return data_start_ms(ds, warehouse) + load_spec().training_burn_in_ms


def read_split(
    ds: str,
    split: str,
    *,
    burn_in: bool = False,
    patterns: bool = False,
    extra_raw_columns: tuple[str, ...] = (),
    allow_test: bool = False,
    warehouse: Path | None = None,
) -> pd.DataFrame:
    if split not in SPLITS:
        raise ValueError(f"split must be one of {SPLITS}, got {split!r}")
    if split == "test" and not allow_test:
        raise PermissionError("the test split is read only by the final evaluation (--final)")
    for col in extra_raw_columns:
        if not _IDENT.match(col):
            raise ValueError(f"not a column name: {col!r}")
    wh = warehouse or common.WAREHOUSE
    names = feature_names()
    cutoff = burn_in_end_ms(ds, wh) if burn_in else None

    feats = ", ".join(f"f.{n}" for n in names)
    extras = "".join(f", r.{c}" for c in extra_raw_columns)
    pat_col = ", p.pattern_type" if patterns else ""
    pat_join = (
        f"LEFT JOIN (SELECT tx_id, pattern_type FROM "
        f"read_parquet('{_glob(wh, ds, 'patterns', False)}') WHERE tx_id IS NOT NULL) p "
        "ON p.tx_id = f.tx_id"
        if patterns
        else ""
    )
    where = "r.split = ?" + (" AND f.event_ts_ms >= ?" if cutoff is not None else "")
    args = [split] + ([cutoff] if cutoff is not None else [])
    sql = f"""
        SELECT f.tx_id, f.event_ts_ms, f.owner_bank_id AS bank, r.is_laundering AS y,
               {feats}{extras}{pat_col}
        FROM read_parquet('{_glob(wh, ds, "features_tx", False)}') f
        JOIN read_parquet('{_glob(wh, ds, "raw_transactions", True)}') r ON r.tx_id = f.tx_id
        {pat_join}
        WHERE {where}
        ORDER BY f.tx_id
    """
    con = common.duckdb_connection()
    try:
        return con.execute(sql, args).fetchdf()
    finally:
        con.close()


def feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """The 85 features, float32, in vector order. Nulls are NaN. No label, bank or pattern."""
    return df[feature_names()].astype("float32")
