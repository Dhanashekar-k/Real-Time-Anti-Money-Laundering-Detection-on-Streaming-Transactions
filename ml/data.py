"""Training and evaluation data: features_tx joined with the label and the split (M02).

The only module that reads the label and the split together with the features, and only for
training and evaluation (CLAUDE.md rule 2 keeps them out of feature lists, topics and prompts).

- `read_matrix(ds, "train" | "val")`: the 85 features as one float32 array plus a small frame of
  ids, bank, label and extras, rows in `tx_id` order. This is the path for training and for the
  final evaluation: the table is streamed in Arrow batches straight into a preallocated float32
  array, so nothing is ever held as float64 pandas (HI-Medium's training set would not fit in 24 GB
  that way). Row positions come from `tx_id`, which is contiguous inside a split (it is the rank in
  time order, D06); if the ids are not contiguous the read falls back to an ORDER BY.
- `read_split(...)`: the same rows as a pandas frame, for small selections (the rules baseline asks
  for the handful of feature columns it uses, with `feature_columns`) and for tests.
- `burn_in=True` drops rows in the first 24 h of the data (D04), as the training set must. The test
  split is refused unless the caller passes the explicit test flag; the only caller that sets it is
  `eval/model_eval.py`, after its `--final` gate (a test scans the sources for that).
- Columns: `tx_id`, `event_ts_ms`, `bank` (owner_bank_id), `y` (the label), the features in
  `feature_names_v1.txt` order, then any requested extras (`pattern_type`, raw columns).

Reads the Parquet files of the current snapshot by glob. That equals the table while each table
has one snapshot, which is how features_tx is written (drop, then create; D36's caveat).
"""

import os
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa

from batch.spark import common
from features import expand
from features.spec import load_spec

SPLITS = ("train", "val", "test")
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")
DEFAULT_BATCH_ROWS = 250_000


class RowFile:
    """A float32 (n, k) matrix stored as a `.npy` file, read with positional reads (no mmap).

    Only the rows asked for are in memory. A memory-mapped file would count every page it touches
    as resident, which is exactly what this class avoids: it is how a training set that does not
    fit in RAM is fed to XGBoost in slices (`ml.train.ChunkIter`) and how its columns are read for
    the drift baseline (`take_columns`, a block of columns per pass over the file).
    """

    ndim = 2

    def __init__(self, path: Path) -> None:
        probe = np.load(path, mmap_mode="r")
        if probe.ndim != 2 or not probe.flags.c_contiguous or probe.dtype != np.float32:
            raise ValueError(f"{path}: RowFile needs a C-ordered 2-D float32 .npy file")
        self.shape: tuple[int, int] = (int(probe.shape[0]), int(probe.shape[1]))
        self.dtype = probe.dtype
        self._offset = int(probe.offset)
        del probe
        self.path = Path(path)
        self._row_bytes = self.shape[1] * self.dtype.itemsize
        self._fd: int | None = os.open(path, os.O_RDONLY)

    def __len__(self) -> int:
        return self.shape[0]

    def rows(self, start: int, stop: int) -> np.ndarray:
        """Rows [start, stop) as a new array; a stop past the end is clipped."""
        if self._fd is None:
            raise ValueError("RowFile is closed")
        if start < 0:
            raise IndexError(f"negative start {start}")
        stop = min(stop, self.shape[0])
        count = max(0, stop - start)
        out = np.empty((count, self.shape[1]), dtype=self.dtype)
        if count == 0:
            return out
        view = memoryview(out).cast("B")
        done, want, at = 0, count * self._row_bytes, self._offset + start * self._row_bytes
        while done < want:
            got = os.preadv(self._fd, [view[done:]], at + done)
            if got == 0:
                raise OSError(f"{self.path}: file ends early")
            done += got
        return out

    def take_columns(self, columns: Sequence[int], chunk_rows: int = 500_000) -> np.ndarray:
        """(n, len(columns)) float32: one pass over the file."""
        out = np.empty((self.shape[0], len(columns)), dtype=self.dtype)
        for a in range(0, self.shape[0], chunk_rows):
            out[a : a + chunk_rows] = self.rows(a, a + chunk_rows)[:, list(columns)]
        return out

    def __getitem__(self, key):  # noqa: ANN001
        if isinstance(key, slice) and key.step in (None, 1):
            return self.rows(key.start or 0, self.shape[0] if key.stop is None else key.stop)
        if isinstance(key, tuple) and len(key) == 2 and key[0] == slice(None):
            if isinstance(key[1], int | np.integer):
                return self.take_columns([int(key[1])])[:, 0]
        raise TypeError("RowFile supports x[a:b] and x[:, j]")

    def close(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None


@dataclass
class SplitMatrix:
    """One split as arrays: `x` float32 (n, n_features), NaN = null; `meta` one row per x row.

    `exact` holds the columns that were asked for in float64 (`exact_columns`): float32 rounds
    values such as 0.9, so a rule that compares a ratio with a threshold needs the exact value.
    """

    x: np.ndarray | RowFile
    meta: pd.DataFrame
    names: list[str]
    exact: pd.DataFrame = field(default_factory=pd.DataFrame)

    def column(self, name: str) -> np.ndarray:
        return self.x[:, self.names.index(name)]

    def close(self) -> None:
        if isinstance(self.x, RowFile):
            self.x.close()


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


def _check_request(split: str, allow_test: bool, extras: Sequence[str]) -> None:
    if split not in SPLITS:
        raise ValueError(f"split must be one of {SPLITS}, got {split!r}")
    if split == "test" and not allow_test:
        raise PermissionError("the test split is read only by the final evaluation (--final)")
    for col in extras:
        if not _IDENT.match(col):
            raise ValueError(f"not a column name: {col!r}")


def _from_clause(wh: Path, ds: str, patterns: bool) -> str:
    pat_join = (
        f"LEFT JOIN (SELECT tx_id, pattern_type FROM "
        f"read_parquet('{_glob(wh, ds, 'patterns', False)}') WHERE tx_id IS NOT NULL) p "
        "ON p.tx_id = f.tx_id"
        if patterns
        else ""
    )
    return (
        f"read_parquet('{_glob(wh, ds, 'features_tx', False)}') f "
        f"JOIN read_parquet('{_glob(wh, ds, 'raw_transactions', True)}') r ON r.tx_id = f.tx_id "
        f"{pat_join}"
    )


def _where(split: str, cutoff: int | None) -> str:
    # `split` is checked against SPLITS and `cutoff` is an int: nothing free-form is formatted in
    return f"r.split = '{split}'" + (
        f" AND f.event_ts_ms >= {int(cutoff)}" if cutoff is not None else ""
    )


def read_split(
    ds: str,
    split: str,
    *,
    burn_in: bool = False,
    patterns: bool = False,
    extra_raw_columns: tuple[str, ...] = (),
    feature_columns: Sequence[str] | None = None,
    allow_test: bool = False,
    warehouse: Path | None = None,
) -> pd.DataFrame:
    """One split as a pandas frame (float64). `feature_columns=None` means all features."""
    _check_request(split, allow_test, extra_raw_columns)
    wh = warehouse or common.WAREHOUSE
    names = feature_names()
    chosen = names if feature_columns is None else list(feature_columns)
    unknown = [c for c in chosen if c not in names]
    if unknown:
        raise ValueError(f"not features: {unknown}")
    cutoff = burn_in_end_ms(ds, wh) if burn_in else None

    feats = ", ".join(f"f.{n}" for n in chosen)
    extras = "".join(f", r.{c}" for c in extra_raw_columns)
    pat_col = ", p.pattern_type" if patterns else ""
    sql = f"""
        SELECT f.tx_id, f.event_ts_ms, f.owner_bank_id AS bank, r.is_laundering AS y,
               {feats}{extras}{pat_col}
        FROM {_from_clause(wh, ds, patterns)}
        WHERE {_where(split, cutoff)}
        ORDER BY f.tx_id
    """
    con = common.duckdb_connection()
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


def read_matrix(
    ds: str,
    split: str,
    *,
    burn_in: bool = False,
    patterns: bool = False,
    extra_raw_columns: tuple[str, ...] = (),
    exact_columns: Sequence[str] = (),
    allow_test: bool = False,
    warehouse: Path | None = None,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    spill_to: Path | None = None,
) -> SplitMatrix:
    """One split streamed into a float32 array; see the module docstring.

    With `spill_to` the rows are written to a `.npy` file with positional writes and `x` is a
    `RowFile` (positional reads): the training set of a big dataset never has to be resident. The
    rows are still placed by tx_id, so the file is in tx_id order and can be re-read in the same
    order as often as needed, which XGBoost's two passes over an iterator require.
    """
    _check_request(split, allow_test, extra_raw_columns)
    wh = warehouse or common.WAREHOUSE
    names = feature_names()
    unknown = [c for c in exact_columns if c not in names]
    if unknown:
        raise ValueError(f"not features: {unknown}")
    cutoff = burn_in_end_ms(ds, wh) if burn_in else None
    where = _where(split, cutoff)

    wfd: int | None = None  # the spill file's write descriptor, closed in the finally below
    con = common.duckdb_connection()
    try:
        n, lo, hi = con.execute(
            f"SELECT count(*), min(f.tx_id), max(f.tx_id) FROM {_from_clause(wh, ds, False)} "
            f"WHERE {where}"
        ).fetchall()[
            0
        ]  # fetchall, not fetchone: a half-read result blocks the streaming read below
        if n == 0:
            raise ValueError(f"no rows for {ds} split {split!r}")
        contiguous = int(hi) - int(lo) + 1 == n  # then tx_id - lo is the row's position

        feats = ", ".join(f"f.{c}" for c in names)
        extras = "".join(f", r.{c}" for c in extra_raw_columns)
        pat_col = ", p.pattern_type" if patterns else ""
        order = "" if contiguous else "ORDER BY f.tx_id"
        sql = f"""
            SELECT f.tx_id, f.event_ts_ms, f.owner_bank_id AS bank, r.is_laundering AS y,
                   {feats}{extras}{pat_col}
            FROM {_from_clause(wh, ds, patterns)} WHERE {where} {order}
        """
        meta_cols = ["tx_id", "event_ts_ms", "bank", "y", *extra_raw_columns]
        meta_cols += ["pattern_type"] if patterns else []

        x: np.ndarray | None = None
        offset = 0
        if spill_to is None:
            x = np.empty((n, len(names)), dtype=np.float32)
        else:
            spill_to.parent.mkdir(parents=True, exist_ok=True)
            probe = np.lib.format.open_memmap(
                spill_to, mode="w+", dtype=np.float32, shape=(n, len(names))
            )  # creates the file with its header; the data is then written with pwrite
            offset = int(probe.offset)
            del probe
            wfd = os.open(spill_to, os.O_WRONLY)
        exact = {c: np.empty(n, dtype=np.float64) for c in exact_columns}
        pieces: list[pd.DataFrame] = []
        seen = 0
        reader = con.sql(sql).to_arrow_reader(batch_rows)
        for batch in reader:
            m = batch.num_rows
            if contiguous:
                pos = batch.column("tx_id").to_numpy(zero_copy_only=False).astype(np.int64) - int(
                    lo
                )
            else:
                pos = np.arange(seen, seen + m)
            block = np.empty((m, len(names)), dtype=np.float32)
            for j, name in enumerate(names):
                col = batch.column(name)
                block[:, j] = col.cast(pa.float32(), safe=False).to_numpy(zero_copy_only=False)
                if name in exact:
                    exact[name][pos] = col.cast(pa.float64()).to_numpy(zero_copy_only=False)
            if x is not None:
                x[pos] = block
            else:
                _pwrite_rows(wfd, offset, block, pos)
            piece = batch.select(meta_cols).to_pandas()
            piece["_pos"] = pos
            pieces.append(piece)
            seen += m
    finally:
        con.close()
        if wfd is not None:
            os.close(wfd)
    if seen != n:
        raise RuntimeError(f"streamed {seen} rows, expected {n}")
    if spill_to is not None:
        x = RowFile(spill_to)
    meta = pd.concat(pieces, ignore_index=True).sort_values("_pos", ignore_index=True)
    if not np.array_equal(meta["_pos"].to_numpy(), np.arange(n)):
        raise RuntimeError("row positions are not a permutation of 0..n-1 (duplicate tx_id?)")
    meta = meta.drop(columns="_pos")
    return SplitMatrix(x, meta, names, pd.DataFrame(exact))


def _pwrite_rows(fd: int, offset: int, block: np.ndarray, pos: np.ndarray) -> None:
    """Write the rows of `block` at row positions `pos`, one write per run of consecutive rows."""
    order = np.argsort(pos, kind="stable")
    pos, block = pos[order], np.ascontiguousarray(block[order])
    breaks = np.flatnonzero(np.diff(pos) != 1) + 1
    row_bytes = block.shape[1] * block.dtype.itemsize
    for a, b in zip(np.r_[0, breaks], np.r_[breaks, len(pos)], strict=True):
        view = memoryview(block[a:b]).cast("B")
        done, at = 0, offset + int(pos[a]) * row_bytes
        while done < len(view):
            done += os.pwrite(fd, view[done:], at + done)


def feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """The features of a `read_split` frame as float32, in vector order. Nulls are NaN."""
    return df[feature_names()].astype("float32")
