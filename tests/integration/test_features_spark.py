"""batch/spark/features.py: the Spark job must give the same vectors as the pure-Python reference.

Needs a JVM and Python workers that can import `features` (no compose stack: catalog=False).

- tx features, Spark vs Python, on a random sample AND on constructed amounts whose log10 lies
  within 1e-6 of a 6-decimal rounding boundary (the result is checked against the mathematically
  exact rounding, computed with 60-digit Decimal arithmetic, not only against the Python helper);
- the exact binary ties 0.0078125 and 0.0234375 (the values D42 quotes) through a Spark UDF;
- the whole job against `features.vectors.compute_vectors` on a synthetic table.
"""

import decimal

import numpy as np
import pytest
from pyspark.sql import functions as F

from batch.spark import common, features
from features import expand, spec, synthetic, tx_features, vectors

pytestmark = [pytest.mark.integration]


@pytest.fixture(scope="module")
def s() -> spec.Spec:
    return spec.load_spec()


@pytest.fixture(scope="module")
def sp():
    session = features.get_session("features-test")
    yield session
    session.stop()


def _raw_frame(spark, rows: list[dict]):
    return features.raw_frame_from_rows(spark, rows)


def _raw_frame_old(spark, rows: list[dict]):
    cols = [
        ("tx_id", "long"),
        ("event_ts_ms", "long"),
        ("sender_bank", "string"),
        ("sender_key", "string"),
        ("receiver_bank", "string"),
        ("receiver_key", "string"),
        ("amount_paid_usd_cents", "long"),
        ("amount_received_usd_cents", "long"),
        ("payment_currency", "string"),
        ("receiving_currency", "string"),
        ("payment_format", "string"),
    ]
    schema = ", ".join(f"{n} {t}" for n, t in cols)
    df = spark.createDataFrame([tuple(r[n] for n, _ in cols) for r in rows], schema)
    return df.withColumn("owner_bank_id", common.owner_bank_id_col(F.col("sender_bank")))


def near_boundary_cents(k: int = 8) -> list[int]:
    """Amounts (cents) whose log10(1 + c/100) is closest to x.xxxxxx5 (a half-way 6-decimal cut)."""
    c = np.arange(1, 3_000_000, dtype=np.int64)
    x = np.log10(1 + c / 100.0) * 1e6
    dist = np.abs((x % 1.0) - 0.5)
    return [int(v) for v in c[np.argsort(dist)[:k]]]


def exact_rounding(cents: int) -> float:
    """log10(1 + cents/100) rounded half-even to 6 places from a 60-digit exact computation."""
    with decimal.localcontext() as ctx:
        ctx.prec = 60
        x = (decimal.Decimal(1) + decimal.Decimal(cents) / 100).log10()
        return float(x.quantize(decimal.Decimal("0.000001"), rounding=decimal.ROUND_HALF_EVEN))


# --- tx features --------------------------------------------------------------------------------


def test_tx_features_match_python(sp, s: spec.Spec) -> None:
    rows = synthetic.random_transactions(10_000, seed=1)
    rows += [
        {**rows[i], "tx_id": 10_001 + n, "amount_paid_usd_cents": cents}
        for n, (i, cents) in enumerate(zip(range(8), near_boundary_cents(8), strict=True))
    ]
    out = features.tx_features_df(_raw_frame(sp, rows), s).collect()
    got = {r["tx_id"]: r.asDict() for r in out}
    assert len(got) == len(rows)
    for r in rows:
        want = tx_features.compute(r, s)
        for name, value in want.items():
            assert got[r["tx_id"]][name] == value, (r["tx_id"], name)
            assert isinstance(got[r["tx_id"]][name], float) == isinstance(value, float), name


def test_near_boundary_amounts_equal_the_exact_rounding(sp, s: spec.Spec) -> None:
    cents = near_boundary_cents(8)
    base = synthetic.random_transactions(8, seed=2)
    rows = [{**b, "amount_paid_usd_cents": c} for b, c in zip(base, cents, strict=True)]
    got = {
        r["tx_id"]: r["log10_amount_paid_usd"]
        for r in features.tx_features_df(_raw_frame(sp, rows), s).collect()
    }
    for b, c in zip(base, cents, strict=True):
        # distance to the cut is >= ~1e-12 absolute, far above the ~4e-16 error of a double log10
        assert got[b["tx_id"]] == exact_rounding(c), c


def test_the_near_boundary_search_really_finds_hard_cases() -> None:
    x = np.log10(1 + np.array(near_boundary_cents(8)) / 100.0) * 1e6
    assert (np.abs((x % 1.0) - 0.5) < 1e-3).all()  # within 0.001 of a 6-decimal half-way point


def test_binary_ties_survive_the_spark_round_trip(sp, s: spec.Spec) -> None:
    """0.0078125 and 0.0234375 are exact binary ties at 6 decimals (D42): HALF_EVEN, not HALF_UP."""
    import pandas as pd

    @F.pandas_udf("double")
    def quantize(x: pd.Series) -> pd.Series:
        sp_ = spec.load_spec()
        return pd.Series([tx_features.quantize_log10(float(v), sp_) for v in x], dtype="float64")

    df = sp.createDataFrame([(0.0078125,), (0.0234375,), (2.0043213737826426,)], "x double")
    got = [r["q"] for r in df.select(quantize("x").alias("q")).collect()]
    assert got == [0.007812, 0.023438, 2.004321]
    # and the helper itself, with no Spark involved
    assert [tx_features.quantize_log10(v, s) for v in (0.0078125, 0.0234375)] == [
        0.007812,
        0.023438,
    ]


def test_the_job_does_no_rounding_in_spark_sql(sp) -> None:
    """The log10 feature goes through the Python helper; no `round`/`bround` in the job."""
    import inspect

    src = inspect.getsource(features)
    assert "F.round" not in src and "F.bround" not in src and "bround(" not in src


# --- the whole job ------------------------------------------------------------------------------


def test_job_equals_the_python_reference_on_a_synthetic_table(sp, s: spec.Spec) -> None:
    rows = synthetic.random_transactions(600, seed=3, n_accounts=12)
    df = features.compute_features(_raw_frame(sp, rows), s)
    names = expand.feature_names(s)
    assert df.columns == ["tx_id", "event_ts_ms", "owner_bank_id", *names]
    got = {r["tx_id"]: r.asDict() for r in df.collect()}
    want = vectors.compute_vectors(rows, s)
    assert set(got) == set(want) == {r["tx_id"] for r in rows}
    problems = []
    for tx_id, vec in want.items():
        for name, expected in vec.items():
            actual = got[tx_id][name]
            same = (actual is None and expected is None) or (
                actual is not None
                and expected is not None
                and actual == expected
                and isinstance(actual, float) == isinstance(expected, float)
            )
            if not same:
                problems.append((tx_id, name, expected, actual))
    assert not problems, problems[:10]


def test_null_not_nan_and_no_null_in_count_columns(sp, s: spec.Spec) -> None:
    rows = synthetic.random_transactions(300, seed=4, n_accounts=8)
    df = features.compute_features(_raw_frame(sp, rows), s)
    for name, dtype in df.dtypes:
        if dtype == "double":
            assert df.where(F.isnan(name)).count() == 0, name  # nulls, never NaN
    assert df.select(F.sum(F.col("s_log_empty").isNull().cast("int"))).first()[0] == 0
    # the ratios do have nulls (first transactions have empty windows)
    assert df.where(F.col("s_pass_through_ratio_1h").isNull()).count() > 0


def test_schema_carries_no_label_or_split(sp, s: spec.Spec) -> None:
    rows = synthetic.random_transactions(50, seed=5)
    raw = _raw_frame(sp, rows).withColumn("is_laundering", F.lit(1)).withColumn("split", F.lit("t"))
    df = features.compute_features(raw, s)  # label columns present on the input, must not survive
    banned = {"is_laundering", "split", "pattern_type", "attempt_id"}
    assert not (set(df.columns) & banned)
    assert [c for c in df.columns if "launder" in c or "split" in c or "pattern" in c] == []


def test_one_row_per_transaction(sp, s: spec.Spec) -> None:
    rows = synthetic.random_transactions(400, seed=6, n_accounts=6, self_rate=0.2)
    df = features.compute_features(_raw_frame(sp, rows), s)
    assert df.count() == len(rows) == df.select("tx_id").distinct().count()
