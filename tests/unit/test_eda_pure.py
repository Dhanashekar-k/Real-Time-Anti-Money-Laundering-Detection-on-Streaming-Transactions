"""EDA helpers that need no Spark: percentiles, sparse-day rule, markdown rendering."""

import collections
import random

import pytest

from batch.spark.eda import find_sparse_days, nearest_rank, nearest_rank_hist


@pytest.mark.parametrize(
    ("p", "expected"),
    [(0.0, 1), (0.1, 1), (0.5, 5), (0.9, 9), (0.91, 10), (1.0, 10)],
)
def test_nearest_rank_on_one_to_ten(p: float, expected: int) -> None:
    assert nearest_rank(list(range(1, 11)), p) == expected


def test_nearest_rank_rejects_empty() -> None:
    with pytest.raises(ValueError):
        nearest_rank([], 0.5)


def test_sparse_tail_is_found_at_the_end() -> None:
    days = [("d1", 100), ("d2", 120), ("d3", 110), ("d4", 5), ("d5", 2)]
    out = find_sparse_days(days)
    assert out["trailing"] == ["d4", "d5"]
    assert out["leading"] == []
    assert out["reference_rows_per_day"] == 110  # median of the busier half: 100, 110, 120
    assert out["threshold_rows"] == pytest.approx(11)


def test_sparse_head_is_found_at_the_start() -> None:
    out = find_sparse_days([("d1", 1), ("d2", 100), ("d3", 100), ("d4", 100)])
    assert out["leading"] == ["d1"]
    assert out["trailing"] == []


def test_a_gap_in_the_middle_is_not_a_tail() -> None:
    out = find_sparse_days([("d1", 100), ("d2", 1), ("d3", 100), ("d4", 100)])
    assert out["leading"] == [] and out["trailing"] == []


def test_a_day_exactly_at_the_threshold_is_kept() -> None:
    out = find_sparse_days([("d1", 100), ("d2", 100), ("d3", 10)])  # reference 100, threshold 10
    assert out["trailing"] == []


def test_no_sparse_days_in_an_even_series() -> None:
    out = find_sparse_days([(f"d{i}", 100 + i) for i in range(18)])
    assert out["leading"] == [] and out["trailing"] == []


def test_all_days_sparse_cannot_double_count() -> None:
    out = find_sparse_days([("d1", 0), ("d2", 0)])  # reference 0, threshold 0: nothing is below it
    assert out["leading"] == [] and out["trailing"] == []


def test_sparse_days_rejects_empty() -> None:
    with pytest.raises(ValueError):
        find_sparse_days([])


def test_sparse_day_totals_sum_rows_and_laundering_in_the_run() -> None:
    from batch.spark.eda import sparse_day_totals

    per_day = [
        {"day": "d1", "rows": 1000, "laundering": 1},
        {"day": "d2", "rows": 10, "laundering": 6},
        {"day": "d3", "rows": 10, "laundering": 4},
    ]
    sparse = {"leading": [], "trailing": ["d2", "d3"]}
    out = sparse_day_totals(sparse, per_day)
    assert out["trailing"] == {"days": 2, "rows": 20, "laundering": 10, "laundering_rate": 0.5}
    assert out["leading"]["rows"] == 0 and out["leading"]["laundering_rate"] == 0.0


def test_a_tail_longer_than_half_the_period_is_still_found() -> None:
    # 7 busy days and 9 sparse ones: a plain median would be a sparse day and flag nothing.
    days = [(f"d{i:02d}", 1000) for i in range(7)] + [(f"d{i:02d}", 5) for i in range(7, 16)]
    out = find_sparse_days(days)
    assert out["reference_rows_per_day"] == 1000
    assert out["trailing"] == [f"d{i:02d}" for i in range(7, 16)]


# --- nearest_rank on a histogram equals nearest_rank on the expanded list ------------------------


@pytest.mark.parametrize("seed", range(20))
def test_nearest_rank_hist_equals_nearest_rank_on_the_expanded_values(seed: int) -> None:
    rng = random.Random(seed)
    values = [rng.choice([1, 1, 1, 2, 3, 5, 8, 100, 4000]) for _ in range(rng.randint(1, 500))]
    histogram = list(collections.Counter(values).items())
    rng.shuffle(histogram)  # order must not matter
    for p in (0.0001, 0.5, 0.9, 0.99, 0.999, 1.0):
        assert nearest_rank_hist(histogram, p) == nearest_rank(sorted(values), p)


def test_nearest_rank_hist_rejects_empty() -> None:
    with pytest.raises(ValueError):
        nearest_rank_hist([], 0.5)
