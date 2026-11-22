"""EDA helpers that need no Spark: percentiles, sparse-day rule, markdown rendering."""

import pytest

from batch.spark.eda import find_sparse_days, nearest_rank


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
    assert out["median_rows_per_day"] == 100  # median of 100,120,110,5,2 -> 100
    assert out["threshold_rows"] == pytest.approx(10)


def test_sparse_head_is_found_at_the_start() -> None:
    out = find_sparse_days([("d1", 1), ("d2", 100), ("d3", 100), ("d4", 100)])
    assert out["leading"] == ["d1"]
    assert out["trailing"] == []


def test_a_gap_in_the_middle_is_not_a_tail() -> None:
    out = find_sparse_days([("d1", 100), ("d2", 1), ("d3", 100), ("d4", 100)])
    assert out["leading"] == [] and out["trailing"] == []


def test_a_day_exactly_at_the_threshold_is_kept() -> None:
    out = find_sparse_days([("d1", 100), ("d2", 100), ("d3", 10)])  # median 100, threshold 10
    assert out["trailing"] == []


def test_no_sparse_days_in_an_even_series() -> None:
    out = find_sparse_days([(f"d{i}", 100 + i) for i in range(18)])
    assert out["leading"] == [] and out["trailing"] == []


def test_all_days_sparse_cannot_double_count() -> None:
    out = find_sparse_days([("d1", 0), ("d2", 0)])  # median 0, threshold 0: nothing is below it
    assert out["leading"] == [] and out["trailing"] == []


def test_sparse_days_rejects_empty() -> None:
    with pytest.raises(ValueError):
        find_sparse_days([])
