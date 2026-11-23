"""The arithmetic of the ingest benchmark (the measuring itself is not unit-testable)."""

import pytest

from stream.connectors.bench import largest_stage, summarize_runs

pytestmark = pytest.mark.unit


def test_summarize_runs_keeps_every_run_and_reports_microseconds_per_record() -> None:
    out = summarize_runs([1.0, 2.0, 4.0], records=1000)
    assert out["runs_seconds"] == [1.0, 2.0, 4.0]
    assert out["runs_us_per_record"] == [1000.0, 2000.0, 4000.0]
    assert out["median_us_per_record"] == 2000.0
    assert out["min_us_per_record"] == 1000.0 and out["max_us_per_record"] == 4000.0
    assert out["median_records_per_second"] == 500.0
    assert out["records"] == 1000


def test_the_median_of_an_even_number_of_runs_is_their_mean_of_the_middle_two() -> None:
    assert summarize_runs([1.0, 3.0], records=100)["median_seconds"] == 2.0


def test_summarize_rejects_nothing_to_summarize() -> None:
    with pytest.raises(ValueError):
        summarize_runs([], records=10)
    with pytest.raises(ValueError):
        summarize_runs([1.0], records=0)


def test_largest_stage_is_the_one_with_the_highest_median_microseconds() -> None:
    stages = {
        "consume": {"median_us_per_record": 3.0},
        "map": {"median_us_per_record": 12.5},
        "avro": {"median_us_per_record": 5.0},
        "produce": {"median_us_per_record": 0.7},
    }
    assert largest_stage(stages) == "map"


def test_largest_stage_ignores_stages_that_are_only_for_comparison() -> None:
    stages = {
        "map": {"median_us_per_record": 1.0},
        "avro": {"median_us_per_record": 2.0},
        "avro_fastavro_for_comparison": {"median_us_per_record": 99.0},
    }
    assert largest_stage(stages) == "avro"
