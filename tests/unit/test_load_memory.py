"""eval/load_memory.py: parsing of `/usr/bin/time -l` output and the straight-line fit."""

import pytest

from eval import load_memory as lm

pytestmark = pytest.mark.unit

TIME_L = """\
      291.27 real      1459.25 user        15.57 sys
          5812748288  maximum resident set size
             1304  page faults
          5932930832  peak memory footprint
"""


def test_parse_time_l() -> None:
    got = lm.parse_time_l(TIME_L)
    assert got == {
        "seconds": 291.27,
        "max_rss_mb": 5812748288 / 1e6,
        "peak_footprint_mb": 5932930832 / 1e6,
    }


def test_straight_line_through_two_points_and_extrapolation() -> None:
    # peak = 1000 MB + 0.5 MB per 1000 rows: points at 2M and 4M rows give 2000 and 3000 MB
    slope, intercept = lm.fit_line([(2_000_000, 2000.0), (4_000_000, 3000.0)])
    assert slope == pytest.approx(0.0005)  # MB per row
    assert intercept == pytest.approx(1000.0)
    assert lm.extrapolate([(2_000_000, 2000.0), (4_000_000, 3000.0)], 10_000_000) == pytest.approx(
        6000.0
    )


def test_fit_needs_two_distinct_points() -> None:
    with pytest.raises(ValueError):
        lm.fit_line([(1, 1.0)])
    with pytest.raises(ValueError):
        lm.fit_line([(5, 1.0), (5, 2.0)])
