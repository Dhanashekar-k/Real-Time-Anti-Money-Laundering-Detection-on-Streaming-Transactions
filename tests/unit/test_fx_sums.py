"""A decimal sum that overflows comes back from Spark as NULL; the FX measurement must refuse it."""

import pytest

from batch.spark import fx


def _row(paid: str | None, recv: str | None, a: str = "USD", b: str = "EUR") -> dict:
    return {"paid_ccy": a, "recv_ccy": b, "sum_paid": paid, "sum_recv": recv}


def test_complete_sums_pass() -> None:
    fx._require_sums([_row("1.5", "1.2"), _row("2", "3", "EUR", "USD")], "all rows")


@pytest.mark.parametrize(("paid", "recv"), [(None, "1"), ("1", None), (None, None)])
def test_a_null_sum_is_refused_and_names_the_pair(paid: str | None, recv: str | None) -> None:
    with pytest.raises(ValueError, match=r"USD>EUR") as err:
        fx._require_sums([_row("1", "1", "EUR", "USD"), _row(paid, recv)], "all rows")
    assert "overflow" in str(err.value) and "all rows" in str(err.value)
