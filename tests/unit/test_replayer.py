"""The replayer's logic without Kafka or the lake (M03 step 4).

A fake clock tests the pacing arithmetic, a list sink stands in for the producer, and a fake table
(an Arrow table behind PyIceberg's `scan(row_filter, selected_fields)` call) stands in for the lake.
The real scan, the real producer and the real day of data are in tests/integration.
"""

import re
from typing import Any

import pyarrow as pa
import pytest

from stream.replayer import render
from stream.replayer.columns import SELECTED_FIELDS
from stream.replayer.replayer import (
    TOPICS,
    ListSink,
    OrderError,
    Pacer,
    count_order_violations,
    day_slices,
    parse_range,
    replay,
    sort_table,
)

pytestmark = pytest.mark.unit

DAY = 86_400_000
D1 = 1_661_990_400_000  # 2022-09-01T00:00:00Z


class FakeClock:
    def __init__(self) -> None:
        self.t = 100.0
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


# --- pacing ---------------------------------------------------------------------------------------


def test_speedup_1440_makes_one_day_one_minute() -> None:
    clock = FakeClock()
    pacer = Pacer.speedup(1440, clock=clock.now, sleep=clock.sleep)
    start = clock.t
    pacer.wait(D1)
    assert clock.sleeps == []  # the first message goes out at once
    pacer.wait(D1 + 60_000)  # one event-minute later = 1/1440 of a minute = 0.041666... s
    assert clock.t - start == pytest.approx(60 / 1440)
    pacer.wait(D1 + DAY)
    assert clock.t - start == pytest.approx(60.0)


def test_speedup_does_not_sleep_when_already_behind() -> None:
    clock = FakeClock()
    pacer = Pacer.speedup(1440, clock=clock.now, sleep=clock.sleep)
    pacer.wait(D1)
    clock.t += 1000.0  # the producer stalled for a long time
    pacer.wait(D1 + 60_000)
    assert clock.sleeps == []


def test_same_event_time_does_not_sleep() -> None:
    clock = FakeClock()
    pacer = Pacer.speedup(10, clock=clock.now, sleep=clock.sleep)
    for _ in range(5):
        pacer.wait(D1)
    assert clock.sleeps == []


def test_rate_spaces_messages_evenly() -> None:
    clock = FakeClock()
    pacer = Pacer.rate(100, clock=clock.now, sleep=clock.sleep)
    start = clock.t
    for _ in range(11):  # messages 0..10
        pacer.wait(D1)  # event time is ignored in rate mode
    assert clock.t - start == pytest.approx(0.10)


def test_asap_never_sleeps() -> None:
    clock = FakeClock()
    pacer = Pacer.asap(clock=clock.now, sleep=clock.sleep)
    for i in range(1000):
        pacer.wait(D1 + i * DAY)
    assert clock.sleeps == []


@pytest.mark.parametrize("bad", [0, -1, -0.5])
def test_speed_and_rate_must_be_positive(bad: float) -> None:
    with pytest.raises(ValueError):
        Pacer.speedup(bad)
    with pytest.raises(ValueError):
        Pacer.rate(bad)


# --- date range -----------------------------------------------------------------------------------


def test_parse_range_is_half_open_at_utc_midnight() -> None:
    assert parse_range("2022-09-01", "2022-09-02") == (D1, D1 + DAY)
    assert parse_range("2022-09-01", None) == (D1, None)
    assert parse_range(None, None) == (None, None)


@pytest.mark.parametrize(
    ("frm", "to"),
    [("2022-09-02", "2022-09-01"), ("2022-09-01", "2022-09-01"), ("09/01/2022", None), ("x", "y")],
)
def test_parse_range_rejects_empty_or_unreadable(frm: str, to: str | None) -> None:
    with pytest.raises(ValueError):
        parse_range(frm, to)


def test_day_slices_cover_the_range_exactly() -> None:
    assert day_slices(D1, D1 + 3 * DAY) == [
        (D1, D1 + DAY),
        (D1 + DAY, D1 + 2 * DAY),
        (D1 + 2 * DAY, D1 + 3 * DAY),
    ]
    assert day_slices(D1 + 5, D1 + DAY + 5) == [(D1 + 5, D1 + DAY), (D1 + DAY, D1 + DAY + 5)]
    assert day_slices(D1, D1) == []


# --- ordering -------------------------------------------------------------------------------------


def test_count_order_violations() -> None:
    assert count_order_violations([1, 2, 2, 3], [1, 2, 3, 4]) == (0, 0)
    assert count_order_violations([1, 3, 2], [1, 2, 3]) == (1, 0)  # time goes back once
    assert count_order_violations([1, 2, 3], [1, 3, 2]) == (0, 1)  # id goes back once
    assert count_order_violations([1, 1], [5, 5]) == (0, 1)  # a repeated id is not increasing


def _table(rows: list[dict[str, Any]], extra_label_columns: bool = True) -> pa.Table:
    cols: dict[str, list[Any]] = {name: [r[name] for r in rows] for name in SELECTED_FIELDS}
    if extra_label_columns:  # what the lake table also holds
        cols["is_laundering"] = [1 for _ in rows]
        cols["split"] = ["test" for _ in rows]
        cols["amount_paid_usd_cents"] = [0 for _ in rows]
    return pa.table(cols)


def _row(tx_id: int, ts: int, owner: str, sender: str = "S") -> dict[str, Any]:
    return {
        "tx_id": tx_id,
        "event_ts_ms": ts,
        "sender_bank": "10",
        "sender_account": sender,
        "receiver_bank": "20",
        "receiver_account": "R",
        "payment_currency": "USD",
        "amount_paid_minor": 100,
        "receiving_currency": "USD",
        "amount_received_minor": 100,
        "payment_format": "ACH",
        "owner_bank_id": owner,
    }


def test_sort_table_orders_by_event_time_then_tx_id() -> None:
    rows = [
        _row(3, D1 + 60_000, "A"),
        _row(2, D1 + 60_000, "B"),
        _row(1, D1, "A"),
        _row(4, D1 + 1, "B"),
    ]
    out = sort_table(_table(rows))
    assert out.column("tx_id").to_pylist() == [1, 4, 2, 3]
    ts, ids = out.column("event_ts_ms").to_pylist(), out.column("tx_id").to_pylist()
    assert list(zip(ts, ids, strict=True)) == sorted(zip(ts, ids, strict=True))


class FakeTable:
    """Stands in for a PyIceberg table: honours `selected_fields` and the time filter."""

    def __init__(self, table: pa.Table) -> None:
        self.table = table
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def scan(self, row_filter: str, selected_fields: tuple[str, ...]) -> "FakeTable":
        self.calls.append((row_filter, tuple(selected_fields)))
        found = re.search(r"event_ts_ms >= (\d+) AND event_ts_ms < (\d+)", row_filter)
        assert found, row_filter
        lo, hi = int(found[1]), int(found[2])
        ts = self.table.column("event_ts_ms").to_pylist()
        keep = [i for i, t in enumerate(ts) if lo <= t < hi]
        picked = self.table.take(pa.array(keep, type=pa.int64())).select(list(selected_fields))
        return type("Scan", (), {"to_arrow": staticmethod(lambda: picked)})()


def _run(rows: list[dict[str, Any]], **kwargs: Any) -> tuple[ListSink, dict[str, Any], FakeTable]:
    table = FakeTable(_table(rows))
    sink = ListSink()
    pacer = Pacer.asap()
    summary = replay(
        table,
        sink,
        pacer,
        lo_ms=kwargs.pop("lo_ms", D1),
        hi_ms=kwargs.pop("hi_ms", D1 + 3 * DAY),
        banks=kwargs.pop("banks", ("A", "B")),
        now_ms=lambda: 1_700_000_000_123,
    )
    return sink, summary, table


def test_messages_are_emitted_in_total_order_and_routed_by_owner() -> None:
    rows = [
        _row(4, D1 + DAY + 5, "B"),
        _row(2, D1 + 60_000, "B"),
        _row(1, D1, "A"),
        _row(3, D1 + 60_000, "A"),
        _row(5, D1 + 2 * DAY, "A"),
    ]
    sink, summary, _ = _run(rows)
    assert [m.topic for m in sink.messages] == [TOPICS[o] for o in ("A", "B", "A", "B", "A")]
    assert summary["messages"] == {"A": 3, "B": 2}
    assert summary["total_messages"] == 5
    assert sink.flushed


def test_the_scan_asks_only_for_allow_listed_columns_one_day_at_a_time() -> None:
    rows = [_row(i, D1 + i * 60_000, "A") for i in range(1, 4)] + [_row(9, D1 + DAY, "B")]
    _, _, table = _run(rows)
    assert len(table.calls) == 3  # three UTC days
    for row_filter, fields in table.calls:
        assert fields == SELECTED_FIELDS
        assert "is_laundering" not in row_filter and "split" not in row_filter


def test_key_header_and_payload() -> None:
    sink, _, _ = _run([_row(7, D1, "A", sender="ACC9")])
    (msg,) = sink.messages
    assert msg.topic == "bank_a.raw"
    assert msg.key == b"10:ACC9"  # the sender key, as on tx.canonical
    assert msg.headers == [("replay_emit_ts_ms", b"1700000000123")]
    assert msg.value == render.render_bank_a(_row(7, D1, "A", sender="ACC9"))


def test_bank_b_gets_json() -> None:
    sink, _, _ = _run([_row(7, D1, "B")])
    assert sink.messages[0].topic == "bank_b.raw"
    assert sink.messages[0].value.startswith(b"{")


def test_one_bank_only_for_sharding() -> None:
    rows = [_row(1, D1, "A"), _row(2, D1 + 60_000, "B"), _row(3, D1 + 120_000, "A")]
    sink, summary, _ = _run(rows, banks=("B",))
    assert [m.topic for m in sink.messages] == ["bank_b.raw"]
    assert summary["messages"] == {"B": 1}


def test_a_label_column_in_the_table_is_never_rendered_or_sent() -> None:
    sink, _, _ = _run([_row(1, D1, "A"), _row(2, D1 + 1, "B")])
    for msg in sink.messages:
        text = msg.value.decode().lower()
        assert (
            "laundering" not in text
            and "test" not in text
            and msg.headers[0][0] == "replay_emit_ts_ms"
        )


def test_a_scan_that_returns_rows_out_of_order_is_sorted_and_reported() -> None:
    rows = [_row(2, D1 + 60_000, "A"), _row(1, D1, "A")]  # table order is not time order
    sink, summary, _ = _run(rows, hi_ms=D1 + DAY)
    assert [json_id(m.value) for m in sink.messages] == [1, 2]
    assert summary["slices"] == 1 and summary["slices_out_of_order_before_sort"] == 1


def json_id(value: bytes) -> int:
    return int(value.split(b",", 1)[0])


def test_the_same_tx_id_twice_in_one_bank_stops_the_replay() -> None:
    rows = [_row(1, D1, "A"), _row(1, D1 + 60_000, "A")]
    with pytest.raises(OrderError):
        _run(rows)


def test_order_is_checked_across_day_slices() -> None:
    rows = [_row(5, D1, "A"), _row(4, D1 + DAY, "A")]  # id goes back across the midnight boundary
    with pytest.raises(OrderError):
        _run(rows)


def test_summary_fields() -> None:
    rows = [_row(1, D1, "A"), _row(2, D1 + DAY, "B")]
    _, summary, _ = _run(rows)
    assert summary["first_tx_id"] == {"A": 1, "B": 2} and summary["last_tx_id"] == {"A": 1, "B": 2}
    assert summary["range_ms"] == [D1, D1 + 3 * DAY]
    assert summary["slices"] == 3 and summary["empty_slices"] == 1


def test_tx_id_order_is_checked_across_banks_not_only_within_one() -> None:
    # bank A's tx 5 comes first in time, bank B's tx 4 later: each bank's own ids increase, but the
    # global sequence (tx_id = position in time order, D06) is broken
    rows = [_row(5, D1, "A"), _row(4, D1 + 60_000, "B")]
    with pytest.raises(OrderError):
        _run(rows)
    with pytest.raises(OrderError):
        _run(rows, banks=("A",))  # sharding one bank must not hide it
