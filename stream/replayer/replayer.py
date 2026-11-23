"""Replay the lake's transactions as two banks' native records onto `bank_a.raw` / `bank_b.raw`.

    make replay DS=hi_small FROM=2022-09-01 TO=2022-09-02 ASAP=1
    uv run python -m stream.replayer.replayer --ds hi_small --from 2022-09-01 --to 2022-09-02 \
        --speedup 1440

Rows are read one UTC day at a time through PyIceberg with an explicit column allow-list
(`columns.SELECTED_FIELDS`: no label, no split), sorted by `(event_ts_ms, tx_id)` (a scan does not
keep that order across data files, D53), checked, rendered in the owner bank's format and produced
keyed by the sender account. The header `replay_emit_ts_ms` carries the wall-clock emit time.

Pacing: `--speedup S` (wall time = start + (event time - first event time) / S, so 1440 is one day
per minute), `--rate R` (a fixed messages per second), or `--asap`. `--bank A|B` shards the stream
for throughput (M9): each process then still emits in `tx_id` order for its bank.

The range is half open at UTC midnight: `--from 2022-09-01 --to 2022-09-02` is one day.
Writes `results/M03/<ds>/replay_<run>.json`.
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.compute as pc

from stream.replayer import render
from stream.replayer.columns import SELECTED_FIELDS
from stream.settings import BROKERS, PRODUCER_CONFIG

TOPICS = {"A": "bank_a.raw", "B": "bank_b.raw"}
DAY_MS = 86_400_000
HEADER = "replay_emit_ts_ms"


class OrderError(RuntimeError):
    """A bank's stream would not be in strictly increasing `tx_id` order."""


# --- pacing ---------------------------------------------------------------------------------------


class Pacer:
    """Decides when to emit a message. `wait(event_ts_ms)` blocks until it is due."""

    def __init__(
        self,
        mode: str,
        value: float | None,
        clock: Callable[[], float],
        sleep: Callable[[float], None],
    ) -> None:
        self.mode, self.value, self._clock, self._sleep = mode, value, clock, sleep
        self._start: float | None = None
        self._first_ts: int | None = None
        self._count = 0

    @classmethod
    def speedup(cls, factor: float, clock=time.monotonic, sleep=time.sleep) -> Pacer:  # noqa: ANN001
        if factor <= 0:
            raise ValueError("speedup must be positive")
        return cls("speedup", factor, clock, sleep)

    @classmethod
    def rate(cls, per_second: float, clock=time.monotonic, sleep=time.sleep) -> Pacer:  # noqa: ANN001
        if per_second <= 0:
            raise ValueError("rate must be positive")
        return cls("rate", per_second, clock, sleep)

    @classmethod
    def asap(cls, clock=time.monotonic, sleep=time.sleep) -> Pacer:  # noqa: ANN001
        return cls("asap", None, clock, sleep)

    def wait(self, event_ts_ms: int) -> None:
        if self.mode == "asap":
            return
        if self._start is None:
            self._start, self._first_ts = self._clock(), event_ts_ms
        if self.mode == "speedup":
            due = self._start + (event_ts_ms - self._first_ts) / 1000.0 / self.value
        else:
            due = self._start + self._count / self.value
        self._count += 1
        delay = due - self._clock()
        if delay > 0:
            self._sleep(delay)


# --- date range and ordering ----------------------------------------------------------------------


def _midnight_ms(text: str) -> int:
    try:
        day = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        raise ValueError(f"expected a date like 2022-09-01, got {text!r}") from None
    return int(day.timestamp()) * 1000


def parse_range(frm: str | None, to: str | None) -> tuple[int | None, int | None]:
    """`--from`/`--to` (UTC dates, half open) as epoch millis; None where not given."""
    lo = _midnight_ms(frm) if frm else None
    hi = _midnight_ms(to) if to else None
    if lo is not None and hi is not None and hi <= lo:
        raise ValueError(f"--to {to} must be after --from {frm}")
    return lo, hi


def day_slices(lo_ms: int, hi_ms: int) -> list[tuple[int, int]]:
    """Split [lo, hi) at UTC midnights."""
    slices: list[tuple[int, int]] = []
    start = lo_ms
    while start < hi_ms:
        end = min((start // DAY_MS + 1) * DAY_MS, hi_ms)
        slices.append((start, end))
        start = end
    return slices


def count_order_violations(ts: Sequence[int], ids: Sequence[int]) -> tuple[int, int]:
    """(event time went backwards, tx_id did not increase) along the sequence."""
    back = bad = 0
    for i in range(1, len(ts)):
        if ts[i] < ts[i - 1]:
            back += 1
        if ids[i] <= ids[i - 1]:
            bad += 1
    return back, bad


def sort_table(table: pa.Table) -> pa.Table:
    """The total order of D06: `(event_ts_ms, tx_id)`."""
    return table.sort_by([("event_ts_ms", "ascending"), ("tx_id", "ascending")])


# --- sinks ----------------------------------------------------------------------------------------


@dataclass(slots=True)
class Message:
    topic: str
    key: bytes
    value: bytes
    headers: list[tuple[str, bytes]]


class Sink(Protocol):
    def produce(
        self, topic: str, key: bytes, value: bytes, headers: list[tuple[str, bytes]]
    ) -> None: ...

    def flush(self) -> None: ...


@dataclass
class ListSink:
    """Keeps every message in memory (tests, `--dry-run`)."""

    messages: list[Message] = field(default_factory=list)
    flushed: bool = False

    def produce(
        self, topic: str, key: bytes, value: bytes, headers: list[tuple[str, bytes]]
    ) -> None:
        self.messages.append(Message(topic, key, value, headers))

    def flush(self) -> None:
        self.flushed = True


class CountingSink:
    """`--dry-run`: renders everything, sends nothing, keeps only counts."""

    def __init__(self) -> None:
        self.count = 0
        self.bytes = 0

    def produce(
        self, topic: str, key: bytes, value: bytes, headers: list[tuple[str, bytes]]
    ) -> None:
        self.count += 1
        self.bytes += len(value)

    def flush(self) -> None:
        pass


class KafkaSink:
    """confluent-kafka producer with the manual's settings; fails loudly on a lost message."""

    def __init__(self, brokers: str) -> None:
        from confluent_kafka import Producer

        self._producer = Producer({"bootstrap.servers": brokers, **PRODUCER_CONFIG})
        self.delivered = 0
        self.errors: list[str] = []

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is not None:
            self.errors.append(str(err))
        else:
            self.delivered += 1

    def produce(
        self, topic: str, key: bytes, value: bytes, headers: list[tuple[str, bytes]]
    ) -> None:
        while True:
            try:
                self._producer.produce(
                    topic, value=value, key=key, headers=headers, on_delivery=self._on_delivery
                )
                break
            except BufferError:
                self._producer.poll(0.05)
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()
        if self.errors:
            raise RuntimeError(
                f"{len(self.errors)} message(s) not delivered, first: {self.errors[0]}"
            )


# --- the replay -----------------------------------------------------------------------------------


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


def replay(
    table: Any,
    sink: Sink,
    pacer: Pacer,
    *,
    lo_ms: int,
    hi_ms: int,
    banks: Sequence[str] = ("A", "B"),
    now_ms: Callable[[], int] = _wall_ms,
    topics: Mapping[str, str] = TOPICS,
) -> dict[str, Any]:
    """Emit [lo, hi) from `table` (a PyIceberg table or anything with its `scan` call)."""
    messages = {b: 0 for b in banks}
    first: dict[str, int] = {}
    last: dict[str, int] = {}
    slices = empty = out_of_order = 0
    for start, end in day_slices(lo_ms, hi_ms):
        slices += 1
        scan = table.scan(
            row_filter=f"event_ts_ms >= {start} AND event_ts_ms < {end}",
            selected_fields=SELECTED_FIELDS,
        )
        day = scan.to_arrow()
        if len(banks) < 2:
            day = day.filter(pc.is_in(day.column("owner_bank_id"), value_set=pa.array(list(banks))))
        if day.num_rows == 0:
            empty += 1
            continue
        back, bad = count_order_violations(
            day.column("event_ts_ms").to_pylist(), day.column("tx_id").to_pylist()
        )
        if back or bad:
            out_of_order += 1
        day = sort_table(day)
        columns = {name: day.column(name).to_pylist() for name in SELECTED_FIELDS}
        owners = columns["owner_bank_id"]
        for i in range(day.num_rows):
            row = {name: values[i] for name, values in columns.items()}
            bank = owners[i]
            tx_id = row["tx_id"]
            if bank in last and tx_id <= last[bank]:
                raise OrderError(
                    f"bank {bank}: tx_id {tx_id} follows {last[bank]} (not strictly increasing)"
                )
            last[bank] = tx_id
            first.setdefault(bank, tx_id)
            pacer.wait(row["event_ts_ms"])
            sink.produce(
                topics[bank],
                f"{row['sender_bank']}:{row['sender_account']}".encode(),
                render.render(bank, row),
                [(HEADER, str(now_ms()).encode())],
            )
            messages[bank] += 1
    sink.flush()
    return {
        "messages": messages,
        "total_messages": sum(messages.values()),
        "first_tx_id": first,
        "last_tx_id": last,
        "range_ms": [lo_ms, hi_ms],
        "slices": slices,
        "empty_slices": empty,
        "slices_out_of_order_before_sort": out_of_order,
    }


# --- command line ---------------------------------------------------------------------------------


def _table_bounds(table: Any) -> tuple[int, int]:
    """First and last event time of the table (a one-column read)."""
    column = table.scan(selected_fields=("event_ts_ms",)).to_arrow().column("event_ts_ms")
    return int(pc.min(column).as_py()), int(pc.max(column).as_py())


def main(argv: Sequence[str] | None = None) -> int:
    from batch.spark import common

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", default="hi_small")
    parser.add_argument("--from", dest="frm", help="UTC date, inclusive (2022-09-01)")
    parser.add_argument("--to", help="UTC date, exclusive")
    pace = parser.add_mutually_exclusive_group(required=True)
    pace.add_argument("--speedup", type=float, help="1440 = one day per minute")
    pace.add_argument("--rate", type=float, help="messages per second")
    pace.add_argument("--asap", action="store_true")
    parser.add_argument("--bank", choices=sorted(TOPICS), help="only this bank's stream")
    parser.add_argument("--run", default=datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))
    parser.add_argument("--brokers", default=BROKERS)
    parser.add_argument("--dry-run", action="store_true", help="render only; no Kafka")
    parser.add_argument("--topic-prefix", default="", help="scratch topics (tests, benchmarks)")
    parser.add_argument("--out-dir", type=Path)
    args = parser.parse_args(argv)

    common.require_data_root()
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog("lake", uri=common.pyiceberg_uri(), warehouse=f"file://{common.WAREHOUSE}")
    table = catalog.load_table(f"aml_{args.ds}.raw_transactions")
    lo, hi = parse_range(args.frm, args.to)
    first_ts, last_ts = _table_bounds(table)
    lo = first_ts if lo is None else lo
    hi = last_ts + 1 if hi is None else hi
    banks = (args.bank,) if args.bank else ("A", "B")
    if args.speedup is not None:
        pacer = Pacer.speedup(args.speedup)
    elif args.rate is not None:
        pacer = Pacer.rate(args.rate)
    else:
        pacer = Pacer.asap()
    sink: Sink = CountingSink() if args.dry_run else KafkaSink(args.brokers)

    started = time.perf_counter()
    topics = {b: args.topic_prefix + t for b, t in TOPICS.items()}
    summary = replay(table, sink, pacer, lo_ms=lo, hi_ms=hi, banks=banks, topics=topics)
    seconds = time.perf_counter() - started
    doc = {
        "dataset": args.ds,
        "run": args.run,
        "config": {
            "pacing": {"mode": pacer.mode, "value": pacer.value},
            "from": args.frm,
            "to": args.to,
            "banks": list(banks),
            "topics": {b: topics[b] for b in banks},
            "dry_run": args.dry_run,
            "brokers": None if args.dry_run else args.brokers,
            "producer": PRODUCER_CONFIG,
            "selected_fields": list(SELECTED_FIELDS),
        },
        **summary,
        "duration_seconds": round(seconds, 3),
        "achieved_messages_per_second": round(summary["total_messages"] / seconds, 1)
        if seconds
        else None,
        **common.provenance(),
    }
    out_dir = args.out_dir or common.ROOT / "results" / "M03" / args.ds
    common.write_json(out_dir / f"replay_{args.run}.json", doc)
    print(
        f"replayed {summary['total_messages']} messages "
        f"({summary['messages']}) in {seconds:.1f}s -> {out_dir / f'replay_{args.run}.json'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
