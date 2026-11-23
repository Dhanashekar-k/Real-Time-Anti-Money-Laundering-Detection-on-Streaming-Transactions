"""The connector runner: `bank_<x>.raw` -> map -> Avro -> `tx.canonical`, bad records -> `tx.dlq`.

    uv run python -m stream.connectors.run --bank A
    uv run python -m stream.connectors.run --bank B --idle-exit 10    # stop when quiet

Delivery is AT LEAST ONCE. A batch is polled, mapped and produced; the input offsets are committed
only after the producer has acknowledged every output of the batch (canonical records and DLQ
records). A crash between the produce and the commit means the batch is read again, so duplicates on
`tx.canonical` are possible; Flink removes them by `tx_id` (D15). The idempotent producer only stops
duplicates from its own retries. A delivery failure stops the process (compose restarts it) rather
than skipping the batch.

Group `connector-<bank>`; start more processes in the same group to share the partitions.
Metrics (Prometheus, `--metrics-port`): `records_in_total`, `records_out_total`,
`dlq_total{error_code}`, `batch_latency_seconds`, each with a `bank` label.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from collections import Counter as TallyCounter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaError, KafkaException, TopicPartition
from prometheus_client import REGISTRY, CollectorRegistry, Counter, Histogram

from stream.connectors.engine import BankConfig, MappingError, load_config, map_record
from stream.settings import (
    BROKERS,
    CANONICAL_TOPIC,
    CLIENT_CONFIG,
    DLQ_TOPIC,
    PRODUCER_CONFIG,
    SCHEMA_REGISTRY,
)

ROOT = Path(__file__).resolve().parents[2]
EMIT_HEADER = "replay_emit_ts_ms"
FLUSH_TIMEOUT_S = 120.0
# A DLQ record escapes the raw text (up to 6 characters per byte) and Kafka's default message limit
# is 1,000,000 bytes: 100,000 characters stay under 700,000 even in the worst case.
MAX_DLQ_RAW_CHARS = 100_000
COMMIT_RETRYABLE = ("REBALANCE_IN_PROGRESS", "ILLEGAL_GENERATION", "UNKNOWN_MEMBER_ID")


class DeliveryError(RuntimeError):
    """An output was not acknowledged: the batch must not be committed."""


class ConsumerError(RuntimeError):
    """The consumer returned an error that is not an end-of-partition marker."""


class Metrics:
    def __init__(self, bank: str, registry: CollectorRegistry = REGISTRY) -> None:
        self.bank = bank
        self._in = Counter(
            "records_in", "Records read from the bank topic", ["bank"], registry=registry
        )
        self._out = Counter(
            "records_out", "Canonical records produced", ["bank"], registry=registry
        )
        self._dlq = Counter(
            "dlq", "Records sent to the DLQ", ["bank", "error_code"], registry=registry
        )
        self._latency = Histogram(
            "batch_latency_seconds",
            "Time to process and deliver one batch",
            ["bank"],
            registry=registry,
        )
        self._skipped = Counter(
            "commit_skipped",
            "Offset commits skipped during a rebalance",
            ["bank"],
            registry=registry,
        )
        self._skipped.labels(bank)
        self._in.labels(bank)
        self._out.labels(bank)
        self._latency.labels(bank)

    def commit_skipped(self) -> None:
        self._skipped.labels(self.bank).inc()

    def record(self, result: BatchResult) -> None:
        self._in.labels(self.bank).inc(result.records_in)
        self._out.labels(self.bank).inc(result.records_out)
        for code, count in result.dlq.items():
            self._dlq.labels(self.bank, code).inc(count)
        self._latency.labels(self.bank).observe(result.seconds)


@dataclass(slots=True)
class BatchResult:
    records_in: int = 0
    records_out: int = 0
    dlq: dict[str, int] = field(default_factory=dict)
    seconds: float = 0.0


def log(event: str, **fields: Any) -> None:
    """One JSON line on stderr (stdout carries the final summary)."""
    print(
        json.dumps({"event": event, "ts_ms": time.time_ns() // 1_000_000, **fields}),
        file=sys.stderr,
    )


def parse_emit_header(headers: Sequence[tuple[str, bytes | None]] | None) -> int | None:
    """`replay_emit_ts_ms` from the Kafka headers, or None. A bad header is not a bad record."""
    for name, value in headers or ():
        if name == EMIT_HEADER:
            try:
                return int(value) if value else None
            except (TypeError, ValueError):
                return None
    return None


def dlq_record(
    raw: bytes | None,
    code: str,
    detail: str,
    source_topic: str,
    partition: int,
    offset: int,
    mapping_version: str,
) -> dict[str, Any]:
    """The JSON the DLQ carries. `raw` is the message text (non-UTF-8 bytes are escaped)."""
    text = (raw or b"").decode("utf-8", errors="backslashreplace")
    if len(text) > MAX_DLQ_RAW_CHARS:
        detail += (
            f" [raw truncated: {len(raw or b'')} bytes, first {MAX_DLQ_RAW_CHARS} characters kept]"
        )
        text = text[:MAX_DLQ_RAW_CHARS]
    return {
        "raw": text,
        "error_code": code,
        "error_detail": detail,
        "source_topic": source_topic,
        "partition": partition,
        "offset": offset,
        "mapping_version": mapping_version,
    }


class IdleClock:
    """When may a connector started with `--idle-exit` leave?

    Only after it has been ASSIGNED partitions and then seen nothing for `idle_exit` seconds. The
    time spent joining the consumer group is not idleness: on a busy machine the join can take
    longer than the idle limit (a connector once exited having read nothing).
    """

    def __init__(
        self,
        idle_exit: float,
        now: Callable[[], float] = time.monotonic,
        assign_timeout: float = 120.0,
    ) -> None:
        self.idle_exit, self._now, self.assign_timeout = idle_exit, now, assign_timeout
        self._started = now()
        self._last: float | None = None

    def assigned(self) -> None:
        if self._last is None:
            self._last = self._now()

    def activity(self) -> None:
        self._last = self._now()

    def expired(self) -> bool:
        return (
            bool(self.idle_exit)
            and self._last is not None
            and self._now() - self._last >= self.idle_exit
        )

    def assignment_overdue(self) -> bool:
        return self._last is None and self._now() - self._started >= self.assign_timeout


class BatchProcessor:
    def __init__(
        self,
        cfg: BankConfig,
        serialize: Callable[[dict[str, Any]], bytes],
        producer: Any,
        consumer: Any,
        metrics: Metrics,
        now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        canonical_topic: str = CANONICAL_TOPIC,
        dlq_topic: str = DLQ_TOPIC,
    ) -> None:
        self.cfg, self.serialize, self.producer = cfg, serialize, producer
        self.consumer, self.metrics, self.now_ms = consumer, metrics, now_ms
        self.canonical_topic, self.dlq_topic = canonical_topic, dlq_topic

    max_produce_wait_s = 120.0

    def _produce(self, topic: str, key: bytes | None, value: bytes, on_delivery: Callable) -> None:
        waited_from = time.monotonic()
        while True:
            try:
                self.producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                return
            except BufferError:
                if time.monotonic() - waited_from > self.max_produce_wait_s:
                    raise DeliveryError(
                        f"producer queue full for {self.max_produce_wait_s:.0f}s (not draining)"
                    ) from None
                self.producer.poll(0.05)

    def process(self, messages: Sequence[Any]) -> BatchResult:
        started = time.perf_counter()
        result = BatchResult()
        errors: list[str] = []
        newest: dict[tuple[str, int], int] = {}

        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(str(err))

        dlq: TallyCounter[str] = TallyCounter()
        for msg in messages:
            error = msg.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise ConsumerError(str(error))
            result.records_in += 1
            raw = msg.value() or b""
            mapped = map_record(raw, self.cfg)
            if isinstance(mapped, MappingError):
                detail = f"{mapped.field}: {mapped.detail}" if mapped.field else mapped.detail
                record = dlq_record(
                    raw,
                    mapped.code,
                    detail,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    self.cfg.mapping_version,
                )
                self._produce(self.dlq_topic, msg.key(), json.dumps(record).encode(), on_delivery)
                dlq[mapped.code] += 1
            else:
                mapped["replay_emit_ts_ms"] = parse_emit_header(msg.headers())
                mapped["connector_ts_ms"] = self.now_ms()
                self._produce(
                    self.canonical_topic,
                    mapped["sender_key"].encode(),
                    self.serialize(mapped),
                    on_delivery,
                )
                result.records_out += 1
            where = (msg.topic(), msg.partition())
            newest[where] = max(newest.get(where, -1), msg.offset())

        remaining = self.producer.flush(FLUSH_TIMEOUT_S)
        if remaining or errors:
            raise DeliveryError(
                f"{remaining} output(s) unacknowledged, {len(errors)} failed"
                + (f", first: {errors[0]}" if errors else "")
            )
        if newest:
            try:
                self.consumer.commit(
                    offsets=[TopicPartition(t, p, o + 1) for (t, p), o in sorted(newest.items())],
                    asynchronous=False,
                )
            except KafkaException as exc:
                # The batch was delivered. During a rebalance the commit is refused; the records
                # are read again by whoever owns the partition (a duplicate, never a loss).
                if exc.args[0].name() not in COMMIT_RETRYABLE:
                    raise
                self.metrics.commit_skipped()
                log("commit_skipped", reason=exc.args[0].name(), partitions=len(newest))
        result.dlq = dict(dlq)
        result.seconds = time.perf_counter() - started
        self.metrics.record(result)
        return result


# --- command line ---------------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--bank", required=True, help="A, B, ... (reads connectors/banks/bank_<x>.yaml)"
    )
    parser.add_argument("--config", type=Path, help="default connectors/banks/bank_<x>.yaml")
    parser.add_argument("--brokers", default=BROKERS)
    parser.add_argument("--registry", default=SCHEMA_REGISTRY)
    parser.add_argument("--group", help="default connector-<bank>")
    parser.add_argument("--topic-prefix", default="", help="scratch topics (tests, benchmarks)")
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--poll-timeout", type=float, default=1.0)
    parser.add_argument("--idle-exit", type=float, default=0.0, help="seconds quiet before exiting")
    parser.add_argument("--metrics-port", type=int, default=0, help="0 = no metrics server")
    parser.add_argument("--summary", type=Path, help="write counts and duration to this JSON file")
    args = parser.parse_args(argv)

    from confluent_kafka import Consumer, Producer
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext
    from prometheus_client import start_http_server

    from stream.connectors import registry

    group = args.group or f"{args.topic_prefix}connector-{args.bank.lower()}"
    path = args.config or ROOT / "connectors" / "banks" / f"bank_{args.bank.lower()}.yaml"
    cfg = load_config(path)
    registry.register_canonical(args.registry)
    serializer = AvroSerializer(
        registry.client(args.registry),
        registry.canonical_schema().schema_str,
        conf={
            "auto.register.schemas": False,
            # one subject whatever the topic is called (scratch topics share the contract)
            "subject.name.strategy": lambda ctx, record_name: registry.SUBJECT,
        },
    )
    canonical_topic = args.topic_prefix + CANONICAL_TOPIC
    context = SerializationContext(canonical_topic, MessageField.VALUE)
    consumer = Consumer(
        {
            "bootstrap.servers": args.brokers,
            **CLIENT_CONFIG,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    idle = IdleClock(args.idle_exit)

    def on_assign(_consumer: Any, partitions: list[Any]) -> None:
        idle.assigned()
        log(
            "assigned",
            topic=args.topic_prefix + cfg.topic,
            partitions=[p.partition for p in partitions],
        )

    consumer.subscribe([args.topic_prefix + cfg.topic], on_assign=on_assign)
    producer = Producer({"bootstrap.servers": args.brokers, **CLIENT_CONFIG, **PRODUCER_CONFIG})
    metrics = Metrics(args.bank)
    if args.metrics_port:
        start_http_server(args.metrics_port)
    processor = BatchProcessor(
        cfg,
        lambda record: serializer(record, context),
        producer,
        consumer,
        metrics,
        canonical_topic=canonical_topic,
        dlq_topic=args.topic_prefix + DLQ_TOPIC,
    )

    stop = False

    def request_stop(*_: Any) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    totals = BatchResult()
    dlq_totals: TallyCounter[str] = TallyCounter()
    started = time.monotonic()
    first_batch_at = last_batch_done = 0.0
    try:
        while not stop:
            batch = consumer.consume(num_messages=args.batch_size, timeout=args.poll_timeout)
            if batch:
                idle.activity()
                first_batch_at = first_batch_at or time.monotonic()
                res = processor.process(batch)
                last_batch_done = time.monotonic()
                totals.records_in += res.records_in
                totals.records_out += res.records_out
                dlq_totals.update(res.dlq)
            elif idle.expired():
                break
            elif idle.assignment_overdue():
                raise RuntimeError(f"no partitions assigned after {idle.assign_timeout:.0f}s")
    finally:
        consumer.close()
    seconds = time.monotonic() - started
    # first batch started to last batch committed: the time the connector was working, without the
    # group join before it and the idle wait after it
    active = last_batch_done - first_batch_at if first_batch_at else 0.0
    print(
        f"connector {args.bank}: in={totals.records_in} out={totals.records_out} "
        f"dlq={dict(dlq_totals)} in {seconds:.1f}s"
    )
    if args.summary:
        from batch.spark import common

        common.write_json(
            args.summary,
            {
                "bank": args.bank,
                "mapping_version": cfg.mapping_version,
                "records_in": totals.records_in,
                "records_out": totals.records_out,
                "dlq": dict(dlq_totals),
                "seconds": round(seconds, 3),
                "active_seconds": round(active, 3),
                "records_per_second_active": round(totals.records_in / active, 1)
                if active
                else None,
                "config": {
                    "group": group,
                    "batch_size": args.batch_size,
                    "idle_exit": args.idle_exit,
                    "serializer": "confluent_kafka AvroSerializer",
                },
                **common.provenance(),
            },
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
