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
import time
from collections import Counter as TallyCounter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaError, TopicPartition
from prometheus_client import REGISTRY, CollectorRegistry, Counter, Histogram

from stream.connectors.engine import BankConfig, MappingError, load_config, map_record
from stream.settings import BROKERS, CANONICAL_TOPIC, DLQ_TOPIC, PRODUCER_CONFIG, SCHEMA_REGISTRY

ROOT = Path(__file__).resolve().parents[2]
EMIT_HEADER = "replay_emit_ts_ms"
FLUSH_TIMEOUT_S = 120.0


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
        self._in.labels(bank)
        self._out.labels(bank)
        self._latency.labels(bank)

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
    return {
        "raw": (raw or b"").decode("utf-8", errors="backslashreplace"),
        "error_code": code,
        "error_detail": detail,
        "source_topic": source_topic,
        "partition": partition,
        "offset": offset,
        "mapping_version": mapping_version,
    }


class BatchProcessor:
    def __init__(
        self,
        cfg: BankConfig,
        serialize: Callable[[dict[str, Any]], bytes],
        producer: Any,
        consumer: Any,
        metrics: Metrics,
        now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.cfg, self.serialize, self.producer = cfg, serialize, producer
        self.consumer, self.metrics, self.now_ms = consumer, metrics, now_ms

    def _produce(self, topic: str, key: bytes | None, value: bytes, on_delivery: Callable) -> None:
        while True:
            try:
                self.producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                return
            except BufferError:
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
                self._produce(DLQ_TOPIC, msg.key(), json.dumps(record).encode(), on_delivery)
                dlq[mapped.code] += 1
            else:
                mapped["replay_emit_ts_ms"] = parse_emit_header(msg.headers())
                mapped["connector_ts_ms"] = self.now_ms()
                self._produce(
                    CANONICAL_TOPIC,
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
            self.consumer.commit(
                offsets=[TopicPartition(t, p, o + 1) for (t, p), o in sorted(newest.items())],
                asynchronous=False,
            )
        result.dlq = dict(dlq)
        result.seconds = time.perf_counter() - started
        self.metrics.record(result)
        return result


# --- command line ---------------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bank", required=True, choices=["A", "B", "C"])
    parser.add_argument("--config", type=Path, help="default connectors/banks/bank_<x>.yaml")
    parser.add_argument("--brokers", default=BROKERS)
    parser.add_argument("--registry", default=SCHEMA_REGISTRY)
    parser.add_argument("--group", help="default connector-<bank>")
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

    path = args.config or ROOT / "connectors" / "banks" / f"bank_{args.bank.lower()}.yaml"
    cfg = load_config(path)
    registry.register_canonical(args.registry)
    serializer = AvroSerializer(
        registry.client(args.registry),
        registry.canonical_schema().schema_str,
        conf={"auto.register.schemas": False},
    )
    context = SerializationContext(CANONICAL_TOPIC, MessageField.VALUE)
    consumer = Consumer(
        {
            "bootstrap.servers": args.brokers,
            "group.id": args.group or f"connector-{args.bank.lower()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([cfg.topic])
    producer = Producer({"bootstrap.servers": args.brokers, **PRODUCER_CONFIG})
    metrics = Metrics(args.bank)
    if args.metrics_port:
        start_http_server(args.metrics_port)
    processor = BatchProcessor(
        cfg, lambda record: serializer(record, context), producer, consumer, metrics
    )

    stop = False

    def request_stop(*_: Any) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    totals = BatchResult()
    dlq_totals: TallyCounter[str] = TallyCounter()
    started = last_activity = time.monotonic()
    try:
        while not stop:
            batch = consumer.consume(num_messages=args.batch_size, timeout=args.poll_timeout)
            if batch:
                last_activity = time.monotonic()
                res = processor.process(batch)
                totals.records_in += res.records_in
                totals.records_out += res.records_out
                dlq_totals.update(res.dlq)
            elif args.idle_exit and time.monotonic() - last_activity >= args.idle_exit:
                break
    finally:
        consumer.close()
    seconds = time.monotonic() - started
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
                "config": {
                    "group": args.group or f"connector-{args.bank.lower()}",
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
