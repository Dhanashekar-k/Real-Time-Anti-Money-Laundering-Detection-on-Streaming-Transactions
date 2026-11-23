"""Ingest benchmark (M03): where does a connector spend its time, and how fast is a process?

    make ingest-bench DS=hi_small      # needs make up and the SSD; runs under eval.run_measured

Per bank (A and B), on the first N rows of one day of the lake rendered in that bank's format:

1. Stages, each timed alone and repeated (every run is kept; the first run of a stage is a warm-up
   and is reported, not hidden): `consume` (read the records from a scratch topic, no processing),
   `map` (parse + mapping engine), `avro` (the confluent-kafka `AvroSerializer` the connector uses;
   `avro_fastavro_for_comparison` is the hand-framed alternative from D53 and is not used), and
   `produce` (send the serialized records to a scratch topic, idempotent producer, zstd).
2. End to end: the real connector process (`stream.connectors.run`) on the same scratch topic, with
   `--idle-exit`, repeated; its rate is records / active seconds (first batch to last commit).

`largest_stage_by_median` is computed from the medians in this file. It names the stage that costs
most per record in isolation; the end-to-end rate is the number to quote for a connector process.
Host processes, not containers (a Docker VM would add its own overhead). The Mac's swap and load are
recorded, because the numbers move with them (D53). Scratch topics are deleted at the end.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stream.settings import CLIENT_CONFIG

BATCH_SIZE = 2000  # the connector's default --batch-size
STAGES_FOR_BOTTLENECK = ("consume", "map", "avro", "produce", "commit")
DAY_FROM, DAY_TO = "2022-09-01", "2022-09-02"


def summarize_runs(seconds: Sequence[float], records: int) -> dict[str, Any]:
    """Per-record cost from the wall time of each repeat. Every run is kept."""
    if not seconds or records <= 0:
        raise ValueError("need at least one run and a positive record count")
    per_record = [s / records * 1e6 for s in seconds]
    median = statistics.median(seconds)
    return {
        "records": records,
        "runs_seconds": [round(s, 4) for s in seconds],
        "runs_us_per_record": [round(u, 3) for u in per_record],
        "median_seconds": round(median, 4),
        "median_us_per_record": round(median / records * 1e6, 3),
        "min_us_per_record": round(min(per_record), 3),
        "max_us_per_record": round(max(per_record), 3),
        "median_records_per_second": round(records / median, 1),
    }


def largest_stage(stages: Mapping[str, Mapping[str, float]]) -> str:
    """The stage with the highest median microseconds per record (comparison stages excluded)."""
    candidates = {k: v for k, v in stages.items() if k in STAGES_FOR_BOTTLENECK}
    return max(candidates, key=lambda k: candidates[k]["median_us_per_record"])


def _timed(fn: Callable[[], Any]) -> float:
    started = time.perf_counter()
    fn()
    return time.perf_counter() - started


def _swap() -> str:
    out = subprocess.run(["sysctl", "-n", "vm.swapusage"], capture_output=True, text=True)
    return out.stdout.strip()


def _bank_rows(table: Any, bank: str, n: int) -> list[dict[str, Any]]:
    import pyarrow.compute as pc

    from stream.replayer.columns import SELECTED_FIELDS
    from stream.replayer.replayer import parse_range, sort_table

    lo, hi = parse_range(DAY_FROM, DAY_TO)
    scan = table.scan(
        row_filter=f"event_ts_ms >= {lo} AND event_ts_ms < {hi}", selected_fields=SELECTED_FIELDS
    )
    day = sort_table(scan.to_arrow())
    rows = day.filter(pc.equal(day.column("owner_bank_id"), bank)).slice(0, n)
    if rows.num_rows < n:
        raise RuntimeError(f"bank {bank} has only {rows.num_rows} rows on {DAY_FROM}, wanted {n}")
    return rows.to_pylist()


def _consume_run(brokers: str, topic: str, n: int, group: str) -> float:
    """Seconds to read n records, from the first batch on (the group join is not counted)."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            **CLIENT_CONFIG,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    got = 0
    started = 0.0
    counted_from = 0
    try:
        while got < n:
            batch = consumer.consume(2000, 1.0)
            good = [m for m in batch if not m.error()]
            if good and not started:
                got += len(good)
                started, counted_from = time.perf_counter(), got
                continue
            got += len(good)
        return time.perf_counter() - started if got > counted_from else float("nan")
    finally:
        consumer.close()


@dataclass
class _Env:
    """What every per-bank benchmark shares."""

    brokers: str
    prefix: str
    serializer: Any
    context: Any
    frame: Callable[[dict[str, Any]], bytes]
    canonical_topic: str
    producer_config: Mapping[str, Any]


def _send_all(
    brokers: str,
    config: Mapping[str, Any],
    topic: str,
    items: Sequence[tuple[bytes, bytes]],
    flush_every: int | None = None,
) -> tuple[int, float]:
    """Produce (key, value) pairs and wait for the acknowledgements.

    Returns (delivery errors, seconds). The seconds start AFTER the producer is created and has
    delivered one message: a new producer's start-up (about 510 ms, `producer_startup_first_ack_ms`)
    is not a per-record cost. `flush_every` mirrors the connector, which waits for the
    acknowledgement of each batch before committing.
    """
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": brokers, **CLIENT_CONFIG, **config})
    errors: list[str] = []

    def on_delivery(err: Any, _msg: Any) -> None:
        if err is not None:
            errors.append(str(err))

    producer.produce(topic, value=b"warm-up", key=b"warm-up", on_delivery=on_delivery)
    producer.flush(60)
    started = time.perf_counter()
    for count, (key, value) in enumerate(items, start=1):
        while True:
            try:
                producer.produce(topic, value=value, key=key, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.05)
        producer.poll(0)
        if flush_every and count % flush_every == 0:
            producer.flush(120)
    producer.flush(120)
    return len(errors), time.perf_counter() - started


def _commit_run(brokers: str, topic: str, group: str, batches: int) -> float:
    """Seconds for `batches` synchronous offset commits, as the connector does after each batch."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            **CLIENT_CONFIG,
            "group.id": group,
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.commit(offsets=[TopicPartition(topic, 0, 1)], asynchronous=False)  # warm-up
        started = time.perf_counter()
        for k in range(batches):
            consumer.commit(offsets=[TopicPartition(topic, 0, 2 + k)], asynchronous=False)
        return time.perf_counter() - started
    finally:
        consumer.close()


def _first_ack_ms(brokers: str, extra: Mapping[str, Any], topic: str) -> float:
    """Milliseconds from creating a producer to its first acknowledgement."""
    from confluent_kafka import Producer

    from stream.settings import PRODUCER_CONFIG

    started = time.perf_counter()
    done: list[float] = []
    producer = Producer({"bootstrap.servers": brokers, **extra, **PRODUCER_CONFIG})
    producer.produce(
        topic, value=b"x", on_delivery=lambda err, msg: done.append(time.perf_counter())
    )
    producer.flush(60)
    if not done:
        raise RuntimeError("no acknowledgement within 60 s")
    return round((done[0] - started) * 1000, 1)


def producer_startup(brokers: str, topic: str, trials: int = 5) -> dict[str, Any]:
    """New-producer first-ack time with librdkafka's default address family against IPv4 only.

    Trials alternate between the two so a drift in machine state cannot explain a difference. The
    local broker listens on IPv4 and `localhost` resolves to ::1 first (D53).
    """
    default: list[float] = []
    v4: list[float] = []
    for _ in range(trials):
        default.append(_first_ack_ms(brokers, {}, topic))
        v4.append(_first_ack_ms(brokers, {"broker.address.family": "v4"}, topic))
    return {
        "trials_each": trials,
        "order": "alternating, default first",
        "default_address_family_ms": default,
        "ipv4_only_ms": v4,
        "median_ms": {"default": statistics.median(default), "ipv4_only": statistics.median(v4)},
    }


def _bench_stages(
    env: _Env, bank: str, cfg: Any, rows: list[dict[str, Any]], repeats: int
) -> dict[str, Any]:
    from stream.connectors.engine import MappingError, map_record
    from stream.replayer import render

    n = len(rows)
    raws = [render.render(bank, r) for r in rows]
    input_topic = f"{env.prefix}{cfg.topic}"
    keys = [f"{r['sender_bank']}:{r['sender_account']}".encode() for r in rows]
    failed, _ = _send_all(
        env.brokers, env.producer_config, input_topic, list(zip(keys, raws, strict=True))
    )
    if failed:
        raise RuntimeError("could not fill the scratch input topic")

    stages: dict[str, Any] = {}
    stages["consume"] = summarize_runs(
        [
            _consume_run(env.brokers, input_topic, n, f"{env.prefix}consume-{bank}-{i}")
            for i in range(repeats)
        ],
        n,
    )

    mapped: list[dict[str, Any]] = []

    def do_map() -> None:
        mapped.clear()
        for raw in raws:
            result = map_record(raw, cfg)
            if isinstance(result, MappingError):
                raise RuntimeError(f"bank {bank}: {result}")
            result["replay_emit_ts_ms"] = 1_700_000_000_000
            result["connector_ts_ms"] = 1_700_000_000_001
            mapped.append(result)

    stages["map"] = summarize_runs([_timed(do_map) for _ in range(repeats)], n)
    stages["avro"] = summarize_runs(
        [_timed(lambda: [env.serializer(r, env.context) for r in mapped]) for _ in range(repeats)],
        n,
    )
    stages["avro_fastavro_for_comparison"] = summarize_runs(
        [_timed(lambda: [env.frame(r) for r in mapped]) for _ in range(repeats)], n
    )

    payloads = [(r["sender_key"].encode(), env.serializer(r, env.context)) for r in mapped]
    errors: list[int] = []
    runs: list[float] = []
    for _ in range(repeats):
        failed, seconds = _send_all(
            env.brokers, env.producer_config, env.canonical_topic, payloads, flush_every=BATCH_SIZE
        )
        errors.append(failed)
        runs.append(seconds)
    stages["produce"] = summarize_runs(runs, n)
    stages["produce"]["delivery_errors_per_run"] = errors
    stages["produce"]["note"] = (
        f"producer start-up excluded; flushed every {BATCH_SIZE} records like a connector batch"
    )
    batches = n // BATCH_SIZE
    stages["commit"] = summarize_runs(
        [
            _commit_run(env.brokers, input_topic, f"{env.prefix}commit-{bank}-{i}", batches)
            for i in range(repeats)
        ],
        batches * BATCH_SIZE,
    )
    stages["commit"]["note"] = f"{batches} synchronous commits, one per {BATCH_SIZE}-record batch"
    stages["sum_of_stage_medians_us_per_record"] = round(
        sum(stages[s]["median_us_per_record"] for s in STAGES_FOR_BOTTLENECK), 3
    )
    stages["largest_stage_by_median"] = largest_stage(stages)
    return stages


def _bench_end_to_end(env: _Env, bank: str, repeats: int, scratch: Path) -> dict[str, Any]:
    from batch.spark import common

    runs = []
    for i in range(repeats):
        summary = scratch / f"_connector_{bank}_{i}.json"
        command = [
            sys.executable, "-m", "stream.connectors.run", "--bank", bank,
            "--topic-prefix", env.prefix, "--group", f"{env.prefix}e2e-{bank}-{i}",
            "--idle-exit", "5", "--summary", str(summary),
        ]  # fmt: skip
        proc = subprocess.run(
            command, cwd=common.ROOT, capture_output=True, text=True, timeout=1800
        )
        if proc.returncode:
            raise RuntimeError(f"connector {bank} failed: {proc.stderr[-1500:]}")
        data = json.loads(summary.read_text())
        summary.unlink()
        keys = ("records_in", "records_out", "dlq", "active_seconds", "records_per_second_active")
        runs.append({k: data[k] for k in keys})
    rates = [r["records_per_second_active"] for r in runs]
    return {
        "runs": runs,
        "median_records_per_second": statistics.median(rates),
        "min_records_per_second": min(rates),
        "max_records_per_second": max(rates),
        "microseconds_per_record_at_median": round(1e6 / statistics.median(rates), 3),
    }


def run(ds: str, n: int, stage_repeats: int, e2e_repeats: int, out: Path) -> dict[str, Any]:
    import fastavro
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext
    from pyiceberg.catalog.sql import SqlCatalog

    from batch.spark import common
    from stream.connectors import registry
    from stream.connectors.engine import load_config
    from stream.settings import BROKERS, CANONICAL_TOPIC, PRODUCER_CONFIG, SCHEMA_REGISTRY

    common.require_data_root()
    catalog = SqlCatalog("lake", uri=common.pyiceberg_uri(), warehouse=f"file://{common.WAREHOUSE}")
    table = catalog.load_table(f"aml_{ds}.raw_transactions")
    prefix = f"bench{int(time.time())}."
    topics = {
        f"{prefix}bank_a.raw": 6,
        f"{prefix}bank_b.raw": 6,
        f"{prefix}{CANONICAL_TOPIC}": 12,
        f"{prefix}tx.dlq": 1,
    }
    admin = AdminClient({"bootstrap.servers": BROKERS, **CLIENT_CONFIG})
    new_topics = [NewTopic(t, num_partitions=p, replication_factor=1) for t, p in topics.items()]
    for future in admin.create_topics(new_topics).values():
        future.result(timeout=30)

    schema_id = registry.register_canonical(SCHEMA_REGISTRY)
    schema_str = registry.canonical_schema().schema_str
    parsed = fastavro.parse_schema(json.loads(schema_str))
    header = b"\x00" + schema_id.to_bytes(4, "big")

    def frame(record: dict[str, Any]) -> bytes:
        buffer = io.BytesIO()
        buffer.write(header)
        fastavro.schemaless_writer(buffer, parsed, record)
        return buffer.getvalue()

    env = _Env(
        brokers=BROKERS,
        prefix=prefix,
        serializer=AvroSerializer(
            registry.client(SCHEMA_REGISTRY),
            schema_str,
            conf={
                "auto.register.schemas": False,
                "subject.name.strategy": lambda ctx, name: registry.SUBJECT,
            },
        ),
        context=SerializationContext(f"{prefix}{CANONICAL_TOPIC}", MessageField.VALUE),
        frame=frame,
        canonical_topic=f"{prefix}{CANONICAL_TOPIC}",
        producer_config=PRODUCER_CONFIG,
    )
    doc: dict[str, Any] = {
        "dataset": ds,
        "check": "M03 ingest benchmark",
        "sample": {"day": DAY_FROM, "records_per_bank": n, "stage_repeats": stage_repeats},
        "machine": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "swap_before": _swap(),
            "load_average_1m": os.getloadavg()[0],
        },
        "config": {
            "processes": "host processes (not containers)",
            "connector_batch_size": BATCH_SIZE,
            "serializer_in_use": "confluent_kafka AvroSerializer",
            "producer": PRODUCER_CONFIG,
            "scratch_topic_prefix": prefix,
            "end_to_end_repeats": e2e_repeats,
        },
        "stages": {},
        "end_to_end": {},
    }
    try:
        doc["producer_startup_first_ack_ms"] = producer_startup(BROKERS, env.canonical_topic)
        for bank in ("A", "B"):
            cfg = load_config(common.ROOT / "connectors" / "banks" / f"bank_{bank.lower()}.yaml")
            rows = _bank_rows(table, bank, n)
            doc["stages"][bank] = _bench_stages(env, bank, cfg, rows, stage_repeats)
            doc["end_to_end"][bank] = _bench_end_to_end(env, bank, e2e_repeats, out.parent)
            stage_sum = doc["stages"][bank]["sum_of_stage_medians_us_per_record"]
            e2e_us = doc["end_to_end"][bank]["microseconds_per_record_at_median"]
            doc["end_to_end"][bank]["stage_sum_us_per_record"] = stage_sum
            doc["end_to_end"][bank]["not_attributed_to_a_stage_us_per_record"] = round(
                e2e_us - stage_sum, 3
            )
            doc["end_to_end"][bank]["not_attributed_pct_of_end_to_end"] = round(
                100 * (e2e_us - stage_sum) / e2e_us, 1
            )
    finally:
        for future in admin.delete_topics(list(topics)).values():
            try:
                future.result(timeout=30)
            except Exception:  # noqa: BLE001 - cleanup must not hide the real error
                pass
    doc["machine"]["swap_after"] = _swap()
    doc["machine"]["load_average_1m_after"] = os.getloadavg()[0]
    doc.update(common.provenance())
    common.write_json(out, doc)
    return doc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", default="hi_small")
    parser.add_argument("--records", type=int, default=300_000, help="per bank")
    parser.add_argument("--stage-repeats", type=int, default=5)
    parser.add_argument("--e2e-repeats", type=int, default=3)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    from batch.spark import common

    out = args.out or common.ROOT / "results" / "M03" / args.ds / "ingest_bench.json"
    doc = run(args.ds, args.records, args.stage_repeats, args.e2e_repeats, out)
    for bank in ("A", "B"):
        e2e = doc["end_to_end"][bank]
        print(
            f"bank {bank}: end to end median {e2e['median_records_per_second']:.0f} msgs/s "
            f"(runs {[r['records_per_second_active'] for r in e2e['runs']]}); "
            f"largest stage {doc['stages'][bank]['largest_stage_by_median']}"
        )
    print(f"-> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
