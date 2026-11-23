"""Settings shared by the replayer and the connectors."""

import os

BROKERS = os.environ.get("AML_KAFKA_BROKERS", "localhost:19092")
SCHEMA_REGISTRY = os.environ.get("AML_SCHEMA_REGISTRY", "http://localhost:18081")

# librdkafka resolves `localhost` to ::1 first and the local broker listens on IPv4 only. The
# refused connection cost a new client between 0 and 5 seconds before its first ack (D53; measured
# in results/M03/<ds>/ingest_bench.json). Forcing IPv4 removes it; set
# AML_KAFKA_ADDRESS_FAMILY=any to undo.
CLIENT_CONFIG: dict[str, object] = {
    "broker.address.family": os.environ.get("AML_KAFKA_ADDRESS_FAMILY", "v4"),
}

CANONICAL_TOPIC = "tx.canonical"
DLQ_TOPIC = "tx.dlq"

# The producer settings from the M03 manual (replayer and connectors alike): the idempotent
# producer prevents duplicates from its own retries, not from reprocessing after a crash.
PRODUCER_CONFIG: dict[str, object] = {
    "enable.idempotence": True,
    "acks": "all",
    "linger.ms": 5,
    "compression.type": "zstd",
    "batch.size": 262144,
}
