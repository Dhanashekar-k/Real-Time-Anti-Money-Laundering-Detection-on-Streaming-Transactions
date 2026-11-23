"""Settings shared by the replayer and the connectors."""

import os

BROKERS = os.environ.get("AML_KAFKA_BROKERS", "localhost:19092")
SCHEMA_REGISTRY = os.environ.get("AML_SCHEMA_REGISTRY", "http://localhost:18081")

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
