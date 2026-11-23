"""Schema registry helpers: register `contracts/canonical_tx.avsc` and check compatibility.

    uv run python -m stream.connectors.registry register    # idempotent; sets BACKWARD
    uv run python -m stream.connectors.registry check       # exit 1 if the file is not compatible

Subject `tx.canonical-value`, compatibility BACKWARD (the registry is Redpanda's built-in one).
"""

import argparse
from collections.abc import Sequence
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from stream.settings import SCHEMA_REGISTRY

SUBJECT = "tx.canonical-value"
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "contracts" / "canonical_tx.avsc"


def client(url: str = SCHEMA_REGISTRY) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": url})


def canonical_schema() -> Schema:
    return Schema(SCHEMA_PATH.read_text(), "AVRO")


def register_canonical(url: str = SCHEMA_REGISTRY) -> int:
    """Set the subject to BACKWARD and register the schema (the same text gets the same id)."""
    registry = client(url)
    registry.set_compatibility(subject_name=SUBJECT, level="BACKWARD")
    return registry.register_schema(SUBJECT, canonical_schema())


def is_compatible(url: str = SCHEMA_REGISTRY) -> bool:
    """Ask the registry whether the file on disk is compatible with the latest registered one."""
    return client(url).test_compatibility(SUBJECT, canonical_schema())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=["register", "check"])
    parser.add_argument("--registry", default=SCHEMA_REGISTRY)
    args = parser.parse_args(argv)
    if args.action == "register":
        schema_id = register_canonical(args.registry)
        print(f"{SUBJECT}: schema id {schema_id}, compatibility BACKWARD")
        return 0
    ok = is_compatible(args.registry)
    print(f"{SUBJECT}: {'compatible' if ok else 'NOT compatible'} with the registered schema")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
