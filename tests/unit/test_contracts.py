"""Contract files must be well-formed: Avro schemas parse, the agent schema is valid JSON Schema."""

import json
from pathlib import Path

import pytest
from fastavro import parse_schema
from jsonschema import Draft202012Validator

pytestmark = pytest.mark.unit

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
AVRO_FILES = ["canonical_tx.avsc", "feature_vector.avsc", "score.avsc"]


@pytest.mark.parametrize("name", AVRO_FILES)
def test_avsc_parse(name: str) -> None:
    schema = json.loads((CONTRACTS / name).read_text())
    parsed = parse_schema(schema)
    assert parsed["type"] == "record"
    # fastavro folds the namespace into the record name
    assert parsed["name"] == f"com.streamaml.avro.{schema['name']}"
    assert schema["namespace"] == "com.streamaml.avro"


def test_every_avsc_is_covered() -> None:
    on_disk = sorted(p.name for p in CONTRACTS.glob("*.avsc"))
    assert on_disk == sorted(AVRO_FILES), "new .avsc file: add it to AVRO_FILES"


def test_agent_output_schema_valid() -> None:
    schema = json.loads((CONTRACTS / "agent_output.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    assert schema["type"] == "object"
