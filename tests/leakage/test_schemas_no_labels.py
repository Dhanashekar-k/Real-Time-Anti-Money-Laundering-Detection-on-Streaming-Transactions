"""CLAUDE.md rule 2: label and split data never appear in any Kafka value schema."""

import json
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.unit

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
FORBIDDEN = ("is_laundering", "laundering", "split", "label", "pattern", "attempt_id")


def _names(node: Any) -> list[str]:
    """Every record, field, symbol and alias name in an Avro schema, at any depth.

    Aliases count: an Avro reader resolves a field through its aliases, so a field
    `risk_flag` with alias `is_laundering` would still carry the label.
    """
    found: list[str] = []
    if isinstance(node, dict):
        if isinstance(node.get("name"), str):
            found.append(node["name"])
        found += [s for s in node.get("symbols", []) if isinstance(s, str)]
        found += [a for a in node.get("aliases", []) if isinstance(a, str)]
        for value in node.values():
            found += _names(value)
    elif isinstance(node, list):
        for item in node:
            found += _names(item)
    return found


def _leaked(schema: Any) -> list[str]:
    return [n for n in _names(schema) for bad in FORBIDDEN if bad in n.lower()]


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "record", "name": "R", "fields": [{"name": "is_laundering", "type": "boolean"}]},
        {"type": "record", "name": "R", "fields": [{"name": "split", "type": "string"}]},
        {  # hidden behind an alias
            "type": "record",
            "name": "R",
            "fields": [{"name": "risk_flag", "type": "boolean", "aliases": ["is_laundering"]}],
        },
        {  # nested, in an enum symbol
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "k", "type": {"type": "enum", "name": "E", "symbols": ["PATTERN"]}}
            ],
        },
    ],
)
def test_walker_catches_label_names_in_every_position(schema: dict[str, Any]) -> None:
    """Positive control: the leak check must be able to fail."""
    assert _leaked(schema)


@pytest.mark.parametrize("path", sorted(CONTRACTS.glob("*.avsc")), ids=lambda p: p.name)
def test_avro_schema_has_no_label_fields(path: Path) -> None:
    schema = json.loads(path.read_text())
    assert _names(schema), f"{path.name}: found no names, the walker is broken"
    leaked = _leaked(schema)
    assert not leaked, f"{path.name} exposes label-like names: {leaked}"
