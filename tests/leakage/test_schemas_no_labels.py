"""CLAUDE.md rule 2: label and split data never appear in any Kafka value schema."""

import json
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.unit

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
FORBIDDEN = ("is_laundering", "laundering", "split", "label", "pattern", "attempt_id")


def _names(node: Any) -> list[str]:
    """Every record, field and symbol name in an Avro schema, at any depth."""
    found: list[str] = []
    if isinstance(node, dict):
        if isinstance(node.get("name"), str):
            found.append(node["name"])
        found += [s for s in node.get("symbols", []) if isinstance(s, str)]
        for value in node.values():
            found += _names(value)
    elif isinstance(node, list):
        for item in node:
            found += _names(item)
    return found


@pytest.mark.parametrize("path", sorted(CONTRACTS.glob("*.avsc")), ids=lambda p: p.name)
def test_avro_schema_has_no_label_fields(path: Path) -> None:
    names = _names(json.loads(path.read_text()))
    assert names, f"{path.name}: found no names, the walker is broken"
    leaked = [n for n in names for bad in FORBIDDEN if bad in n.lower()]
    assert not leaked, f"{path.name} exposes label-like names: {leaked}"
