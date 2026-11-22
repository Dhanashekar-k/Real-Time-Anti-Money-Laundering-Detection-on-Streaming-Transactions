"""CLAUDE.md rule 2, beyond the Avro schemas: the inputs a feature or a message can be built from.

The label (`is_laundering`), the pattern data (`pattern_type`, `attempt_id`) and `split` exist only
in the batch lake. They must not appear in the feature spec's definitions, the connector
mappings, the FX table or the money helper, because those are what Flink, the connectors and the
scorer are built from.
"""

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
FORBIDDEN = (
    "is_laundering",
    "laundering",
    "pattern_type",
    "pattern",
    "attempt_id",
    "split",
    "label",
)
_PATTERN = re.compile(r"(?<![a-z0-9])(" + "|".join(FORBIDDEN) + r")(?![a-z0-9])", re.IGNORECASE)


def leaked_words(text: str) -> list[str]:
    """Label-like words in free text, matched as whole words (so `splits_ok` does not count)."""
    return sorted({m.group(1).lower() for m in _PATTERN.finditer(text)})


def strings_in(node: Any) -> list[str]:
    """Every key and every string value of a parsed YAML/JSON document, at any depth."""
    out: list[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            out.append(str(key))
            out += strings_in(value)
    elif isinstance(node, list):
        for item in node:
            out += strings_in(item)
    elif isinstance(node, str):
        out.append(node)
    return out


# --- positive controls: the scanner must be able to fail ------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "is_laundering",
        "uses IS_LAUNDERING as a feature",
        "x = row['split']",
        "pattern_type == 'CYCLE'",
        "the attempt_id of the row",
        "a label column",
    ],
)
def test_scanner_catches_label_words(text: str) -> None:
    assert leaked_words(text)


def test_scanner_ignores_words_that_only_contain_them() -> None:
    assert leaked_words("splitter resplit labelled patterns") == []


def test_strings_in_reaches_keys_and_nested_values() -> None:
    found = strings_in({"a": [{"is_laundering": ["x", {"y": "split"}]}]})
    assert {"is_laundering", "split"} <= set(found)


# --- the files ------------------------------------------------------------------------------------


def test_the_spec_forbids_the_label_inputs() -> None:
    spec = yaml.safe_load((ROOT / "features" / "spec.yaml").read_text(encoding="utf-8"))
    assert set(spec["forbidden_inputs"]) >= {"is_laundering", "pattern_type", "attempt_id", "split"}


def test_no_feature_definition_mentions_a_label_word() -> None:
    spec = yaml.safe_load((ROOT / "features" / "spec.yaml").read_text(encoding="utf-8"))
    spec.pop("forbidden_inputs")  # the one place the words belong
    hits = {s: leaked_words(s) for s in strings_in(spec) if leaked_words(s)}
    assert not hits, (
        f"features/spec.yaml mentions label-like words outside forbidden_inputs: {hits}"
    )


@pytest.mark.parametrize(
    "path", sorted((ROOT / "connectors").rglob("*.yaml")), ids=lambda p: p.name
)
def test_connector_mappings_have_no_label_words(path: Path) -> None:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    hits = {s: leaked_words(s) for s in strings_in(doc) if leaked_words(s)}
    assert strings_in(doc), f"{path.name}: nothing parsed"
    assert not hits, f"{path.name}: {hits}"


def test_fx_table_has_no_label_words() -> None:
    doc = yaml.safe_load((ROOT / "features" / "fx_rates.yaml").read_text(encoding="utf-8"))
    assert strings_in(doc), "fx_rates.yaml parsed to nothing"
    hits = {s: leaked_words(s) for s in strings_in(doc) if leaked_words(s)}
    assert not hits, hits


def test_money_helper_has_no_label_words() -> None:
    text = (ROOT / "features" / "money.py").read_text(encoding="utf-8")
    assert text.strip(), "money.py is empty"
    assert leaked_words(text) == []
