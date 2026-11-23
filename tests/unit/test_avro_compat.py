"""`contracts/canonical_tx.avsc` stays BACKWARD compatible (M03 step 4, CI check).

BACKWARD means: a reader using the NEW schema can read data written with the OLD one. This is what
the registry's `/compatibility` endpoint checks (also done in the integration test); here the same
rule is applied with fastavro's reader/writer resolution so it runs with no services. It is a check
of resolution on a generated sample record, not a re-implementation of every registry rule.
"""

import copy
import io
import json
import subprocess
from pathlib import Path
from typing import Any

import fastavro
import pytest
from fastavro.utils import generate_one

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "contracts" / "canonical_tx.avsc"


def backward_compatible(old: dict[str, Any], new: dict[str, Any]) -> bool:
    """True if a record written with `old` can be read with `new`."""
    writer = fastavro.parse_schema(old)
    reader = fastavro.parse_schema(new)
    record = generate_one(writer)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, writer, record)
    buf.seek(0)
    try:
        fastavro.schemaless_reader(buf, writer, reader)
    except fastavro.read.SchemaResolutionError:
        return False
    return True


def shipped() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text())


def test_the_schema_is_compatible_with_itself() -> None:
    assert backward_compatible(shipped(), shipped())


def test_a_new_field_without_a_default_is_not_backward_compatible() -> None:
    new = copy.deepcopy(shipped())
    new["fields"].append({"name": "branch", "type": "string"})
    assert not backward_compatible(shipped(), new)


def test_a_new_field_with_a_default_is_backward_compatible() -> None:
    new = copy.deepcopy(shipped())
    new["fields"].append({"name": "branch", "type": ["null", "string"], "default": None})
    assert backward_compatible(shipped(), new)


def test_changing_a_field_type_is_not_backward_compatible() -> None:
    new = copy.deepcopy(shipped())
    for field in new["fields"]:
        if field["name"] == "tx_id":
            field["type"] = "string"
    assert not backward_compatible(shipped(), new)


def test_removing_a_field_is_backward_compatible() -> None:
    new = copy.deepcopy(shipped())
    new["fields"] = [f for f in new["fields"] if f["name"] != "fx_version"]
    assert backward_compatible(shipped(), new)


def _on_main() -> dict[str, Any] | None:
    proc = subprocess.run(
        ["git", "show", "main:contracts/canonical_tx.avsc"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    return json.loads(proc.stdout) if proc.returncode == 0 else None


def test_the_working_copy_is_backward_compatible_with_main() -> None:
    old = _on_main()
    if old is None:
        pytest.skip("no `main` branch to compare with (shallow clone?)")
    assert backward_compatible(old, shipped()), (
        "contracts/canonical_tx.avsc changed in a way old data cannot be read with: "
        "add new fields with a default, never change or reorder types"
    )


def test_a_canonical_record_round_trips() -> None:
    schema = fastavro.parse_schema(shipped())
    record = generate_one(schema)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    buf.seek(0)
    assert fastavro.schemaless_reader(buf, schema) == record
