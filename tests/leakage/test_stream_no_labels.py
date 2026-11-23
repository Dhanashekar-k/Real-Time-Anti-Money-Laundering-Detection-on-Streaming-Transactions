"""CLAUDE.md rule 2 for the M03 stream code: labels, pattern data and `split` never reach a topic.

The label columns sit in the same lake table as the fields the replayer needs, so the protection is
an explicit column allow-list, pinned here exactly: adding a column means editing this test on
purpose. Then the renderers and the engine are shown to ignore a label that is handed to them, and
a source scan checks that the stream code does not mention a label-like name.
"""

import ast
import json
import re
from pathlib import Path
from typing import Any

import pytest
from stream.connectors.engine import map_record
from stream.replayer.columns import SELECTED_FIELDS

from stream.replayer import render

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
_WORD = re.compile(r"(?<![a-z0-9])(" + "|".join(FORBIDDEN) + r")(?![a-z0-9])", re.IGNORECASE)

EXPECTED_COLUMNS = {
    "tx_id",
    "event_ts_ms",
    "sender_bank",
    "sender_account",
    "receiver_bank",
    "receiver_account",
    "payment_currency",
    "amount_paid_minor",
    "receiving_currency",
    "amount_received_minor",
    "payment_format",
    "owner_bank_id",
}
LABELLED_ROW: dict[str, Any] = {
    "tx_id": 7,
    "event_ts_ms": 1661991600000,
    "sender_bank": "10",
    "sender_account": "AAA",
    "receiver_bank": "20",
    "receiver_account": "BBB",
    "payment_currency": "USD",
    "amount_paid_minor": 100,
    "receiving_currency": "USD",
    "amount_received_minor": 100,
    "payment_format": "ACH",
    # what the lake table also holds; must never be rendered
    "is_laundering": 1,
    "split": "test",
    "pattern_type": "CYCLE",
    "attempt_id": 90909091,
}


def test_the_replayer_reads_exactly_the_allow_listed_columns() -> None:
    assert set(SELECTED_FIELDS) == EXPECTED_COLUMNS
    assert len(SELECTED_FIELDS) == len(set(SELECTED_FIELDS))


def test_no_allow_listed_column_looks_like_a_label() -> None:
    assert [c for c in SELECTED_FIELDS if _WORD.search(c)] == []


@pytest.mark.parametrize("bank", ["A", "B"])
def test_a_rendered_record_contains_no_label_data(bank: str) -> None:
    text = render.render(bank, LABELLED_ROW).decode()
    for needle in (
        "laundering",
        "split",
        "CYCLE",
        "pattern",
        "attempt",
        "90909091",
        '"test"',
        ",test",
    ):
        assert needle not in text, (bank, needle, text)


def test_rendering_a_labelled_row_equals_rendering_the_same_row_without_labels() -> None:
    clean = {k: v for k, v in LABELLED_ROW.items() if k in EXPECTED_COLUMNS}
    for bank in ("A", "B"):
        assert render.render(bank, LABELLED_ROW) == render.render(bank, clean)


def test_the_engine_drops_a_label_a_bank_sends_anyway(bank_b_cfg: Any) -> None:
    doc = {
        "transactionId": 1,
        "bookedAt": 1661991600000,
        "originator": {"bankCode": "10", "accountNo": "A1"},
        "beneficiary": {"bankCode": "10", "accountNo": "B1"},
        "debit": {"amountMinor": 100, "currency": "USD"},
        "credit": {"amountMinor": 100, "currency": "USD"},
        "method": "WIRE_TRANSFER",
        "isLaundering": True,
        "riskNotes": "split into 5",
    }
    out = map_record(json.dumps(doc).encode(), bank_b_cfg)
    assert isinstance(out, dict)
    assert not [k for k in out if _WORD.search(k)]
    assert "riskNotes" not in out and "split into 5" not in json.dumps(out)


def test_canonical_keys_are_exactly_the_avro_fields_minus_the_runner_fields(
    bank_a_cfg: Any,
) -> None:
    schema = json.loads((ROOT / "contracts" / "canonical_tx.avsc").read_text())
    avro = {f["name"] for f in schema["fields"]}
    out = map_record(render.render_bank_a(LABELLED_ROW), bank_a_cfg)
    assert isinstance(out, dict)
    assert set(out) == avro - {"replay_emit_ts_ms", "connector_ts_ms"}


# --- a source scan of everything under stream/ -------------------------------------------------


def _docstring_nodes(tree: ast.AST) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                ids.add(id(body[0].value))
    return ids


def label_like_names(source: str) -> list[str]:
    """Names, parameters, keyword names and string constants (not docstrings or attribute names).

    Attribute names are skipped on purpose: `line.split(",")` is a string method, not the split
    column. Comments and docstrings may say what the code must not do.
    """
    tree = ast.parse(source)
    skip = _docstring_nodes(tree)
    found: list[str] = []
    for node in ast.walk(tree):
        names: list[str] = []
        if isinstance(node, ast.Name):
            names = [node.id]
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names = [node.name]
        elif isinstance(node, ast.arg):
            names = [node.arg]
        elif isinstance(node, ast.keyword) and node.arg:
            names = [node.arg]
        elif (
            isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in skip
        ):
            names = [node.value]
        found += [n for n in names if _WORD.search(n)]
    return found


@pytest.mark.parametrize(
    "source",
    [
        "x = row['is_laundering']",
        "df['split']",
        "def pattern_type(): pass",
        "f(label=1)",
        "SQL = 'SELECT attempt_id FROM t'",
    ],
)
def test_the_source_scan_can_fail(source: str) -> None:
    assert label_like_names(source), source


@pytest.mark.parametrize(
    "source",
    [
        "line.split(',')",
        "labelnames = ['bank']",
        "def f():\n    '''never reads the label or the split'''\n    return 1",
        "splits_ok = True",
    ],
)
def test_the_source_scan_ignores_methods_docstrings_and_longer_words(source: str) -> None:
    assert label_like_names(source) == [], source


def test_no_stream_source_mentions_a_label_like_name() -> None:
    files = sorted((ROOT / "stream").rglob("*.py"))
    assert files, "stream/ has no Python files to scan"
    leaks = {str(p.relative_to(ROOT)): label_like_names(p.read_text()) for p in files}
    assert {k: v for k, v in leaks.items() if v} == {}
