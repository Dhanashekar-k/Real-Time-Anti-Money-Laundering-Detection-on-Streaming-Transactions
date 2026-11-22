"""CLAUDE.md rule 2, feature side: no label, pattern or split information in the feature list,
the feature definitions, or what the oracle accepts as input.

Static half only. The behavioural half (no feature looks at the future) is
`test_future_invariance.py`, and the `features_tx` table schema check is added with the Spark job.
"""

import re
from pathlib import Path

import pytest

from features import expand, reference, spec

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


def leaked(text: str) -> list[str]:
    """Forbidden whole words, plus the compound identifiers as substrings.

    `is_laundering` inside `s_is_laundering_rate_24h` is not a whole word, hence the second test.
    """
    whole = {m.group(1).lower() for m in _WORD.finditer(text)}
    sub = {w for w in ("is_laundering", "pattern_type", "attempt_id") if w in text.lower()}
    return sorted(whole | sub)


# --- positive controls: the scanner must be able to fail -----------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "is_laundering",
        "s_is_laundering_rate_24h",
        "pattern_type_code",
        "avg of the label over the window",
        "split",
        "attempt_id",
    ],
)
def test_scanner_catches(text: str) -> None:
    assert leaked(text)


@pytest.mark.parametrize(
    "text", ["s_out_count_1h", "r_returned_funds_24h", "log10_amount_paid_usd", "splits_ok"]
)
def test_scanner_passes_clean_names(text: str) -> None:
    assert leaked(text) == []


# --- the checks -----------------------------------------------------------------------------------


def test_feature_names_file_is_clean() -> None:
    text = (ROOT / "features" / "feature_names_v1.txt").read_text()
    assert leaked(text) == []


def test_expanded_names_are_clean() -> None:
    names = expand.feature_names(spec.load_spec())
    assert [n for n in names if leaked(n)] == []


def test_spec_feature_definitions_are_clean() -> None:
    doc = spec.load_raw()
    for section in ("tx_features", "account_features", "relational_features"):
        assert leaked(repr(doc[section])) == [], section
    assert leaked(repr(doc["legs"])) == []


def test_forbidden_inputs_in_the_spec_cover_what_this_test_forbids() -> None:
    declared = set(spec.load_spec().forbidden_inputs)
    assert {"is_laundering", "pattern_type", "attempt_id", "split"} <= declared


def test_oracle_inputs_and_outputs_carry_no_label() -> None:
    s = spec.load_spec()
    assert [c for c in reference.LEG_COLUMNS if leaked(c)] == []
    assert [c for c in reference.output_columns(s) if leaked(c)] == []


def test_oracle_refuses_every_forbidden_input_column() -> None:
    import pandas as pd

    s = spec.load_spec()
    legs = pd.DataFrame([], columns=list(reference.LEG_COLUMNS))
    for bad in s.forbidden_inputs:
        with pytest.raises(ValueError, match=bad):
            reference.compute_account_legs(legs.assign(**{bad: []}), s)


def test_feature_job_reads_and_writes_no_label_column() -> None:
    """The Spark job's input list and output keys name no label, split or pattern column."""
    from batch.spark import features

    columns = [*features.RAW_COLUMNS, *features.KEY_COLUMNS]
    assert [c for c in columns if leaked(c)] == []
    # the job selects RAW_COLUMNS first, so the list is the whole input surface
    assert "is_laundering" not in features.RAW_COLUMNS and "split" not in features.RAW_COLUMNS
