"""Parsing the IBM patterns file: BEGIN/END LAUNDERING ATTEMPT blocks of transaction lines."""

from pathlib import Path

import pytest

from batch.spark.patterns import parse_patterns

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "aml" / "mini_patterns.txt"

LINE = "2022/09/01 00:10,30,ACC010,40,ACC011,7000.00,US Dollar,1000000.00,Yen,Wire,1"


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "patterns.txt"
    path.write_text(text)
    return path


def test_fixture_has_two_attempts_and_three_lines() -> None:
    rows = parse_patterns(FIXTURE)
    assert len(rows) == 3
    assert [r["attempt_id"] for r in rows] == [0, 0, 1]
    assert [r["line_in_attempt"] for r in rows] == [0, 1, 0]
    assert [r["pattern_type"] for r in rows] == ["FAN-OUT", "FAN-OUT", "CYCLE"]
    assert rows[0]["pattern_detail"] == "Max 2-degree Fan-Out"
    assert rows[2]["pattern_detail"] == "Max 1 hops"


def test_fields_are_the_eleven_transaction_columns_as_strings() -> None:
    row = parse_patterns(FIXTURE)[0]
    assert row["raw_from_account"] == "ACC010"
    assert row["raw_amount_paid"] == "1000000.00"
    assert row["raw_payment_currency"] == "Yen"
    assert row["raw_is_laundering"] == "1"


def test_types_are_read_from_the_file_not_assumed(tmp_path: Path) -> None:
    kind = "SOME-NEW-TYPE"
    text = f"BEGIN LAUNDERING ATTEMPT - {kind}\n{LINE}\nEND LAUNDERING ATTEMPT - {kind}\n"
    (row,) = parse_patterns(_write(tmp_path, text))
    assert row["pattern_type"] == "SOME-NEW-TYPE"
    assert row["pattern_detail"] == ""


def test_blank_lines_between_blocks_are_ignored(tmp_path: Path) -> None:
    block = f"BEGIN LAUNDERING ATTEMPT - STACK: x\n{LINE}\nEND LAUNDERING ATTEMPT - STACK\n"
    rows = parse_patterns(_write(tmp_path, "\n" + block + "\n\n" + block + "\n"))
    assert [r["attempt_id"] for r in rows] == [0, 1]


def test_mismatched_end_is_an_error(tmp_path: Path) -> None:
    text = f"BEGIN LAUNDERING ATTEMPT - STACK\n{LINE}\nEND LAUNDERING ATTEMPT - CYCLE\n"
    with pytest.raises(ValueError, match="END"):
        parse_patterns(_write(tmp_path, text))


def test_unclosed_block_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unclosed"):
        parse_patterns(_write(tmp_path, f"BEGIN LAUNDERING ATTEMPT - STACK\n{LINE}\n"))


def test_data_line_outside_a_block_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="outside"):
        parse_patterns(_write(tmp_path, LINE + "\n"))


def test_wrong_field_count_is_an_error(tmp_path: Path) -> None:
    short = "2022/09/01 00:10,30,ACC010"
    text = f"BEGIN LAUNDERING ATTEMPT - STACK\n{short}\nEND LAUNDERING ATTEMPT - STACK\n"
    with pytest.raises(ValueError, match="11 fields"):
        parse_patterns(_write(tmp_path, text))


def test_begin_inside_a_block_is_an_error(tmp_path: Path) -> None:
    text = "BEGIN LAUNDERING ATTEMPT - STACK\nBEGIN LAUNDERING ATTEMPT - CYCLE\n"
    with pytest.raises(ValueError, match="nested|unclosed|BEGIN"):
        parse_patterns(_write(tmp_path, text))
