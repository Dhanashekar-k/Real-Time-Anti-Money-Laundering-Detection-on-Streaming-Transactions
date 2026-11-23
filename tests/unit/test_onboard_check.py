"""`make onboard-check`: map a sample file with no Kafka and report what an FDE needs (M03 step 5).

The two samples are built by tests/fixtures/make_stream_samples.py: 200 good records plus a
fixed set of bad ones, so every expected number below follows from how the files were made:

  Bank A, 207 records, 7 bad: MALFORMED_RECORD 1, MISSING_FIELD 1, UNKNOWN_VALUE 3 ("Crypto" x2 in
  payment_format, "Zloty" in payment_currency), NEGATIVE_AMOUNT 1, BAD_TIMESTAMP 1.
  Bank B, 207 records, 7 bad: MALFORMED_RECORD 1, MISSING_FIELD 1, UNKNOWN_VALUE 3 ("HAWALA" x2 in
  payment_format, "PLN" in payment_currency), NEGATIVE_AMOUNT 1, BAD_TIMESTAMP 1.
"""

import json
from pathlib import Path

import pytest

from stream.connectors.engine import MappingError, diagnose, load_config, map_record
from stream.connectors.onboard_check import check_sample, format_report, main, read_sample

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures"
BANKS = ROOT / "connectors" / "banks"
EXPECTED_CODES = {
    "MALFORMED_RECORD": 1,
    "MISSING_FIELD": 1,
    "UNKNOWN_VALUE": 3,
    "NEGATIVE_AMOUNT": 1,
    "BAD_TIMESTAMP": 1,
}


def report_for(bank: str, sample: str) -> dict:
    cfg = load_config(BANKS / f"bank_{bank}.yaml")
    return check_sample(cfg, read_sample(FIXTURES / sample))


@pytest.fixture(scope="module", params=[("a", "bank_a_sample.csv"), ("b", "bank_b_sample.jsonl")])
def report(request: pytest.FixtureRequest) -> dict:
    return report_for(*request.param)


def test_counts_and_dlq_rate_by_code(report: dict) -> None:
    assert report["records"] == 207 and report["mapped"] == 200
    assert report["dlq"]["count"] == 7
    assert report["dlq"]["by_code"] == EXPECTED_CODES
    assert report["dlq"]["rate"] == pytest.approx(7 / 207)


UNMAPPED = {
    "a": {"payment_currency": {"Zloty": 1}, "payment_format": {"Crypto": 2}},
    "b": {"payment_currency": {"PLN": 1}, "payment_format": {"HAWALA": 2}},
}
# Records that map field by field, out of 207. A record that cannot be parsed at all (the truncated
# JSON / the short CSV line) counts against every field. Bank A checks currencies, amounts and
# times inside the fields; Bank B's unknown currency and negative amount are caught later, by
# `validate`, so they cost a DLQ record but not field coverage.
COVERAGE = {
    "a": {
        "tx_id": 206,
        "event_ts_ms": 205,  # malformed, bad time
        "sender_bank": 206,
        "sender_account": 205,  # malformed, empty account
        "receiver_bank": 206,
        "receiver_account": 206,
        "payment_currency": 205,  # malformed, "Zloty"
        "receiving_currency": 206,
        "amount_paid_minor": 204,  # malformed, "-5.00", and the Zloty line (its currency failed)
        "amount_received_minor": 206,
        "payment_format": 204,  # malformed, "Crypto" x2
    },
    "b": {
        "tx_id": 206,
        "event_ts_ms": 205,  # malformed, "yesterday"
        "sender_bank": 206,
        "sender_account": 205,  # malformed, no accountNo
        "receiver_bank": 206,
        "receiver_account": 206,
        "payment_currency": 206,  # "PLN" is a string; `validate` rejects it
        "receiving_currency": 206,
        "amount_paid_minor": 206,  # -1 is a long; `validate` rejects it
        "amount_received_minor": 206,
        "payment_format": 204,  # malformed, "HAWALA" x2
    },
}


@pytest.mark.parametrize(
    ("bank", "sample"), [("a", "bank_a_sample.csv"), ("b", "bank_b_sample.jsonl")]
)
def test_unmapped_vocabulary_and_per_field_coverage_are_exact(bank: str, sample: str) -> None:
    rep = report_for(bank, sample)
    assert rep["unmapped_values"] == UNMAPPED[bank]
    assert {f: c["ok"] for f, c in rep["field_coverage"].items()} == COVERAGE[bank]
    assert all(c["of"] == 207 for c in rep["field_coverage"].values())
    assert rep["field_coverage"]["tx_id"]["pct"] == pytest.approx(100 * 206 / 207)


def test_three_canonical_examples_are_the_first_mapped_records(report: dict) -> None:
    examples = report["examples"]["canonical"]
    assert len(examples) == 3
    assert [e["tx_id"] for e in examples] == [1, 2, 3]
    assert all("sender_key" in e and "amount_paid_usd_cents" in e for e in examples)


def test_failed_examples_name_the_line_and_the_reason(report: dict) -> None:
    failed = report["examples"]["failed"]
    assert 1 <= len(failed) <= 3
    assert failed[0]["line"] == 201 and failed[0]["code"] == "MALFORMED_RECORD"
    assert set(failed[0]) >= {"line", "code", "field", "detail"}


def test_the_text_report_shows_the_numbers_an_fde_looks_for(report: dict) -> None:
    text = format_report(report)
    for needle in (
        "207",
        "200",
        "MALFORMED_RECORD",
        "UNKNOWN_VALUE",
        "payment_format",
        "sender_account",
    ):
        assert needle in text, needle


# --- the report agrees with the engine ------------------------------------------------------------


def test_a_clean_sample_has_no_dlq_and_full_coverage(tmp_path: Path) -> None:
    lines = (FIXTURES / "bank_b_sample.jsonl").read_text().splitlines()[:50]
    sample = tmp_path / "clean.jsonl"
    sample.write_text("\n".join(lines) + "\n")
    rep = check_sample(load_config(BANKS / "bank_b.yaml"), read_sample(sample))
    assert rep["records"] == 50 and rep["mapped"] == 50 and rep["dlq"]["count"] == 0
    assert all(c["ok"] == c["of"] == 50 for c in rep["field_coverage"].values())
    assert rep["unmapped_values"] == {}


def test_mapped_count_equals_what_map_record_returns() -> None:
    cfg = load_config(BANKS / "bank_a.yaml")
    lines = read_sample(FIXTURES / "bank_a_sample.csv")
    results = [map_record(line, cfg) for line in lines]
    assert sum(not isinstance(r, MappingError) for r in results) == 200


def test_diagnose_evaluates_each_field_on_its_own() -> None:
    cfg = load_config(BANKS / "bank_b.yaml")
    line = read_sample(FIXTURES / "bank_b_sample.jsonl")[
        201
    ]  # the record without originator.accountNo
    result = diagnose(line, cfg)
    assert not isinstance(result, MappingError)
    assert result["sender_account"] == "MISSING_FIELD"
    assert result["receiver_account"] is None and result["amount_paid_minor"] is None


def test_diagnose_of_an_unparseable_record_is_a_mapping_error() -> None:
    cfg = load_config(BANKS / "bank_b.yaml")
    assert isinstance(diagnose(b"{", cfg), MappingError)


# --- reading the sample file ----------------------------------------------------------------------


def test_bom_crlf_and_a_trailing_newline_are_handled(tmp_path: Path) -> None:
    good = (FIXTURES / "bank_b_sample.jsonl").read_text().splitlines()[0]
    sample = tmp_path / "windows.jsonl"
    sample.write_bytes(b"\xef\xbb\xbf" + (good + "\r\n" + good + "\r\n").encode())
    lines = read_sample(sample)
    assert lines == [good.encode(), good.encode()]


def test_a_blank_line_in_the_middle_is_a_malformed_record(tmp_path: Path) -> None:
    good = (FIXTURES / "bank_b_sample.jsonl").read_text().splitlines()[0]
    sample = tmp_path / "blank.jsonl"
    sample.write_text(f"{good}\n\n{good}\n")
    rep = check_sample(load_config(BANKS / "bank_b.yaml"), read_sample(sample))
    assert rep["records"] == 3 and rep["dlq"]["by_code"] == {"MALFORMED_RECORD": 1}


def test_unused_source_columns_are_listed(tmp_path: Path) -> None:
    cfg = load_config(
        {
            "bank_id": "T",
            "mapping_version": "t-1",
            "source": {
                "topic": "t.raw",
                "format": "csv",
                "csv": {"columns": ["id", "memo", "extra"], "header": False},
                "ignore": ["memo"],
            },
            "fields": {"id": {"from": "id", "type": "long"}},
        }
    )
    rep = check_sample(cfg, [b"1,hello,x", b"2,world,y"])
    assert rep["unused_source_columns"] == ["extra"]  # `memo` is ignored on purpose


# --- the command ----------------------------------------------------------------------------------


def test_main_exits_one_when_records_fail_and_writes_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "report.json"
    code = main(
        ["--bank", "b", "--sample", str(FIXTURES / "bank_b_sample.jsonl"), "--json", str(out)]
    )
    assert code == 1
    assert "UNKNOWN_VALUE" in capsys.readouterr().out
    doc = json.loads(out.read_text())
    assert doc["records"] == 207 and doc["dlq"]["count"] == 7 and doc["bank"] == "B"
    # a results file says what it was run on and by which code
    import hashlib

    sample = FIXTURES / "bank_b_sample.jsonl"
    assert doc["sample"]["sha256"] == hashlib.sha256(sample.read_bytes()).hexdigest()
    assert doc["sample"]["bytes"] == sample.stat().st_size
    assert len(doc["git_sha"]) == 40 and doc["timestamp"].endswith("+00:00")
    assert doc["config"] == "connectors/banks/bank_b.yaml"  # relative: no machine-local path
    assert doc["dataset"].startswith("none")


def test_main_exits_zero_for_a_clean_sample(tmp_path: Path) -> None:
    good = (FIXTURES / "bank_a_sample.csv").read_text().splitlines()[:5]
    sample = tmp_path / "clean.csv"
    sample.write_text("\n".join(good) + "\n")
    assert main(["--bank", "A", "--sample", str(sample)]) == 0


def test_main_accepts_either_case_for_the_bank() -> None:
    for bank in ("a", "A"):
        main(["--bank", bank, "--sample", str(FIXTURES / "bank_a_sample.csv")])
