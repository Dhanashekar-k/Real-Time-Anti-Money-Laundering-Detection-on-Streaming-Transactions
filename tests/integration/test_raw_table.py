"""CSV -> Iceberg `raw_transactions`, checked on the 16-row synthetic fixture.

The fixture file is read with a tiny `maxPartitionBytes` so it splits into several Spark
partitions: the exact-row-index logic is only tested if more than one partition exists.
Needs `make up` (the Iceberg catalog lives in the compose Postgres).
"""

import csv
import hashlib
import json
import shutil
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from batch.spark import common
from batch.spark.csv_to_iceberg import ingest
from features import money

pytestmark = [pytest.mark.integration, pytest.mark.slow]

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "aml" / "mini_trans.csv"
NAMESPACE = "aml_mini"
TABLE = f"lake.{NAMESPACE}.raw_transactions"

FX = {
    "USD": "1", "EUR": "1.0850", "GBP": "1.2500", "CHF": "1.1000", "JPY": "0.0067",
    "CNY": "0.14", "INR": "0.012", "CAD": "0.73", "AUD": "0.65", "MXN": "0.05", "BRL": "0.2",
    "RUB": "0.011", "SAR": "0.2667", "ILS": "0.27", "BTC": "50000.00",
}  # fmt: skip

KEY_COLUMNS = [
    "tx_id", "src_row", "event_ts", "event_ts_ms", "sender_bank", "sender_account", "sender_key",
    "receiver_bank", "receiver_account", "receiver_key", "amount_paid_minor", "payment_currency",
    "amount_received_minor", "receiving_currency", "amount_paid_usd_cents",
    "amount_received_usd_cents", "payment_format", "owner_bank_id", "is_laundering", "split",
]  # fmt: skip


def _source_rows() -> list[dict[str, str]]:
    """Independent reader: Python csv module, positional columns, header dropped."""
    names = ["ts", "from_bank", "from_acc", "to_bank", "to_acc", "recv_amt", "recv_ccy",
             "paid_amt", "paid_ccy", "fmt", "laundering"]  # fmt: skip
    with FIXTURE.open() as handle:
        reader = csv.reader(handle)
        next(reader)
        return [dict(zip(names, row, strict=True)) for row in reader]


def _ts_ms(text: str) -> int:
    return int(datetime.strptime(text, "%Y/%m/%d %H:%M").replace(tzinfo=UTC).timestamp() * 1000)


@pytest.fixture(scope="module")
def result(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict]:
    results_dir = tmp_path_factory.mktemp("results")
    spark = common.get_spark("raw-table-test", max_partition_bytes=256)
    try:
        common.drop_namespace(spark, NAMESPACE)
        summary = ingest(
            spark,
            ds="mini",
            trans_path=FIXTURE,
            namespace=NAMESPACE,
            fx={k: Decimal(v) for k, v in FX.items()},
            results_dir=results_dir,
        )
        rows = [r.asDict() for r in spark.table(TABLE).orderBy("tx_id").collect()]
        yield {"summary": summary, "rows": rows, "results_dir": results_dir, "spark": spark}
    finally:
        try:
            common.drop_namespace(spark, NAMESPACE)
        finally:
            shutil.rmtree(common.WAREHOUSE / NAMESPACE, ignore_errors=True)
            spark.stop()


def test_row_count_matches_the_source_file(result: dict) -> None:
    source_lines = sum(1 for _ in FIXTURE.open()) - 1  # header
    assert len(result["rows"]) == source_lines == 16
    assert result["summary"]["rows"] == source_lines
    assert result["summary"]["source_lines_minus_header"] == source_lines


def test_the_file_was_read_in_several_partitions(result: dict) -> None:
    # If this were 1 the exact-row-index tests below could not catch a cross-partition bug.
    assert result["summary"]["source_partitions"] > 1


def test_header_is_dropped_and_not_trusted(result: dict) -> None:
    assert all(r["sender_bank"] != "From Bank" for r in result["rows"])
    assert all(r["sender_account"] != "Account" for r in result["rows"])


def test_tx_id_is_contiguous_and_unique(result: dict) -> None:
    assert [r["tx_id"] for r in result["rows"]] == list(range(16))


def test_order_is_event_time_then_source_row(result: dict) -> None:
    expected = sorted(
        range(16), key=lambda i: (_ts_ms(_source_rows()[i]["ts"]), i)
    )  # independent: file row index i is the tie-break
    assert [r["src_row"] for r in result["rows"]] == expected
    keys = [(r["event_ts_ms"], r["tx_id"]) for r in result["rows"]]
    assert all(a < b for a, b in zip(keys, keys[1:], strict=False))


def test_src_row_is_the_exact_file_row_index(result: dict) -> None:
    source = _source_rows()
    for r in result["rows"]:
        raw = source[r["src_row"]]
        assert r["sender_account"] == raw["from_acc"]
        assert r["receiver_account"] == raw["to_acc"]
        assert r["event_ts_ms"] == _ts_ms(raw["ts"])


def test_no_nulls_in_any_column(result: dict) -> None:
    for r in result["rows"]:
        for column in KEY_COLUMNS:
            assert r[column] is not None, (r["tx_id"], column)


def test_keys_are_bank_colon_account(result: dict) -> None:
    for r in result["rows"]:
        assert r["sender_key"] == f"{r['sender_bank']}:{r['sender_account']}"
        assert r["receiver_key"] == f"{r['receiver_bank']}:{r['receiver_account']}"


def test_money_matches_the_python_implementation(result: dict) -> None:
    source = _source_rows()
    fx = {k: Decimal(v) for k, v in FX.items()}
    codes = {
        "US Dollar": "USD", "Euro": "EUR", "UK Pound": "GBP", "Swiss Franc": "CHF",
        "Yen": "JPY", "Bitcoin": "BTC",
    }  # fmt: skip
    for r in result["rows"]:
        raw = source[r["src_row"]]
        paid = money.to_minor(raw["paid_amt"], codes[raw["paid_ccy"]])
        got = money.to_minor(raw["recv_amt"], codes[raw["recv_ccy"]])
        assert r["payment_currency"] == codes[raw["paid_ccy"]]
        assert r["amount_paid_minor"] == paid
        assert r["amount_received_minor"] == got
        assert r["amount_paid_usd_cents"] == money.minor_to_usd_cents(
            paid, r["payment_currency"], fx=fx
        )
        assert r["amount_received_usd_cents"] == money.minor_to_usd_cents(
            got, r["receiving_currency"], fx=fx
        )


def test_known_half_even_and_exponent_cases(result: dict) -> None:
    by_src = {r["src_row"]: r for r in result["rows"]}
    assert by_src[2]["amount_paid_minor"] == 12  # 0.125 USD: 12.5 cents, half-even -> 12
    assert by_src[12]["amount_paid_minor"] == 10000  # 99.995 -> 100.00
    assert by_src[4]["amount_paid_minor"] == 1_000_000  # JPY has exponent 0
    assert by_src[5]["amount_paid_minor"] == 123456  # BTC 0.00123456, exponent 8


def test_owner_bank_id_is_a_stable_hash_of_the_sender_bank(result: dict) -> None:
    for r in result["rows"]:
        digit = int(hashlib.md5(r["sender_bank"].encode()).hexdigest()[-1], 16)  # noqa: S324
        assert r["owner_bank_id"] == ("A" if digit % 2 == 0 else "B")
        assert r["owner_bank_id"] == common.owner_bank_id(r["sender_bank"])


def test_laundering_flag_is_an_integer_and_counted(result: dict) -> None:
    assert {r["is_laundering"] for r in result["rows"]} <= {0, 1}
    assert sum(r["is_laundering"] for r in result["rows"]) == 5
    assert result["summary"]["laundering"] == 5


def test_split_values_cover_every_row_once(result: dict) -> None:
    assert {r["split"] for r in result["rows"]} <= {"train", "val", "test"}
    counts = result["summary"]["split_counts"]
    assert sum(counts.values()) == 16


def test_table_is_partitioned_by_day_of_event_ts(result: dict) -> None:
    catalog = SqlCatalog(
        "lake",
        uri=common.pyiceberg_uri(),
        warehouse=f"file://{common.WAREHOUSE}",
    )
    table = catalog.load_table(f"{NAMESPACE}.raw_transactions")
    fields = table.spec().fields
    assert len(fields) == 1
    assert fields[0].transform.__class__.__name__ == "DayTransform"
    assert table.schema().find_field(fields[0].source_id).name == "event_ts"


def test_ingest_json_records_provenance(result: dict) -> None:
    path = result["results_dir"] / "mini" / "ingest.json"
    doc = json.loads(path.read_text())
    for key in ("git_sha", "dataset", "config", "timestamp", "rows", "laundering"):
        assert key in doc, key
    assert doc["dataset"] == "mini"
    assert doc["fx_version"] == 1 or doc["fx_version"] is None
    assert doc["source"]["path"].endswith("mini_trans.csv")
