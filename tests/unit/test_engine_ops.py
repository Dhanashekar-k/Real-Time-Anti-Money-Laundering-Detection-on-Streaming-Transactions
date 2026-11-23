"""The mapping engine's operations, one at a time (M03 step 2; the YAML syntax is in connectors/).

Each test builds a tiny inline config, so a failure names the operation, not a bank. The real bank
files are exercised in test_dlq_cases.py and test_mapping_equivalence.py.

Order of operations inside one field (documented in connectors/README.md, pinned here):
select (csv column or json path) -> trim -> upper -> strip_prefix -> extract -> default_from (when
the value is empty) -> map -> type / parse. Fields are processed in the order listed in the YAML.
"""

import json
from typing import Any

import pytest

from features import money
from stream.connectors.engine import ConfigError, MappingError, load_config, map_record

pytestmark = pytest.mark.unit


def csv_cfg(
    columns: list[str],
    fields: dict[str, Any],
    *,
    value_maps: dict[str, Any] | None = None,
    validate: dict[str, Any] | None = None,
    derive: list[str] | None = None,
    ignore: list[str] | None = None,
    **csv_extra: Any,
) -> Any:
    return load_config(
        {
            "bank_id": "T",
            "mapping_version": "t-1",
            "source": {
                "topic": "t.raw",
                "format": "csv",
                "csv": {
                    "delimiter": ",",
                    "header": False,
                    "quotechar": '"',
                    "columns": columns,
                    **csv_extra,
                },
                "ignore": ignore or [],
            },
            "fields": fields,
            "derive": derive or [],
            "validate": validate or {},
            "value_maps": value_maps or {},
        }
    )


def json_cfg(fields: dict[str, Any], **extra: Any) -> Any:
    return load_config(
        {
            "bank_id": "T",
            "mapping_version": "t-1",
            "source": {"topic": "t.raw", "format": "json"},
            "fields": fields,
            "derive": extra.pop("derive", []),
            "validate": extra.pop("validate", {}),
            "value_maps": extra.pop("value_maps", {}),
        }
    )


def run(cfg: Any, raw: str | dict | bytes) -> Any:
    if isinstance(raw, dict):
        raw = json.dumps(raw)
    return map_record(raw.encode() if isinstance(raw, str) else raw, cfg)


def one(cfg: Any, raw: str | dict | bytes, name: str) -> Any:
    out = run(cfg, raw)
    assert not isinstance(out, MappingError), out
    return out[name]


# --- parse: csv -----------------------------------------------------------------------------------


def test_csv_columns_are_positional_and_quotes_protect_the_delimiter() -> None:
    cfg = csv_cfg(["x", "y", "z"], {"y": {"from": "y", "type": "string"}})
    assert one(cfg, 'a,"b,c",d', "y") == "b,c"


def test_csv_other_delimiter() -> None:
    cfg = csv_cfg(["x", "y"], {"y": {"from": "y", "type": "string"}}, delimiter=";")
    assert one(cfg, "a;b,c", "y") == "b,c"


def test_csv_wrong_column_count_is_malformed() -> None:
    cfg = csv_cfg(["x", "y"], {"y": {"from": "y", "type": "string"}})
    for line in ("a", "a,b,c"):
        out = run(cfg, line)
        assert isinstance(out, MappingError) and out.code == "MALFORMED_RECORD", line


def test_csv_header_line_is_not_special_when_header_is_false() -> None:
    cfg = csv_cfg(["x", "n"], {"n": {"from": "n", "type": "long"}})
    out = run(cfg, "x,n")  # a header row slipped into the stream: it is data, and it is bad data
    assert isinstance(out, MappingError) and out.code == "BAD_NUMBER" and out.field == "n"


# --- parse: json ----------------------------------------------------------------------------------


def test_json_dotted_paths() -> None:
    cfg = json_cfg({"c": {"path": "a.b.c", "type": "long"}})
    assert one(cfg, {"a": {"b": {"c": 5}}}, "c") == 5


@pytest.mark.parametrize(
    "doc", [{}, {"a": {}}, {"a": {"b": None}}, {"a": 5}], ids=["none", "mid", "null", "scalar-mid"]
)
def test_json_missing_or_null_path_is_missing_field(doc: dict) -> None:
    out = run(json_cfg({"v": {"path": "a.b", "type": "long"}}), doc)
    assert isinstance(out, MappingError) and out.code == "MISSING_FIELD" and out.field == "v"


@pytest.mark.parametrize("raw", ["{", '{"a": 1', "[1, 2]", '"text"', "null", "", b"\xff\xfe\x00"])
def test_json_that_is_not_an_object_is_malformed(raw: str | bytes) -> None:
    out = run(json_cfg({"v": {"path": "a", "type": "long"}}), raw)
    assert isinstance(out, MappingError) and out.code == "MALFORMED_RECORD"


# --- type -----------------------------------------------------------------------------------------


@pytest.mark.parametrize(("text", "expected"), [("12", 12), ("-3", -3), ("0", 0)])
def test_type_long_accepts_integers(text: str, expected: int) -> None:
    assert one(csv_cfg(["n"], {"n": {"from": "n", "type": "long"}}), text, "n") == expected


@pytest.mark.parametrize("text", ["12.5", "1e3", "abc", "0x10", "1_000", "--1"])
def test_type_long_rejects_everything_else(text: str) -> None:
    out = run(csv_cfg(["n"], {"n": {"from": "n", "type": "long"}}), text)
    assert isinstance(out, MappingError) and out.code == "BAD_NUMBER" and out.field == "n"


@pytest.mark.parametrize("value", [12.5, True, [1], {"a": 1}])
def test_type_long_rejects_non_integer_json(value: Any) -> None:
    out = run(json_cfg({"n": {"path": "n", "type": "long"}}), {"n": value})
    assert isinstance(out, MappingError) and out.code == "BAD_NUMBER", value


def test_type_long_accepts_json_integer_and_numeric_string() -> None:
    cfg = json_cfg({"n": {"path": "n", "type": "long"}})
    assert one(cfg, {"n": 7}, "n") == 7
    assert one(cfg, {"n": "7"}, "n") == 7


def test_type_long_rejects_values_outside_int64() -> None:
    out = run(csv_cfg(["n"], {"n": {"from": "n", "type": "long"}}), str(2**63))
    assert isinstance(out, MappingError) and out.code == "BAD_NUMBER"


def test_type_string_turns_a_json_number_into_text() -> None:
    assert one(json_cfg({"c": {"path": "c", "type": "string"}}), {"c": 10}, "c") == "10"


# --- trim, upper, ignore --------------------------------------------------------------------------


def test_trim_and_upper() -> None:
    cfg = csv_cfg(["c"], {"c": {"from": "c", "type": "string", "trim": True, "upper": True}})
    assert one(cfg, "  usd ", "c") == "USD"


def test_without_trim_whitespace_is_kept() -> None:
    cfg = csv_cfg(["c"], {"c": {"from": "c", "type": "string"}})
    assert one(cfg, "  usd ", "c") == "  usd "


def test_ignored_column_is_dropped_and_cannot_cause_an_error() -> None:
    cfg = csv_cfg(
        ["id", "memo", "n"],
        {"id": {"from": "id", "type": "long"}, "n": {"from": "n", "type": "long"}},
        ignore=["memo"],
    )
    out = run(cfg, '1,"free text, with ""quotes"" and <script>",5')
    assert not isinstance(out, MappingError)
    assert "memo" not in out and out["id"] == 1 and out["n"] == 5
    assert "free text" not in json.dumps(out)


def test_ignoring_a_column_that_is_also_mapped_is_a_config_error() -> None:
    with pytest.raises(ConfigError):
        csv_cfg(["a"], {"a": {"from": "a", "type": "string"}}, ignore=["a"])


# --- cleanup: strip_prefix, extract, default_from -------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"), [("BR01-12345", "12345"), ("12345", "12345"), ("A-B-C", "B-C")]
)
def test_strip_prefix_removes_up_to_the_first_separator(text: str, expected: str) -> None:
    cfg = csv_cfg(["a"], {"a": {"from": "a", "type": "string", "strip_prefix": "-"}})
    assert one(cfg, text, "a") == expected


@pytest.mark.parametrize(("text", "expected"), [("BR01-12345", "12345"), ("12345", "12345")])
def test_extract_keeps_the_regex_group(text: str, expected: str) -> None:
    pattern = r"^(?:[A-Z]{2}\d{2}-)?(\d+)$"
    cfg = csv_cfg(["a"], {"a": {"from": "a", "type": "string", "extract": pattern}})
    assert one(cfg, text, "a") == expected


def test_extract_without_a_match_is_bad_format() -> None:
    cfg = csv_cfg(["a"], {"a": {"from": "a", "type": "string", "extract": r"^(\d+)$"}})
    out = run(cfg, "xx")
    assert isinstance(out, MappingError) and out.code == "BAD_FORMAT" and out.field == "a"


def test_default_from_uses_an_earlier_field_when_empty() -> None:
    cfg = csv_cfg(
        ["ccy", "ccy2"],
        {
            "ccy": {"from": "ccy", "type": "string"},
            "ccy2": {"from": "ccy2", "type": "string", "trim": True, "default_from": "ccy"},
        },
    )
    assert one(cfg, "USD,", "ccy2") == "USD"
    assert one(cfg, "USD,   ", "ccy2") == "USD"  # empty after trim counts as empty
    assert one(cfg, "USD,EUR", "ccy2") == "EUR"


def test_empty_value_without_default_is_missing_field() -> None:
    cfg = csv_cfg(["a"], {"a": {"from": "a", "type": "string", "trim": True}})
    out = run(cfg, "  ")
    assert isinstance(out, MappingError) and out.code == "MISSING_FIELD" and out.field == "a"


def test_cleanup_order_is_trim_upper_strip_prefix_extract_default_map() -> None:
    cfg = csv_cfg(
        ["a"],
        {
            "a": {
                "from": "a",
                "type": "string",
                "trim": True,
                "upper": True,
                "strip_prefix": "-",
                "map": "m",
            }
        },
        value_maps={"m": {"SEVEN": "7"}},
    )
    # trim, then upper ("br01-seven"), then strip_prefix ("SEVEN"), then map
    assert one(cfg, "  br01-seven ", "a") == "7"


@pytest.mark.parametrize(
    "fields",
    [
        {
            "b": {"from": "b", "type": "string", "default_from": "a"},
            "a": {"from": "a", "type": "string"},
        },
        {
            "m": {"from": "a", "parse": {"decimal_to_minor": {"currency_from": "later"}}},
            "later": {"from": "b", "type": "string"},
        },
    ],
    ids=["default_from-later-field", "currency_from-later-field"],
)
def test_a_reference_to_a_later_field_is_a_config_error(fields: dict) -> None:
    with pytest.raises(ConfigError):
        csv_cfg(["a", "b"], fields)


def test_unknown_operation_key_is_a_config_error() -> None:
    with pytest.raises(ConfigError):
        csv_cfg(["a"], {"a": {"from": "a", "type": "string", "uper": True}})  # typo


def test_unknown_value_map_name_is_a_config_error() -> None:
    with pytest.raises(ConfigError):
        csv_cfg(["a"], {"a": {"from": "a", "type": "string", "map": "nope"}})


# --- map ------------------------------------------------------------------------------------------


def test_map_hit_and_miss() -> None:
    cfg = csv_cfg(
        ["f"],
        {"f": {"from": "f", "map": "m"}},
        value_maps={"m": {"Cash": "CASH", "Credit Card": "CREDIT_CARD"}},
    )
    assert one(cfg, "Credit Card", "f") == "CREDIT_CARD"
    out = run(cfg, "Hawala")
    assert isinstance(out, MappingError) and out.code == "UNKNOWN_VALUE" and out.field == "f"
    assert "Hawala" in out.detail


def test_map_is_case_sensitive() -> None:
    cfg = csv_cfg(["f"], {"f": {"from": "f", "map": "m"}}, value_maps={"m": {"Cash": "CASH"}})
    assert isinstance(run(cfg, "cash"), MappingError)


# --- timestamps -----------------------------------------------------------------------------------


def test_timestamp_format_utc() -> None:
    cfg = csv_cfg(
        ["t"],
        {"t": {"from": "t", "parse": {"timestamp_format": "%Y/%m/%d %H:%M", "timezone": "UTC"}}},
    )
    assert one(cfg, "2022/09/01 00:20", "t") == 1_661_991_600_000


def test_timestamp_format_with_a_named_timezone() -> None:
    cfg = csv_cfg(
        ["t"],
        {
            "t": {
                "from": "t",
                "parse": {"timestamp_format": "%d/%m/%Y %H:%M", "timezone": "Europe/Madrid"},
            }
        },
    )
    # Madrid is UTC+2 on 1 September 2022, so 02:00 local is 00:00 UTC
    assert one(cfg, "01/09/2022 02:00", "t") == 1_661_990_400_000


@pytest.mark.parametrize(
    "text", ["2022/13/45 25:61", "yesterday", "2022/09/01", "2022/09/01 00:20:30"]
)
def test_unparseable_timestamp(text: str) -> None:
    cfg = csv_cfg(
        ["t"],
        {"t": {"from": "t", "parse": {"timestamp_format": "%Y/%m/%d %H:%M", "timezone": "UTC"}}},
    )
    out = run(cfg, text)
    assert isinstance(out, MappingError) and out.code == "BAD_TIMESTAMP" and out.field == "t"


@pytest.mark.parametrize(("unit", "text"), [("s", "1661991600"), ("ms", "1661991600000")])
def test_epoch_seconds_and_millis(unit: str, text: str) -> None:
    cfg = csv_cfg(["t"], {"t": {"from": "t", "parse": {"epoch": unit}}})
    assert one(cfg, text, "t") == 1_661_991_600_000


@pytest.mark.parametrize("text", ["abc", "1.5", "99999999999999999999"])
def test_bad_epoch(text: str) -> None:
    out = run(csv_cfg(["t"], {"t": {"from": "t", "parse": {"epoch": "ms"}}}), text)
    assert isinstance(out, MappingError) and out.code == "BAD_TIMESTAMP" and out.field == "t"


def test_an_empty_message_is_malformed_even_for_one_column() -> None:
    out = run(csv_cfg(["t"], {"t": {"from": "t", "parse": {"epoch": "ms"}}}), "")
    assert isinstance(out, MappingError) and out.code == "MALFORMED_RECORD"


def test_epoch_from_a_json_number() -> None:
    cfg = json_cfg({"t": {"path": "t", "parse": {"epoch": "ms"}}})
    assert one(cfg, {"t": 1_661_991_600_000}, "t") == 1_661_991_600_000


@pytest.mark.parametrize("text", ["2022-09-01T00:20:00Z", "2022-09-01T02:20:00+02:00"])
def test_iso8601_with_an_offset(text: str) -> None:
    cfg = csv_cfg(["t"], {"t": {"from": "t", "parse": {"iso8601": True}}})
    assert one(cfg, text, "t") == 1_661_991_600_000


def test_iso8601_garbage() -> None:
    out = run(csv_cfg(["t"], {"t": {"from": "t", "parse": {"iso8601": True}}}), "not a time")
    assert isinstance(out, MappingError) and out.code == "BAD_TIMESTAMP"


# --- decimal_to_minor and decimal_locale ----------------------------------------------------------

_MONEY_FIELDS = {
    "ccy": {"from": "ccy", "type": "string"},
    "amt": {"from": "amt", "parse": {"decimal_to_minor": {"currency_from": "ccy"}}},
}


@pytest.mark.parametrize(
    ("amount", "currency", "expected"),
    [
        ("3697.34", "USD", 369734),
        ("0.125", "USD", 12),  # half-even: 12.5 cents -> 12
        ("0.135", "USD", 14),  # 13.5 -> 14
        ("123.5", "JPY", 124),
        ("122.5", "JPY", 122),
        ("0.5", "JPY", 0),
        ("0.00123456", "BTC", 123456),
        ("0.000000005", "BTC", 0),
        ("0", "USD", 0),
        ("0.00", "EUR", 0),
    ],
)
def test_decimal_to_minor_anchors(amount: str, currency: str, expected: int) -> None:
    cfg = csv_cfg(["ccy", "amt"], _MONEY_FIELDS)
    assert one(cfg, f"{currency},{amount}", "amt") == expected
    assert money.to_minor(amount, currency) == expected  # the engine and money.py agree


def test_decimal_to_minor_equals_money_py_for_every_currency() -> None:
    cfg = csv_cfg(["ccy", "amt"], _MONEY_FIELDS)
    for currency in money.CURRENCIES:
        for amount in ("0", "0.005", "1", "1.5", "2.5", "99.995", "1234567.891", "0.00000001"):
            assert one(cfg, f"{currency},{amount}", "amt") == money.to_minor(amount, currency)


def test_decimal_to_minor_bad_input() -> None:
    cfg = csv_cfg(["ccy", "amt"], _MONEY_FIELDS)
    out = run(cfg, "USD,abc")
    assert isinstance(out, MappingError) and out.code == "BAD_NUMBER" and out.field == "amt"
    out = run(cfg, "USD,-5.00")
    assert isinstance(out, MappingError) and out.code == "NEGATIVE_AMOUNT" and out.field == "amt"
    out = run(cfg, "ZZZ,5.00")
    assert isinstance(out, MappingError) and out.code == "UNKNOWN_VALUE" and out.field == "amt"


def test_decimal_locale_comma_decimal_and_dot_grouping() -> None:
    fields = {
        "ccy": {"from": "ccy", "type": "string"},
        "amt": {
            "from": "amt",
            "decimal_locale": {"decimal": ",", "group": "."},
            "parse": {"decimal_to_minor": {"currency_from": "ccy"}},
        },
    }
    cfg = csv_cfg(["ccy", "amt"], fields, delimiter=";")
    assert one(cfg, "EUR;1.234,56", "amt") == 123456
    assert one(cfg, "EUR;0,5", "amt") == 50
    assert one(cfg, "JPY;1.234", "amt") == 1234


def test_the_default_locale_does_not_accept_a_comma_decimal() -> None:
    cfg = csv_cfg(["ccy", "amt"], _MONEY_FIELDS, delimiter=";")
    out = run(cfg, "EUR;1.234,56")
    assert isinstance(out, MappingError) and out.code == "BAD_NUMBER"


# --- derive ---------------------------------------------------------------------------------------


def test_derive_keys_and_usd_cents() -> None:
    fields = {
        "sender_bank": {"from": "sb", "type": "string"},
        "sender_account": {"from": "sa", "type": "string"},
        "receiver_bank": {"from": "rb", "type": "string"},
        "receiver_account": {"from": "ra", "type": "string"},
        "payment_currency": {"from": "pc", "type": "string"},
        "receiving_currency": {"from": "rc", "type": "string"},
        "amount_paid_minor": {"from": "ap", "type": "long"},
        "amount_received_minor": {"from": "ar", "type": "long"},
    }
    cfg = csv_cfg(
        ["sb", "sa", "rb", "ra", "pc", "rc", "ap", "ar"],
        fields,
        derive=[
            "sender_key",
            "receiver_key",
            "amount_paid_usd_cents",
            "amount_received_usd_cents",
        ],
    )
    out = run(cfg, "10,AAA,20,BBB,EUR,JPY,12345,678")
    assert not isinstance(out, MappingError)
    assert out["sender_key"] == "10:AAA" and out["receiver_key"] == "20:BBB"
    assert out["amount_paid_usd_cents"] == money.minor_to_usd_cents(12345, "EUR")
    assert out["amount_received_usd_cents"] == money.minor_to_usd_cents(678, "JPY")
    assert out["bank_id"] == "T" and out["mapping_version"] == "t-1" and out["fx_version"] == 1


def test_unknown_derive_name_is_a_config_error() -> None:
    with pytest.raises(ConfigError):
        csv_cfg(["a"], {"a": {"from": "a", "type": "string"}}, derive=["sender_keyy"])


# --- validate -------------------------------------------------------------------------------------


def test_event_ts_range_is_inclusive() -> None:
    cfg = csv_cfg(
        ["event_ts_ms"],
        {"event_ts_ms": {"from": "event_ts_ms", "type": "long"}},
        validate={"event_ts_range": ["2022-09-01T00:00:00Z", "2022-09-02T00:00:00Z"]},
    )
    lo, hi = 1_661_990_400_000, 1_662_076_800_000
    assert one(cfg, str(lo), "event_ts_ms") == lo
    assert one(cfg, str(hi), "event_ts_ms") == hi
    for bad in (lo - 1, hi + 1):
        out = run(cfg, str(bad))
        assert isinstance(out, MappingError) and out.code == "TIMESTAMP_OUT_OF_RANGE"
        assert out.field == "event_ts_ms"


def test_non_negative_accepts_zero_and_rejects_negative() -> None:
    cfg = csv_cfg(
        ["amount_paid_minor"],
        {"amount_paid_minor": {"from": "amount_paid_minor", "type": "long"}},
        validate={"non_negative": ["amount_paid_minor"]},
    )
    assert one(cfg, "0", "amount_paid_minor") == 0
    out = run(cfg, "-1")
    assert isinstance(out, MappingError) and out.code == "NEGATIVE_AMOUNT"
    assert out.field == "amount_paid_minor"


def test_required_fields() -> None:
    cfg = csv_cfg(
        ["a", "b"],
        {"a": {"from": "a", "type": "string"}, "b": {"from": "b", "type": "string"}},
        validate={"required": ["a", "b"]},
    )
    assert not isinstance(run(cfg, "x,y"), MappingError)
    out = run(cfg, "x,")
    assert isinstance(out, MappingError) and out.code == "MISSING_FIELD" and out.field == "b"


def test_allowed_currencies_spec_checks_both_canonical_currency_fields() -> None:
    cfg = json_cfg(
        {
            "payment_currency": {"path": "p", "type": "string", "upper": True},
            "receiving_currency": {"path": "r", "type": "string", "upper": True},
        },
        validate={"allowed_currencies": "spec"},
    )
    assert one(cfg, {"p": "usd", "r": "JPY"}, "payment_currency") == "USD"
    for doc, field in (
        ({"p": "PLN", "r": "USD"}, "payment_currency"),
        ({"p": "USD", "r": "XXX"}, "receiving_currency"),
    ):
        out = run(cfg, doc)
        assert isinstance(out, MappingError) and out.code == "UNKNOWN_VALUE" and out.field == field


def test_the_first_problem_in_yaml_order_is_reported() -> None:
    cfg = csv_cfg(
        ["a", "b"],
        {"a": {"from": "a", "type": "long"}, "b": {"from": "b", "type": "long"}},
    )
    out = run(cfg, "x,y")
    assert isinstance(out, MappingError) and out.field == "a"


# --- review findings: text that cannot be sent, strict inputs -------------------------------------


def test_a_json_string_that_is_not_valid_unicode_is_malformed_not_a_crash() -> None:
    cfg = json_cfg({"c": {"path": "c", "type": "string"}})
    out = run(cfg, '{"c": "\\ud800"}')  # a lone surrogate escape: json.loads accepts it
    assert isinstance(out, MappingError) and out.code == "MALFORMED_RECORD" and out.field == "c"


def test_a_json_decimal_is_not_text() -> None:
    cfg = json_cfg({"c": {"path": "c", "type": "string"}})
    out = run(cfg, '{"c": 10.0}')  # a bank code sent as 10.0 would become "10.0:..." in the key
    assert isinstance(out, MappingError) and out.code == "BAD_FORMAT" and out.field == "c"


@pytest.mark.parametrize("line", ["a\nb", 'a,"b\nc"', "a\r"])
def test_a_newline_anywhere_in_a_csv_message_is_malformed(line: str) -> None:
    out = run(csv_cfg(["x", "y"], {"y": {"from": "y", "type": "string"}}), line + ",z")
    assert isinstance(out, MappingError) and out.code == "MALFORMED_RECORD"


def test_a_colon_in_a_bank_or_account_would_make_two_accounts_share_a_key() -> None:
    fields = {
        "sender_bank": {"from": "sb", "type": "string"},
        "sender_account": {"from": "sa", "type": "string"},
    }
    cfg = csv_cfg(["sb", "sa"], fields, derive=["sender_key"])
    for line in ("10,X:Y", "10:X,Y"):
        out = run(cfg, line)
        assert isinstance(out, MappingError) and out.code == "BAD_FORMAT", line
        assert out.field == "sender_key"
    assert one(cfg, "10,XY", "sender_key") == "10:XY"


def test_a_huge_timestamp_text_is_refused_without_being_cached() -> None:
    cfg = csv_cfg(
        ["t"],
        {"t": {"from": "t", "parse": {"timestamp_format": "%Y/%m/%d %H:%M", "timezone": "UTC"}}},
    )
    out = run(cfg, "9" * 200_000)
    assert isinstance(out, MappingError) and out.code == "BAD_TIMESTAMP"
