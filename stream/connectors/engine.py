"""The mapping engine: one raw bank record + a bank YAML -> a canonical record, or a MappingError.

    cfg = load_config(Path("connectors/banks/bank_a.yaml"))
    out = map_record(b"123,2022/09/01 00:20,...", cfg)      # dict (CanonicalTx) or MappingError

`map_record` is pure and never raises: a bad record comes back as a `MappingError` with a code the
DLQ carries. The YAML is compiled once by `load_config` into per-field closures, so the per-record
path does no interpretation. The operations and their order are described in connectors/README.md;
tests/unit/test_engine_ops.py pins them. Nothing here is specific to a bank: a new bank is a new
YAML (M10 relies on that).

Money goes through `features/money.py` only. The runner, not the engine, sets `connector_ts_ms` and
`replay_emit_ts_ms`.
"""

from __future__ import annotations

import csv
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

from features import money

CanonicalTx = dict[str, Any]

INT64_MAX = 2**63 - 1
INT64_MIN = -(2**63)
_MAX_NUMBER_CHARS = 64  # a longer numeric text is refused before any parsing (no 10^5-digit DoS)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MS = timedelta(milliseconds=1)
_INTEGER = re.compile(r"[+-]?[0-9]+")
_DECIMAL = re.compile(r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)")

ERROR_CODES = (
    "MALFORMED_RECORD",  # not UTF-8, not parseable, wrong shape or column count
    "MISSING_FIELD",  # empty or absent
    "UNKNOWN_VALUE",  # not in a value map, or a currency outside the spec
    "BAD_NUMBER",  # not a number of the declared type, or out of int64
    "BAD_TIMESTAMP",  # not parseable in the declared format
    "TIMESTAMP_OUT_OF_RANGE",  # outside validate.event_ts_range
    "NEGATIVE_AMOUNT",  # below zero (zero is valid, D53)
    "BAD_FORMAT",  # `extract` found no match
    "INTERNAL_ERROR",  # an engine bug: reported, never raised
)


@dataclass(frozen=True, slots=True)
class MappingError:
    """Why a record cannot be mapped. `field` is the canonical field, `value` the offending text."""

    code: str
    detail: str
    field: str | None = None
    value: str | None = None


class ConfigError(ValueError):
    """The bank YAML is wrong. Raised by `load_config` only, never by `map_record`."""


class _Fail(Exception):
    """Internal: a field step or check found a problem (turned into a MappingError)."""

    def __init__(self, code: str, detail: str, value: Any = None, field_name: str | None = None):
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.value = None if value is None else str(value)[:80]
        self.field_name = field_name


Step = Callable[[Any, dict[str, Any]], None]
Check = Callable[[dict[str, Any]], None]


@dataclass(slots=True)
class BankConfig:
    bank_id: str
    mapping_version: str
    topic: str
    fmt: str
    parse: Callable[[str], Any]
    steps: list[tuple[str, Step]]
    checks: list[Check]
    derivations: list[tuple[str, Callable[[dict[str, Any]], Any]]]
    fx_version: int
    field_names: tuple[str, ...]
    ignored: tuple[str, ...] = ()
    value_maps: dict[str, dict[str, str]] = field(default_factory=dict)


# --- record parsing -------------------------------------------------------------------------------


def _csv_parser(columns: list[str], delimiter: str, quotechar: str) -> Callable[[str], Any]:
    width = len(columns)

    def parse(text: str) -> Any:
        if quotechar in text:
            try:
                rows = list(
                    csv.reader([text], delimiter=delimiter, quotechar=quotechar, strict=True)
                )
            except csv.Error as exc:
                raise _Fail("MALFORMED_RECORD", f"CSV: {exc}") from None
            row = rows[0] if rows else []
        else:
            row = text.split(delimiter) if text else []
        if len(row) != width:
            raise _Fail("MALFORMED_RECORD", f"expected {width} columns, got {len(row)}")
        return row

    return parse


def _json_parser() -> Callable[[str], Any]:
    def parse(text: str) -> Any:
        try:
            doc = json.loads(text, parse_float=Decimal)
        except (ValueError, RecursionError) as exc:
            raise _Fail("MALFORMED_RECORD", f"JSON: {str(exc)[:120]}") from None
        if not isinstance(doc, dict):
            raise _Fail("MALFORMED_RECORD", f"JSON value is a {type(doc).__name__}, not an object")
        return doc

    return parse


def _json_getter(path: str) -> Callable[[Any], Any]:
    keys = tuple(path.split("."))

    def get(doc: Any) -> Any:
        node = doc
        for key in keys:
            if not isinstance(node, dict):
                return None
            node = node.get(key)
            if node is None:
                return None
        return node

    return get


def _csv_getter(index: int) -> Callable[[Any], Any]:
    return lambda row: row[index]


# --- value conversions ----------------------------------------------------------------------------


def _text_of_number(value: Any) -> str | None:
    """A number's text for decimal parsing; None for anything that is not a number or string."""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    return None


def _to_string(value: Any) -> str:
    if isinstance(value, str):
        return value
    text = _text_of_number(value)
    if text is None:
        raise _Fail("BAD_FORMAT", f"cannot use a {type(value).__name__} as text", value)
    return text


def _to_long(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise _Fail("BAD_NUMBER", f"not an integer: {type(value).__name__} {value!r}", value)
    if isinstance(value, str):
        if len(value) > _MAX_NUMBER_CHARS or not _INTEGER.fullmatch(value):
            raise _Fail("BAD_NUMBER", f"not an integer: {value[:40]!r}", value)
        value = int(value)
    if not INT64_MIN <= value <= INT64_MAX:
        raise _Fail("BAD_NUMBER", f"outside int64: {value}", value)
    return value


def _ms_of(moment: datetime) -> int:
    return (moment - _EPOCH) // _MS


def _timezone(name: str) -> Any:
    if name == "UTC":
        return UTC
    try:
        return ZoneInfo(name)
    except (KeyError, ValueError, OSError) as exc:
        raise ConfigError(f"unknown timezone {name!r}") from exc


def _timestamp_format_parser(fmt: str, tz: Any) -> Callable[[Any], int]:
    @lru_cache(maxsize=8192)  # many records share a minute
    def parse_text(text: str) -> int | None:
        try:
            return _ms_of(datetime.strptime(text, fmt).replace(tzinfo=tz))
        except (ValueError, OverflowError):
            return None

    def convert(value: Any) -> int:
        ms = parse_text(value) if isinstance(value, str) else None
        if ms is None:
            raise _Fail("BAD_TIMESTAMP", f"does not match {fmt!r}: {str(value)[:40]!r}", value)
        return ms

    return convert


def _epoch_parser(unit: str) -> Callable[[Any], int]:
    factor = 1000 if unit == "s" else 1

    def convert(value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise _Fail("BAD_TIMESTAMP", f"not an epoch integer: {value!r}", value)
        if isinstance(value, str):
            if len(value) > _MAX_NUMBER_CHARS or not _INTEGER.fullmatch(value):
                raise _Fail("BAD_TIMESTAMP", f"not an epoch integer: {value[:40]!r}", value)
            value = int(value)
        ms = value * factor
        if not INT64_MIN <= ms <= INT64_MAX:
            raise _Fail("BAD_TIMESTAMP", f"epoch outside int64: {value}", value)
        return ms

    return convert


def _iso8601_parser(tz: Any) -> Callable[[Any], int]:
    def convert(value: Any) -> int:
        if not isinstance(value, str):
            raise _Fail("BAD_TIMESTAMP", f"not an ISO 8601 text: {value!r}", value)
        try:
            moment = datetime.fromisoformat(value)
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=tz)
            return _ms_of(moment)
        except (ValueError, OverflowError):
            raise _Fail("BAD_TIMESTAMP", f"not ISO 8601: {value[:40]!r}", value) from None

    return convert


def _decimal_to_minor(
    currency_field: str, locale: Mapping[str, str] | None
) -> Callable[[Any, dict[str, Any]], int]:
    decimal_sep = (locale or {}).get("decimal", ".")
    group_sep = (locale or {}).get("group", "")

    def convert(value: Any, out: dict[str, Any]) -> int:
        text = _text_of_number(value)
        if text is None:
            raise _Fail("BAD_NUMBER", f"not an amount: {value!r}", value)
        if group_sep:
            text = text.replace(group_sep, "")
        if decimal_sep != ".":
            text = text.replace(decimal_sep, ".")
        if len(text) > _MAX_NUMBER_CHARS or not _DECIMAL.fullmatch(text):
            raise _Fail("BAD_NUMBER", f"not a decimal amount: {text[:40]!r}", value)
        currency = out[currency_field]
        if currency not in money.EXPONENTS:
            raise _Fail("UNKNOWN_VALUE", f"currency {currency!r} is not in the spec", currency)
        if text[0] == "-" and Decimal(text) < 0:
            raise _Fail("NEGATIVE_AMOUNT", f"negative amount {text!r}", text)
        try:
            return money.to_minor(text, currency)
        except ValueError as exc:
            raise _Fail("BAD_NUMBER", str(exc)[:120], text) from None

    return convert


# --- compiling the YAML ---------------------------------------------------------------------------

_FIELD_KEYS = {
    "from",
    "path",
    "type",
    "trim",
    "upper",
    "strip_prefix",
    "extract",
    "default_from",
    "map",
    "decimal_locale",
    "parse",
}
_PARSE_KEYS = {"timestamp_format", "timezone", "epoch", "iso8601", "decimal_to_minor"}
_VALIDATE_KEYS = {"required", "allowed_currencies", "non_negative", "event_ts_range"}
_DERIVE_NAMES = {
    "sender_key",
    "receiver_key",
    "amount_paid_usd_cents",
    "amount_received_usd_cents",
}
_DERIVE_NEEDS = {
    "sender_key": ("sender_bank", "sender_account"),
    "receiver_key": ("receiver_bank", "receiver_account"),
    "amount_paid_usd_cents": ("amount_paid_minor", "payment_currency"),
    "amount_received_usd_cents": ("amount_received_minor", "receiving_currency"),
}


def _compile_field(
    name: str,
    spec: Mapping[str, Any],
    *,
    fmt: str,
    columns: list[str],
    earlier: set[str],
    value_maps: Mapping[str, Mapping[str, str]],
) -> Step:
    if not isinstance(spec, Mapping):
        raise ConfigError(f"field {name}: expected a mapping, got {spec!r}")
    unknown = set(spec) - _FIELD_KEYS
    if unknown:
        raise ConfigError(f"field {name}: unknown key(s) {sorted(unknown)}")

    selector = "from" if fmt == "csv" else "path"
    other = "path" if fmt == "csv" else "from"
    if selector not in spec or other in spec:
        raise ConfigError(f"field {name}: a {fmt} source needs `{selector}` (and not `{other}`)")
    if fmt == "csv":
        if spec["from"] not in columns:
            raise ConfigError(f"field {name}: column {spec['from']!r} is not in source.csv.columns")
        get = _csv_getter(columns.index(spec["from"]))
    else:
        get = _json_getter(str(spec["path"]))

    trim, upper = bool(spec.get("trim")), bool(spec.get("upper"))
    strip_prefix = spec.get("strip_prefix")
    extract = spec.get("extract")
    regex = None
    if extract is not None:
        try:
            regex = re.compile(extract)
        except re.error as exc:
            raise ConfigError(f"field {name}: bad extract regex: {exc}") from exc
        if regex.groups < 1:
            raise ConfigError(f"field {name}: `extract` needs a group to keep")
    default_from = spec.get("default_from")
    if default_from is not None and default_from not in earlier:
        raise ConfigError(f"field {name}: default_from {default_from!r} is not an earlier field")

    mapping = None
    if "map" in spec:
        if spec["map"] not in value_maps:
            raise ConfigError(f"field {name}: unknown value map {spec['map']!r}")
        mapping = dict(value_maps[spec["map"]])
    locale = spec.get("decimal_locale")
    if locale is not None and not (
        isinstance(locale, Mapping) and set(locale) <= {"decimal", "group"}
    ):
        raise ConfigError(f"field {name}: decimal_locale takes `decimal` and `group`")

    parse = spec.get("parse")
    kind = spec.get("type")
    convert: Callable[[Any, dict[str, Any]], Any]
    if parse is not None and kind is not None:
        raise ConfigError(f"field {name}: use `type` or `parse`, not both")
    if parse is not None:
        if not isinstance(parse, Mapping) or set(parse) - _PARSE_KEYS:
            raise ConfigError(f"field {name}: parse takes {sorted(_PARSE_KEYS)}")
        tz = _timezone(parse.get("timezone", "UTC"))
        if "timestamp_format" in parse:
            fn = _timestamp_format_parser(parse["timestamp_format"], tz)
            convert = lambda v, out, fn=fn: fn(v)  # noqa: E731
        elif "epoch" in parse:
            if parse["epoch"] not in ("s", "ms"):
                raise ConfigError(f"field {name}: epoch is `s` or `ms`")
            fn = _epoch_parser(parse["epoch"])
            convert = lambda v, out, fn=fn: fn(v)  # noqa: E731
        elif "iso8601" in parse:
            fn = _iso8601_parser(tz)
            convert = lambda v, out, fn=fn: fn(v)  # noqa: E731
        elif "decimal_to_minor" in parse:
            currency_field = (parse["decimal_to_minor"] or {}).get("currency_from")
            if currency_field not in earlier:
                raise ConfigError(
                    f"field {name}: currency_from {currency_field!r} is not an earlier field"
                )
            convert = _decimal_to_minor(currency_field, locale)
        else:
            raise ConfigError(f"field {name}: parse needs one operation")
    elif kind == "long":
        convert = lambda v, out: _to_long(v)  # noqa: E731
    elif kind == "string" or (kind is None and mapping is not None):
        convert = lambda v, out: _to_string(v)  # noqa: E731
    elif kind == "decimal":
        raise ConfigError(
            f"field {name}: type decimal is not an output type; use parse.decimal_to_minor"
        )
    else:
        raise ConfigError(f"field {name}: needs `type`, `map` or `parse`")

    def step(src: Any, out: dict[str, Any]) -> None:
        value = get(src)
        if isinstance(value, str):
            if trim:
                value = value.strip()
            if upper:
                value = value.upper()
            if value:
                if strip_prefix is not None:
                    at = value.find(strip_prefix)
                    if at >= 0:
                        value = value[at + len(strip_prefix) :]
                if regex is not None and value:
                    found = regex.search(value)
                    if found is None:
                        raise _Fail(
                            "BAD_FORMAT", f"does not match {extract!r}: {value[:40]!r}", value
                        )
                    value = found.group(1)
        if value is None or value == "":
            if default_from is None:
                raise _Fail("MISSING_FIELD", f"{name} is empty or absent")
            out[name] = out[default_from]  # the earlier field's canonical value, as is
            return
        if mapping is not None:
            key = value if isinstance(value, str) else str(value)
            if key not in mapping:
                raise _Fail(
                    "UNKNOWN_VALUE", f"{key[:40]!r} is not in value map {spec['map']!r}", key
                )
            value = mapping[key]
        out[name] = convert(value, out)

    return step


def _compile_checks(validate: Mapping[str, Any], names: set[str]) -> list[Check]:
    unknown = set(validate) - _VALIDATE_KEYS
    if unknown:
        raise ConfigError(
            f"validate: unknown key(s) {sorted(unknown)}; known: {sorted(_VALIDATE_KEYS)}"
        )
    checks: list[Check] = []

    def need(*fields: str) -> None:
        missing = [f for f in fields if f not in names]
        if missing:
            raise ConfigError(f"validate refers to field(s) not in `fields`: {missing}")

    if "required" in validate:
        required = list(validate["required"])
        need(*required)

        def check_required(out: dict[str, Any]) -> None:
            for name in required:
                if out.get(name) in (None, ""):
                    raise _Fail("MISSING_FIELD", f"{name} is empty or absent", field_name=name)

        checks.append(check_required)

    if "allowed_currencies" in validate:
        allowed = validate["allowed_currencies"]
        codes = set(money.CURRENCIES) if allowed == "spec" else set(allowed)
        targets = [f for f in ("payment_currency", "receiving_currency") if f in names]

        def check_currencies(out: dict[str, Any]) -> None:
            for name in targets:
                if out[name] not in codes:
                    raise _Fail(
                        "UNKNOWN_VALUE", f"currency {out[name]!r} is not allowed", out[name], name
                    )

        checks.append(check_currencies)

    if "non_negative" in validate:
        fields = list(validate["non_negative"])
        need(*fields)

        def check_non_negative(out: dict[str, Any]) -> None:
            for name in fields:
                if out[name] < 0:
                    raise _Fail("NEGATIVE_AMOUNT", f"{name} is negative", out[name], name)

        checks.append(check_non_negative)

    if "event_ts_range" in validate:
        need("event_ts_ms")
        try:
            low, high = (_ms_of(datetime.fromisoformat(s)) for s in validate["event_ts_range"])
        except (ValueError, TypeError) as exc:
            raise ConfigError(f"validate.event_ts_range needs two ISO 8601 times: {exc}") from exc

        def check_range(out: dict[str, Any]) -> None:
            if not low <= out["event_ts_ms"] <= high:
                raise _Fail(
                    "TIMESTAMP_OUT_OF_RANGE",
                    f"event time {out['event_ts_ms']} is outside [{low}, {high}]",
                    out["event_ts_ms"],
                    "event_ts_ms",
                )

        checks.append(check_range)
    return checks


def _compile_derivations(
    derive: list[str], names: set[str], fx: Mapping[str, Decimal]
) -> list[tuple[str, Callable[[dict[str, Any]], Any]]]:
    result: list[tuple[str, Callable[[dict[str, Any]], Any]]] = []
    for name in derive:
        if name not in _DERIVE_NAMES:
            raise ConfigError(f"derive: unknown name {name!r}; known: {sorted(_DERIVE_NAMES)}")
        missing = [f for f in _DERIVE_NEEDS[name] if f not in names]
        if missing:
            raise ConfigError(f"derive {name} needs field(s) {missing}")
        if name.endswith("_key"):
            bank, account = _DERIVE_NEEDS[name]
            result.append((name, lambda out, b=bank, a=account: f"{out[b]}:{out[a]}"))
        else:
            minor, currency = _DERIVE_NEEDS[name]

            def usd(out: dict[str, Any], n=name, m=minor, c=currency) -> int:
                try:
                    return money.minor_to_usd_cents(out[m], out[c], fx)
                except ValueError as exc:
                    raise _Fail("BAD_NUMBER", str(exc)[:120], out[m], n) from None

            result.append((name, usd))
    return result


def load_config(source: Path | str | Mapping[str, Any]) -> BankConfig:
    """Compile a bank YAML (a path, or the parsed dict) into a `BankConfig`. Raises ConfigError."""
    if isinstance(source, (str, Path)):
        doc = yaml.safe_load(Path(source).read_text(encoding="utf-8"))
    else:
        doc = dict(source)
    if not isinstance(doc, Mapping):
        raise ConfigError("the bank file must be a mapping")
    for key in ("bank_id", "mapping_version", "source", "fields"):
        if key not in doc:
            raise ConfigError(f"missing top-level key {key!r}")
    unknown = set(doc) - {
        "bank_id",
        "mapping_version",
        "source",
        "fields",
        "derive",
        "validate",
        "value_maps",
    }
    if unknown:
        raise ConfigError(f"unknown top-level key(s) {sorted(unknown)}")

    src = doc["source"]
    fmt = src.get("format")
    if fmt not in ("csv", "json"):
        raise ConfigError(f"source.format must be csv or json, got {fmt!r}")
    if "topic" not in src:
        raise ConfigError("source.topic is required")
    columns: list[str] = []
    ignored = tuple(src.get("ignore") or ())
    if fmt == "csv":
        csv_cfg = src.get("csv") or {}
        columns = list(csv_cfg.get("columns") or [])
        if not columns:
            raise ConfigError("source.csv.columns is required")
        if csv_cfg.get("header"):
            raise ConfigError("source.csv.header: true is not supported (one record per message)")
        parse = _csv_parser(columns, csv_cfg.get("delimiter", ","), csv_cfg.get("quotechar", '"'))
        unknown_ignored = [c for c in ignored if c not in columns]
        if unknown_ignored:
            raise ConfigError(f"source.ignore names columns that do not exist: {unknown_ignored}")
    else:
        parse = _json_parser()

    value_maps = {k: dict(v) for k, v in (doc.get("value_maps") or {}).items()}
    fx = money.load_fx()  # also checks fx_version against the spec
    steps: list[tuple[str, Step]] = []
    earlier: set[str] = set()
    ordered: list[str] = []
    for name, spec in doc["fields"].items():
        if fmt == "csv" and isinstance(spec, Mapping) and spec.get("from") in ignored:
            raise ConfigError(f"field {name}: column {spec['from']!r} is listed in source.ignore")
        steps.append(
            (
                name,
                _compile_field(
                    name, spec, fmt=fmt, columns=columns, earlier=earlier, value_maps=value_maps
                ),
            )
        )
        earlier.add(name)
        ordered.append(name)

    checks = _compile_checks(doc.get("validate") or {}, earlier)
    derivations = _compile_derivations(list(doc.get("derive") or []), earlier, fx)
    return BankConfig(
        bank_id=str(doc["bank_id"]),
        mapping_version=str(doc["mapping_version"]),
        topic=str(src["topic"]),
        fmt=fmt,
        parse=parse,
        steps=steps,
        checks=checks,
        derivations=derivations,
        fx_version=money.FX_VERSION_REQUIRED,
        field_names=tuple(ordered) + tuple(n for n, _ in derivations),
        ignored=ignored,
        value_maps=value_maps,
    )


# --- mapping one record ---------------------------------------------------------------------------


def map_record(raw: bytes, cfg: BankConfig) -> CanonicalTx | MappingError:
    """Map one raw message. Never raises: a bad record is returned as a `MappingError`."""
    try:
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            return MappingError("MALFORMED_RECORD", f"not UTF-8 ({exc.reason} at byte {exc.start})")
        src = cfg.parse(text)
        out: dict[str, Any] = {}
        for name, step in cfg.steps:
            try:
                step(src, out)
            except _Fail as fail:
                return MappingError(fail.code, fail.detail, fail.field_name or name, fail.value)
        for check in cfg.checks:
            check(out)
        for name, derive in cfg.derivations:
            out[name] = derive(out)
        out["bank_id"] = cfg.bank_id
        out["fx_version"] = cfg.fx_version
        out["mapping_version"] = cfg.mapping_version
        return out
    except _Fail as fail:
        return MappingError(fail.code, fail.detail, fail.field_name, fail.value)
    except Exception as exc:  # noqa: BLE001 - a bad record must never stop a connector
        return MappingError("INTERNAL_ERROR", f"{type(exc).__name__}: {str(exc)[:120]}")
