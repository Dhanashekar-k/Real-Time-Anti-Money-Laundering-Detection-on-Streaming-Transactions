# connectors/

One YAML file per bank in `banks/`. The mapping engine (`stream/connectors/engine.py`) compiles it once and maps every raw record with no bank-specific code. Onboarding a bank is a new YAML plus `make onboard-check` (M03 step 5, M10).

`tests/unit/test_engine_ops.py` pins everything below. A mistake in a file is a `ConfigError` at load, never a silent default: unknown keys, a reference to a field that comes later, an unknown value map, a bad regex or timezone.

## File layout

```yaml
bank_id: A                 # reporting customer, goes into `bank_id` of every record
mapping_version: a-1       # goes into `mapping_version`; bump it when the file changes
source:
  topic: bank_a.raw
  format: csv              # csv | json
  csv:                     # csv only; one record per Kafka message, no header line
    delimiter: ","
    header: false          # true is refused
    quotechar: '"'
    columns: [txn_ref, timestamp, ...]
  ignore: [memo]           # csv columns that are dropped and may not be mapped
fields:                    # processed IN THIS ORDER; later fields may refer to earlier ones
  <canonical_name>: {<selector>, <operations>}
derive: [sender_key, ...]
validate: {...}
value_maps: {<name>: {<bank value>: <canonical value>}}
```

## A field

Selector: `from: <csv column>` (csv) or `path: a.b.c` (json, dotted). A missing path, a JSON `null` and an empty text are all "empty".

Operations run in this order inside one field, and every one is optional except the last group:

| Order | Key | Effect |
|---|---|---|
| 1 | `trim: true` | strip whitespace (text only) |
| 2 | `upper: true` | upper-case |
| 3 | `strip_prefix: "-"` | drop everything up to and including the first `-`; unchanged if absent |
| 4 | `extract: "<regex>"` | keep group 1 of the first match; no match is `BAD_FORMAT` |
| 5 | `default_from: <earlier field>` | when the value is empty, copy that field's canonical value (as is; steps 6 and 7 are skipped). Without it, empty is `MISSING_FIELD` |
| 6 | `map: <value map>` | exact, case-sensitive lookup; a miss is `UNKNOWN_VALUE` |
| 7 | one of `type: string \| long` or `parse: {...}` | convert (below) |

`type: long`: ASCII digits only, int64 range; `12.5`, `1e3`, `0x10`, a JSON float or boolean are `BAD_NUMBER`. `type: string` turns a JSON number into text. `type: decimal` is refused (use `decimal_to_minor`).

`parse` takes exactly one of:

| Key | Meaning |
|---|---|
| `timestamp_format: "%Y/%m/%d %H:%M"` (+ `timezone: UTC \| Europe/Madrid \| ...`) | strptime text, read in that zone (default UTC), to epoch millis. Anything left over is `BAD_TIMESTAMP` |
| `epoch: s \| ms` | integer epoch to millis |
| `iso8601: true` (+ `timezone` for a time with no offset) | ISO 8601 text, `Z` and offsets accepted |
| `decimal_to_minor: {currency_from: <earlier field>}` | amount text to integer minor units with `features/money.py` (half-even, exponent from the spec). Unknown currency is `UNKNOWN_VALUE`, a negative is `NEGATIVE_AMOUNT`, anything else wrong is `BAD_NUMBER` |

`decimal_locale: {decimal: ",", group: "."}` next to a `decimal_to_minor` reads `1.234,56`. Without it only `1234.56` is a number. JSON numbers with decimals are read exactly (not as floats).

## derive

`sender_key`, `receiver_key` (`<bank>:<account>`; ids are only unique within a bank), `amount_paid_usd_cents`, `amount_received_usd_cents` (`features/money.py` and the frozen FX table; outside int64 is `BAD_NUMBER`, never wrapped). Each needs its source fields to exist.

## validate

Run after all fields, in this order: `required: [..]`, `allowed_currencies: spec` (payment and receiving currency must be in `features/spec.yaml`), `non_negative: [..]` (**zero is valid**, D53; this replaces the manual's `positive`, which is not accepted), `event_ts_range: [low, high]` (ISO 8601, both ends inclusive).

## Error codes

`MALFORMED_RECORD`, `MISSING_FIELD`, `UNKNOWN_VALUE`, `BAD_NUMBER`, `BAD_TIMESTAMP`, `TIMESTAMP_OUT_OF_RANGE`, `NEGATIVE_AMOUNT`, `BAD_FORMAT`, `INTERNAL_ERROR` (an engine bug, reported instead of raised). Each carries the canonical field and the offending text (first 80 characters). The first problem in YAML order wins; the record is not mapped further.

## What is not here

A field the YAML does not list is dropped, whatever the bank sent (a label field a bank adds is never carried). `connector_ts_ms` and `replay_emit_ts_ms` are set by the runner, not the engine.
