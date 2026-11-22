# tests/golden/

Golden feature vectors derived from `features/spec.yaml`. Spark (`features/reference.py`) and Flink must both reproduce them exactly. Added in M2.

## Format

`s<NN>_<name>.json`: `{name, description, time_origin, notes, transactions, expected}`.

- `transactions`: canonical fields only (`tx_id`, `event_ts_ms`, `sender_key`, `receiver_key`, the two USD-cent amounts, the two currencies, `payment_format`). `tx_id` order must agree with `event_ts_ms` order. Time is `1662033600000 + minutes * 60000` (2022-09-01T12:00:00Z, a Thursday).
- `expected`: `{"<tx_id>": {"<feature name from features/feature_names_v1.txt>": value}}`. A scenario lists only the features it is about. `null` means the model sees NaN. Ints stay ints and floats stay floats; the comparison is exact, with no tolerance.
- `"TODO(human)"` is an expected value nobody has computed yet. A scenario with any is skipped (the skip message says how many are left); once none are left it runs for real.

## Rules for filling them in

Compute each value by hand from `features/spec.yaml`, not by running the oracle. Write the value down before you run the test, so a mismatch is a finding about your reading of the spec or about the oracle, not something to "fix" by copying the output.

## Run

`make golden` (name list check, these scenarios, the oracle contract tests). Skips and xfails are printed.

Scenarios 1-9 are the nine of the manual (M02 §3). `s10` is added: eviction must not erase `first_seen` or `last_in_ts`. `s11` is added for coverage of D40 (single division on fractional holds), D41 (zero average), D42 (log10 rounding), the 72h start edge, a self-transfer IN leg as `last_in_ts`, and a distinct count that drops on eviction.
