"""The only `raw_transactions` columns the replayer reads (CLAUDE.md rule 2).

The lake table also holds `is_laundering` and `split`; they must never reach a Kafka topic. The
scan passes this tuple as `selected_fields`, so those columns are never even read.
`tests/leakage/test_stream_no_labels.py` pins this list exactly: adding a column is a deliberate
edit of that test.

`owner_bank_id` picks the bank stream (A or B) and is not sent. USD cents are not read either: the
connectors compute them from the minor units with `features/money.py`, and the end-to-end test
compares them with the lake's values.
"""

SELECTED_FIELDS: tuple[str, ...] = (
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
)
