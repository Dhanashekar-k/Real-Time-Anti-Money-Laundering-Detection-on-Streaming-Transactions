"""The nine per-transaction features (spec.yaml `tx_features`), for one transaction.

Pure Python, one dict in and one dict out. The Spark job writes the same expressions in Spark SQL
and a test compares the two; Flink reimplements them in Java and must reproduce `tests/golden/`.
The function reads only the columns it names, so label-like columns in the input are ignored.
"""

import math
from collections.abc import Mapping
from typing import Any

from features.spec import Spec

_HOUR_MS = 3_600_000
_DAY_MS = 86_400_000


def _bank(account_key: str) -> str:
    """`<bank_id>:<account_id>` -> bank_id. Account ids are only unique within a bank."""
    return account_key.split(":", 1)[0]


def _code(value: str, vocabulary: tuple[str, ...], what: str) -> int:
    try:
        return vocabulary.index(value)
    except ValueError:
        raise ValueError(f"unknown {what} {value!r}: not in the spec vocabulary") from None


def compute(tx: Mapping[str, Any], spec: Spec) -> dict[str, float | int]:
    """The tx features of one transaction, in spec order (the first nine names of the vector)."""
    # Integer arithmetic only: the hour and weekday must not depend on a float.
    ts = tx["event_ts_ms"]
    return {
        "log10_amount_paid_usd": math.log10(1 + tx["amount_paid_usd_cents"] / 100.0),
        "is_cross_currency": int(tx["payment_currency"] != tx["receiving_currency"]),
        "payment_format_code": _code(tx["payment_format"], spec.payment_formats, "payment_format"),
        "payment_currency_code": _code(tx["payment_currency"], spec.currencies, "currency"),
        "receiving_currency_code": _code(tx["receiving_currency"], spec.currencies, "currency"),
        "hour_of_day": (ts // _HOUR_MS) % 24,
        "day_of_week": (ts // _DAY_MS + 3) % 7,  # 1970-01-01 was a Thursday; Monday = 0
        "same_bank": int(_bank(tx["sender_key"]) == _bank(tx["receiver_key"])),
        "self_transfer": int(tx["sender_key"] == tx["receiver_key"]),
    }
