"""Render a lake row in a bank's own record format (what the bank would put on its topic).

Bank A sends the IBM CSV with a leading transaction reference, no header, minute-resolution local
text. Bank B sends JSON with its own names, minor units, epoch millis and ISO currencies.

Each bank's vocabulary is a constant here, deliberately not read from `connectors/banks/*.yaml`:
the YAML is what is under test. Bank A's names are the ones M1 verified against the real IBM files
(D36: nothing unmapped, nothing unused). Bank B's names are the simulated bank's own.

Bank B's field names are all in `BANK_B_NAMES`. The "no code change" demonstration (M03 step 6)
renames one entry here and the matching `path:` in `bank_b.yaml`, and touches no connector code.

The lake keeps no original amount text, so Bank A's amounts are rendered from minor units and the
currency exponent (D53). Only the columns in `stream.replayer.columns.SELECTED_FIELDS` are used;
any other key in the row is ignored.
"""

import csv
import io
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from features import money

BANK_A_CURRENCY_NAMES: dict[str, str] = {
    "USD": "US Dollar",
    "EUR": "Euro",
    "GBP": "UK Pound",
    "CHF": "Swiss Franc",
    "JPY": "Yen",
    "CNY": "Yuan",
    "INR": "Rupee",
    "CAD": "Canadian Dollar",
    "AUD": "Australian Dollar",
    "MXN": "Mexican Peso",
    "BRL": "Brazil Real",
    "RUB": "Ruble",
    "SAR": "Saudi Riyal",
    "ILS": "Shekel",
    "BTC": "Bitcoin",
}
BANK_A_FORMAT_NAMES: dict[str, str] = {
    "ACH": "ACH",
    "BITCOIN": "Bitcoin",
    "CASH": "Cash",
    "CHEQUE": "Cheque",
    "CREDIT_CARD": "Credit Card",
    "REINVESTMENT": "Reinvestment",
    "WIRE": "Wire",
}
BANK_B_METHODS: dict[str, str] = {
    "ACH": "ACH_TRANSFER",
    "BITCOIN": "CRYPTO",
    "CASH": "CASH_DEPOSIT",
    "CHEQUE": "CHECK",
    "CREDIT_CARD": "CARD",
    "REINVESTMENT": "INTERNAL_REINVEST",
    "WIRE": "WIRE_TRANSFER",
}
# Bank B's JSON field names, by role. Rename an entry and the renderer follows.
BANK_B_NAMES: dict[str, str] = {
    "tx_id": "transactionId",
    "booked_at": "bookedAt",
    "sender": "originator",
    "receiver": "beneficiary",
    "bank_code": "bankCode",
    "account_no": "accountNo",
    "paid": "debit",
    "received": "credit",
    "amount_minor": "amountMinor",
    "currency": "currency",
    "method": "method",
}

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_A_TIME_FORMAT = "%Y/%m/%d %H:%M"


def amount_text(minor: int, currency: str) -> str:
    """Integer minor units -> the amount as plain decimal text with the currency's exponent."""
    if minor < 0:
        raise ValueError(f"minor units must not be negative: {minor}")
    try:
        exponent = money.EXPONENTS[currency]
    except KeyError:
        raise ValueError(f"unknown currency {currency!r}") from None
    if exponent == 0:
        return str(minor)
    whole, fraction = divmod(minor, 10**exponent)
    return f"{whole}.{fraction:0{exponent}d}"


def _csv_line(fields: list[str]) -> str:
    if any(("," in f or '"' in f or "\n" in f or "\r" in f) for f in fields):
        buffer = io.StringIO()
        csv.writer(buffer, lineterminator="").writerow(fields)
        return buffer.getvalue()
    return ",".join(fields)


def _name(table: Mapping[str, str], key: str, what: str) -> str:
    try:
        return table[key]
    except KeyError:
        raise ValueError(f"no name for {what} {key!r}") from None


def render_bank_a(row: Mapping[str, Any]) -> bytes:
    """One IBM-format CSV line (no header, no newline) with the transaction reference first."""
    ts = row["event_ts_ms"]
    if ts % 60_000:
        raise ValueError(f"Bank A has minute resolution; event_ts_ms {ts} has seconds")
    stamp = (_EPOCH + timedelta(milliseconds=ts)).strftime(_A_TIME_FORMAT)
    paid_ccy, recv_ccy = row["payment_currency"], row["receiving_currency"]
    fields = [
        str(row["tx_id"]),
        stamp,
        str(row["sender_bank"]),
        str(row["sender_account"]),
        str(row["receiver_bank"]),
        str(row["receiver_account"]),
        amount_text(row["amount_received_minor"], recv_ccy),
        _name(BANK_A_CURRENCY_NAMES, recv_ccy, "currency"),
        amount_text(row["amount_paid_minor"], paid_ccy),
        _name(BANK_A_CURRENCY_NAMES, paid_ccy, "currency"),
        _name(BANK_A_FORMAT_NAMES, row["payment_format"], "payment format"),
    ]
    return _csv_line(fields).encode("utf-8")


def render_bank_b(row: Mapping[str, Any]) -> bytes:
    """One JSON object (compact, no newline) in Bank B's names."""
    n = BANK_B_NAMES
    doc = {
        n["tx_id"]: row["tx_id"],
        n["booked_at"]: row["event_ts_ms"],
        n["sender"]: {
            n["bank_code"]: str(row["sender_bank"]),
            n["account_no"]: str(row["sender_account"]),
        },
        n["receiver"]: {
            n["bank_code"]: str(row["receiver_bank"]),
            n["account_no"]: str(row["receiver_account"]),
        },
        n["paid"]: {
            n["amount_minor"]: row["amount_paid_minor"],
            n["currency"]: row["payment_currency"],
        },
        n["received"]: {
            n["amount_minor"]: row["amount_received_minor"],
            n["currency"]: row["receiving_currency"],
        },
        n["method"]: _name(BANK_B_METHODS, row["payment_format"], "payment format"),
    }
    return json.dumps(doc, separators=(",", ":")).encode("utf-8")


def render(bank: str, row: Mapping[str, Any]) -> bytes:
    """Render for `bank` ("A" or "B")."""
    if bank == "A":
        return render_bank_a(row)
    if bank == "B":
        return render_bank_b(row)
    raise ValueError(f"no renderer for bank {bank!r}")
