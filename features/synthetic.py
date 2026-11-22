"""Synthetic transactions for tests (never used by a job): random, seeded, shaped like the data.

The shape that matters for the feature code: a few hot accounts and many quiet ones, transactions
that share a minute (ties), self-transfers, repeated counterparties, gaps longer than the 72 h
state retention, zero-amount payments, and a share of cross-currency payments. `tx_id` is the rank
in (event_ts_ms, source order), as in M1. No label column is generated.
"""

import random
from typing import Any

from features import money

CURRENCIES = ["USD", "EUR", "JPY", "GBP"]
FORMATS = ["ACH", "WIRE", "CASH", "CHEQUE", "CREDIT_CARD", "REINVESTMENT", "BITCOIN"]
MIN_MS = 60_000
T0 = 1662033600000  # 2022-09-01T12:00:00Z


def random_transactions(
    n: int, seed: int = 0, n_accounts: int = 30, self_rate: float = 0.05
) -> list[dict[str, Any]]:
    rnd = random.Random(seed)
    banks = ["001", "002", "010"]
    accounts = [f"{rnd.choice(banks)}:{i:03d}" for i in range(n_accounts)]
    hot = accounts[:3]
    ts = T0
    out = []
    for tx_id in range(1, n + 1):
        ts += rnd.choice([0, 0, 0, 1, 2, 5, 30, 60, 90, 600, 1500, 4000, 6000]) * MIN_MS
        sender = rnd.choice(hot) if rnd.random() < 0.4 else rnd.choice(accounts)
        receiver = sender if rnd.random() < self_rate else rnd.choice(accounts)
        pay = rnd.choice(CURRENCIES)
        recv = pay if rnd.random() < 0.9 else rnd.choice(CURRENCIES)
        cents = 0 if rnd.random() < 0.05 else int(10 ** rnd.uniform(0, 7))
        out.append(
            {
                "tx_id": tx_id,
                "event_ts_ms": ts,
                "sender_bank": sender.split(":")[0],
                "sender_key": sender,
                "receiver_bank": receiver.split(":")[0],
                "receiver_key": receiver,
                "amount_paid_usd_cents": cents,
                "amount_received_usd_cents": cents if recv == pay else int(cents * 0.9),
                "payment_currency": pay,
                "receiving_currency": recv,
                "payment_format": rnd.choice(FORMATS),
            }
        )
    return out


def check_currencies_are_in_the_spec() -> None:
    """Guard for the generator itself: it must only use currencies the spec knows."""
    missing = [c for c in CURRENCIES if c not in money.EXPONENTS]
    if missing:
        raise ValueError(f"synthetic currencies not in the spec: {missing}")
