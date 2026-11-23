"""Shared fixtures for the M03 stream tests.

`synthetic_rows` makes rows shaped like the lake's `raw_transactions` columns the replayer reads,
without needing the lake: every spec currency and payment format, ties in time, zero amounts and
large amounts. Real-lake checks live in tests/integration.
"""

import random
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
BANK_DIR = ROOT / "connectors" / "banks"
DAY1_START_MS = 1_661_990_400_000  # 2022-09-01T00:00:00Z

_BANK_CODES = ("10", "020", "1234", "70", "00110")
_HEX = "0123456789ABCDEF"


def _minor(rng: random.Random, currency: str) -> int:
    """A minor-unit amount: 2% zero, then a mix of tiny, ordinary and large."""
    r = rng.random()
    if r < 0.02:
        return 0
    if r < 0.30:
        return rng.randrange(1, 1000)
    if r < 0.90:
        return rng.randrange(1, 10**7)
    return rng.randrange(10**7, 10**11 if currency != "BTC" else 10**10)


def make_rows(n: int, seed: int = 7) -> list[dict[str, Any]]:
    """`n` rows with the lake's column names; `tx_id` 1..n increasing, minute-aligned times."""
    from features.money import CURRENCIES
    from features.spec import load_spec

    formats = load_spec().payment_formats
    rng = random.Random(seed)
    rows: list[dict[str, Any]] = []
    for i in range(n):
        paid = rng.choice(CURRENCIES)
        received = paid if rng.random() < 0.9 else rng.choice(CURRENCIES)
        rows.append(
            {
                "tx_id": i + 1,
                "event_ts_ms": DAY1_START_MS + 60_000 * (i // 3),  # three rows share a minute
                "sender_bank": rng.choice(_BANK_CODES),
                "sender_account": "".join(rng.choice(_HEX) for _ in range(9)),
                "receiver_bank": rng.choice(_BANK_CODES),
                "receiver_account": "".join(rng.choice(_HEX) for _ in range(9)),
                "payment_currency": paid,
                "amount_paid_minor": _minor(rng, paid),
                "receiving_currency": received,
                "amount_received_minor": _minor(rng, received),
                "payment_format": rng.choice(formats),
            }
        )
    return rows


@pytest.fixture(scope="session")
def synthetic_rows() -> list[dict[str, Any]]:
    return make_rows(10_000)


@pytest.fixture(scope="session")
def bank_a_cfg() -> Any:
    from stream.connectors.engine import load_config

    return load_config(BANK_DIR / "bank_a.yaml")


@pytest.fixture(scope="session")
def bank_b_cfg() -> Any:
    from stream.connectors.engine import load_config

    return load_config(BANK_DIR / "bank_b.yaml")
