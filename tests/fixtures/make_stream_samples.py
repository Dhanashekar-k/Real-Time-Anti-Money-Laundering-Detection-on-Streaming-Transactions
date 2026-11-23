"""Writes the two onboarding samples: bank_a_sample.csv and bank_b_sample.jsonl.

    uv run python tests/fixtures/make_stream_samples.py

Each file has 200 good records (synthetic rows, seed 11, rendered by the replayer's renderers) and
a fixed set of deliberately bad ones, so the numbers in tests/unit/test_onboard_check.py are known
by construction, not measured from the code under test:

Bank A (207 records):  bad = 1 too few columns (MALFORMED_RECORD), 1 empty account (MISSING_FIELD),
    2 payment format "Crypto" + 1 currency "Zloty" (UNKNOWN_VALUE x3), 1 negative amount
    (NEGATIVE_AMOUNT), 1 bad time (BAD_TIMESTAMP).
Bank B (207 records):  bad = 1 truncated JSON (MALFORMED_RECORD), 1 missing originator account
    (MISSING_FIELD), 2 method "HAWALA" + 1 currency "PLN" (UNKNOWN_VALUE x3), 1 negative amount
    (NEGATIVE_AMOUNT), 1 bad time (BAD_TIMESTAMP).

No label column exists in these files.
"""

import importlib.util
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def _make_rows(n: int, seed: int) -> list[dict]:
    spec = importlib.util.spec_from_file_location("stream_conftest", ROOT / "tests" / "conftest.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.make_rows(n, seed)


def main() -> None:
    import sys

    sys.path.insert(0, str(ROOT))
    from stream.replayer import render

    rows = _make_rows(200, seed=11)

    a = [render.render_bank_a(r).decode() for r in rows]
    cols = a[0].split(",")
    bad_a = [
        ",".join(cols[:-1]),  # too few columns
        ",".join(cols[:3] + [""] + cols[4:]),  # empty from_account
        ",".join(cols[:-1] + ["Crypto"]),
        ",".join(cols[:-1] + ["Crypto"]),
        ",".join(cols[:9] + ["Zloty"] + cols[10:]),
        ",".join(cols[:8] + ["-5.00"] + cols[9:]),  # negative amount_paid
        ",".join([cols[0], "2022/13/45 25:61"] + cols[2:]),
    ]
    (HERE / "bank_a_sample.csv").write_text("\n".join(a + bad_a) + "\n")

    b = [render.render_bank_b(r).decode() for r in rows]
    doc = json.loads(b[0])

    def variant(**changes: object) -> str:
        d = json.loads(json.dumps(doc))
        for path, value in changes.items():
            *parents, last = path.split("__")
            node = d
            for p in parents:
                node = node[p]
            if value is ...:
                del node[last]
            else:
                node[last] = value
        return json.dumps(d, separators=(",", ":"))

    bad_b = [
        b[0][: len(b[0]) // 2],  # truncated
        variant(originator__accountNo=...),
        variant(method="HAWALA"),
        variant(method="HAWALA"),
        variant(debit__currency="PLN"),
        variant(debit__amountMinor=-1),
        variant(bookedAt="yesterday"),
    ]
    (HERE / "bank_b_sample.jsonl").write_text("\n".join(b + bad_b) + "\n")


if __name__ == "__main__":
    main()
