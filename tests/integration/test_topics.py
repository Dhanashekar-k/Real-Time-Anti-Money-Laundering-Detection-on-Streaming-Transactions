"""Kafka topics from manual/02_architecture_and_stack.md §3 (local column). Needs `make up`.

EXPECTED is typed from the manual on purpose, not read from the script that creates the topics,
so a mistake in that script cannot pass its own test.
"""

import os
import subprocess
import uuid
from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient, ConfigResource

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
BROKERS = "localhost:19092"
HOUR = 3_600_000
DAY = 24 * HOUR

# topic -> (partitions, retention.ms)
EXPECTED: dict[str, tuple[int, int]] = {
    "bank_a.raw": (6, 24 * HOUR),
    "bank_b.raw": (6, 24 * HOUR),
    "bank_c.raw": (3, 24 * HOUR),
    "tx.canonical": (12, 48 * HOUR),
    "tx.dlq": (1, 7 * DAY),
    "tx.late": (1, 7 * DAY),
    "tx.features": (12, 24 * HOUR),
    "tx.scores": (12, 48 * HOUR),
}


@pytest.fixture(scope="module")
def admin() -> AdminClient:
    # v4 only: the broker advertises "localhost", which tries IPv6 first and logs a refused connect
    return AdminClient({"bootstrap.servers": BROKERS, "broker.address.family": "v4"})


def _snapshot(admin: AdminClient) -> dict[str, tuple[int, str]]:
    """{topic: (partitions, retention.ms)} for the topics we manage."""
    meta = admin.list_topics(timeout=10).topics
    present = [t for t in EXPECTED if t in meta]
    if not present:  # describe_configs rejects an empty list
        return {}
    resources = [ConfigResource(ConfigResource.Type.TOPIC, t) for t in present]
    out: dict[str, tuple[int, str]] = {}
    for res, fut in admin.describe_configs(resources).items():
        out[res.name] = (len(meta[res.name].partitions), fut.result()["retention.ms"].value)
    return out


def _make_topics(spec: str | None = None) -> subprocess.CompletedProcess[str]:
    """Run `make topics`. `spec` ("name partitions retention.ms") runs it on that topic only."""
    env = os.environ.copy()
    env.pop("AML_TOPICS_SPEC", None)
    if spec is not None:
        env["AML_TOPICS_SPEC"] = spec
    return subprocess.run(
        ["make", "topics"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_expected_topics(admin: AdminClient) -> None:
    got = _snapshot(admin)
    missing = sorted(set(EXPECTED) - set(got))
    assert not missing, f"missing topics (run `make topics`): {missing}"
    wrong = {
        t: {"expected": EXPECTED[t], "got": (got[t][0], int(got[t][1]))}
        for t in EXPECTED
        if (got[t][0], int(got[t][1])) != EXPECTED[t]
    }
    assert not wrong, f"partitions/retention differ from the manual: {wrong}"


def test_topics_idempotent(admin: AdminClient) -> None:
    first = _make_topics()
    assert first.returncode == 0, first.stdout + first.stderr
    before = _snapshot(admin)
    second = _make_topics()
    assert second.returncode == 0, second.stdout + second.stderr
    assert _snapshot(admin) == before
    # The config snapshot alone could not tell "left alone" from "dropped and recreated".
    assert f"created=0 unchanged={len(EXPECTED)} drift=0" in second.stdout, second.stdout


def test_topics_fails_on_drift_and_does_not_change_the_topic(admin: AdminClient) -> None:
    """A topic whose retention was changed by hand makes `make topics` fail and stay untouched.

    Runs on a scratch topic (AML_TOPICS_SPEC), never on a real one: tx.dlq will hold rejected
    records from M3 on, and a 1 s retention would delete them.
    """
    topic = f"itest.drift.{uuid.uuid4().hex[:8]}"
    wanted = 3_600_000
    spec = f"{topic} 1 {wanted}"
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
    try:
        first = _make_topics(spec)
        assert first.returncode == 0 and "created=1" in first.stdout, first.stdout + first.stderr

        subprocess.run(
            ["rpk", "topic", "alter-config", topic, "--set", "retention.ms=1000"]
            + ["-X", f"brokers={BROKERS}"],
            check=True,
            capture_output=True,
            text=True,
        )
        drifted = _make_topics(spec)
        assert drifted.returncode != 0, "make topics accepted a drifted retention.ms"
        assert "DRIFT" in drifted.stderr and topic in drifted.stderr, drifted.stderr

        value = admin.describe_configs([resource])[resource].result()["retention.ms"].value
        assert value == "1000", "make topics changed a drifted topic instead of reporting it"
    finally:
        for fut in admin.delete_topics([topic]).values():
            try:
                fut.result()
            except Exception:  # noqa: BLE001 - the topic may not exist if the first run failed
                pass
