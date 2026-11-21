# tests/integration/

Tests that need the local stack: topics, the Iceberg three-engine smoke test, Postgres contracts. Marked `integration`; run with `make itest`.

Prerequisites: `make up`, then `make topics` (`test_topics.py` checks that the topics exist; it does not create them).
