"""Postgres contracts against the local stack (`make up`): schema, init, tenant isolation."""

import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

pytestmark = pytest.mark.integration

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
HOST, PORT = "localhost", 5432
# Local-dev defaults from infra/compose/docker-compose.yml; override with the same env vars.
ADMIN_PW = os.environ.get("AML_PG_PASSWORD", "postgres")
API_PW = os.environ.get("AML_API_DB_PASSWORD", "aml_api_dev")
SERVICE_PW = os.environ.get("AML_SERVICE_DB_PASSWORD", "aml_service_dev")

EXPECTED_TABLES = {
    "alerts",
    "cases",
    "case_jobs",
    "case_evidence",
    "case_drafts",
    "decisions",
    "audit_log",
    "study_events",
    "draft_feedback",
    "qa_samples",
    "approvals",
    "bank_thresholds",
}


def _connect(dbname: str, user: str = "postgres", password: str = ADMIN_PW) -> psycopg.Connection:
    return psycopg.connect(
        host=HOST, port=PORT, dbname=dbname, user=user, password=password, autocommit=True
    )


@pytest.fixture
def throwaway_db() -> Iterator[str]:
    """An empty database, dropped afterwards. The contracts say to run the RLS test on one."""
    name = f"aml_test_{uuid.uuid4().hex[:8]}"
    with _connect("postgres") as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        yield name
    finally:
        with _connect("postgres") as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
            )


def _apply(dbname: str, filename: str) -> None:
    with _connect(dbname) as conn:
        conn.execute((CONTRACTS / filename).read_text())


def _public_tables(dbname: str) -> set[str]:
    with _connect(dbname) as conn:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        ).fetchall()
    return {r[0] for r in rows}


def test_schema_applies_clean(throwaway_db: str) -> None:
    _apply(throwaway_db, "postgres_schema.sql")
    missing = EXPECTED_TABLES - _public_tables(throwaway_db)
    assert not missing, f"tables missing after applying postgres_schema.sql: {sorted(missing)}"


def test_rls_isolation(throwaway_db: str) -> None:
    _apply(throwaway_db, "postgres_schema.sql")
    _apply(throwaway_db, "rls_isolation_test.sql")  # raises on the first broken rule


def test_rls_test_catches_disabled_rls(throwaway_db: str) -> None:
    """Negative control: with RLS off on `cases`, the isolation test must fail."""
    _apply(throwaway_db, "postgres_schema.sql")
    with _connect(throwaway_db) as conn:
        conn.execute("ALTER TABLE cases DISABLE ROW LEVEL SECURITY")
    with pytest.raises(psycopg.errors.RaiseException):
        _apply(throwaway_db, "rls_isolation_test.sql")


def test_init_created_all_databases() -> None:
    with _connect("postgres") as conn:
        names = {r[0] for r in conn.execute("SELECT datname FROM pg_database").fetchall()}
    assert {"aml", "mlflow", "airflow", "iceberg_catalog"} <= names


def test_init_applied_schema_to_aml() -> None:
    assert EXPECTED_TABLES <= _public_tables("aml")


def _security_snapshot(dbname: str) -> dict[str, list[tuple]]:
    """Everything tenant isolation (rule 11) rests on: RLS flags, policies and grants."""
    queries = {
        "rls_flags": (
            "SELECT relname, relrowsecurity, relforcerowsecurity FROM pg_class "
            "WHERE relnamespace = 'public'::regnamespace AND relkind = 'r' ORDER BY 1"
        ),
        "policies": (
            "SELECT tablename, policyname, cmd, roles::text, qual, with_check FROM pg_policies "
            "WHERE schemaname = 'public' ORDER BY 1, 2"
        ),
        "grants": (
            "SELECT grantee, table_name, privilege_type FROM information_schema.role_table_grants "
            "WHERE table_schema = 'public' AND grantee IN ('aml_api', 'aml_service') "
            "ORDER BY 1, 2, 3"
        ),
    }
    with _connect(dbname) as conn:
        return {name: conn.execute(q).fetchall() for name, q in queries.items()}


def test_live_aml_security_matches_the_contract(throwaway_db: str) -> None:
    """The init scripts run only on an empty volume, so a later edit to the contract never
    reaches the running `aml` database. This catches that drift (policies, RLS flags, grants)."""
    _apply(throwaway_db, "postgres_schema.sql")
    fresh, live = _security_snapshot(throwaway_db), _security_snapshot("aml")
    for part in fresh:
        assert live[part] == fresh[part], (
            f"live `aml` {part} differ from contracts/postgres_schema.sql: the database was "
            "initialised from an older contract. Apply the change, or `make nuke-local && make up`."
        )


@pytest.mark.parametrize(
    ("role", "password", "bypass_rls"),
    [("aml_api", API_PW, False), ("aml_service", SERVICE_PW, True)],
)
def test_init_roles_can_log_in(role: str, password: str, bypass_rls: bool) -> None:
    with _connect("aml", user=role, password=password) as conn:
        row = conn.execute(
            "SELECT current_user, r.rolsuper, r.rolbypassrls FROM pg_roles r "
            "WHERE r.rolname = current_user"
        ).fetchone()
    assert row == (role, False, bypass_rls)
