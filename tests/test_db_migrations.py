"""
tests/test_db_migrations.py — Integration tests for the migration runner.

Each test that needs an isolated database schema creates its own fresh schema,
runs apply_migrations() inside it, and tears down the schema on exit.

TEST_DATABASE_URL (or DATABASE_URL as fallback) must point to a live Postgres
instance before running these tests.
"""

import os
import uuid

import psycopg2
import pytest
from psycopg2 import errors as pg_errors

from src.db.runner import apply_migrations

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MIGRATIONS_DIR = "db/migrations"

_EXPECTED_MIGRATIONS = [
    "001_create_companies.sql",
    "002_create_persons.sql",
    "003_create_deals.sql",
    "004_create_interactions.sql",
    "005_create_recommendations.sql",
    "006_create_schema_versions.sql",
    "007_add_deals_name_unique.sql",
    "008_create_email_raw.sql",
    "009_create_ingestion_watermarks.sql",
    "010_create_calendar_raw.sql",
    "011_create_interaction_signals.sql",
    "012_create_company_aliases.sql",
    "013_merge_duplicate_companies.sql",
    "014_add_email_raw_thread_labels.sql",
    "015_add_email_raw_skip_reason.sql",
    "016_add_persons_is_internal.sql",
    "017_create_contact_scores.sql",
]

_CORE_TABLES = {
    "companies",
    "persons",
    "deals",
    "interactions",
    "recommendations",
}


def _make_schema_conn(database_url: str):
    """
    Create a unique test schema, return (conn, schema_name, database_url).
    Sets search_path on the connection so DDL lands in the test schema.
    """
    schema_name = f"test_slice2_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(database_url)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema_name}")
        cur.execute(f"SET search_path TO {schema_name}, public")
    conn.commit()
    return conn, schema_name, database_url


def _drop_schema(database_url: str, conn, schema_name: str) -> None:
    """Drop the test schema unconditionally."""
    # Use a fresh autocommit connection to avoid being inside an aborted txn.
    conn.close()
    drop_conn = psycopg2.connect(database_url)
    drop_conn.autocommit = True
    with drop_conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
    drop_conn.close()


def _get_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL and DATABASE_URL are both unset")
    return url


# ---------------------------------------------------------------------------
# Primary fixture — one isolated schema per test
# ---------------------------------------------------------------------------


@pytest.fixture()
def migrated_conn():
    """
    Provide a psycopg2 connection with:
    - A unique test schema created and set as search_path.
    - All six Slice 2 migrations applied.
    Tears down the schema on exit.
    """
    url = _get_database_url()
    conn, schema_name, db_url = _make_schema_conn(url)
    try:
        apply_migrations(conn, _MIGRATIONS_DIR)
        yield conn
    finally:
        _drop_schema(db_url, conn, schema_name)


# ---------------------------------------------------------------------------
# Helper to read schema_name from an existing connection's search_path
# ---------------------------------------------------------------------------


def _current_schema(conn) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_schema()")
        return cur.fetchone()[0]


def _columns_for(conn, table: str, schema: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name   = %s
            """,
            (schema, table),
        )
        return {row[0] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_schema_versions_table_is_created(migrated_conn):
    """schema_versions must exist after running all migrations."""
    schema = _current_schema(migrated_conn)
    with migrated_conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name   = 'schema_versions'
            """,
            (schema,),
        )
        assert cur.fetchone()[0] == 1


def test_all_six_migrations_applied(migrated_conn):
    """All expected rows in schema_versions with the expected filenames."""
    with migrated_conn.cursor() as cur:
        cur.execute("SELECT migration_name FROM schema_versions ORDER BY migration_name")
        applied = [row[0] for row in cur.fetchall()]
    assert applied == _EXPECTED_MIGRATIONS, f"Expected {_EXPECTED_MIGRATIONS}, got {applied}"


def test_all_core_tables_exist(migrated_conn):
    """companies, persons, deals, interactions, recommendations must all exist."""
    schema = _current_schema(migrated_conn)
    with migrated_conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            """,
            (schema,),
        )
        tables = {row[0] for row in cur.fetchall()}
    assert _CORE_TABLES.issubset(tables), f"Missing tables: {_CORE_TABLES - tables}"


def test_companies_columns(migrated_conn):
    """companies must have exactly the columns defined in the migration."""
    schema = _current_schema(migrated_conn)
    expected = {"company_id", "name", "type", "geography", "notes"}
    actual = _columns_for(migrated_conn, "companies", schema)
    assert actual == expected, f"Column mismatch: got {actual}"


def test_persons_columns(migrated_conn):
    """persons must have exactly the columns defined in the migration."""
    schema = _current_schema(migrated_conn)
    expected = {
        "person_id",
        "full_name",
        "email",
        "company_id",
        "title",
        "last_contact_at",
        "relationship_strength",
        "responsiveness_score",
        "priority_override",
        "tags",
        "is_internal",
    }
    actual = _columns_for(migrated_conn, "persons", schema)
    assert actual == expected, f"Column mismatch: got {actual}"


def test_deals_columns(migrated_conn):
    """deals must have exactly the columns defined in the migration."""
    schema = _current_schema(migrated_conn)
    expected = {
        "deal_id",
        "name",
        "market",
        "asset_type",
        "size",
        "stage",
        "strategy_tags",
        "status",
        "owner_user_id",
    }
    actual = _columns_for(migrated_conn, "deals", schema)
    assert actual == expected, f"Column mismatch: got {actual}"


def test_interactions_columns(migrated_conn):
    """interactions must have exactly the columns defined in the migration."""
    schema = _current_schema(migrated_conn)
    expected = {
        "interaction_id",
        "type",
        "timestamp",
        "direction",
        "participants",
        "company_refs",
        "deal_refs",
        "summary",
        "extracted_signals",
    }
    actual = _columns_for(migrated_conn, "interactions", schema)
    assert actual == expected, f"Column mismatch: got {actual}"


def test_recommendations_columns(migrated_conn):
    """recommendations must have exactly the columns defined in the migration."""
    schema = _current_schema(migrated_conn)
    expected = {
        "recommendation_id",
        "date",
        "person_id",
        "related_deal_id",
        "priority_score",
        "why_now",
        "suggested_action",
        "draft_text",
        "status",
    }
    actual = _columns_for(migrated_conn, "recommendations", schema)
    assert actual == expected, f"Column mismatch: got {actual}"


def test_runner_is_idempotent():
    """Running apply_migrations twice on the same schema produces no error and still 11 rows."""
    url = _get_database_url()
    conn, schema_name, db_url = _make_schema_conn(url)
    try:
        apply_migrations(conn, _MIGRATIONS_DIR)
        apply_migrations(conn, _MIGRATIONS_DIR)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM schema_versions")
            count = cur.fetchone()[0]
        assert count == 17
    finally:
        _drop_schema(db_url, conn, schema_name)


def test_runner_skips_already_applied():
    """
    Pre-inserting a migration name into schema_versions must prevent that
    migration's DDL from running.

    We pre-mark 006_create_schema_versions.sql (a standalone table with no FK
    dependencies) to avoid cascading failures from skipping a table that later
    migrations reference via foreign keys.
    """
    url = _get_database_url()
    conn, schema_name, db_url = _make_schema_conn(url)
    try:
        # Bootstrap schema_versions manually, then pre-mark 006 as applied.
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_versions (
                    migration_name TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                "INSERT INTO schema_versions (migration_name) VALUES (%s)",
                ("006_create_schema_versions.sql",),
            )
        conn.commit()

        apply_migrations(conn, _MIGRATIONS_DIR)

        # 006 was pre-marked, so the runner should not have re-applied it.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM schema_versions WHERE migration_name = %s",
                ("006_create_schema_versions.sql",),
            )
            count = cur.fetchone()[0]
        assert count == 1, "006 must appear exactly once — it was pre-applied"

        # The runner should still have applied all 6 (5 new + 1 pre-marked).
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM schema_versions")
            total = cur.fetchone()[0]
        assert total == 17
    finally:
        _drop_schema(db_url, conn, schema_name)


def test_persons_email_unique(migrated_conn):
    """Inserting two persons with the same email must raise UniqueViolation."""
    with migrated_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO persons (full_name, email) VALUES (%s, %s)",
            ("Alice Smith", "alice@example.com"),
        )
    migrated_conn.commit()

    with pytest.raises(pg_errors.UniqueViolation):
        with migrated_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO persons (full_name, email) VALUES (%s, %s)",
                ("Alice Jones", "alice@example.com"),
            )
        migrated_conn.commit()

    migrated_conn.rollback()


def test_deals_status_default(migrated_conn):
    """Inserting a deal without a status value must produce status='active'."""
    with migrated_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO deals (name) VALUES (%s) RETURNING status",
            ("Riverside Apartments",),
        )
        status = cur.fetchone()[0]
    migrated_conn.commit()
    assert status == "active"


def test_recommendations_status_default(migrated_conn):
    """Inserting a recommendation without a status value must produce status='pending'."""
    # Insert prerequisite rows: a company, person, and deal.
    with migrated_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO persons (full_name, email) VALUES (%s, %s) RETURNING person_id",
            ("Bob Lender", "bob@example.com"),
        )
        person_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO recommendations
                (date, person_id, priority_score, why_now, suggested_action)
            VALUES
                (CURRENT_DATE, %s, 75.0, 'Strong recent signal', 'email')
            RETURNING status
            """,
            (person_id,),
        )
        status = cur.fetchone()[0]
    migrated_conn.commit()
    assert status == "pending"
