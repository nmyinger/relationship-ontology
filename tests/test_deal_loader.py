"""
tests/test_deal_loader.py — Tests for the CSV deal loader (Slice 3).

Uses the same isolated-schema pattern from test_db_migrations.py.
Each test gets a fresh Postgres schema with migrations applied.
"""

import os
import tempfile
import uuid

import psycopg2
import pytest

from src.db.runner import apply_migrations
from src.ingestion.deal_loader import load_deals

_MIGRATIONS_DIR = "db/migrations"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL and DATABASE_URL are both unset")
    return url


def _make_schema_conn(database_url: str):
    schema_name = f"test_slice3_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(database_url)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema_name}")
        cur.execute(f"SET search_path TO {schema_name}, public")
    conn.commit()
    return conn, schema_name


def _drop_schema(database_url: str, conn, schema_name: str) -> None:
    conn.close()
    drop_conn = psycopg2.connect(database_url)
    drop_conn.autocommit = True
    with drop_conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
    drop_conn.close()


def _write_csv(content: str) -> str:
    """Write CSV content to a temp file and return its path."""
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    )
    f.write(content)
    f.close()
    return f.name


def _deal_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM deals")
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Fixture — isolated schema with all migrations applied
# ---------------------------------------------------------------------------


@pytest.fixture()
def db():
    """Yield (database_url_with_options, schema_name) with migrations applied."""
    url = _get_database_url()
    conn, schema_name = _make_schema_conn(url)
    try:
        apply_migrations(conn, _MIGRATIONS_DIR)
        # Build a connection string that sets search_path so the deal_loader's
        # own connection lands in the test schema.
        sep = "&" if "?" in url else "?"
        scoped_url = f"{url}{sep}options=-csearch_path%3D{schema_name},public"
        yield scoped_url, conn
    finally:
        _drop_schema(url, conn, schema_name)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

VALID_CSV = """\
name,market,asset_type,size,stage,strategy_tags,status,owner_user_id
Alpha Deal,Boston,multifamily,25000000,underwriting,"value-add,northeast",active,nik
Beta Deal,Miami,office,40000000,sourcing,"core-plus,southeast",active,nik
"""


def test_valid_csv_inserts_correct_rows(db):
    """A well-formed CSV inserts the expected number of rows."""
    scoped_url, conn = db
    csv_path = _write_csv(VALID_CSV)
    try:
        count = load_deals(csv_path, database_url=scoped_url)
        assert count == 2

        # Verify in the test schema via the fixture connection.
        assert _deal_count(conn) == 2
    finally:
        os.unlink(csv_path)


def test_upsert_does_not_duplicate(db):
    """Loading the same CSV twice leaves the same row count."""
    scoped_url, conn = db
    csv_path = _write_csv(VALID_CSV)
    try:
        load_deals(csv_path, database_url=scoped_url)
        load_deals(csv_path, database_url=scoped_url)

        assert _deal_count(conn) == 2
    finally:
        os.unlink(csv_path)


def test_missing_required_column_raises_valueerror(db):
    """A CSV missing the 'name' column raises ValueError naming the column."""
    scoped_url, _conn = db
    bad_csv = "market,asset_type\nBoston,multifamily\n"
    csv_path = _write_csv(bad_csv)
    try:
        with pytest.raises(ValueError, match="name"):
            load_deals(csv_path, database_url=scoped_url)
    finally:
        os.unlink(csv_path)


def test_status_defaults_to_active(db):
    """When the status column is empty, it defaults to 'active'."""
    scoped_url, conn = db
    csv_no_status = "name,market\nGamma Deal,Denver\n"
    csv_path = _write_csv(csv_no_status)
    try:
        load_deals(csv_path, database_url=scoped_url)

        with conn.cursor() as cur:
            cur.execute("SELECT status FROM deals WHERE name = 'Gamma Deal'")
            status = cur.fetchone()[0]
        assert status == "active"
    finally:
        os.unlink(csv_path)
