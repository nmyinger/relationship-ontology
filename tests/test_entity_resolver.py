"""
tests/test_entity_resolver.py — Tests for person/company dedup (Slice 6).

Uses the isolated-schema pattern with real Postgres.
"""

import os
import uuid

import psycopg2
import pytest

from src.db.runner import apply_migrations
from src.extraction.entity_resolver import resolve_company, resolve_person

_MIGRATIONS_DIR = "db/migrations"


def _get_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL and DATABASE_URL are both unset")
    return url


def _make_schema_conn(database_url: str):
    schema_name = f"test_resolver_{uuid.uuid4().hex[:8]}"
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


@pytest.fixture()
def db():
    url = _get_database_url()
    conn, schema_name = _make_schema_conn(url)
    try:
        apply_migrations(conn, _MIGRATIONS_DIR)
        yield conn
    finally:
        _drop_schema(url, conn, schema_name)


def test_resolve_person_creates_new(db):
    pid = resolve_person("alice@example.com", "Alice Smith", None, "VP", db)
    assert pid is not None
    with db.cursor() as cur:
        cur.execute("SELECT full_name, email, title FROM persons WHERE person_id = %s", (pid,))
        row = cur.fetchone()
    assert row == ("Alice Smith", "alice@example.com", "VP")


def test_resolve_person_dedup_by_email(db):
    pid1 = resolve_person("alice@example.com", "Alice Smith", None, "VP", db)
    pid2 = resolve_person("alice@example.com", "Alice S.", None, "Director", db)
    assert pid1 == pid2

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM persons WHERE email = %s", ("alice@example.com",))
        assert cur.fetchone()[0] == 1


def test_resolve_person_updates_title_on_conflict(db):
    resolve_person("bob@example.com", "Bob Jones", None, "Analyst", db)
    resolve_person("bob@example.com", "Bob Jones", None, "Director", db)

    with db.cursor() as cur:
        cur.execute("SELECT title FROM persons WHERE email = %s", ("bob@example.com",))
        assert cur.fetchone()[0] == "Director"


def test_resolve_company_creates_new(db):
    cid = resolve_company("Acme Corp", db)
    assert cid is not None
    with db.cursor() as cur:
        cur.execute("SELECT name FROM companies WHERE company_id = %s", (cid,))
        assert cur.fetchone()[0] == "Acme Corp"


def test_resolve_company_dedup_by_name(db):
    cid1 = resolve_company("Acme Corp", db)
    cid2 = resolve_company("acme corp", db)
    assert cid1 == cid2

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM companies WHERE lower(name) = %s", ("acme corp",))
        assert cur.fetchone()[0] == 1


def test_resolve_company_suffix_normalization(db):
    """Companies with different legal suffixes should resolve to the same record."""
    cid1 = resolve_company("Altitude Development Partners", db)
    cid2 = resolve_company("Altitude Development Partners LLC", db)
    cid3 = resolve_company("Altitude Development Partners, LLC", db)
    assert cid1 == cid2 == cid3

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM companies")
        assert cur.fetchone()[0] == 1


def test_resolve_person_with_company(db):
    cid = resolve_company("BigCo", db)
    pid = resolve_person("charlie@bigco.com", "Charlie", cid, None, db)

    with db.cursor() as cur:
        cur.execute("SELECT company_id FROM persons WHERE person_id = %s", (pid,))
        assert cur.fetchone()[0] == cid


def test_resolve_company_whitespace_normalization(db):
    """Names differing only in whitespace/punctuation resolve to the same company."""
    cid1 = resolve_company("Altitude DP", db)
    cid2 = resolve_company("AltitudeDP", db)
    cid3 = resolve_company("altitude dp", db)
    assert cid1 == cid2 == cid3

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM companies")
        assert cur.fetchone()[0] == 1


def test_resolve_company_alias_lookup(db):
    """A company can be found via an alias in company_aliases."""
    cid = resolve_company("Ava Labs", db)
    # Insert an alias manually
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO company_aliases (alias, company_id) VALUES (%s, %s)",
            ("Avalanche Labs", cid),
        )
    db.commit()

    # Resolve by alias — should return the same company_id
    cid2 = resolve_company("Avalanche Labs", db)
    assert cid2 == cid

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM companies")
        assert cur.fetchone()[0] == 1


def test_resolve_person_is_internal_flag(db):
    """resolve_person stores the is_internal flag correctly."""
    pid = resolve_person("colleague@myco.com", "Colleague", None, None, db,
                         is_internal=True)
    with db.cursor() as cur:
        cur.execute("SELECT is_internal FROM persons WHERE person_id = %s", (pid,))
        assert cur.fetchone()[0] is True


def test_resolve_person_is_internal_sticky(db):
    """Once a person is marked internal, subsequent upserts keep it True."""
    resolve_person("colleague@myco.com", "Colleague", None, None, db,
                   is_internal=True)
    # Second call with is_internal=False should NOT flip it back.
    resolve_person("colleague@myco.com", "Colleague", None, None, db,
                   is_internal=False)
    with db.cursor() as cur:
        cur.execute("SELECT is_internal FROM persons WHERE email = %s",
                    ("colleague@myco.com",))
        assert cur.fetchone()[0] is True
