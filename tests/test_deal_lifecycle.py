"""
tests/test_deal_lifecycle.py — Tests for deal lifecycle timestamps (Slice 6b).

Verifies created_at, updated_at, closed_at behaviour on insert and upsert,
and status-based transitions.
"""

import os
import tempfile
import time
import uuid

import psycopg2
import pytest

from src.db.runner import apply_migrations
from src.ingestion.deal_loader import load_deals

_MIGRATIONS_DIR = "db/migrations"


# ---------------------------------------------------------------------------
# Helpers (same pattern as test_deal_loader.py)
# ---------------------------------------------------------------------------


def _get_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL and DATABASE_URL are both unset")
    return url


def _make_schema_conn(database_url: str):
    schema_name = f"test_lifecycle_{uuid.uuid4().hex[:8]}"
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
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    )
    f.write(content)
    f.close()
    return f.name


@pytest.fixture()
def db():
    url = _get_database_url()
    conn, schema_name = _make_schema_conn(url)
    try:
        apply_migrations(conn, _MIGRATIONS_DIR)
        sep = "&" if "?" in url else "?"
        scoped_url = f"{url}{sep}options=-csearch_path%3D{schema_name},public"
        yield scoped_url, conn
    finally:
        _drop_schema(url, conn, schema_name)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_deal_has_timestamps_on_insert(db):
    """A newly inserted deal has created_at and updated_at set."""
    scoped_url, conn = db
    csv = "name,market,status\nAlpha Deal,Boston,active\n"
    path = _write_csv(csv)
    try:
        load_deals(path, database_url=scoped_url)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT created_at, updated_at, closed_at FROM deals WHERE name = 'Alpha Deal'"
            )
            row = cur.fetchone()
        assert row[0] is not None  # created_at
        assert row[1] is not None  # updated_at
        assert row[2] is None      # closed_at (active deal)
    finally:
        os.unlink(path)


def test_deal_updated_at_changes_on_upsert(db):
    """Re-loading a CSV updates updated_at."""
    scoped_url, conn = db
    csv = "name,market,status\nBeta Deal,Miami,active\n"
    path = _write_csv(csv)
    try:
        load_deals(path, database_url=scoped_url)
        with conn.cursor() as cur:
            cur.execute("SELECT updated_at FROM deals WHERE name = 'Beta Deal'")
            first_updated = cur.fetchone()[0]

        time.sleep(0.05)  # ensure timestamp difference
        load_deals(path, database_url=scoped_url)
        with conn.cursor() as cur:
            cur.execute("SELECT updated_at FROM deals WHERE name = 'Beta Deal'")
            second_updated = cur.fetchone()[0]

        assert second_updated > first_updated
    finally:
        os.unlink(path)


def test_deal_closed_at_set_on_status_change(db):
    """Changing status to 'closed' sets closed_at."""
    scoped_url, conn = db
    csv_active = "name,status\nGamma Deal,active\n"
    csv_closed = "name,status\nGamma Deal,closed\n"
    p1 = _write_csv(csv_active)
    p2 = _write_csv(csv_closed)
    try:
        load_deals(p1, database_url=scoped_url)
        with conn.cursor() as cur:
            cur.execute("SELECT closed_at FROM deals WHERE name = 'Gamma Deal'")
            assert cur.fetchone()[0] is None

        load_deals(p2, database_url=scoped_url)
        with conn.cursor() as cur:
            cur.execute("SELECT closed_at FROM deals WHERE name = 'Gamma Deal'")
            assert cur.fetchone()[0] is not None
    finally:
        os.unlink(p1)
        os.unlink(p2)


def test_deal_closed_at_cleared_on_reactivation(db):
    """Changing status back to 'active' clears closed_at."""
    scoped_url, conn = db
    csv_active = "name,status\nDelta Deal,active\n"
    csv_closed = "name,status\nDelta Deal,closed\n"
    p1 = _write_csv(csv_active)
    p2 = _write_csv(csv_closed)
    try:
        load_deals(p1, database_url=scoped_url)
        load_deals(p2, database_url=scoped_url)
        with conn.cursor() as cur:
            cur.execute("SELECT closed_at FROM deals WHERE name = 'Delta Deal'")
            assert cur.fetchone()[0] is not None

        # Reactivate
        load_deals(p1, database_url=scoped_url)
        with conn.cursor() as cur:
            cur.execute("SELECT closed_at FROM deals WHERE name = 'Delta Deal'")
            assert cur.fetchone()[0] is None
    finally:
        os.unlink(p1)
        os.unlink(p2)


def test_deal_status_values(db):
    """All four status values are accepted."""
    scoped_url, conn = db
    csv = (
        "name,status\n"
        "S1,active\n"
        "S2,closed\n"
        "S3,dead\n"
        "S4,on_hold\n"
    )
    path = _write_csv(csv)
    try:
        count = load_deals(path, database_url=scoped_url)
        assert count == 4
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM deals ORDER BY name")
            statuses = [r[0] for r in cur.fetchall()]
        assert statuses == ["active", "closed", "dead", "on_hold"]
    finally:
        os.unlink(path)


def test_closed_deal_excluded_from_active_query(db):
    """A query for active deals excludes closed/dead/on_hold."""
    scoped_url, conn = db
    csv = (
        "name,status\n"
        "Active One,active\n"
        "Closed One,closed\n"
        "Dead One,dead\n"
        "Hold One,on_hold\n"
    )
    path = _write_csv(csv)
    try:
        load_deals(path, database_url=scoped_url)
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM deals WHERE status = 'active'")
            names = [r[0] for r in cur.fetchall()]
        assert names == ["Active One"]
    finally:
        os.unlink(path)


def test_deal_loader_sets_updated_at(db):
    """Integration: CSV load sets updated_at."""
    scoped_url, conn = db
    csv = "name,market\nEpsilon Deal,Denver\n"
    path = _write_csv(csv)
    try:
        load_deals(path, database_url=scoped_url)
        with conn.cursor() as cur:
            cur.execute("SELECT updated_at FROM deals WHERE name = 'Epsilon Deal'")
            assert cur.fetchone()[0] is not None
    finally:
        os.unlink(path)


def test_deal_loader_handles_dead_status(db):
    """CSV with status='dead' sets closed_at on transition from active."""
    scoped_url, conn = db
    csv_active = "name,status\nZeta Deal,active\n"
    csv_dead = "name,status\nZeta Deal,dead\n"
    p1 = _write_csv(csv_active)
    p2 = _write_csv(csv_dead)
    try:
        load_deals(p1, database_url=scoped_url)
        load_deals(p2, database_url=scoped_url)
        with conn.cursor() as cur:
            cur.execute("SELECT closed_at, status FROM deals WHERE name = 'Zeta Deal'")
            row = cur.fetchone()
        assert row[0] is not None  # closed_at set
        assert row[1] == "dead"
    finally:
        os.unlink(p1)
        os.unlink(p2)
