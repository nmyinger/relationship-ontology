"""
tests/test_watermark.py — Tests for the ingestion watermark utility (Slice 4).

Uses the same isolated-schema pattern from test_db_migrations.py.
"""

import os
import uuid
from datetime import datetime, timezone

import psycopg2
import pytest

from src.db.runner import apply_migrations
from src.ingestion.watermark import get_watermark, set_watermark

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
    schema_name = f"test_wm_{uuid.uuid4().hex[:8]}"
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


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def wm_conn():
    """Provide a connection with migrations applied in an isolated schema."""
    url = _get_database_url()
    conn, schema_name = _make_schema_conn(url)
    try:
        apply_migrations(conn, _MIGRATIONS_DIR)
        yield conn
    finally:
        _drop_schema(url, conn, schema_name)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_get_watermark_returns_none_when_no_row_exists(wm_conn):
    """A source that has never been synced returns None."""
    assert get_watermark("gmail", wm_conn) is None


def test_set_then_get_returns_the_same_timestamp(wm_conn):
    """Setting a watermark and reading it back returns the same value."""
    ts = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
    set_watermark("gmail", ts, wm_conn)
    result = get_watermark("gmail", wm_conn)
    assert result == ts


def test_set_overwrites_existing_watermark(wm_conn):
    """Setting the watermark twice keeps only the latest value."""
    ts1 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    ts2 = datetime(2026, 3, 15, 9, 30, 0, tzinfo=timezone.utc)

    set_watermark("gmail", ts1, wm_conn)
    set_watermark("gmail", ts2, wm_conn)

    result = get_watermark("gmail", wm_conn)
    assert result == ts2


def test_watermark_is_source_scoped(wm_conn):
    """Gmail and calendar watermarks are independent."""
    gmail_ts = datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
    cal_ts = datetime(2026, 3, 10, 0, 0, 0, tzinfo=timezone.utc)

    set_watermark("gmail", gmail_ts, wm_conn)
    set_watermark("calendar", cal_ts, wm_conn)

    assert get_watermark("gmail", wm_conn) == gmail_ts
    assert get_watermark("calendar", wm_conn) == cal_ts


def test_watermark_timestamp_is_timezone_aware(wm_conn):
    """The returned watermark timestamp must be timezone-aware."""
    ts = datetime(2026, 3, 19, 8, 0, 0, tzinfo=timezone.utc)
    set_watermark("gmail", ts, wm_conn)

    result = get_watermark("gmail", wm_conn)
    assert result.tzinfo is not None
