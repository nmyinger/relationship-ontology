"""
tests/test_scoring_integration.py — DB integration tests for the scoring engine.

Each test uses an isolated schema (CREATE SCHEMA / DROP CASCADE).
"""

import os
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

from src.db.runner import apply_migrations
from src.scoring.scorer import score_all

_MIGRATIONS_DIR = "db/migrations"


# ---------------------------------------------------------------------------
# Helpers (same pattern as test_db_migrations.py)
# ---------------------------------------------------------------------------

def _get_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL and DATABASE_URL are both unset")
    return url


def _make_schema_conn(database_url: str):
    schema_name = f"test_score_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(database_url)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema_name}")
        cur.execute(f"SET search_path TO {schema_name}, public")
    conn.commit()
    return conn, schema_name, database_url


def _drop_schema(database_url: str, conn, schema_name: str) -> None:
    conn.close()
    drop_conn = psycopg2.connect(database_url)
    drop_conn.autocommit = True
    with drop_conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
    drop_conn.close()


def _scoped_url(database_url: str, schema_name: str) -> str:
    sep = "&" if "?" in database_url else "?"
    return f"{database_url}{sep}options=-csearch_path%3D{schema_name},public"


@pytest.fixture()
def scoring_env():
    """Provide (conn, scoped_url, schema_name) with migrations applied."""
    url = _get_database_url()
    conn, schema_name, db_url = _make_schema_conn(url)
    try:
        apply_migrations(conn, _MIGRATIONS_DIR)
        scoped = _scoped_url(db_url, schema_name)
        yield conn, scoped, schema_name
    finally:
        _drop_schema(db_url, conn, schema_name)


def _insert_person(conn, full_name: str, email: str, *,
                   is_internal: bool = False,
                   priority_override: str | None = None) -> str:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO persons (full_name, email, is_internal, priority_override)
            VALUES (%s, %s, %s, %s)
            RETURNING person_id
        """, (full_name, email, is_internal, priority_override))
        pid = str(cur.fetchone()[0])
    conn.commit()
    return pid


def _insert_interaction(conn, participants: list[str], direction: str,
                        itype: str = "email", *,
                        timestamp: datetime | None = None) -> str:
    if timestamp is None:
        timestamp = datetime.now(timezone.utc) - timedelta(days=1)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO interactions (type, timestamp, direction, participants)
            VALUES (%s, %s, %s, %s)
            RETURNING interaction_id
        """, (itype, timestamp, direction, participants))
        iid = str(cur.fetchone()[0])
    conn.commit()
    return iid


def _get_scores(conn, person_id: str | None = None) -> list[dict]:
    query = """
        SELECT person_id, importance, urgency, rescue, deficit,
               total_score, dunbar_layer
        FROM contact_scores
    """
    params: list = []
    if person_id:
        query += " WHERE person_id = %s"
        params.append(person_id)
    query += " ORDER BY total_score DESC"
    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_score_empty_db(scoring_env):
    """Scoring an empty database returns 0 and doesn't crash."""
    conn, scoped_url, _ = scoring_env
    count = score_all(database_url=scoped_url)
    assert count == 0


def test_internal_persons_excluded(scoring_env):
    """is_internal=true persons are never scored."""
    conn, scoped_url, _ = scoring_env
    pid = _insert_person(conn, "Internal User", "internal@example.com",
                         is_internal=True)
    count = score_all(database_url=scoped_url)
    assert count == 0
    scores = _get_scores(conn, pid)
    assert len(scores) == 0


def test_zero_interaction_person(scoring_env):
    """Person with zero interactions gets a very low score."""
    conn, scoped_url, _ = scoring_env
    pid = _insert_person(conn, "Ghost Person", "ghost@example.com")
    count = score_all(database_url=scoped_url)
    assert count == 1
    scores = _get_scores(conn, pid)
    assert len(scores) == 1
    # No interactions → urgency=0, rescue=0, so I*(0.6*0+0.4*0)=0.
    # Only deficit contributes, which is small.
    assert scores[0]["total_score"] < 10.0


def test_priority_override_high(scoring_env):
    """Person with priority_override='high' has importance >= 0.8."""
    conn, scoped_url, _ = scoring_env
    pid = _insert_person(conn, "VIP Contact", "vip@example.com",
                         priority_override="high")
    count = score_all(database_url=scoped_url)
    assert count == 1
    scores = _get_scores(conn, pid)
    assert scores[0]["importance"] >= 0.8


def test_writes_contact_scores(scoring_env):
    """Verify rows are written to contact_scores."""
    conn, scoped_url, _ = scoring_env
    _insert_person(conn, "Alice", "alice@example.com")
    _insert_person(conn, "Bob", "bob@example.com")
    _insert_interaction(conn, ["alice@example.com"], "inbound")
    count = score_all(database_url=scoped_url)
    assert count == 2
    scores = _get_scores(conn)
    assert len(scores) == 2


def test_idempotent(scoring_env):
    """Running score_all twice doesn't double rows for today."""
    conn, scoped_url, _ = scoring_env
    _insert_person(conn, "Alice", "alice@example.com")
    now = datetime.now(timezone.utc)
    score_all(database_url=scoped_url, today=now)
    score_all(database_url=scoped_url, today=now)
    scores = _get_scores(conn)
    assert len(scores) == 1


def test_outbound_boosts_frequency(scoring_env):
    """Person with outbound interactions scores higher importance than
    same count of inbound (due to 1.5x outbound weight)."""
    conn, scoped_url, _ = scoring_env
    pid_out = _insert_person(conn, "Outbound Contact", "out@example.com")
    pid_in = _insert_person(conn, "Inbound Contact", "in@example.com")

    now = datetime.now(timezone.utc)
    for i in range(5):
        ts = now - timedelta(days=i + 1)
        _insert_interaction(conn, ["out@example.com"], "outbound",
                            timestamp=ts)
        _insert_interaction(conn, ["in@example.com"], "inbound",
                            timestamp=ts)

    score_all(database_url=scoped_url, today=now)
    out_scores = _get_scores(conn, pid_out)
    in_scores = _get_scores(conn, pid_in)
    # Outbound person has higher frequency component in importance
    assert out_scores[0]["importance"] > in_scores[0]["importance"]


def test_unreturned_inbound_boosts_urgency(scoring_env):
    """Person with unreplied inbound has higher urgency than
    person with only old interactions (via inbound spike bonus)."""
    conn, scoped_url, _ = scoring_env
    pid_unr = _insert_person(conn, "Unreplied", "unreplied@example.com")
    pid_old = _insert_person(conn, "Old Contact", "old@example.com")

    now = datetime.now(timezone.utc)
    # Both have the same old interaction history
    for i in range(5):
        ts = now - timedelta(days=30 + i * 7)
        _insert_interaction(conn, ["unreplied@example.com"], "outbound",
                            timestamp=ts)
        _insert_interaction(conn, ["old@example.com"], "outbound",
                            timestamp=ts)

    # Unreplied also has a recent inbound with no reply
    _insert_interaction(conn, ["unreplied@example.com"], "inbound",
                        timestamp=now - timedelta(days=1))

    score_all(database_url=scoped_url, today=now)
    unr_scores = _get_scores(conn, pid_unr)
    old_scores = _get_scores(conn, pid_old)
    assert unr_scores[0]["urgency"] > old_scores[0]["urgency"]
