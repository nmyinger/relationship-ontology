"""
tests/test_recommendation_generator.py — Integration tests for the recommendation generator.

Uses isolated DB schemas with mock LLM. Same pattern as test_scoring_integration.py.
"""

import os
import re
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

from src.db.runner import apply_migrations
from src.recommendations.generator import generate_recommendations

_MIGRATIONS_DIR = "db/migrations"


# ---------------------------------------------------------------------------
# Helpers (same pattern as other integration tests)
# ---------------------------------------------------------------------------

def _get_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL and DATABASE_URL are both unset")
    return url


def _make_schema_conn(database_url: str):
    schema_name = f"test_rec_{uuid.uuid4().hex[:8]}"
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
def rec_env():
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
                   is_internal: bool = False) -> str:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO persons (full_name, email, is_internal)
            VALUES (%s, %s, %s)
            RETURNING person_id
        """, (full_name, email, is_internal))
        pid = str(cur.fetchone()[0])
    conn.commit()
    return pid


def _insert_score(conn, person_id: str, total_score: float, *,
                  scored_date=None, importance: float = 0.5,
                  urgency: float = 0.3, rescue: float = 0.4,
                  deficit: float = 0.2, dunbar_layer: int = 1) -> None:
    if scored_date is None:
        scored_date = datetime.now(timezone.utc).date()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO contact_scores
                (person_id, importance, urgency, rescue, deficit,
                 total_score, dunbar_layer, scored_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (person_id, importance, urgency, rescue, deficit,
              total_score, dunbar_layer, scored_date))
    conn.commit()


def _insert_interaction(conn, participants: list[str], direction: str,
                        itype: str = "email", *,
                        timestamp: datetime | None = None,
                        summary: str | None = None) -> str:
    if timestamp is None:
        timestamp = datetime.now(timezone.utc) - timedelta(days=1)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO interactions (type, timestamp, direction, participants,
                                      summary)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING interaction_id
        """, (itype, timestamp, direction, participants, summary))
        iid = str(cur.fetchone()[0])
    conn.commit()
    return iid


def _get_recommendations(conn, date=None) -> list[dict]:
    query = """
        SELECT person_id, priority_score, why_now, suggested_action,
               draft_text, status, related_deal_id
        FROM recommendations
    """
    params: list = []
    if date:
        query += " WHERE date = %s"
        params.append(date)
    query += " ORDER BY priority_score DESC"
    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Mock LLM
# ---------------------------------------------------------------------------

def _mock_llm(system, user):
    """Return a valid recommendation response. Extracts person name from prompt."""
    name_match = re.search(r"## Contact: (.+)", user)
    name = name_match.group(1) if name_match else "the contact"
    return {
        "why_now": f"Recent activity from {name} suggests follow-up needed.",
        "suggested_action": "email",
        "draft_text": f"Hi {name},\n\nFollowing up on our recent conversation.",
        "confidence": 0.8,
        "source_trace": ["recent inbound email", "high rescue score"],
    }


def _mock_llm_bad_action(system, user):
    """Return a response with invalid suggested_action."""
    return {
        "why_now": "Test reason.",
        "suggested_action": "invalid_action",
        "draft_text": "Test draft.",
        "confidence": 0.5,
        "source_trace": [],
    }


# ---------------------------------------------------------------------------
# Prompt-capturing mock
# ---------------------------------------------------------------------------

class _CapturingMock:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    def __call__(self, system, user):
        self.calls.append((system, user))
        return _mock_llm(system, user)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_generate_empty_db(rec_env):
    """No scores → returns 0, no crash."""
    conn, scoped_url, _ = rec_env
    count = generate_recommendations(
        database_url=scoped_url, llm_fn=_mock_llm,
    )
    assert count == 0


def test_generate_writes_recommendations(rec_env):
    """Mock LLM returns valid JSON → rows in recommendations table."""
    conn, scoped_url, _ = rec_env
    now = datetime.now(timezone.utc)
    pid = _insert_person(conn, "Alice Test", "alice@example.com")
    _insert_score(conn, pid, 35.0, scored_date=now.date())
    _insert_interaction(conn, ["alice@example.com"], "inbound",
                        summary="Asked about pipeline")

    count = generate_recommendations(
        database_url=scoped_url, today=now, llm_fn=_mock_llm,
    )
    assert count == 1

    recs = _get_recommendations(conn, now.date())
    assert len(recs) == 1
    assert str(recs[0]["person_id"]) == pid
    assert recs[0]["priority_score"] == 35.0


def test_why_now_non_empty(rec_env):
    """Every recommendation has non-empty why_now."""
    conn, scoped_url, _ = rec_env
    now = datetime.now(timezone.utc)
    pid = _insert_person(conn, "Bob Test", "bob@example.com")
    _insert_score(conn, pid, 30.0, scored_date=now.date())

    generate_recommendations(
        database_url=scoped_url, today=now, llm_fn=_mock_llm,
    )
    recs = _get_recommendations(conn, now.date())
    for rec in recs:
        assert rec["why_now"] and len(rec["why_now"]) > 0


def test_draft_text_non_empty_no_placeholders(rec_env):
    """draft_text is non-empty and has no [NAME], [COMPANY], {{...}} tokens."""
    conn, scoped_url, _ = rec_env
    now = datetime.now(timezone.utc)
    pid = _insert_person(conn, "Carol Test", "carol@example.com")
    _insert_score(conn, pid, 28.0, scored_date=now.date())

    generate_recommendations(
        database_url=scoped_url, today=now, llm_fn=_mock_llm,
    )
    recs = _get_recommendations(conn, now.date())
    for rec in recs:
        draft = rec.get("draft_text") or ""
        assert len(draft) > 0, "draft_text should be non-empty"
        assert "[NAME]" not in draft
        assert "[COMPANY]" not in draft
        assert "{{" not in draft


def test_max_10_recommendations(rec_env):
    """Even with 15 scored contacts, max 10 recommendations."""
    conn, scoped_url, _ = rec_env
    now = datetime.now(timezone.utc)
    for i in range(15):
        pid = _insert_person(conn, f"Person {i}", f"p{i}@example.com")
        _insert_score(conn, pid, 40.0 - i, scored_date=now.date())

    count = generate_recommendations(
        database_url=scoped_url, today=now, llm_fn=_mock_llm,
    )
    assert count == 10


def test_suggested_action_valid_enum(rec_env):
    """suggested_action is one of the 5 valid values, even if LLM returns bad one."""
    conn, scoped_url, _ = rec_env
    now = datetime.now(timezone.utc)
    pid = _insert_person(conn, "Dan Test", "dan@example.com")
    _insert_score(conn, pid, 25.0, scored_date=now.date())

    generate_recommendations(
        database_url=scoped_url, today=now, llm_fn=_mock_llm_bad_action,
    )
    recs = _get_recommendations(conn, now.date())
    valid = {"email", "call", "send_update", "request_intro",
             "schedule_meeting"}
    for rec in recs:
        assert rec["suggested_action"] in valid


def test_prompt_includes_context(rec_env):
    """Mock LLM captures prompt → assert prompt includes person name and interaction."""
    conn, scoped_url, _ = rec_env
    now = datetime.now(timezone.utc)
    pid = _insert_person(conn, "Eve Context", "eve@example.com")
    _insert_score(conn, pid, 32.0, scored_date=now.date())
    _insert_interaction(conn, ["eve@example.com"], "inbound",
                        summary="Wants to discuss fund terms",
                        timestamp=now - timedelta(days=2))

    mock = _CapturingMock()
    generate_recommendations(
        database_url=scoped_url, today=now, llm_fn=mock,
    )
    assert len(mock.calls) == 1
    system_prompt, user_prompt = mock.calls[0]
    assert "Eve Context" in user_prompt
    assert "Wants to discuss fund terms" in user_prompt
    assert "relationship intelligence" in system_prompt.lower()


def test_prompt_includes_deal_name(rec_env):
    """When a deal signal exists, the prompt sent to the LLM includes the deal name."""
    conn, scoped_url, _ = rec_env
    now = datetime.now(timezone.utc)
    pid = _insert_person(conn, "Grace Deal", "grace@example.com")
    _insert_score(conn, pid, 30.0, scored_date=now.date())

    # Insert a deal
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO deals (name, market, asset_type, status)
            VALUES ('Harbor Point Acquisition', 'Boston', 'multifamily', 'active')
            RETURNING deal_id
        """)
        cur.fetchone()  # consume RETURNING
    conn.commit()

    # Insert an interaction with a deal_mention signal
    iid = _insert_interaction(conn, ["grace@example.com"], "inbound",
                              summary="Discussed Harbor Point",
                              timestamp=now - timedelta(days=3))
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO interaction_signals
                (interaction_id, signal_type, signal_value, confidence)
            VALUES (%s, 'deal_mention', 'Harbor Point', 0.9)
        """, (iid,))
    conn.commit()

    mock = _CapturingMock()
    generate_recommendations(
        database_url=scoped_url, today=now, llm_fn=mock,
    )
    assert len(mock.calls) == 1
    _, user_prompt = mock.calls[0]
    assert "Harbor Point" in user_prompt


def test_idempotent(rec_env):
    """Running twice for same date doesn't double rows."""
    conn, scoped_url, _ = rec_env
    now = datetime.now(timezone.utc)
    pid = _insert_person(conn, "Frank Idem", "frank@example.com")
    _insert_score(conn, pid, 20.0, scored_date=now.date())

    generate_recommendations(
        database_url=scoped_url, today=now, llm_fn=_mock_llm,
    )
    generate_recommendations(
        database_url=scoped_url, today=now, llm_fn=_mock_llm,
    )
    recs = _get_recommendations(conn, now.date())
    assert len(recs) == 1
