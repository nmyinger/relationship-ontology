"""
tests/test_pdf_renderer.py — Unit and integration tests for the PDF renderer.

Unit tests use fixture dicts (no database).
Integration tests use isolated DB schemas with real WeasyPrint rendering.
"""

import os
import uuid
from datetime import datetime, timezone

import psycopg2
import pytest

from src.db.runner import apply_migrations
from src.delivery.pdf_renderer import (
    ACTION_LABELS,
    _priority_label,
    fetch_brief_data,
    render_html,
    render_pdf,
    render_pdf_bytes,
)

_MIGRATIONS_DIR = "db/migrations"


# ---------------------------------------------------------------------------
# Helpers — fixture data (no DB)
# ---------------------------------------------------------------------------

def _fixture_rec(**overrides) -> dict:
    rec = {
        "full_name": "Jane Smith",
        "title": "Managing Director",
        "company_name": "Acme Capital",
        "company_type": "LP",
        "priority_score": 35.0,
        "priority_label": "High Priority",
        "why_now": "Jane emailed 2 days ago about the Q2 pipeline and you haven't replied.",
        "suggested_action": "email",
        "action_label": "Send an email",
        "draft_text": "Hi Jane,\n\nFollowing up on your Q2 pipeline question.",
        "deal_name": "Harbor Point Acquisition",
    }
    rec.update(overrides)
    return rec


def _fixture_meeting(**overrides) -> dict:
    mtg = {
        "title": "Catch-up call with Jane",
        "date_display": "Monday, March 24 at 10:00 AM",
        "attendees": "jane@acme.com",
        "context": "Recent: Discussed Q2 pipeline",
    }
    mtg.update(overrides)
    return mtg


def _fixture_deal(**overrides) -> dict:
    deal = {
        "name": "Harbor Point Acquisition",
        "market": "Boston",
        "asset_type": "multifamily",
        "status": "active",
        "contacts": ["Jane Smith"],
    }
    deal.update(overrides)
    return deal


def _fixture_brief(**overrides) -> dict:
    brief = {
        "date": datetime(2026, 3, 21, tzinfo=timezone.utc).date(),
        "recommendations": [_fixture_rec()],
        "meetings": [_fixture_meeting()],
        "deal_matches": [_fixture_deal()],
    }
    brief.update(overrides)
    return brief


# ---------------------------------------------------------------------------
# DB helpers (same pattern as other integration tests)
# ---------------------------------------------------------------------------

def _get_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL and DATABASE_URL are both unset")
    return url


def _make_schema_conn(database_url: str):
    schema_name = f"test_pdf_{uuid.uuid4().hex[:8]}"
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
def pdf_env():
    """Provide (conn, scoped_url, schema_name) with migrations applied."""
    url = _get_database_url()
    conn, schema_name, db_url = _make_schema_conn(url)
    try:
        apply_migrations(conn, _MIGRATIONS_DIR)
        scoped = _scoped_url(db_url, schema_name)
        yield conn, scoped, schema_name
    finally:
        _drop_schema(db_url, conn, schema_name)


# ---------------------------------------------------------------------------
# Unit tests — render_html + render_pdf_bytes (no DB)
# ---------------------------------------------------------------------------

def test_render_html_returns_string():
    """render_html produces a non-empty HTML string."""
    brief = _fixture_brief()
    html = render_html(brief)
    assert isinstance(html, str)
    assert len(html) > 0
    assert "<html" in html


def test_render_html_includes_person_name():
    """HTML contains the contact name."""
    brief = _fixture_brief()
    html = render_html(brief)
    assert "Jane Smith" in html


def test_render_html_includes_why_now():
    """HTML contains the why_now text."""
    brief = _fixture_brief()
    html = render_html(brief)
    assert "Q2 pipeline" in html


def test_render_html_includes_action_label():
    """HTML contains human-friendly action label, not raw enum."""
    brief = _fixture_brief()
    html = render_html(brief)
    assert "Send an email" in html


def test_render_html_includes_draft():
    """HTML contains the draft message text."""
    brief = _fixture_brief()
    html = render_html(brief)
    assert "Following up on your Q2 pipeline question" in html


def test_render_html_includes_deal_name():
    """HTML contains the related deal name."""
    brief = _fixture_brief()
    html = render_html(brief)
    assert "Harbor Point Acquisition" in html


def test_render_html_includes_meeting():
    """HTML contains the upcoming meeting title and context."""
    brief = _fixture_brief()
    html = render_html(brief)
    assert "Catch-up call with Jane" in html
    assert "Discussed Q2 pipeline" in html


def test_render_html_includes_deal_matching():
    """HTML contains the deal matching section with deal name and contacts."""
    brief = _fixture_brief()
    html = render_html(brief)
    assert "Active Deals" in html
    assert "Boston" in html


def test_render_html_no_recommendations():
    """Empty recommendations list produces a graceful empty state."""
    brief = _fixture_brief(recommendations=[])
    html = render_html(brief)
    assert "0 recommendations today" in html
    assert "No recommendations for today" in html


def test_render_html_no_meetings():
    """No meetings — the meetings section is omitted entirely."""
    brief = _fixture_brief(meetings=[])
    html = render_html(brief)
    assert "Upcoming Meetings" not in html


def test_render_html_no_deals():
    """No deal matches — the deal section is omitted entirely."""
    brief = _fixture_brief(deal_matches=[])
    html = render_html(brief)
    assert "Active Deals" not in html


def test_render_html_none_draft():
    """A recommendation with draft_text=None does not crash."""
    rec = _fixture_rec(draft_text=None)
    brief = _fixture_brief(recommendations=[rec])
    html = render_html(brief)
    assert "Jane Smith" in html
    assert "Draft message" not in html


def test_render_html_none_title_and_company():
    """A recommendation with no title or company does not crash."""
    rec = _fixture_rec(title=None, company_name=None, company_type=None)
    brief = _fixture_brief(recommendations=[rec])
    html = render_html(brief)
    assert "Jane Smith" in html


def test_render_pdf_bytes_returns_valid_pdf():
    """render_pdf_bytes produces bytes starting with %PDF."""
    brief = _fixture_brief()
    html = render_html(brief)
    pdf_bytes = render_pdf_bytes(html)
    assert isinstance(pdf_bytes, bytes)
    assert len(pdf_bytes) > 0
    assert pdf_bytes[:5] == b"%PDF-"


def test_render_pdf_bytes_empty_recommendations():
    """PDF is still valid even with no recommendations."""
    brief = _fixture_brief(recommendations=[])
    html = render_html(brief)
    pdf_bytes = render_pdf_bytes(html)
    assert pdf_bytes[:5] == b"%PDF-"


def test_priority_label_high():
    assert _priority_label(39.6) == "High Priority"
    assert _priority_label(35.0) == "High Priority"


def test_priority_label_medium():
    assert _priority_label(30.0) == "Medium Priority"
    assert _priority_label(25.0) == "Medium Priority"


def test_priority_label_default():
    assert _priority_label(20.0) == "Priority"
    assert _priority_label(10.0) == "Priority"


def test_action_labels_complete():
    """All five valid actions have human-friendly labels."""
    expected = {"email", "call", "send_update", "request_intro", "schedule_meeting"}
    assert set(ACTION_LABELS.keys()) == expected


# ---------------------------------------------------------------------------
# Integration tests — render_pdf with real DB
# ---------------------------------------------------------------------------

def _insert_person(conn, full_name: str, email: str) -> str:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO persons (full_name, email)
            VALUES (%s, %s)
            RETURNING person_id
        """, (full_name, email))
        pid = str(cur.fetchone()[0])
    conn.commit()
    return pid


def _insert_score(conn, person_id: str, total_score: float, scored_date) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO contact_scores
                (person_id, importance, urgency, rescue, deficit,
                 total_score, dunbar_layer, scored_date)
            VALUES (%s, 0.5, 0.3, 0.4, 0.2, %s, 1, %s)
        """, (person_id, total_score, scored_date))
    conn.commit()


def _insert_recommendation(conn, person_id: str, scored_date, *,
                           priority_score: float = 35.0,
                           why_now: str = "Test reason.",
                           action: str = "email",
                           draft: str | None = "Test draft.") -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO recommendations
                (date, person_id, priority_score, why_now,
                 suggested_action, draft_text)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (scored_date, person_id, priority_score, why_now, action, draft))
    conn.commit()


def test_render_pdf_writes_file(pdf_env, tmp_path):
    """render_pdf writes a valid PDF file to the output directory."""
    conn, scoped_url, _ = pdf_env
    now = datetime.now(timezone.utc)
    scored_date = now.date()

    pid = _insert_person(conn, "Alice Test", "alice@example.com")
    _insert_score(conn, pid, 35.0, scored_date)
    _insert_recommendation(conn, pid, scored_date)

    out_dir = str(tmp_path)
    path = render_pdf(database_url=scoped_url, today=now, output_dir=out_dir)

    assert path is not None
    assert os.path.exists(path)
    assert path.endswith(".pdf")
    with open(path, "rb") as f:
        assert f.read(5) == b"%PDF-"


def test_render_pdf_no_recommendations(pdf_env, tmp_path):
    """render_pdf returns None when no recommendations exist."""
    conn, scoped_url, _ = pdf_env
    now = datetime.now(timezone.utc)

    out_dir = str(tmp_path)
    path = render_pdf(database_url=scoped_url, today=now, output_dir=out_dir)
    assert path is None


def test_render_pdf_none_draft_integration(pdf_env, tmp_path):
    """render_pdf handles a recommendation with NULL draft_text."""
    conn, scoped_url, _ = pdf_env
    now = datetime.now(timezone.utc)
    scored_date = now.date()

    pid = _insert_person(conn, "Bob NoDraft", "bob@example.com")
    _insert_score(conn, pid, 30.0, scored_date)
    _insert_recommendation(conn, pid, scored_date, draft=None)

    out_dir = str(tmp_path)
    path = render_pdf(database_url=scoped_url, today=now, output_dir=out_dir)
    assert path is not None
    with open(path, "rb") as f:
        assert f.read(5) == b"%PDF-"


def test_fetch_brief_data_structure(pdf_env):
    """fetch_brief_data returns the expected dict keys."""
    conn, scoped_url, _ = pdf_env
    now = datetime.now(timezone.utc)

    # Connect with scoped search_path for the query
    scoped_conn = psycopg2.connect(scoped_url)
    scoped_conn.autocommit = False
    try:
        data = fetch_brief_data(scoped_conn, now)
        assert "date" in data
        assert "recommendations" in data
        assert "meetings" in data
        assert "deal_matches" in data
        assert isinstance(data["recommendations"], list)
    finally:
        scoped_conn.close()
