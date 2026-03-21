"""
tests/test_extractor.py — Tests for the extraction orchestrator (Slice 6).

Uses isolated DB schemas and a mocked LLM function.
"""

import json
import os
import uuid
from datetime import datetime, timezone

import psycopg2
import pytest

from src.db.runner import apply_migrations
from src.extraction.extractor import _is_service_sender, extract_batch

_MIGRATIONS_DIR = "db/migrations"


def _get_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL and DATABASE_URL are both unset")
    return url


def _make_schema_conn(database_url: str):
    schema_name = f"test_extractor_{uuid.uuid4().hex[:8]}"
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


def _scoped_url(base_url: str, schema_name: str) -> str:
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}options=-csearch_path%3D{schema_name},public"


@pytest.fixture()
def db():
    url = _get_database_url()
    conn, schema_name = _make_schema_conn(url)
    try:
        apply_migrations(conn, _MIGRATIONS_DIR)
        scoped = _scoped_url(url, schema_name)
        yield scoped, conn, schema_name
    finally:
        _drop_schema(url, conn, schema_name)


def _insert_email_raw(conn, sender="alice@example.com", subject="Deal Update",
                      raw_payload=None):
    """Insert a test email_raw row and return its id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO email_raw
                (external_id, timestamp, direction, sender,
                 recipients, subject, body_text, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                f"ext_{uuid.uuid4().hex[:8]}",
                datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc),
                "inbound",
                sender,
                ["bob@example.com"],
                subject,
                "Let's discuss the Acme deal.",
                json.dumps(raw_payload) if raw_payload else None,
            ),
        )
        row_id = cur.fetchone()[0]
    conn.commit()
    return row_id


def _insert_calendar_raw(conn, title="Team Sync"):
    """Insert a test calendar_raw row and return its id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO calendar_raw (external_id, timestamp, title, participants)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (
                f"cal_{uuid.uuid4().hex[:8]}",
                datetime(2026, 3, 2, 14, 0, 0, tzinfo=timezone.utc),
                title,
                ["alice@example.com", "bob@example.com"],
            ),
        )
        row_id = cur.fetchone()[0]
    conn.commit()
    return row_id


MOCK_LLM_RESPONSE = {
    "persons": [
        {"name": "Alice Smith", "email": "alice@example.com",
         "company": "Acme Corp", "title": "VP"},
        {"name": "Bob Jones", "email": "bob@example.com", "company": None, "title": None},
    ],
    "companies": [
        {"name": "Acme Corp", "type": "investor"},
    ],
    "signals": [
        {"type": "deal_mention", "value": "Acme deal discussed", "confidence": 0.9},
    ],
    "summary": "Alice and Bob discussed the Acme deal.",
}


def _mock_llm(system_prompt, user_message):
    return MOCK_LLM_RESPONSE


def test_extract_email_creates_interaction_and_entities(db):
    scoped_url, conn, schema_name = db
    _insert_email_raw(conn)

    count = extract_batch(source="email", database_url=scoped_url, llm_fn=_mock_llm)
    assert count == 1

    # Verify interaction created
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema_name}, public")
        cur.execute("SELECT type, summary FROM interactions")
        row = cur.fetchone()
    assert row[0] == "email"
    assert "Acme" in row[1]


def test_extract_email_creates_persons(db):
    scoped_url, conn, schema_name = db
    _insert_email_raw(conn)
    extract_batch(source="email", database_url=scoped_url, llm_fn=_mock_llm)

    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema_name}, public")
        cur.execute("SELECT email FROM persons ORDER BY email")
        emails = [r[0] for r in cur.fetchall()]
    assert "alice@example.com" in emails
    assert "bob@example.com" in emails


def test_extract_email_creates_signals(db):
    scoped_url, conn, schema_name = db
    _insert_email_raw(conn)
    extract_batch(source="email", database_url=scoped_url, llm_fn=_mock_llm)

    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema_name}, public")
        cur.execute("SELECT signal_type, signal_value FROM interaction_signals")
        row = cur.fetchone()
    assert row[0] == "deal_mention"
    assert "Acme" in row[1]


def test_extract_sets_processed_at(db):
    scoped_url, conn, schema_name = db
    _insert_email_raw(conn)
    extract_batch(source="email", database_url=scoped_url, llm_fn=_mock_llm)

    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema_name}, public")
        cur.execute("SELECT processed_at FROM email_raw")
        row = cur.fetchone()
    assert row[0] is not None


def test_extract_skips_already_processed(db):
    scoped_url, conn, schema_name = db
    _insert_email_raw(conn)

    extract_batch(source="email", database_url=scoped_url, llm_fn=_mock_llm)
    count = extract_batch(source="email", database_url=scoped_url, llm_fn=_mock_llm)
    assert count == 0


def test_extract_calendar(db):
    scoped_url, conn, schema_name = db
    _insert_calendar_raw(conn)

    count = extract_batch(source="calendar", database_url=scoped_url, llm_fn=_mock_llm)
    assert count == 1

    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema_name}, public")
        cur.execute("SELECT type FROM interactions")
        assert cur.fetchone()[0] == "calendar"


def test_extract_dedup_persons_across_emails(db):
    scoped_url, conn, schema_name = db
    _insert_email_raw(conn, sender="alice@example.com", subject="First")
    _insert_email_raw(conn, sender="alice@example.com", subject="Second")

    extract_batch(source="email", batch_size=10, database_url=scoped_url, llm_fn=_mock_llm)

    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema_name}, public")
        cur.execute("SELECT COUNT(*) FROM persons WHERE email = %s", ("alice@example.com",))
        assert cur.fetchone()[0] == 1


def test_extract_continues_on_llm_error(db):
    """If the LLM fails on one row, processing continues with the next."""
    scoped_url, conn, schema_name = db
    _insert_email_raw(conn, subject="Fail")
    _insert_email_raw(conn, subject="Succeed")

    call_count = 0

    def _flaky_llm(system, user):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("LLM timeout")
        return MOCK_LLM_RESPONSE

    count = extract_batch(source="email", database_url=scoped_url, llm_fn=_flaky_llm)
    assert count == 1  # one succeeded, one failed


def test_skip_newsletter_with_list_id(db):
    """Emails with List-ID header are skipped without calling the LLM."""
    scoped_url, conn, schema_name = db
    payload = {
        "payload": {
            "headers": [
                {"name": "List-ID", "value": "<news.example.com>"},
            ],
        },
    }
    _insert_email_raw(conn, raw_payload=payload)

    call_count = 0

    def _counting_llm(system, user):
        nonlocal call_count
        call_count += 1
        return MOCK_LLM_RESPONSE

    count = extract_batch(source="email", database_url=scoped_url, llm_fn=_counting_llm)
    assert count == 0
    assert call_count == 0

    # processed_at should still be set
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema_name}, public")
        cur.execute("SELECT processed_at FROM email_raw")
        assert cur.fetchone()[0] is not None

    # No interaction should be created
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM interactions")
        assert cur.fetchone()[0] == 0


def test_user_email_excluded_from_extraction(db):
    """The user's own email should not create a person record."""
    scoped_url, conn, schema_name = db

    user_email = "me@example.com"
    os.environ["USER_EMAIL"] = user_email
    try:
        response_with_user = {
            "persons": [
                {"name": "Alice Smith", "email": "alice@example.com",
                 "company": None, "title": None},
                {"name": "Me", "email": user_email,
                 "company": None, "title": None},
            ],
            "companies": [],
            "signals": [],
            "summary": "Test email.",
        }

        def _mock(system, user):
            return response_with_user

        _insert_email_raw(conn)
        extract_batch(source="email", database_url=scoped_url, llm_fn=_mock)

        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {schema_name}, public")
            cur.execute("SELECT email FROM persons ORDER BY email")
            emails = [r[0] for r in cur.fetchall()]
        assert "alice@example.com" in emails
        assert user_email not in emails
    finally:
        del os.environ["USER_EMAIL"]


def test_internal_persons_flagged_by_domain(db):
    """Persons sharing the user's email domain are marked is_internal=True."""
    scoped_url, conn, schema_name = db

    os.environ["USER_EMAIL"] = "nikolai@mycompany.com"
    try:
        response = {
            "persons": [
                {"name": "Internal Guy", "email": "bob@mycompany.com",
                 "company": None, "title": None},
                {"name": "External Gal", "email": "alice@otherfirm.com",
                 "company": None, "title": None},
            ],
            "companies": [],
            "signals": [],
            "summary": "Test.",
        }

        _insert_email_raw(conn)
        extract_batch(source="email", database_url=scoped_url,
                      llm_fn=lambda s, u: response)

        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {schema_name}, public")
            cur.execute(
                "SELECT email, is_internal FROM persons ORDER BY email"
            )
            rows = {r[0]: r[1] for r in cur.fetchall()}

        assert rows["bob@mycompany.com"] is True
        assert rows["alice@otherfirm.com"] is False
    finally:
        del os.environ["USER_EMAIL"]


def test_extract_concurrent_processes_all_rows(db):
    """Thread pool fans out LLM calls and all rows get processed."""
    scoped_url, conn, schema_name = db
    for i in range(5):
        _insert_email_raw(conn, sender=f"user{i}@example.com", subject=f"Deal {i}")

    count = extract_batch(
        source="email", database_url=scoped_url, llm_fn=_mock_llm, concurrency=3,
    )
    assert count == 5

    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema_name}, public")
        cur.execute("SELECT COUNT(*) FROM interactions")
        assert cur.fetchone()[0] == 5
        cur.execute("SELECT COUNT(*) FROM email_raw WHERE processed_at IS NOT NULL")
        assert cur.fetchone()[0] == 5


def test_skip_reason_recorded_for_newsletter(db):
    """When an email is skipped as a newsletter, skip_reason is stored in email_raw."""
    scoped_url, conn, schema_name = db
    payload = {
        "payload": {
            "headers": [
                {"name": "List-Unsubscribe", "value": "<mailto:unsub@example.com>"},
            ],
        },
    }
    _insert_email_raw(conn, raw_payload=payload)

    extract_batch(source="email", database_url=scoped_url, llm_fn=_mock_llm)

    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema_name}, public")
        cur.execute("SELECT skip_reason FROM email_raw")
        row = cur.fetchone()
    assert row[0] == "newsletter"


def test_skip_does_not_affect_normal_email(db):
    """Normal emails without list headers are processed normally."""
    scoped_url, conn, schema_name = db
    payload = {
        "payload": {
            "headers": [
                {"name": "From", "value": "alice@example.com"},
                {"name": "Subject", "value": "Deal Update"},
            ],
        },
    }
    _insert_email_raw(conn, raw_payload=payload)

    count = extract_batch(source="email", database_url=scoped_url, llm_fn=_mock_llm)
    assert count == 1


# --- Service sender filter tests ---


def test_service_sender_noreply_skipped():
    """noreply@ addresses and @noreply. subdomains are detected."""
    assert _is_service_sender("noreply@uber.com") == "noreply_sender"
    assert _is_service_sender("no-reply@github.com") == "noreply_sender"
    assert _is_service_sender("donotreply@bank.com") == "noreply_sender"
    assert _is_service_sender("unemployment@noreply.mass.gov") == "noreply_sender"


def test_service_sender_service_domain_skipped():
    """Addresses from transactional subdomains are detected."""
    assert _is_service_sender("message@service.suitsupply.com") == "service_domain"
    assert _is_service_sender("alerts@notifications.chase.com") == "service_domain"
    assert _is_service_sender("info@mail.example.com") == "service_domain"
    assert _is_service_sender("events@calendar.luma-mail.com") == "service_domain"
    assert _is_service_sender("AmericanExpress@welcome.americanexpress.com") == "service_domain"
    assert _is_service_sender("Julia <comms@charlesgatepm.mailer.appfolio.us>") == "service_domain"
    assert _is_service_sender("Hotel@contact.hotelchain.com") == "service_domain"
    assert _is_service_sender("deals@promo.retailer.com") == "service_domain"
    assert _is_service_sender("report@news.company.com") == "service_domain"


def test_service_sender_auto_local_skipped():
    """Automated local-part addresses are detected."""
    assert _is_service_sender("billing@avacloud.io") == "auto_sender"
    assert _is_service_sender("invoice@stripe.com") == "auto_sender"
    assert _is_service_sender("receipts@square.com") == "auto_sender"
    # domain pattern wins first for postmaster@mail.*
    assert _is_service_sender("postmaster@mail.example.com") == "service_domain"
    assert _is_service_sender("alerts@company.com") == "auto_sender"


def test_service_sender_normal_not_skipped():
    """Normal email addresses are not flagged."""
    assert _is_service_sender("alice@example.com") is None
    assert _is_service_sender("bob@bigcorp.com") is None
    assert _is_service_sender("support@startup.com") is None
    assert _is_service_sender("hello@company.com") is None
    assert _is_service_sender(None) is None


def test_signal_confidence_stored(db):
    """When the LLM returns confidence on a signal, it is stored in the DB."""
    scoped_url, conn, schema_name = db
    _insert_email_raw(conn)

    response_with_confidence = {
        "persons": [],
        "companies": [],
        "signals": [
            {"type": "deal_mention", "value": "Acme deal", "confidence": 0.92},
        ],
        "summary": "Deal discussion.",
    }
    extract_batch(
        source="email", database_url=scoped_url,
        llm_fn=lambda s, u: response_with_confidence,
    )

    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema_name}, public")
        cur.execute("SELECT confidence FROM interaction_signals")
        row = cur.fetchone()
    assert row[0] == pytest.approx(0.92)
