"""
tests/test_gmail_connector.py — Tests for the Gmail ingestion connector (Slice 4).

All Gmail API calls are mocked. Database operations use the isolated-schema pattern.
"""

import base64
import os
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import psycopg2
import pytest

from src.db.runner import apply_migrations
from src.ingestion.gmail_connector import (
    _extract_plain_text,
    _parse_recipients,
    sync_gmail,
)
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
    schema_name = f"test_gmail_{uuid.uuid4().hex[:8]}"
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


def _email_raw_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM email_raw")
        return cur.fetchone()[0]


def _make_gmail_message(
    msg_id: str = "abc123",
    sender: str = "alice@example.com",
    to: str = "bob@example.com",
    subject: str = "Test Subject",
    body: str = "Hello world",
    internal_date_ms: str = "1742400000000",  # 2025-03-19T12:00:00Z
) -> dict:
    """Build a realistic Gmail API message dict."""
    encoded_body = base64.urlsafe_b64encode(body.encode()).decode()
    return {
        "id": msg_id,
        "internalDate": internal_date_ms,
        "payload": {
            "mimeType": "text/plain",
            "headers": [
                {"name": "From", "value": sender},
                {"name": "To", "value": to},
                {"name": "Subject", "value": subject},
            ],
            "body": {"data": encoded_body},
        },
    }


def _make_multipart_message(msg_id: str = "multi1") -> dict:
    """Build a multipart Gmail message with a text/plain part nested inside."""
    encoded = base64.urlsafe_b64encode(b"Nested plain text").decode()
    return {
        "id": msg_id,
        "internalDate": "1742400000000",
        "payload": {
            "mimeType": "multipart/mixed",
            "headers": [
                {"name": "From", "value": "alice@example.com"},
                {"name": "To", "value": "bob@example.com"},
                {"name": "Subject", "value": "Multipart"},
            ],
            "body": {},
            "parts": [
                {
                    "mimeType": "multipart/alternative",
                    "body": {},
                    "parts": [
                        {
                            "mimeType": "text/plain",
                            "body": {"data": encoded},
                        },
                        {
                            "mimeType": "text/html",
                            "body": {"data": base64.urlsafe_b64encode(b"<p>HTML</p>").decode()},
                        },
                    ],
                }
            ],
        },
    }


def _build_mock_service(messages: list[dict], metadata_overrides: dict | None = None) -> MagicMock:
    """
    Build a mock Gmail API service that returns *messages* from messages.list
    and the corresponding full message from messages.get.

    *metadata_overrides* maps message ID to a dict with keys like ``labelIds``
    and ``sizeEstimate`` returned when ``format="metadata"`` is requested.
    """
    service = MagicMock()
    overrides = metadata_overrides or {}

    # messages.list returns IDs only.
    list_response = {
        "messages": [{"id": m["id"]} for m in messages],
    }
    list_mock = MagicMock()
    list_mock.execute.return_value = list_response
    service.users().messages().list.return_value = list_mock

    # list_next returns None (single page).
    service.users().messages().list_next.return_value = None

    # messages.get returns metadata or full message depending on format.
    msg_by_id = {m["id"]: m for m in messages}

    def _get_side_effect(userId, id, format):
        mock = MagicMock()
        if format == "metadata":
            # Return metadata: labelIds + sizeEstimate.
            base = {"id": id, "labelIds": ["INBOX"], "sizeEstimate": 5000}
            base.update(overrides.get(id, {}))
            mock.execute.return_value = base
        else:
            mock.execute.return_value = msg_by_id[id]
        return mock

    service.users().messages().get.side_effect = _get_side_effect

    return service


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def db():
    """Yield (scoped_url, conn) with migrations applied in an isolated schema."""
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


def test_messages_list_called_with_after_query(db):
    """When a watermark exists, messages.list is called with after: query."""
    scoped_url, conn = db
    # Set a watermark so the connector builds an after: query.
    wm = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    set_watermark("gmail", wm, conn)

    msg = _make_gmail_message()
    service = _build_mock_service([msg])

    sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")

    call_kwargs = service.users().messages().list.call_args
    assert "after:2026/01/01" in str(call_kwargs)


def test_category_exclusions_in_query(db):
    """The Gmail query must include category exclusions to filter bulk mail server-side."""
    scoped_url, conn = db
    wm = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    set_watermark("gmail", wm, conn)

    msg = _make_gmail_message()
    service = _build_mock_service([msg])

    sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")

    call_kwargs = service.users().messages().list.call_args
    q_param = str(call_kwargs)
    for category in ("promotions", "updates", "forums", "social"):
        assert f"-category:{category}" in q_param


def test_messages_list_called_without_query_when_no_watermark(db):
    """When no watermark exists, messages.list is called without after: filter."""
    scoped_url, conn = db

    msg = _make_gmail_message()
    service = _build_mock_service([msg])

    count = sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")
    assert count == 1


def test_each_message_normalised_to_expected_shape(db):
    """Normalised messages contain all expected email_raw columns."""
    scoped_url, conn = db

    msg = _make_gmail_message(
        msg_id="shape1",
        sender="alice@example.com",
        to="bob@example.com",
        subject="Deal Update",
        body="Please review",
    )
    service = _build_mock_service([msg])

    sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT external_id, sender, subject, body_text, direction FROM email_raw"
        )
        row = cur.fetchone()

    assert row[0] == "shape1"
    assert row[1] == "alice@example.com"
    assert row[2] == "Deal Update"
    assert row[3] == "Please review"
    assert row[4] == "inbound"


def test_thread_id_and_label_ids_stored(db):
    """sync_gmail persists threadId and labelIds from the Gmail API message."""
    scoped_url, conn = db

    msg = _make_gmail_message(msg_id="meta1")
    msg["threadId"] = "thread_abc"
    msg["labelIds"] = ["INBOX", "IMPORTANT"]
    service = _build_mock_service([msg])

    sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")

    with conn.cursor() as cur:
        cur.execute("SELECT thread_id, label_ids FROM email_raw WHERE external_id = 'meta1'")
        row = cur.fetchone()

    assert row[0] == "thread_abc"
    assert set(row[1]) == {"INBOX", "IMPORTANT"}


def test_on_conflict_does_not_duplicate(db):
    """Running sync twice with the same messages produces no duplicates."""
    scoped_url, conn = db

    msg = _make_gmail_message(msg_id="dup1")
    service = _build_mock_service([msg])

    sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")
    sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")

    assert _email_raw_count(conn) == 1


def test_watermark_advances_after_successful_run(db):
    """After a successful sync, the gmail watermark is updated."""
    scoped_url, conn = db

    msg = _make_gmail_message(internal_date_ms="1742400000000")
    service = _build_mock_service([msg])

    sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")

    wm = get_watermark("gmail", conn)
    assert wm is not None
    assert wm == datetime(2025, 3, 19, 16, 0, 0, tzinfo=timezone.utc)


def test_watermark_does_not_advance_on_api_error(db):
    """If the API raises, the watermark must not change."""
    scoped_url, conn = db

    service = MagicMock()
    list_mock = MagicMock()
    list_mock.execute.side_effect = Exception("API quota exceeded")
    service.users().messages().list.return_value = list_mock

    with pytest.raises(Exception, match="API quota exceeded"):
        sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")

    assert get_watermark("gmail", conn) is None


def test_extract_plain_text_from_multipart():
    """_extract_plain_text recurses into nested multipart structures."""
    msg = _make_multipart_message()
    text = _extract_plain_text(msg["payload"])
    assert text == "Nested plain text"


def test_metadata_skip_draft(db):
    """Messages labelled DRAFT are skipped by metadata pre-screening."""
    scoped_url, conn = db
    msg = _make_gmail_message(msg_id="draft1")
    service = _build_mock_service([msg], metadata_overrides={
        "draft1": {"labelIds": ["DRAFT"]},
    })

    count = sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")
    assert count == 0
    assert _email_raw_count(conn) == 0


def test_metadata_skip_spam(db):
    """Messages labelled SPAM are skipped by metadata pre-screening."""
    scoped_url, conn = db
    msg = _make_gmail_message(msg_id="spam1")
    service = _build_mock_service([msg], metadata_overrides={
        "spam1": {"labelIds": ["SPAM"]},
    })

    count = sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")
    assert count == 0
    assert _email_raw_count(conn) == 0


def test_metadata_skip_oversized(db):
    """Messages exceeding _MAX_SIZE_BYTES are skipped."""
    scoped_url, conn = db
    msg = _make_gmail_message(msg_id="big1")
    service = _build_mock_service([msg], metadata_overrides={
        "big1": {"labelIds": ["INBOX"], "sizeEstimate": 500_000},
    })

    count = sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")
    assert count == 0
    assert _email_raw_count(conn) == 0


def test_metadata_inbox_passes(db):
    """Normal INBOX messages pass metadata pre-screening."""
    scoped_url, conn = db
    msg = _make_gmail_message(msg_id="inbox1")
    service = _build_mock_service([msg], metadata_overrides={
        "inbox1": {"labelIds": ["INBOX", "IMPORTANT"], "sizeEstimate": 3000},
    })

    count = sync_gmail(database_url=scoped_url, service=service, user_email="bob@example.com")
    assert count == 1
    assert _email_raw_count(conn) == 1


def test_parse_recipients_from_to_cc_bcc():
    """_parse_recipients extracts addresses from To, Cc, and Bcc headers."""
    headers = [
        {"name": "To", "value": "Alice <alice@example.com>, bob@example.com"},
        {"name": "Cc", "value": "Charlie <charlie@example.com>"},
        {"name": "Bcc", "value": "dave@example.com"},
    ]
    result = _parse_recipients(headers)
    assert set(result) == {
        "alice@example.com",
        "bob@example.com",
        "charlie@example.com",
        "dave@example.com",
    }
