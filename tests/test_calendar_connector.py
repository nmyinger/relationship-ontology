"""
tests/test_calendar_connector.py — Tests for the Calendar ingestion connector (Slice 5).

All Calendar API calls are mocked. Database operations use the isolated-schema pattern.
"""

import os
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import psycopg2
import pytest

from src.db.runner import apply_migrations
from src.ingestion.calendar_connector import sync_calendar
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
    schema_name = f"test_cal_{uuid.uuid4().hex[:8]}"
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


def _calendar_raw_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM calendar_raw")
        return cur.fetchone()[0]


def _make_calendar_event(
    event_id: str = "evt_001",
    summary: str = "Deal Review",
    start_dt: str = "2026-03-20T10:00:00-04:00",
    attendees: list[dict] | None = None,
) -> dict:
    """Build a minimal Calendar API event dict."""
    event = {
        "id": event_id,
        "summary": summary,
        "start": {"dateTime": start_dt},
        "end": {"dateTime": start_dt},
    }
    if attendees is not None:
        event["attendees"] = attendees
    return event


def _make_allday_event(
    event_id: str = "allday_001",
    summary: str = "Offsite",
    start_date: str = "2026-03-25",
    attendees: list[dict] | None = None,
) -> dict:
    """Build an all-day Calendar event (start.date, no dateTime)."""
    event = {
        "id": event_id,
        "summary": summary,
        "start": {"date": start_date},
        "end": {"date": start_date},
    }
    if attendees is not None:
        event["attendees"] = attendees
    return event


def _build_mock_service(events: list[dict]) -> MagicMock:
    """Build a mock Calendar API service that returns *events* from events().list()."""
    service = MagicMock()

    list_response = {"items": events}
    list_mock = MagicMock()
    list_mock.execute.return_value = list_response
    service.events().list.return_value = list_mock

    # Single page — no nextPageToken.
    service.events().list_next.return_value = None

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

_USER = "me@mycompany.com"
_EXTERNAL = "partner@otherfirm.com"


def test_events_list_called_with_time_min_from_watermark(db):
    """When a watermark exists, events().list() uses it as timeMin."""
    scoped_url, conn = db
    wm = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    set_watermark("calendar", wm, conn)

    event = _make_calendar_event(
        attendees=[{"email": _USER}, {"email": _EXTERNAL}]
    )
    service = _build_mock_service([event])

    sync_calendar(database_url=scoped_url, service=service, user_email=_USER)

    call_kwargs = service.events().list.call_args
    time_min_arg = str(call_kwargs)
    assert "2026-03-01" in time_min_arg


def test_events_list_called_without_time_min_when_no_watermark(db):
    """When no watermark exists, the connector falls back to 90-day lookback."""
    scoped_url, conn = db

    event = _make_calendar_event(
        attendees=[{"email": _USER}, {"email": _EXTERNAL}]
    )
    service = _build_mock_service([event])

    count = sync_calendar(
        database_url=scoped_url, service=service, user_email=_USER
    )
    assert count == 1

    # Verify timeMin was passed (should be ~90 days ago).
    call_kwargs = service.events().list.call_args
    assert "timeMin" in str(call_kwargs)


def test_each_event_normalised_to_expected_shape(db):
    """Normalised events contain all expected calendar_raw columns."""
    scoped_url, conn = db

    event = _make_calendar_event(
        event_id="shape1",
        summary="LP Meeting",
        start_dt="2026-03-20T14:00:00+00:00",
        attendees=[{"email": _USER}, {"email": _EXTERNAL}],
    )
    service = _build_mock_service([event])

    sync_calendar(database_url=scoped_url, service=service, user_email=_USER)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT external_id, title, direction, participants FROM calendar_raw"
        )
        row = cur.fetchone()

    assert row[0] == "shape1"
    assert row[1] == "LP Meeting"
    assert row[2] == "meeting"
    assert _EXTERNAL in row[3]
    assert _USER not in row[3]


def test_internal_only_events_are_skipped(db):
    """Events where all attendees are the user are excluded."""
    scoped_url, conn = db

    event = _make_calendar_event(
        event_id="internal1",
        attendees=[{"email": _USER}],
    )
    service = _build_mock_service([event])

    count = sync_calendar(
        database_url=scoped_url, service=service, user_email=_USER
    )
    assert count == 0
    assert _calendar_raw_count(conn) == 0


def test_event_with_no_attendees_is_skipped(db):
    """Events with no attendees key (solo blocks) are excluded."""
    scoped_url, conn = db

    event = _make_calendar_event(event_id="solo1", attendees=None)
    service = _build_mock_service([event])

    count = sync_calendar(
        database_url=scoped_url, service=service, user_email=_USER
    )
    assert count == 0
    assert _calendar_raw_count(conn) == 0


def test_on_conflict_does_not_duplicate(db):
    """Running sync twice with the same event produces no duplicates."""
    scoped_url, conn = db

    event = _make_calendar_event(
        event_id="dup1",
        attendees=[{"email": _USER}, {"email": _EXTERNAL}],
    )
    service = _build_mock_service([event])

    sync_calendar(database_url=scoped_url, service=service, user_email=_USER)
    sync_calendar(database_url=scoped_url, service=service, user_email=_USER)

    assert _calendar_raw_count(conn) == 1


def test_watermark_advances_after_successful_run(db):
    """After a successful sync, the calendar watermark is updated."""
    scoped_url, conn = db

    event = _make_calendar_event(
        event_id="wm1",
        start_dt="2026-03-20T14:00:00+00:00",
        attendees=[{"email": _USER}, {"email": _EXTERNAL}],
    )
    service = _build_mock_service([event])

    sync_calendar(database_url=scoped_url, service=service, user_email=_USER)

    wm = get_watermark("calendar", conn)
    assert wm is not None
    assert wm == datetime(2026, 3, 20, 14, 0, 0, tzinfo=timezone.utc)


def test_watermark_does_not_advance_on_api_error(db):
    """If the API raises, the watermark must not change."""
    scoped_url, conn = db

    service = MagicMock()
    list_mock = MagicMock()
    list_mock.execute.side_effect = Exception("Calendar API error")
    service.events().list.return_value = list_mock

    with pytest.raises(Exception, match="Calendar API error"):
        sync_calendar(
            database_url=scoped_url, service=service, user_email=_USER
        )

    assert get_watermark("calendar", conn) is None


def test_all_day_event_uses_date_not_datetime(db):
    """All-day events are normalised to midnight UTC, not skipped."""
    scoped_url, conn = db

    event = _make_allday_event(
        event_id="allday1",
        summary="Team Offsite",
        start_date="2026-03-25",
        attendees=[{"email": _USER}, {"email": _EXTERNAL}],
    )
    service = _build_mock_service([event])

    count = sync_calendar(
        database_url=scoped_url, service=service, user_email=_USER
    )
    assert count == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT timestamp AT TIME ZONE 'UTC' "
            "FROM calendar_raw WHERE external_id = 'allday1'"
        )
        ts_naive = cur.fetchone()[0]

    # All-day event normalised to midnight UTC.
    ts_utc = ts_naive.replace(tzinfo=timezone.utc)
    expected = datetime(2026, 3, 25, 0, 0, 0, tzinfo=timezone.utc)
    assert ts_utc == expected
