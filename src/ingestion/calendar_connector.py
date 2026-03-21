"""
src/ingestion/calendar_connector.py — Google Calendar API ingestion connector (Slice 5).

Authenticates via OAuth2 (pre-existing token with refresh token),
fetches calendar events within a rolling window, filters out internal-only
events, normalises each event into a flat dict, and writes raw records to
the calendar_raw staging table.

No entity extraction. No LLM calls. The staging table is the only output.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

from src.db.connection import get_connection
from src.ingestion.google_auth import build_google_service
from src.ingestion.watermark import get_watermark, set_watermark

_INSERT_SQL = """
INSERT INTO calendar_raw
    (external_id, timestamp, title, participants, direction, raw_payload)
VALUES
    (%(external_id)s, %(timestamp)s, %(title)s, %(participants)s,
     %(direction)s, %(raw_payload)s)
ON CONFLICT (external_id) DO NOTHING
"""

_DEFAULT_LOOKBACK_DAYS = 90
_DEFAULT_LOOKAHEAD_DAYS = 30


# ---------------------------------------------------------------------------
# Event fetching with pagination
# ---------------------------------------------------------------------------


def _fetch_events(service, time_min: str, time_max: str) -> list[dict]:
    """Return all events between time_min and time_max, handling pagination."""
    events: list[dict] = []
    request = service.events().list(
        calendarId="primary",
        timeMin=time_min,
        timeMax=time_max,
        singleEvents=True,
        orderBy="startTime",
        maxResults=250,
    )
    while request is not None:
        response = request.execute()
        for event in response.get("items", []):
            events.append(event)
        request = service.events().list_next(request, response)
    return events


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


def _is_external_event(event: dict, user_email: str | None) -> bool:
    """
    Return True if the event has at least one external attendee.

    Events with no attendees key or where all attendees match user_email
    are considered internal and excluded.
    """
    attendees = event.get("attendees")
    if not attendees:
        return False

    if not user_email:
        # Cannot determine internal vs external; include the event.
        return True

    user_lower = user_email.lower()
    for attendee in attendees:
        email = attendee.get("email", "")
        if email.lower() != user_lower:
            return True

    return False


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def _parse_event_timestamp(event: dict) -> datetime:
    """
    Parse the event start time to a tz-aware UTC datetime.

    Handles both `start.dateTime` (RFC3339) and `start.date` (all-day).
    """
    start = event.get("start", {})
    date_time_str = start.get("dateTime")
    if date_time_str:
        return datetime.fromisoformat(date_time_str).astimezone(timezone.utc)

    # All-day event: start.date is "YYYY-MM-DD". Normalise to midnight UTC.
    date_str = start.get("date")
    if date_str:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    return datetime.now(tz=timezone.utc)


def _parse_participants(event: dict, user_email: str | None) -> list[str]:
    """Return external attendee emails, excluding the user's own address."""
    attendees = event.get("attendees", [])
    user_lower = (user_email or "").lower()
    return [
        a.get("email", "")
        for a in attendees
        if a.get("email", "").lower() != user_lower and a.get("email")
    ]


def _normalise(event: dict, user_email: str | None) -> dict:
    """Convert a Calendar API event dict into params for the INSERT SQL."""
    return {
        "external_id": event["id"],
        "timestamp": _parse_event_timestamp(event),
        "title": event.get("summary"),
        "participants": _parse_participants(event, user_email),
        "direction": "meeting",
        "raw_payload": json.dumps(event),
    }


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------


def _insert_batch(rows: list[dict], conn) -> int:
    """Insert normalised rows into calendar_raw. Returns count of rows attempted."""
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(_INSERT_SQL, row)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def sync_calendar(
    database_url: str | None = None,
    service=None,
    user_email: str | None = None,
    credentials_path: str | None = None,
) -> int:
    """
    Fetch calendar events since the last watermark and insert into calendar_raw.

    Parameters
    ----------
    database_url:
        Postgres connection string. Falls back to DATABASE_URL env var.
    service:
        A pre-built Calendar API service (for testing). If None, one is built
        from *credentials_path*.
    user_email:
        The user's email address, used to filter internal events.
        Falls back to USER_EMAIL env var.
    credentials_path:
        Path to the OAuth2 token JSON. Falls back to
        GOOGLE_CALENDAR_CREDENTIALS_PATH env var.

    Returns
    -------
    int
        Number of events ingested.
    """
    if service is None:
        service = build_google_service("calendar", "v3", credentials_path)

    if user_email is None:
        user_email = os.environ.get("USER_EMAIL")

    conn = get_connection(database_url)
    try:
        # Determine time window.
        now = datetime.now(tz=timezone.utc)
        watermark = get_watermark("calendar", conn)
        time_min = watermark or (now - timedelta(days=_DEFAULT_LOOKBACK_DAYS))
        time_max = now + timedelta(days=_DEFAULT_LOOKAHEAD_DAYS)

        # Fetch events.
        events = _fetch_events(
            service,
            time_min.isoformat(),
            time_max.isoformat(),
        )

        if not events:
            return 0

        # Filter and normalise.
        rows: list[dict] = []
        latest_ts: datetime | None = None
        for event in events:
            if event.get("status") == "cancelled":
                continue
            if not _is_external_event(event, user_email):
                continue

            normalised = _normalise(event, user_email)
            rows.append(normalised)

            event_ts = normalised["timestamp"]
            if latest_ts is None or event_ts > latest_ts:
                latest_ts = event_ts

        if not rows:
            return 0

        # Insert into staging table.
        count = _insert_batch(rows, conn)

        # Advance watermark.
        if latest_ts:
            set_watermark("calendar", latest_ts, conn)

        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        count = sync_calendar()
        print(f"Ingested {count} calendar event(s).")
    except (EnvironmentError, FileNotFoundError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
