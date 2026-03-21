"""
src/ingestion/gmail_connector.py — Gmail API ingestion connector (Slice 4).

Authenticates via OAuth2 (pre-existing token.json with refresh token),
fetches email metadata and body text since the stored watermark, and
writes raw records to the email_raw staging table.

No entity extraction. No LLM calls. The staging table is the only output.
"""

import base64
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from email.utils import getaddresses

from src.db.connection import get_connection
from src.ingestion.google_auth import build_google_service
from src.ingestion.watermark import get_watermark, set_watermark

# When no watermark exists (first run), only look back this many days
# instead of fetching the entire mailbox.
_DEFAULT_LOOKBACK_DAYS = 90

# Gmail category tabs to exclude server-side (zero extra API calls).
_CATEGORY_EXCLUSIONS = "-category:promotions -category:updates -category:forums -category:social"

# Labels that indicate non-personal mail — skip before downloading full body.
_SKIP_LABELS = frozenset({
    "TRASH", "SPAM", "DRAFT",
    "CATEGORY_PROMOTIONS", "CATEGORY_UPDATES", "CATEGORY_FORUMS", "CATEGORY_SOCIAL",
})

# Messages larger than this are likely attachments/newsletters — skip.
_MAX_SIZE_BYTES = 250_000

_INSERT_SQL = """
INSERT INTO email_raw
    (external_id, timestamp, direction, sender, recipients, subject, body_text,
     raw_payload, thread_id, label_ids)
VALUES
    (%(external_id)s, %(timestamp)s, %(direction)s, %(sender)s,
     %(recipients)s, %(subject)s, %(body_text)s, %(raw_payload)s,
     %(thread_id)s, %(label_ids)s)
ON CONFLICT (external_id) DO NOTHING
"""


# ---------------------------------------------------------------------------
# Message fetching with pagination
# ---------------------------------------------------------------------------


def _fetch_message_ids(service, user_id: str, query: str | None) -> list[str]:
    """Return all message IDs matching *query*, handling pagination."""
    ids: list[str] = []
    request = service.users().messages().list(
        userId=user_id, q=query or "", maxResults=500
    )
    while request is not None:
        response = request.execute()
        for msg in response.get("messages", []):
            ids.append(msg["id"])
        request = service.users().messages().list_next(request, response)
    return ids


def _fetch_metadata(service, user_id: str, msg_id: str) -> dict:
    """Fetch only message metadata (labels, size) — cheaper than format=full."""
    return service.users().messages().get(
        userId=user_id, id=msg_id, format="metadata"
    ).execute()


def _should_skip_by_metadata(meta: dict) -> str | None:
    """Return a skip reason if the message should be skipped, else None."""
    labels = set(meta.get("labelIds", []))
    overlap = labels & _SKIP_LABELS
    if overlap:
        return f"label:{sorted(overlap)[0]}"

    size = meta.get("sizeEstimate", 0)
    if size > _MAX_SIZE_BYTES:
        return f"oversized:{size}"

    return None


def _fetch_full_message(service, user_id: str, msg_id: str) -> dict:
    """Fetch the full message payload for a single message ID."""
    return service.users().messages().get(
        userId=user_id, id=msg_id, format="full"
    ).execute()


# ---------------------------------------------------------------------------
# MIME body extraction
# ---------------------------------------------------------------------------


def _extract_plain_text(payload: dict) -> str | None:
    """
    Recursively extract text/plain body from a Gmail message payload.

    Returns the decoded text or None if no text/plain part is found.
    """
    mime_type = payload.get("mimeType", "")

    if mime_type == "text/plain":
        data = payload.get("body", {}).get("data")
        if data:
            return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        return None

    # Recurse into multipart structures.
    for part in payload.get("parts", []):
        text = _extract_plain_text(part)
        if text is not None:
            return text

    return None


# ---------------------------------------------------------------------------
# Header helpers
# ---------------------------------------------------------------------------


def _get_header(headers: list[dict], name: str) -> str | None:
    """Return the value of the first header matching *name* (case-insensitive)."""
    name_lower = name.lower()
    for h in headers:
        if h.get("name", "").lower() == name_lower:
            return h.get("value")
    return None


def _parse_timestamp(internal_date_ms: str | None) -> datetime:
    """Convert Gmail's internalDate (epoch millis string) to a tz-aware datetime."""
    if internal_date_ms:
        return datetime.fromtimestamp(int(internal_date_ms) / 1000, tz=timezone.utc)
    return datetime.now(tz=timezone.utc)


def _parse_recipients(headers: list[dict]) -> list[str]:
    """Extract email addresses from To, Cc, and Bcc headers."""
    raw_parts: list[str] = []
    for field in ("To", "Cc", "Bcc"):
        value = _get_header(headers, field)
        if value:
            raw_parts.append(value)

    combined = ", ".join(raw_parts)
    pairs = getaddresses([combined])
    return [addr for _name, addr in pairs if addr]


def _determine_direction(sender: str | None, user_email: str | None) -> str:
    """Return 'outbound' if the user sent the message, else 'inbound'."""
    if not sender or not user_email:
        return "inbound"
    # sender may be "Name <email>" — extract the address part.
    pairs = getaddresses([sender])
    if pairs and pairs[0][1].lower() == user_email.lower():
        return "outbound"
    return "inbound"


# ---------------------------------------------------------------------------
# Normalise + insert
# ---------------------------------------------------------------------------


def _normalise(message: dict, user_email: str | None) -> dict:
    """Convert a full Gmail message dict into params for the INSERT SQL."""
    headers = message.get("payload", {}).get("headers", [])
    sender = _get_header(headers, "From")
    return {
        "external_id": message["id"],
        "timestamp": _parse_timestamp(message.get("internalDate")),
        "direction": _determine_direction(sender, user_email),
        "sender": sender,
        "recipients": _parse_recipients(headers),
        "subject": _get_header(headers, "Subject"),
        "body_text": _extract_plain_text(message.get("payload", {})),
        "raw_payload": json.dumps(message),
        "thread_id": message.get("threadId"),
        "label_ids": message.get("labelIds"),
    }


def _insert_batch(rows: list[dict], conn) -> int:
    """Insert normalised rows into email_raw. Returns count of rows attempted."""
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(_INSERT_SQL, row)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def sync_gmail(
    database_url: str | None = None,
    service=None,
    user_email: str | None = None,
    credentials_path: str | None = None,
) -> int:
    """
    Fetch new Gmail messages since the last watermark and insert into email_raw.

    Parameters
    ----------
    database_url:
        Postgres connection string. Falls back to DATABASE_URL env var.
    service:
        A pre-built Gmail API service (for testing). If None, one is built
        from *credentials_path*.
    user_email:
        The user's email address, used to determine message direction.
        Falls back to USER_EMAIL env var.
    credentials_path:
        Path to the OAuth2 token JSON. Falls back to GMAIL_CREDENTIALS_PATH
        env var.

    Returns
    -------
    int
        Number of messages ingested.
    """
    if service is None:
        service = build_google_service("gmail", "v1", credentials_path)

    if user_email is None:
        user_email = os.environ.get("USER_EMAIL")

    conn = get_connection(database_url)
    try:
        # Build the query from watermark.
        watermark = get_watermark("gmail", conn)
        if watermark:
            after_date = watermark
        else:
            after_date = datetime.now(tz=timezone.utc) - timedelta(days=_DEFAULT_LOOKBACK_DAYS)
        query = f"after:{after_date.strftime('%Y/%m/%d')} {_CATEGORY_EXCLUSIONS}"

        # Fetch message IDs.
        msg_ids = _fetch_message_ids(service, "me", query)
        if not msg_ids:
            return 0

        print(f"Fetching {len(msg_ids)} message(s)...")

        # Fetch full messages and normalise (with metadata pre-screening).
        rows: list[dict] = []
        skipped = 0
        latest_ts: datetime | None = None
        for i, msg_id in enumerate(msg_ids, 1):
            if i % 50 == 0:
                print(f"  {i}/{len(msg_ids)}...")

            # Two-pass: cheap metadata check first, full download only if needed.
            meta = _fetch_metadata(service, "me", msg_id)
            skip_reason = _should_skip_by_metadata(meta)
            if skip_reason:
                skipped += 1
                continue

            full_msg = _fetch_full_message(service, "me", msg_id)
            normalised = _normalise(full_msg, user_email)
            rows.append(normalised)

            msg_ts = normalised["timestamp"]
            if latest_ts is None or msg_ts > latest_ts:
                latest_ts = msg_ts

        if skipped:
            print(f"Skipped {skipped} message(s) by metadata pre-screen.")

        # Insert into staging table.
        count = _insert_batch(rows, conn)

        # Advance watermark.
        if latest_ts:
            set_watermark("gmail", latest_ts, conn)

        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        count = sync_gmail()
        print(f"Ingested {count} Gmail message(s).")
    except (EnvironmentError, FileNotFoundError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
