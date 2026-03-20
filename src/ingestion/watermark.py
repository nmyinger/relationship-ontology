"""
src/ingestion/watermark.py — Per-source ingestion watermark (Slice 4).

Stores and retrieves the last-synced timestamp for each ingestion source
(e.g. "gmail", "calendar") in the ingestion_watermarks table.

No external API access. No LLM calls.
"""

from datetime import datetime, timezone


def get_watermark(source: str, conn) -> datetime | None:
    """
    Return the last-synced timestamp for *source*, or None if never synced.

    The returned datetime is always timezone-aware (UTC).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_synced AT TIME ZONE 'UTC' FROM ingestion_watermarks WHERE source = %s",
            (source,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    # AT TIME ZONE 'UTC' returns a naive datetime in UTC; re-attach tzinfo.
    return row[0].replace(tzinfo=timezone.utc)


def set_watermark(source: str, ts: datetime, conn) -> None:
    """
    Upsert the watermark for *source* to *ts*.

    *ts* must be timezone-aware. Raises ValueError otherwise.
    """
    if ts.tzinfo is None:
        raise ValueError("Watermark timestamp must be timezone-aware")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion_watermarks (source, last_synced)
            VALUES (%s, %s)
            ON CONFLICT (source) DO UPDATE SET last_synced = EXCLUDED.last_synced
            """,
            (source, ts),
        )
    conn.commit()
