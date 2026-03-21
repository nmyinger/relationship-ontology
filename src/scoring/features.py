"""
src/scoring/features.py — Batch SQL queries for the scoring engine.

All queries run once for all persons (no per-person round-trips).
Returns dicts keyed by email or person_id.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def fetch_all_persons(conn) -> list[dict]:
    """All non-internal persons with company_type and priority_override."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.person_id, p.email, p.full_name,
                   p.priority_override, c.type AS company_type
            FROM persons p
            LEFT JOIN companies c ON p.company_id = c.company_id
            WHERE p.is_internal = FALSE
        """)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_interaction_history(conn) -> dict[str, list[dict]]:
    """email → sorted list of {timestamp, direction, type}.

    Single query joining persons to interactions via ANY(participants).
    Returns all-time history (scorer splits into window vs full).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.email, i.timestamp, i.direction, i.type
            FROM persons p
            JOIN interactions i ON p.email = ANY(i.participants)
            WHERE p.is_internal = FALSE
            ORDER BY p.email, i.timestamp
        """)
        result: dict[str, list[dict]] = {}
        for email, ts, direction, itype in cur.fetchall():
            if email not in result:
                result[email] = []
            result[email].append({
                "timestamp": ts,
                "direction": direction,
                "type": itype,
            })
    return result


def fetch_deal_signals(conn, window_days: int = 90) -> dict[str, list[str]]:
    """email → list of signal_value for signal_type='deal_mention' in window."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.email, s.signal_value
            FROM persons p
            JOIN interactions i ON p.email = ANY(i.participants)
            JOIN interaction_signals s ON i.interaction_id = s.interaction_id
            WHERE p.is_internal = FALSE
              AND s.signal_type = 'deal_mention'
              AND i.timestamp >= %s
        """, (cutoff,))
        result: dict[str, list[str]] = {}
        for email, val in cur.fetchall():
            if email not in result:
                result[email] = []
            if val:
                result[email].append(val)
    return result


def fetch_active_deals(conn) -> list[dict]:
    """Active deals with market, asset_type, strategy_tags."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT deal_id, name, market, asset_type, strategy_tags
            FROM deals
            WHERE status = 'active'
        """)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_recommendation_history(conn,
                                 window_days: int = 30) -> dict[str, dict]:
    """person_id → {rec_count: int, acted: bool}.

    Checks if outbound interaction followed within 3 days of each recommendation.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.person_id, r.date, p.email
            FROM recommendations r
            JOIN persons p ON r.person_id = p.person_id
            WHERE r.date >= %s
            ORDER BY r.person_id, r.date
        """, (cutoff.date(),))
        recs = cur.fetchall()

    if not recs:
        return {}

    # Get all outbound interactions in the window for follow-up detection
    with conn.cursor() as cur:
        cur.execute("""
            SELECT i.participants, i.timestamp
            FROM interactions i
            WHERE i.direction = 'outbound'
              AND i.timestamp >= %s
            ORDER BY i.timestamp
        """, (cutoff,))
        outbound = cur.fetchall()

    result: dict[str, dict] = {}
    for person_id, rec_date, email in recs:
        pid = str(person_id)
        if pid not in result:
            result[pid] = {"rec_count": 0, "acted": False}
        result[pid]["rec_count"] += 1

        if not result[pid]["acted"]:
            # Check if outbound interaction to this person within 3 days
            rec_dt = datetime.combine(rec_date, datetime.min.time(),
                                      tzinfo=timezone.utc)
            for participants, ts in outbound:
                if participants and email in participants:
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    diff = (ts - rec_dt).total_seconds() / 86400
                    if 0 <= diff <= 3:
                        result[pid]["acted"] = True
                        break

    return result
