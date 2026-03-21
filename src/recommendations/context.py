"""
src/recommendations/context.py — Assembles the contact intelligence packet per person.

Provides SQL queries to fetch enrichment data and a builder that combines
scores, interactions, signals, deals, calendar events, and raw emails into
a structured context dict for the LLM prompt.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

MAX_BODY_SNIPPET = 1500


# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

def fetch_top_scored_contacts(conn, today, limit: int = 10) -> list[dict]:
    """Top N contacts by total_score for a given scored_date.

    JOINs contact_scores + persons + companies.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cs.person_id, cs.importance, cs.urgency, cs.rescue,
                   cs.deficit, cs.total_score, cs.dunbar_layer,
                   p.full_name, p.email, p.title, p.priority_override, p.tags,
                   c.name AS company_name, c.type AS company_type
            FROM contact_scores cs
            JOIN persons p ON cs.person_id = p.person_id
            LEFT JOIN companies c ON p.company_id = c.company_id
            WHERE cs.scored_date = %s
            ORDER BY cs.total_score DESC
            LIMIT %s
        """, (today, limit))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_recent_interactions(conn, email: str, limit: int = 5) -> list[dict]:
    """Last N interactions for a person (by participant email), with summaries."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT i.timestamp, i.type, i.direction, i.summary
            FROM interactions i
            WHERE %s = ANY(i.participants)
            ORDER BY i.timestamp DESC
            LIMIT %s
        """, (email, limit))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_recent_signals(conn, email: str, window_days: int = 90) -> list[dict]:
    """Recent interaction_signals for a person within window."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.signal_type, s.signal_value, s.confidence
            FROM interaction_signals s
            JOIN interactions i ON s.interaction_id = i.interaction_id
            WHERE %s = ANY(i.participants)
              AND i.timestamp >= %s
            ORDER BY i.timestamp DESC
        """, (email, cutoff))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_upcoming_calendar(conn, email: str,
                            days_ahead: int = 14) -> list[dict]:
    """Calendar events in the next N days involving this person."""
    now = datetime.now(timezone.utc)
    horizon = now + timedelta(days=days_ahead)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT title, timestamp
            FROM calendar_raw
            WHERE %s = ANY(participants)
              AND timestamp >= %s
              AND timestamp <= %s
            ORDER BY timestamp
        """, (email, now, horizon))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_recent_emails(conn, email: str, limit: int = 2) -> dict:
    """Latest inbound + outbound email_raw for a person.

    Returns dict with optional 'latest_inbound' and 'latest_outbound' keys.
    """
    result = {}
    # Latest inbound (from this person)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT subject, body_text, timestamp
            FROM email_raw
            WHERE sender ILIKE %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (f"%{email}%",))
        row = cur.fetchone()
        if row:
            body = row[1] or ""
            result["latest_inbound"] = {
                "subject": row[0] or "",
                "body_snippet": body[:MAX_BODY_SNIPPET],
                "date": row[2].strftime("%Y-%m-%d") if row[2] else "",
            }

    # Latest outbound (to this person)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT subject, body_text, timestamp
            FROM email_raw
            WHERE %s = ANY(recipients)
              AND direction = 'outbound'
            ORDER BY timestamp DESC
            LIMIT 1
        """, (email,))
        row = cur.fetchone()
        if row:
            body = row[1] or ""
            result["latest_outbound"] = {
                "subject": row[0] or "",
                "body_snippet": body[:MAX_BODY_SNIPPET],
                "date": row[2].strftime("%Y-%m-%d") if row[2] else "",
            }

    return result


# ---------------------------------------------------------------------------
# Score drivers (pure Python)
# ---------------------------------------------------------------------------

_DUNBAR_LABELS = {
    0: "Inner circle (top 5)",
    1: "Close layer (top 15)",
    2: "Active layer (top 50)",
    3: "Extended network (top 150)",
}


def compute_score_drivers(score: dict, interaction_state: dict) -> list[str]:
    """Translate component values into human-readable driver strings."""
    drivers: list[str] = []

    rescue = score.get("rescue", 0)
    if rescue > 0.6:
        days = interaction_state.get("days_since_contact", "?")
        drivers.append(
            f"Relationship at risk \u2014 no contact in {days} days "
            f"(rescue: {rescue:.2f})"
        )

    if interaction_state.get("awaiting_reply_from_user"):
        drivers.append("Unreplied inbound email (urgency spike)")

    urgency = score.get("urgency", 0)
    if urgency > 0.5 and not interaction_state.get("awaiting_reply_from_user"):
        drivers.append("Time-sensitive: recent activity spike")

    importance = score.get("importance", 0)
    if importance > 0.7:
        drivers.append(f"High importance contact (importance: {importance:.2f})")

    deficit = score.get("deficit", 0)
    if deficit > 0.3:
        drivers.append("Underserved relative to Dunbar tier")

    layer = score.get("dunbar_layer")
    if layer == 0:
        drivers.append("Inner circle (top 5)")

    return drivers


# ---------------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------------

def _compute_interaction_state(interactions: list[dict]) -> dict:
    """Derive interaction state summary from recent interactions."""
    now = datetime.now(timezone.utc)
    state: dict = {
        "last_interaction_at": None,
        "last_inbound_at": None,
        "last_outbound_at": None,
        "days_since_contact": None,
        "awaiting_reply_from_user": False,
        "recent_interactions": [],
    }

    if not interactions:
        return state

    # interactions are already sorted DESC by timestamp
    state["last_interaction_at"] = interactions[0]["timestamp"]
    ts = interactions[0]["timestamp"]
    if ts:
        if hasattr(ts, "tzinfo") and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        state["days_since_contact"] = max(
            0, (now - ts).total_seconds() / 86400
        )

    for ix in interactions:
        direction = ix.get("direction")
        if direction == "inbound" and state["last_inbound_at"] is None:
            state["last_inbound_at"] = ix["timestamp"]
        elif direction == "outbound" and state["last_outbound_at"] is None:
            state["last_outbound_at"] = ix["timestamp"]

    # awaiting reply: last inbound is more recent than last outbound
    if state["last_inbound_at"] and (
        state["last_outbound_at"] is None
        or state["last_inbound_at"] > state["last_outbound_at"]
    ):
        state["awaiting_reply_from_user"] = True

    for ix in interactions:
        state["recent_interactions"].append({
            "date": (ix["timestamp"].strftime("%Y-%m-%d")
                     if ix.get("timestamp") else ""),
            "type": ix.get("type", "unknown"),
            "direction": ix.get("direction", "unknown"),
            "summary": ix.get("summary", ""),
        })

    return state


def _match_deal(signals: list[dict], active_deals: list[dict]) -> list[dict]:
    """Match person's deal_mention signals to active deals by token overlap."""
    mentions = [
        s["signal_value"].lower()
        for s in signals
        if s.get("signal_type") == "deal_mention" and s.get("signal_value")
    ]
    if not mentions or not active_deals:
        return []

    matched = []
    for deal in active_deals:
        deal_tokens = set()
        for field in ("name", "market", "asset_type"):
            val = deal.get(field)
            if val:
                deal_tokens.update(val.lower().split())
        for mention in mentions:
            mention_tokens = set(mention.lower().split())
            if mention_tokens & deal_tokens:
                matched.append({
                    "deal_id": str(deal["deal_id"]),
                    "name": deal.get("name", ""),
                    "market": deal.get("market", ""),
                    "asset_type": deal.get("asset_type", ""),
                    "stage": deal.get("stage", ""),
                })
                break
    return matched


def build_contact_context(
    scored_contact: dict,
    interactions: list[dict],
    signals: list[dict],
    active_deals: list[dict],
    calendar_events: list[dict],
    recent_emails: dict,
) -> dict:
    """Assemble the full contact intelligence packet for one person."""
    interaction_state = _compute_interaction_state(interactions)

    score = {
        "total": scored_contact.get("total_score", 0),
        "importance": scored_contact.get("importance", 0),
        "urgency": scored_contact.get("urgency", 0),
        "rescue": scored_contact.get("rescue", 0),
        "deficit": scored_contact.get("deficit", 0),
        "dunbar_layer": scored_contact.get("dunbar_layer"),
    }
    score["score_drivers"] = compute_score_drivers(score, interaction_state)

    related_deals = _match_deal(signals, active_deals)

    return {
        "person": {
            "name": scored_contact.get("full_name", ""),
            "title": scored_contact.get("title"),
            "email": scored_contact.get("email", ""),
            "company": scored_contact.get("company_name"),
            "company_type": scored_contact.get("company_type"),
            "tags": scored_contact.get("tags") or [],
            "priority_override": scored_contact.get("priority_override"),
        },
        "score": score,
        "interaction_state": interaction_state,
        "signals": [
            {
                "type": s.get("signal_type", ""),
                "value": s.get("signal_value", ""),
                "confidence": s.get("confidence"),
            }
            for s in signals
        ],
        "related_deals": related_deals,
        "upcoming_calendar": [
            {
                "title": ev.get("title", ""),
                "date": (ev["timestamp"].strftime("%Y-%m-%d")
                         if ev.get("timestamp") else ""),
            }
            for ev in calendar_events
        ],
        "recent_email_content": recent_emails,
    }
