"""
src/linking/linker.py — Entity linking orchestrator (Slice 6b).

Reads extraction output (persons, interactions, interaction_signals) and resolves
references into canonical graph edges in junction tables:
  - person_interactions
  - interaction_companies
  - interaction_deals

All matching is deterministic. No LLM calls.
"""

from __future__ import annotations

import sys

from src.db.connection import get_connection
from src.linking.matchers import compute_company_confidence, match_deal_to_signal


def _fetch_active_deals(conn) -> list[dict]:
    """Fetch all active deals for matching."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT deal_id, name, market, asset_type, strategy_tags
            FROM deals
            WHERE status = 'active'
        """)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _fetch_interactions(conn, mode: str) -> list[dict]:
    """Fetch interactions to link.

    incremental: only interactions not yet in person_interactions.
    full: all interactions.
    """
    if mode == "full":
        sql = """
            SELECT interaction_id, type, direction, participants
            FROM interactions
        """
    else:
        sql = """
            SELECT i.interaction_id, i.type, i.direction, i.participants
            FROM interactions i
            WHERE NOT EXISTS (
                SELECT 1 FROM person_interactions pi
                WHERE pi.interaction_id = i.interaction_id
            )
        """
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _fetch_email_to_person(conn) -> dict[str, dict]:
    """email (lowered) → {person_id, company_id}."""
    with conn.cursor() as cur:
        cur.execute("SELECT person_id, email, company_id FROM persons")
        result = {}
        for pid, email, cid in cur.fetchall():
            result[email.lower()] = {"person_id": str(pid), "company_id": str(cid) if cid else None}
        return result


def _fetch_deal_signals(conn, interaction_ids: list[str]) -> dict[str, list[dict]]:
    """interaction_id → list of deal_mention signals."""
    if not interaction_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT signal_id, interaction_id, signal_value
            FROM interaction_signals
            WHERE signal_type = 'deal_mention'
              AND interaction_id = ANY(%s::uuid[])
        """, (interaction_ids,))
        result: dict[str, list[dict]] = {}
        for sid, iid, val in cur.fetchall():
            key = str(iid)
            if key not in result:
                result[key] = []
            result[key].append({"signal_id": str(sid), "signal_value": val})
        return result


def _link_persons(conn, interaction: dict, email_to_person: dict) -> int:
    """Write person_interactions rows for one interaction. Returns count."""
    participants = interaction.get("participants") or []
    direction = interaction.get("direction") or ""
    itype = interaction.get("type") or ""
    iid = str(interaction["interaction_id"])
    count = 0

    for idx, email in enumerate(participants):
        person = email_to_person.get(email.lower())
        if not person:
            continue
        # Determine role
        if itype == "meeting" or direction == "meeting":
            role = "attendee"
        elif idx == 0 and direction == "inbound":
            role = "sender"
        elif idx == 0 and direction == "outbound":
            role = "sender"
        else:
            role = "recipient"

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO person_interactions (person_id, interaction_id, role)
                VALUES (%s, %s, %s)
                ON CONFLICT (person_id, interaction_id) DO NOTHING
            """, (person["person_id"], iid, role))
            count += cur.rowcount
    return count


def _link_companies(conn, interaction: dict, email_to_person: dict) -> int:
    """Write interaction_companies rows from person company associations. Returns count."""
    participants = interaction.get("participants") or []
    iid = str(interaction["interaction_id"])
    seen_companies: set[str] = set()
    count = 0

    for email in participants:
        person = email_to_person.get(email.lower())
        if not person or not person["company_id"]:
            continue
        cid = person["company_id"]
        if cid in seen_companies:
            continue
        seen_companies.add(cid)

        mention_type = "inferred_from_person"
        confidence = compute_company_confidence(mention_type)

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO interaction_companies
                    (interaction_id, company_id, mention_type, confidence)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (interaction_id, company_id)
                DO UPDATE SET confidence = EXCLUDED.confidence
            """, (iid, cid, mention_type, confidence))
            count += cur.rowcount
    return count


def _link_deals(
    conn, interaction: dict, signals: list[dict], active_deals: list[dict]
) -> int:
    """Write interaction_deals rows from deal_mention signals. Returns count."""
    iid = str(interaction["interaction_id"])
    count = 0

    for sig in signals:
        deal_id, confidence = match_deal_to_signal(sig["signal_value"], active_deals)
        if deal_id is None:
            continue
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO interaction_deals
                    (interaction_id, deal_id, mention_type, confidence, signal_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (interaction_id, deal_id)
                DO UPDATE SET confidence = EXCLUDED.confidence,
                              signal_id  = EXCLUDED.signal_id
            """, (iid, deal_id, "deal_mention_signal", confidence, sig["signal_id"]))
            count += cur.rowcount
    return count


def link_entities(database_url: str | None = None, mode: str = "incremental") -> dict:
    """Resolve extracted references into canonical graph edges.

    Parameters
    ----------
    database_url:
        Postgres connection string. Falls back to DATABASE_URL env var.
    mode:
        'incremental' (default) — only process unlinked interactions.
        'full' — truncate junction tables and re-link everything.

    Returns
    -------
    dict with counts: person_interactions, interaction_companies, interaction_deals.
    """
    conn = get_connection(database_url)
    try:
        if mode == "full":
            with conn.cursor() as cur:
                cur.execute("DELETE FROM interaction_deals")
                cur.execute("DELETE FROM interaction_companies")
                cur.execute("DELETE FROM person_interactions")
            conn.commit()

        active_deals = _fetch_active_deals(conn)
        interactions = _fetch_interactions(conn, mode)
        email_to_person = _fetch_email_to_person(conn)

        interaction_ids = [str(i["interaction_id"]) for i in interactions]
        deal_signals = _fetch_deal_signals(conn, interaction_ids)

        totals = {"person_interactions": 0, "interaction_companies": 0, "interaction_deals": 0}

        for idx, interaction in enumerate(interactions):
            iid = str(interaction["interaction_id"])

            totals["person_interactions"] += _link_persons(conn, interaction, email_to_person)
            totals["interaction_companies"] += _link_companies(conn, interaction, email_to_person)

            signals = deal_signals.get(iid, [])
            if signals:
                totals["interaction_deals"] += _link_deals(conn, interaction, signals, active_deals)

            # Commit in batches of 100
            if (idx + 1) % 100 == 0:
                conn.commit()

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return totals


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "incremental"
    result = link_entities(mode=mode)
    print(f"Linked: {result}")
