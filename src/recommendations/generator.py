"""
src/recommendations/generator.py — Orchestrator: fetch → context → LLM → write.

Entry point: generate_recommendations(database_url, today, limit, llm_fn) -> int
Also runnable as: python -m src.recommendations.generator
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import psycopg2

from src.config import load_config
from src.llm.client import call_llm
from src.recommendations.context import (
    build_contact_context,
    fetch_recent_emails,
    fetch_recent_interactions,
    fetch_recent_signals,
    fetch_top_scored_contacts,
    fetch_upcoming_calendar,
)
from src.recommendations.prompts import (
    RECOMMENDATION_SYSTEM_PROMPT,
    VALID_ACTIONS,
    format_recommendation_prompt,
)
from src.scoring.features import fetch_active_deals

logger = logging.getLogger(__name__)

_MAX_RECOMMENDATIONS = 10


def _find_best_deal_id(related_deals: list[dict]) -> str | None:
    """Return the deal_id of the first matched deal, or None."""
    if related_deals:
        return related_deals[0].get("deal_id")
    return None


def generate_recommendations(
    database_url: str | None = None,
    today: datetime | None = None,
    limit: int = _MAX_RECOMMENDATIONS,
    llm_fn=None,
) -> int:
    """Generate recommendations for today's top scored contacts.

    Parameters
    ----------
    database_url : str, optional
        Override DB connection string.
    today : datetime, optional
        Override the scoring date. Defaults to now (UTC).
    limit : int
        Max recommendations to generate (default 10).
    llm_fn : callable, optional
        Override LLM call for testing. Signature: (system, user) -> dict.

    Returns
    -------
    int
        Number of recommendations written.
    """
    if database_url is None:
        config = load_config()
        database_url = config["DATABASE_URL"]

    if today is None:
        today = datetime.now(timezone.utc)

    if llm_fn is None:
        llm_fn = call_llm

    # Enforce max
    limit = min(limit, _MAX_RECOMMENDATIONS)

    conn = psycopg2.connect(database_url)
    conn.autocommit = False

    try:
        scored_date = today.date() if hasattr(today, "date") else today
        scored_contacts = fetch_top_scored_contacts(conn, scored_date, limit)

        if not scored_contacts:
            print("No scores found for today. Run `make score` first.")
            return 0

        active_deals = fetch_active_deals(conn)

        recommendations = []
        for sc in scored_contacts:
            email = sc["email"]
            person_id = sc["person_id"]
            try:
                interactions = fetch_recent_interactions(conn, email)
                signals = fetch_recent_signals(conn, email)
                calendar = fetch_upcoming_calendar(conn, email)
                emails = fetch_recent_emails(conn, email)

                context = build_contact_context(
                    scored_contact=sc,
                    interactions=interactions,
                    signals=signals,
                    active_deals=active_deals,
                    calendar_events=calendar,
                    recent_emails=emails,
                )

                user_msg = format_recommendation_prompt(context)
                result = llm_fn(RECOMMENDATION_SYSTEM_PROMPT, user_msg)

                # Validate and extract fields
                why_now = result.get("why_now", "")
                suggested_action = result.get("suggested_action", "email")
                if suggested_action not in VALID_ACTIONS:
                    suggested_action = "email"
                draft_text = result.get("draft_text", "")
                confidence = result.get("confidence", 0.5)
                source_trace = result.get("source_trace", [])

                related_deal_id = _find_best_deal_id(
                    context.get("related_deals", []),
                )

                recommendations.append({
                    "person_id": person_id,
                    "related_deal_id": related_deal_id,
                    "priority_score": sc["total_score"],
                    "why_now": why_now,
                    "suggested_action": suggested_action,
                    "draft_text": draft_text,
                    "confidence": confidence,
                    "source_trace": source_trace,
                })

                print(f"  [{sc['full_name']}] {suggested_action}: "
                      f"{why_now[:80]}...")

            except Exception as exc:
                logger.warning("Error generating recommendation for %s: %s",
                               email, exc)
                print(f"  [SKIP] {sc.get('full_name', email)}: {exc}")
                continue

        if not recommendations:
            return 0

        # Idempotent: delete existing recommendations for today
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM recommendations WHERE date = %s",
                (scored_date,),
            )

            for rec in recommendations:
                cur.execute("""
                    INSERT INTO recommendations
                        (date, person_id, related_deal_id, priority_score,
                         why_now, suggested_action, draft_text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    scored_date,
                    rec["person_id"],
                    rec["related_deal_id"],
                    rec["priority_score"],
                    rec["why_now"],
                    rec["suggested_action"],
                    rec["draft_text"],
                ))

        conn.commit()

        print(f"\nWrote {len(recommendations)} recommendations for "
              f"{scored_date}.")
        return len(recommendations)

    finally:
        conn.close()


if __name__ == "__main__":
    print("=== Recommendation Generator ===")
    count = generate_recommendations()
    print(f"\nGenerated {count} recommendations.")
