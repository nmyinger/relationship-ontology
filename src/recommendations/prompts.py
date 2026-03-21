"""
src/recommendations/prompts.py — System prompt and context formatting for the LLM.

Converts a contact intelligence packet into a structured, human-readable
prompt that the LLM can reason over to produce actionable recommendations.
"""

from __future__ import annotations

from src.recommendations.context import _DUNBAR_LABELS

VALID_ACTIONS = ("email", "call", "send_update", "request_intro",
                 "schedule_meeting")

RECOMMENDATION_SYSTEM_PROMPT = """\
You are a relationship intelligence advisor for a real estate private equity \
professional. Your job is to analyze a contact's data and recommend what \
action to take, why now is the right time, and draft the outreach.

You MUST respond with a single JSON object (no other text) with these fields:

{
    "why_now": "<string: 1-3 sentences explaining why now is the right time \
to reach out. Reference specific data points — dates, deals, signals, \
calendar events. Be concrete, not generic.>",
    "suggested_action": "<string: one of: email, call, send_update, \
request_intro, schedule_meeting>",
    "draft_text": "<string: the actual message to send. Match a professional \
but warm tone. Reference specific context from the data. Do NOT use \
placeholder tokens like [NAME], [COMPANY], {{...}} — use the real names \
and details provided.>",
    "confidence": <float: 0.0-1.0 based on how much supporting data you \
have for this recommendation>,
    "source_trace": ["<list of specific data points that drove this \
recommendation>"]
}

Guidelines:
- why_now: Be specific. "You haven't spoken in 34 days" is better than \
"It's been a while." Reference actual signals, dates, and deals.
- suggested_action: Pick the most natural next step given the context.
- draft_text: Write a real, sendable message. Short (2-5 sentences for \
email). Reference shared context naturally.
- confidence: Higher when you have recent interactions, active signals, \
and clear next steps. Lower when data is sparse.
- source_trace: List the 2-5 most important data points you used.
"""


def format_recommendation_prompt(context: dict) -> str:
    """Convert a contact intelligence packet into the user message for the LLM."""
    lines: list[str] = []

    # --- Person header ---
    p = context.get("person", {})
    name = p.get("name", "Unknown")
    lines.append(f"## Contact: {name}")
    parts = []
    if p.get("title"):
        parts.append(f"Title: {p['title']}")
    company_str = p.get("company") or ""
    if p.get("company_type"):
        company_str += f" ({p['company_type']})"
    if company_str.strip():
        parts.append(f"Company: {company_str.strip()}")
    if parts:
        lines.append(" | ".join(parts))
    extras = []
    if p.get("priority_override"):
        extras.append(f"Priority Override: {p['priority_override']}")
    layer = context.get("score", {}).get("dunbar_layer")
    if layer is not None:
        extras.append(f"Dunbar Layer: {_DUNBAR_LABELS.get(layer, str(layer))}")
    if extras:
        lines.append(" | ".join(extras))
    lines.append("")

    # --- Score breakdown ---
    s = context.get("score", {})
    lines.append("## Score Breakdown")
    lines.append(
        f"Total: {s.get('total', 0):.1f} | "
        f"Importance: {s.get('importance', 0):.2f} | "
        f"Urgency: {s.get('urgency', 0):.2f} | "
        f"Rescue: {s.get('rescue', 0):.2f} | "
        f"Deficit: {s.get('deficit', 0):.2f}"
    )
    drivers = s.get("score_drivers", [])
    if drivers:
        lines.append("Drivers:")
        for d in drivers:
            lines.append(f"- {d}")
    lines.append("")

    # --- Interaction state ---
    ix_state = context.get("interaction_state", {})
    recent = ix_state.get("recent_interactions", [])
    if recent:
        lines.append(f"## Recent Interactions (last {len(recent)})")
        for i, ix in enumerate(recent, 1):
            direction = (ix.get("direction") or "unknown").upper()
            itype = (ix.get("type") or "unknown").upper()
            summary = ix.get("summary") or ""
            date = ix.get("date", "")
            lines.append(f"{i}. [{date}] {itype} {direction}: \"{summary}\"")
        lines.append("")

    days = ix_state.get("days_since_contact")
    if days is not None:
        lines.append(f"Days since last contact: {int(days)}")
    if ix_state.get("awaiting_reply_from_user"):
        lines.append("STATUS: Awaiting your reply to their last message")
    lines.append("")

    # --- Signals ---
    signals = context.get("signals", [])
    if signals:
        lines.append("## Active Signals")
        for sig in signals:
            conf = sig.get("confidence")
            conf_str = f" (confidence: {conf:.2f})" if conf is not None else ""
            lines.append(
                f"- {sig.get('type', 'unknown')}: "
                f"\"{sig.get('value', '')}\"" + conf_str
            )
        lines.append("")

    # --- Related deals ---
    deals = context.get("related_deals", [])
    if deals:
        lines.append("## Related Deals")
        for deal in deals:
            parts = [deal.get("name", "")]
            if deal.get("market"):
                parts.append(deal["market"])
            if deal.get("asset_type"):
                parts.append(deal["asset_type"])
            if deal.get("stage"):
                parts.append(f"{deal['stage']} stage")
            lines.append(f"- {', '.join(p for p in parts if p)}")
        lines.append("")

    # --- Upcoming calendar ---
    cal = context.get("upcoming_calendar", [])
    if cal:
        lines.append("## Upcoming Calendar")
        for ev in cal:
            lines.append(f"- {ev.get('date', '')}: \"{ev.get('title', '')}\"")
        lines.append("")

    # --- Raw email content ---
    emails = context.get("recent_email_content", {})
    inbound = emails.get("latest_inbound")
    if inbound:
        lines.append("## Latest Email from Contact")
        lines.append(f"Subject: {inbound.get('subject', '')}")
        lines.append(f"Date: {inbound.get('date', '')}")
        lines.append("---")
        lines.append(inbound.get("body_snippet", ""))
        lines.append("")

    outbound = emails.get("latest_outbound")
    if outbound:
        lines.append("## Your Last Email to Contact")
        lines.append(f"Subject: {outbound.get('subject', '')}")
        lines.append(f"Date: {outbound.get('date', '')}")
        lines.append("---")
        lines.append(outbound.get("body_snippet", ""))
        lines.append("")

    return "\n".join(lines)
