"""
src/extraction/prompts.py — LLM prompt templates for entity extraction.

No logic here — just the system prompt and formatting functions for email
and calendar event payloads.
"""

EXTRACTION_SYSTEM_PROMPT = """\
You are an entity-extraction assistant for a deal-flow intelligence system.

Given a communication record (email or calendar event), extract structured data
as JSON with exactly this shape:

{
  "persons": [
    {"name": "Full Name", "email": "addr@example.com",
     "company": "Company Inc", "title": "VP of Sales"}
  ],
  "companies": [
    {"name": "Company Inc", "type": "investor|operator|broker|lender|other"}
  ],
  "signals": [
    {"type": "signal_type", "value": "brief description", "confidence": 0.85}
  ],
  "summary": "One-sentence summary of the interaction."
}

Rules:
- Extract ALL persons mentioned (sender, recipients, and anyone named in the body).
- For each person, include email if available, company and title if inferrable.
- Extract companies mentioned in the text, even if not tied to a specific person.
- Company type should be one of: investor, operator, broker, lender, other.
- Signal types include: deal_mention, follow_up_request, meeting_scheduled,
  introduction, investment_interest, property_mention, closing_activity,
  relationship_building, information_request, status_update.
- confidence is a float 0.0–1.0 indicating how clearly the signal is present in the text.
- Only include signals that are clearly present — do not speculate.
- The summary should capture the key business intent in one sentence.
- Return ONLY valid JSON, no markdown fences or extra text.
- For each person, ONLY assign a company if the email explicitly states they work there.
  Do NOT guess company associations from context or email domain alone.
- If an email has more than 5 recipients, focus on the sender and anyone
  mentioned by name in the body. List remaining recipients with email only
  (no inferred name/company/title).
- For forwarded emails, extract entities from the most recent message only.
  Ignore the forwarded chain unless it contains new names not in the top message.
- Each person entry must have a unique email. Do not create duplicate entries.
"""


def format_email_prompt(
    sender: str,
    recipients: list[str],
    subject: str,
    body_text: str,
    direction: str,
) -> str:
    """Format an email record as a user message for the extraction LLM."""
    recip_str = ", ".join(recipients) if recipients else "(none)"
    body_preview = (body_text or "")[:3000]
    return (
        f"Email ({direction})\n"
        f"From: {sender}\n"
        f"To: {recip_str}\n"
        f"Subject: {subject}\n"
        f"---\n"
        f"{body_preview}"
    )


def format_calendar_prompt(
    title: str,
    participants: list[str],
    timestamp: str,
) -> str:
    """Format a calendar event as a user message for the extraction LLM."""
    parts_str = ", ".join(participants) if participants else "(none)"
    return (
        f"Calendar Event\n"
        f"Title: {title}\n"
        f"When: {timestamp}\n"
        f"Participants: {parts_str}"
    )
