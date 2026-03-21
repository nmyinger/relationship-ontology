"""
src/delivery/pdf_renderer.py — Renders the daily brief PDF.

Reads recommendations from the database, assembles template data,
renders HTML via Jinja2, and converts to PDF via WeasyPrint.

Entry point: render_pdf(database_url, today, output_dir) -> str | None
Also runnable as: python -m src.delivery.pdf_renderer
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jinja2
import psycopg2
import weasyprint

from src.config import load_config

_TEMPLATE_DIR = Path(__file__).parent / "templates"

ACTION_LABELS = {
    "email": "Send an email",
    "call": "Give them a call",
    "send_update": "Send a deal update",
    "request_intro": "Request an introduction",
    "schedule_meeting": "Schedule a meeting",
}


def _priority_label(score: float) -> str:
    """Human-friendly priority label from a numeric score."""
    if score >= 35:
        return "High Priority"
    if score >= 25:
        return "Medium Priority"
    return "Priority"


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_brief_data(conn, today) -> dict:
    """Fetch all data needed for the daily brief PDF.

    Returns a dict with keys: recommendations, meetings, deal_matches, date.
    """
    scored_date = today.date() if hasattr(today, "date") else today

    recommendations = _fetch_recommendations(conn, scored_date)
    meetings = _fetch_upcoming_meetings(conn, today)
    deal_matches = _fetch_deal_matches(conn, scored_date)

    return {
        "date": scored_date,
        "recommendations": recommendations,
        "meetings": meetings,
        "deal_matches": deal_matches,
    }


def _fetch_recommendations(conn, scored_date) -> list[dict]:
    """Recommendations joined with persons, companies, and deals."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.priority_score, r.why_now, r.suggested_action, r.draft_text,
                   p.full_name, p.title,
                   c.name AS company_name, c.type AS company_type,
                   d.name AS deal_name
            FROM recommendations r
            JOIN persons p ON r.person_id = p.person_id
            LEFT JOIN companies c ON p.company_id = c.company_id
            LEFT JOIN deals d ON r.related_deal_id = d.deal_id
            WHERE r.date = %s
            ORDER BY r.priority_score DESC
        """, (scored_date,))
        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    for row in rows:
        row["priority_label"] = _priority_label(row["priority_score"])
        row["action_label"] = ACTION_LABELS.get(
            row["suggested_action"], row["suggested_action"],
        )

    return rows


def _fetch_upcoming_meetings(conn, today, days_ahead: int = 3) -> list[dict]:
    """Calendar events in the next N days with external participants."""
    now = today if isinstance(today, datetime) else datetime.combine(
        today, datetime.min.time(), tzinfo=timezone.utc,
    )
    cutoff = now + timedelta(days=days_ahead)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT cr.title, cr.timestamp, cr.participants
            FROM calendar_raw cr
            WHERE cr.timestamp >= %s
              AND cr.timestamp < %s
            ORDER BY cr.timestamp
        """, (now, cutoff))
        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    meetings = []
    for row in rows:
        start = row["timestamp"]
        date_str = start.strftime("%A, %B %-d") if start else ""
        time_str = start.strftime("%-I:%M %p") if start else ""

        participants = row.get("participants") or []
        attendee_str = ", ".join(participants[:4])
        if len(participants) > 4:
            attendee_str += f" +{len(participants) - 4} more"

        # Look up recent interaction context for attendees
        context = _meeting_context(conn, participants)

        meetings.append({
            "title": row.get("title") or "Untitled meeting",
            "date_display": f"{date_str} at {time_str}" if date_str else "",
            "attendees": attendee_str,
            "context": context,
        })

    return meetings


def _meeting_context(conn, participant_emails: list[str]) -> str:
    """Get a brief context line for meeting attendees from recent interactions."""
    if not participant_emails:
        return ""

    with conn.cursor() as cur:
        cur.execute("""
            SELECT i.summary
            FROM interactions i
            WHERE i.participants && %s
            ORDER BY i.timestamp DESC
            LIMIT 1
        """, (participant_emails,))
        row = cur.fetchone()

    if row and row[0]:
        return f"Recent: {row[0]}"
    return ""


def _fetch_deal_matches(conn, scored_date) -> list[dict]:
    """Active deals with their recommended contacts for today."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.deal_id, d.name, d.market, d.asset_type, d.status
            FROM deals d
            WHERE d.status = 'active'
            ORDER BY d.name
        """)
        deal_cols = [desc[0] for desc in cur.description]
        deals = [dict(zip(deal_cols, row)) for row in cur.fetchall()]

    if not deals:
        return []

    # Get today's recommendations that have a related_deal_id
    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.related_deal_id, p.full_name
            FROM recommendations r
            JOIN persons p ON r.person_id = p.person_id
            WHERE r.date = %s AND r.related_deal_id IS NOT NULL
            ORDER BY r.priority_score DESC
        """, (scored_date,))
        deal_contacts: dict[str, list[str]] = {}
        for row in cur.fetchall():
            did = str(row[0])
            deal_contacts.setdefault(did, []).append(row[1])

    result = []
    for deal in deals:
        did = str(deal["deal_id"])
        contacts = deal_contacts.get(did, [])
        result.append({
            "name": deal["name"],
            "market": deal.get("market") or "",
            "asset_type": deal.get("asset_type") or "",
            "status": deal.get("status") or "",
            "contacts": contacts,
        })

    return result


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_html(brief_data: dict) -> str:
    """Render the daily brief HTML from template data."""
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(_TEMPLATE_DIR)),
        autoescape=True,
    )
    template = env.get_template("daily_brief.html")
    css_path = _TEMPLATE_DIR / "print.css"
    css_text = css_path.read_text()

    scored_date = brief_data["date"]
    if hasattr(scored_date, "strftime"):
        date_display = scored_date.strftime("%A, %B %-d, %Y")
    else:
        date_display = str(scored_date)

    return template.render(
        css=css_text,
        date_display=date_display,
        rec_count=len(brief_data["recommendations"]),
        recommendations=brief_data["recommendations"],
        meetings=brief_data["meetings"],
        deal_matches=brief_data["deal_matches"],
    )


def render_pdf_bytes(html: str) -> bytes:
    """Convert rendered HTML to PDF bytes via WeasyPrint."""
    doc = weasyprint.HTML(string=html)
    return doc.write_pdf()


def render_pdf(
    database_url: str | None = None,
    today: datetime | None = None,
    output_dir: str = "output",
) -> str | None:
    """Generate the daily brief PDF and write to disk.

    Returns the output file path, or None if no recommendations exist.
    """
    if database_url is None:
        config = load_config()
        database_url = config["DATABASE_URL"]

    if today is None:
        today = datetime.now(timezone.utc)

    conn = psycopg2.connect(database_url)
    conn.autocommit = False

    try:
        brief_data = fetch_brief_data(conn, today)
    finally:
        conn.close()

    if not brief_data["recommendations"]:
        print("No recommendations for today. Skipping PDF.")
        return None

    html = render_html(brief_data)
    pdf_bytes = render_pdf_bytes(html)

    scored_date = brief_data["date"]
    os.makedirs(output_dir, exist_ok=True)
    filename = f"daily_brief_{scored_date}.pdf"
    out_path = os.path.join(output_dir, filename)

    with open(out_path, "wb") as f:
        f.write(pdf_bytes)

    print(f"PDF written to {out_path} ({len(pdf_bytes):,} bytes)")
    return out_path


if __name__ == "__main__":
    print("=== PDF Renderer ===")
    path = render_pdf()
    if path:
        print(f"Done: {path}")
    else:
        print("No PDF generated (no recommendations for today).")
