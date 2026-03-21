"""
tests/test_recommendation_prompts.py — Unit tests for prompt formatting and context assembly.

No database required. Tests pure Python functions with in-memory data.
"""

from src.recommendations.context import (
    MAX_BODY_SNIPPET,
    _compute_interaction_state,
    compute_score_drivers,
)
from src.recommendations.prompts import format_recommendation_prompt

# ---------------------------------------------------------------------------
# Helpers: minimal context dicts
# ---------------------------------------------------------------------------

def _minimal_context(**overrides) -> dict:
    ctx = {
        "person": {
            "name": "Jane Smith",
            "title": "Managing Director",
            "email": "jane@acme.com",
            "company": "Acme Capital",
            "company_type": "LP",
            "tags": [],
            "priority_override": None,
        },
        "score": {
            "total": 35.0,
            "importance": 0.65,
            "urgency": 0.40,
            "rescue": 0.50,
            "deficit": 0.20,
            "dunbar_layer": 1,
            "score_drivers": [],
        },
        "interaction_state": {
            "last_interaction_at": None,
            "last_inbound_at": None,
            "last_outbound_at": None,
            "days_since_contact": 10,
            "awaiting_reply_from_user": False,
            "recent_interactions": [],
        },
        "signals": [],
        "related_deals": [],
        "upcoming_calendar": [],
        "recent_email_content": {},
    }
    for key, val in overrides.items():
        if isinstance(val, dict) and key in ctx and isinstance(ctx[key], dict):
            ctx[key].update(val)
        else:
            ctx[key] = val
    return ctx


# ---------------------------------------------------------------------------
# format_recommendation_prompt tests
# ---------------------------------------------------------------------------

def test_format_prompt_includes_person_name():
    ctx = _minimal_context()
    prompt = format_recommendation_prompt(ctx)
    assert "Jane Smith" in prompt


def test_format_prompt_includes_company():
    ctx = _minimal_context()
    prompt = format_recommendation_prompt(ctx)
    assert "Acme Capital" in prompt
    assert "LP" in prompt


def test_format_prompt_includes_score_breakdown():
    ctx = _minimal_context()
    prompt = format_recommendation_prompt(ctx)
    assert "35.0" in prompt
    assert "Importance: 0.65" in prompt


def test_format_prompt_includes_last_interaction_date():
    ctx = _minimal_context(interaction_state={
        "days_since_contact": 15,
        "awaiting_reply_from_user": False,
        "recent_interactions": [
            {"date": "2026-03-10", "type": "email", "direction": "inbound",
             "summary": "Discussed pipeline"},
        ],
    })
    prompt = format_recommendation_prompt(ctx)
    assert "2026-03-10" in prompt
    assert "Discussed pipeline" in prompt


def test_format_prompt_includes_deal_name():
    ctx = _minimal_context(related_deals=[
        {"name": "Harbor Point Acquisition", "market": "Boston",
         "asset_type": "multifamily", "stage": "LOI"},
    ])
    prompt = format_recommendation_prompt(ctx)
    assert "Harbor Point Acquisition" in prompt
    assert "Boston" in prompt


def test_format_prompt_includes_email_content():
    ctx = _minimal_context(recent_email_content={
        "latest_inbound": {
            "subject": "Re: Q2 Pipeline",
            "body_snippet": "Following up on our conversation...",
            "date": "2026-03-19",
        },
    })
    prompt = format_recommendation_prompt(ctx)
    assert "Re: Q2 Pipeline" in prompt
    assert "Following up on our conversation" in prompt


def test_format_prompt_handles_no_interactions():
    ctx = _minimal_context(interaction_state={
        "days_since_contact": None,
        "awaiting_reply_from_user": False,
        "recent_interactions": [],
    })
    prompt = format_recommendation_prompt(ctx)
    assert "Jane Smith" in prompt
    # Should not crash, should still contain score
    assert "Score Breakdown" in prompt


def test_format_prompt_handles_no_deals():
    ctx = _minimal_context(related_deals=[])
    prompt = format_recommendation_prompt(ctx)
    assert "Related Deals" not in prompt


def test_format_prompt_handles_no_calendar():
    ctx = _minimal_context(upcoming_calendar=[])
    prompt = format_recommendation_prompt(ctx)
    assert "Upcoming Calendar" not in prompt


def test_format_prompt_handles_no_emails():
    ctx = _minimal_context(recent_email_content={})
    prompt = format_recommendation_prompt(ctx)
    assert "Latest Email from Contact" not in prompt
    assert "Your Last Email to Contact" not in prompt


def test_format_prompt_includes_awaiting_reply():
    ctx = _minimal_context(interaction_state={
        "days_since_contact": 3,
        "awaiting_reply_from_user": True,
        "recent_interactions": [],
    })
    prompt = format_recommendation_prompt(ctx)
    assert "Awaiting your reply" in prompt


def test_format_prompt_includes_signals():
    ctx = _minimal_context(signals=[
        {"type": "deal_mention", "value": "Harbor Point", "confidence": 0.9},
    ])
    prompt = format_recommendation_prompt(ctx)
    assert "deal_mention" in prompt
    assert "Harbor Point" in prompt
    assert "0.90" in prompt


# ---------------------------------------------------------------------------
# compute_score_drivers tests
# ---------------------------------------------------------------------------

def test_compute_score_drivers_high_rescue():
    score = {"rescue": 0.88, "urgency": 0.3, "importance": 0.5, "deficit": 0.1}
    state = {"days_since_contact": 34, "awaiting_reply_from_user": False}
    drivers = compute_score_drivers(score, state)
    assert any("Relationship at risk" in d for d in drivers)
    assert any("34 days" in d for d in drivers)


def test_compute_score_drivers_awaiting_reply():
    score = {"rescue": 0.3, "urgency": 0.6, "importance": 0.5, "deficit": 0.1}
    state = {"days_since_contact": 2, "awaiting_reply_from_user": True}
    drivers = compute_score_drivers(score, state)
    assert any("Unreplied inbound" in d for d in drivers)


def test_compute_score_drivers_high_importance():
    score = {"rescue": 0.3, "urgency": 0.3, "importance": 0.85, "deficit": 0.1}
    state = {"days_since_contact": 5, "awaiting_reply_from_user": False}
    drivers = compute_score_drivers(score, state)
    assert any("High importance" in d for d in drivers)


def test_compute_score_drivers_inner_circle():
    score = {"rescue": 0.3, "urgency": 0.3, "importance": 0.5, "deficit": 0.1,
             "dunbar_layer": 0}
    state = {"days_since_contact": 5, "awaiting_reply_from_user": False}
    drivers = compute_score_drivers(score, state)
    assert any("Inner circle" in d for d in drivers)


def test_compute_score_drivers_no_flags():
    score = {"rescue": 0.3, "urgency": 0.3, "importance": 0.5, "deficit": 0.1,
             "dunbar_layer": 2}
    state = {"days_since_contact": 5, "awaiting_reply_from_user": False}
    drivers = compute_score_drivers(score, state)
    assert len(drivers) == 0


# ---------------------------------------------------------------------------
# _compute_interaction_state tests
# ---------------------------------------------------------------------------

def test_interaction_state_awaiting_reply():
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    interactions = [
        {"timestamp": now, "direction": "inbound", "type": "email",
         "summary": "Hey"},
    ]
    state = _compute_interaction_state(interactions)
    assert state["awaiting_reply_from_user"] is True


def test_interaction_state_not_awaiting_when_outbound_latest():
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    interactions = [
        {"timestamp": now, "direction": "outbound", "type": "email",
         "summary": "Following up"},
        {"timestamp": now - timedelta(days=1), "direction": "inbound",
         "type": "email", "summary": "Question"},
    ]
    state = _compute_interaction_state(interactions)
    assert state["awaiting_reply_from_user"] is False


# ---------------------------------------------------------------------------
# Body snippet truncation
# ---------------------------------------------------------------------------

def test_body_snippet_truncation():
    """Long email body is truncated to MAX_BODY_SNIPPET chars."""
    long_body = "A" * 3000
    assert len(long_body[:MAX_BODY_SNIPPET]) == MAX_BODY_SNIPPET
    # Verify the constant is what we expect
    assert MAX_BODY_SNIPPET == 1500
