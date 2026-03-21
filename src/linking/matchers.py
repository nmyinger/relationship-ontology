"""
src/linking/matchers.py — Pure matching logic for entity linking (Slice 6b).

No database access. All functions are deterministic given the same inputs.
"""

from __future__ import annotations

import re


def _tokenize(text: str) -> set[str]:
    """Lowercase, split on non-alphanumeric, drop tokens <= 2 chars."""
    return {
        tok
        for tok in re.split(r"[^a-z0-9]+", text.lower())
        if len(tok) > 2
    }


def _deal_tokens(deal: dict) -> set[str]:
    """Build a token set from deal name, market, asset_type, strategy_tags."""
    parts = [
        deal.get("name") or "",
        deal.get("market") or "",
        deal.get("asset_type") or "",
    ]
    tags = deal.get("strategy_tags")
    if tags:
        if isinstance(tags, list):
            parts.extend(tags)
        else:
            parts.append(str(tags))
    return _tokenize(" ".join(parts))


def match_deal_to_signal(
    signal_value: str, deals: list[dict]
) -> tuple[str | None, float]:
    """Match a deal_mention signal_value to active deals by token overlap.

    Returns (deal_id, confidence) or (None, 0.0).
    Confidence = overlap_tokens / max(signal_tokens, deal_tokens).
    Minimum 2 overlapping tokens required to produce any match.
    """
    signal_tokens = _tokenize(signal_value)
    if len(signal_tokens) < 1:
        return None, 0.0

    best_id: str | None = None
    best_confidence = 0.0

    for deal in deals:
        dtokens = _deal_tokens(deal)
        if not dtokens:
            continue
        overlap = signal_tokens & dtokens
        if len(overlap) < 2:
            continue
        denom = max(len(signal_tokens), len(dtokens))
        confidence = len(overlap) / denom
        if confidence > best_confidence:
            best_confidence = confidence
            best_id = str(deal["deal_id"])

    return best_id, best_confidence


def compute_company_confidence(mention_type: str) -> float:
    """Confidence for company links based on how they were inferred."""
    return {
        "direct": 1.0,
        "inferred_from_person": 0.8,
        "signal": 0.6,
    }.get(mention_type, 0.5)
