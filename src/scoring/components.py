"""
src/scoring/components.py — Pure math for all scoring formulas.

No database access. All functions take plain Python scalars/lists.
Fully unit-testable in isolation.

The priority model:
    Priority(i) = I(i) × [0.6·U(i) + 0.4·R(i)] × ResponseModifier + 0.15·D(i)

Components:
    I — Importance (Granovetter tie strength + deal relevance)
    U — Urgency (Hawkes self-exciting process)
    R — Rescue (Weibull survival)
    D — Deficit (Dunbar + Saramaki attention)
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALPHA = 0.5            # Hawkes excitation magnitude
BETA = 0.2             # Hawkes decay rate (1/day, half-life ~3.5 days)
K_DEFAULT = 1.5        # Weibull shape: increasing hazard for regular contacts
K_BURSTY = 0.9         # Weibull shape: decreasing hazard for bursty contacts
B_THRESHOLD = 0.5      # Burstiness cutoff for k selection
W_U = 0.6              # Urgency weight in urgency/rescue split
W_R = 0.4              # Rescue weight in urgency/rescue split
W_D = 0.15             # Attention deficit additive weight
OUTBOUND_WEIGHT = 1.5  # Outbound interactions count 1.5x for frequency
FREQUENCY_WINDOW = 90  # Days for frequency and attention calculations
DUNBAR_LAYERS = [5, 15, 50, 150]               # Cumulative layer sizes
DUNBAR_ATTENTION = [0.40, 0.20, 0.20, 0.20]    # Expected attention per layer

_EPSILON = 1e-9


# ---------------------------------------------------------------------------
# Importance (Granovetter)
# ---------------------------------------------------------------------------

def compute_frequency(interactions: list[dict], p95_count: float) -> float:
    """F = weighted_count / p95. Outbound weighted 1.5x. Capped at 1.0."""
    if not interactions or p95_count <= 0:
        return 0.0
    weighted = sum(
        OUTBOUND_WEIGHT if ix.get("direction") == "outbound" else 1.0
        for ix in interactions
    )
    return min(1.0, weighted / p95_count)


def compute_reciprocity(n_out: int, n_in: int) -> float:
    """R = 1 - |n_out - n_in| / (n_out + n_in + 1). Range [0, 1]."""
    return 1.0 - abs(n_out - n_in) / (n_out + n_in + 1)


def compute_multiplexity(interaction_types: set[str]) -> float:
    """M = distinct channels / 2. Range [0, 1]."""
    return min(1.0, len(interaction_types) / 2.0)


def compute_deal_relevance(signal_values: list[str],
                           active_deals: list[dict]) -> float:
    """Token overlap between person's deal_mention signals and active deal attributes."""
    if not signal_values or not active_deals:
        return 0.0

    signal_tokens = set()
    for val in signal_values:
        if val:
            signal_tokens.update(val.lower().split())

    if not signal_tokens:
        return 0.0

    best = 0.0
    for deal in active_deals:
        deal_tokens = set()
        for field in ("name", "market", "asset_type"):
            val = deal.get(field)
            if val:
                deal_tokens.update(val.lower().split())
        for tag in (deal.get("strategy_tags") or []):
            deal_tokens.update(tag.lower().split())

        if not deal_tokens:
            continue
        overlap = len(signal_tokens & deal_tokens)
        score = overlap / len(deal_tokens)
        best = max(best, score)

    return min(1.0, best)


def compute_importance(F: float, R: float, M: float, D_rel: float) -> float:
    """S(i) = 0.30*F + 0.30*R + 0.15*M + 0.25*D_rel"""
    return 0.30 * F + 0.30 * R + 0.15 * M + 0.25 * D_rel


def apply_priority_override(importance: float,
                            override: str | None) -> float:
    """'high' → max(I, 0.8). 'low' → min(I, 0.2). None → unchanged."""
    if override == "high":
        return max(importance, 0.8)
    if override == "low":
        return min(importance, 0.2)
    return importance


# ---------------------------------------------------------------------------
# Urgency (Hawkes)
# ---------------------------------------------------------------------------

def compute_hawkes_intensity(timestamps_days_ago: list[float],
                             mu: float) -> float:
    """λ(now) = μ + Σ α·exp(-β·t_k). Returns excitation/max(μ, ε), capped at 1.0.

    When there are no events, returns 0.0 (no excitation above baseline).
    """
    if not timestamps_days_ago:
        return 0.0
    mu = max(mu, _EPSILON)
    excitation = sum(
        ALPHA * math.exp(-BETA * t) for t in timestamps_days_ago if t >= 0
    )
    return min(1.0, excitation / mu)


def compute_inbound_spike(window_interactions: list[dict],
                          now: datetime) -> float:
    """0.3 if most recent is inbound with no outbound reply within 3 days."""
    if not window_interactions:
        return 0.0

    # Find most recent interaction
    sorted_ix = sorted(window_interactions,
                       key=lambda x: x["timestamp"], reverse=True)
    most_recent = sorted_ix[0]

    if most_recent.get("direction") != "inbound":
        return 0.0

    mr_ts = most_recent["timestamp"]
    if isinstance(mr_ts, datetime):
        if mr_ts.tzinfo is None:
            mr_ts = mr_ts.replace(tzinfo=timezone.utc)
    else:
        return 0.0

    # Check for outbound reply within 3 days after the inbound
    for ix in sorted_ix:
        if ix.get("direction") == "outbound":
            ix_ts = ix["timestamp"]
            if isinstance(ix_ts, datetime):
                if ix_ts.tzinfo is None:
                    ix_ts = ix_ts.replace(tzinfo=timezone.utc)
                if ix_ts > mr_ts:
                    days_diff = (ix_ts - mr_ts).total_seconds() / 86400
                    if days_diff <= 3:
                        return 0.0

    return 0.3


def compute_urgency(hawkes: float, inbound_spike: float) -> float:
    """U = min(1, hawkes + inbound_spike)"""
    return min(1.0, hawkes + inbound_spike)


# ---------------------------------------------------------------------------
# Rescue (Weibull Survival)
# ---------------------------------------------------------------------------

def compute_burstiness(inter_event_days: list[float]) -> float:
    """B = (σ - μ) / (σ + μ). Returns 0.0 if < 2 values."""
    if len(inter_event_days) < 2:
        return 0.0
    mu = sum(inter_event_days) / len(inter_event_days)
    variance = sum((x - mu) ** 2 for x in inter_event_days) / len(inter_event_days)
    sigma = math.sqrt(variance)
    denom = sigma + mu
    if denom < _EPSILON:
        return 0.0
    return (sigma - mu) / denom


def compute_weibull_rescue(inter_event_days: list[float],
                           t_since_last: float) -> float:
    """R_raw = h(t) × S(t).

    S = exp(-(t/τ)^k), h = (k/τ)·(t/τ)^(k-1).
    τ = 1.26 × median(inter_event_days).
    k chosen by burstiness.
    Returns 0.0 if < 2 inter-event values or t_since_last <= 0.
    """
    if len(inter_event_days) < 2 or t_since_last <= 0:
        return 0.0

    sorted_gaps = sorted(inter_event_days)
    n = len(sorted_gaps)
    median = (sorted_gaps[n // 2] if n % 2 == 1
              else (sorted_gaps[n // 2 - 1] + sorted_gaps[n // 2]) / 2)

    tau = 1.26 * median
    if tau < _EPSILON:
        return 0.0

    B = compute_burstiness(inter_event_days)
    k = K_BURSTY if B > B_THRESHOLD else K_DEFAULT

    t_over_tau = t_since_last / tau
    # S(t) = exp(-(t/τ)^k)
    survival = math.exp(-(t_over_tau ** k))
    # h(t) = (k/τ) · (t/τ)^(k-1)
    hazard = (k / tau) * (t_over_tau ** (k - 1))

    return hazard * survival


def normalize_min_max(values: list[float]) -> list[float]:
    """Min-max normalize. All-equal → all zeros."""
    if not values:
        return []
    lo = min(values)
    hi = max(values)
    span = hi - lo
    if span < _EPSILON:
        return [0.0] * len(values)
    return [(v - lo) / span for v in values]


# ---------------------------------------------------------------------------
# Deficit (Dunbar + Saramaki)
# ---------------------------------------------------------------------------

def assign_dunbar_layer(rank: int) -> int:
    """Rank 1-indexed by importance. Returns layer 0–3."""
    for layer, cutoff in enumerate(DUNBAR_LAYERS):
        if rank <= cutoff:
            return layer
    return len(DUNBAR_LAYERS) - 1


def compute_attention_deficit(layer: int, n_in_layer: int,
                              a_actual: float) -> float:
    """max(0, DUNBAR_ATTENTION[layer]/n_in_layer - a_actual)"""
    if layer >= len(DUNBAR_ATTENTION) or n_in_layer <= 0:
        return 0.0
    expected = DUNBAR_ATTENTION[layer] / n_in_layer
    return max(0.0, expected - a_actual)


# ---------------------------------------------------------------------------
# Response Modifier
# ---------------------------------------------------------------------------

def compute_response_modifier(rec_count: int, acted: bool) -> float:
    """acted → 1.1. count 0-2 not acted → 1.0. count 3+ not acted → 0.7^(n-2)."""
    if rec_count == 0:
        return 1.0
    if acted:
        return 1.1
    if rec_count <= 2:
        return 1.0
    return 0.7 ** (rec_count - 2)


# ---------------------------------------------------------------------------
# Final Score
# ---------------------------------------------------------------------------

def compute_priority(imp: float, U: float, R: float, D: float,
                     modifier: float = 1.0) -> float:
    """imp × (W_U·U + W_R·R) × modifier + W_D·D. Then × 100, clamped [0, 100]."""
    raw = imp * (W_U * U + W_R * R) * modifier + W_D * D
    return max(0.0, min(100.0, raw * 100))
