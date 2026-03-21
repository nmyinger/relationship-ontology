"""
tests/test_scoring_components.py — Pure math tests for scoring components.

No database access. All tests use plain Python data structures.
"""

from datetime import datetime, timezone

from src.scoring.components import (
    apply_priority_override,
    assign_dunbar_layer,
    compute_attention_deficit,
    compute_burstiness,
    compute_deal_relevance,
    compute_frequency,
    compute_hawkes_intensity,
    compute_importance,
    compute_inbound_spike,
    compute_multiplexity,
    compute_priority,
    compute_reciprocity,
    compute_response_modifier,
    compute_urgency,
    compute_weibull_rescue,
    normalize_min_max,
)

# ---------------------------------------------------------------------------
# Frequency
# ---------------------------------------------------------------------------

class TestComputeFrequency:
    def test_empty(self):
        assert compute_frequency([], 10.0) == 0.0

    def test_outbound_weighted(self):
        ix = [{"direction": "outbound"}, {"direction": "inbound"}]
        # outbound=1.5, inbound=1.0 → 2.5 / 5.0 = 0.5
        assert compute_frequency(ix, 5.0) == 0.5

    def test_capped_at_one(self):
        ix = [{"direction": "outbound"}] * 20
        assert compute_frequency(ix, 5.0) == 1.0

    def test_zero_p95(self):
        assert compute_frequency([{"direction": "inbound"}], 0.0) == 0.0


# ---------------------------------------------------------------------------
# Reciprocity
# ---------------------------------------------------------------------------

class TestComputeReciprocity:
    def test_balanced(self):
        # |5-5|/(5+5+1) = 0/11 = 0 → 1.0
        assert compute_reciprocity(5, 5) == 1.0

    def test_all_outbound(self):
        # |5-0|/(5+0+1) = 5/6 ≈ 0.833 → ~0.167
        r = compute_reciprocity(5, 0)
        assert abs(r - (1 - 5 / 6)) < 0.01

    def test_both_zero(self):
        # |0-0|/(0+0+1) = 0 → 1.0
        assert compute_reciprocity(0, 0) == 1.0


# ---------------------------------------------------------------------------
# Multiplexity
# ---------------------------------------------------------------------------

class TestComputeMultiplexity:
    def test_both_channels(self):
        assert compute_multiplexity({"email", "calendar"}) == 1.0

    def test_single_channel(self):
        assert compute_multiplexity({"email"}) == 0.5

    def test_empty(self):
        assert compute_multiplexity(set()) == 0.0


# ---------------------------------------------------------------------------
# Deal Relevance
# ---------------------------------------------------------------------------

class TestComputeDealRelevance:
    def test_no_deals(self):
        assert compute_deal_relevance(["downtown tower"], []) == 0.0

    def test_no_signals(self):
        deals = [{"name": "Downtown Tower", "market": "NYC",
                  "asset_type": "office", "strategy_tags": []}]
        assert compute_deal_relevance([], deals) == 0.0

    def test_overlap(self):
        deals = [{"name": "Downtown Tower", "market": "NYC",
                  "asset_type": "office", "strategy_tags": ["value-add"]}]
        signals = ["downtown tower deal"]
        score = compute_deal_relevance(signals, deals)
        assert score > 0.0

    def test_no_overlap(self):
        deals = [{"name": "Sunset Apartments", "market": "LA",
                  "asset_type": "multifamily", "strategy_tags": []}]
        signals = ["completely unrelated terms xyz"]
        assert compute_deal_relevance(signals, deals) == 0.0


# ---------------------------------------------------------------------------
# Importance
# ---------------------------------------------------------------------------

class TestComputeImportance:
    def test_weights_sum(self):
        # All inputs 1.0 → 0.30+0.30+0.15+0.25 = 1.0
        assert abs(compute_importance(1.0, 1.0, 1.0, 1.0) - 1.0) < 1e-9

    def test_all_zero(self):
        assert compute_importance(0.0, 0.0, 0.0, 0.0) == 0.0


# ---------------------------------------------------------------------------
# Priority Override
# ---------------------------------------------------------------------------

class TestApplyPriorityOverride:
    def test_high_floors(self):
        assert apply_priority_override(0.3, "high") == 0.8

    def test_high_already_above(self):
        assert apply_priority_override(0.9, "high") == 0.9

    def test_low_caps(self):
        assert apply_priority_override(0.5, "low") == 0.2

    def test_low_already_below(self):
        assert apply_priority_override(0.1, "low") == 0.1

    def test_none_unchanged(self):
        assert apply_priority_override(0.5, None) == 0.5


# ---------------------------------------------------------------------------
# Hawkes Intensity
# ---------------------------------------------------------------------------

class TestComputeHawkesIntensity:
    def test_no_events(self):
        assert compute_hawkes_intensity([], 0.5) == 0.0

    def test_recent_event_higher(self):
        recent = compute_hawkes_intensity([1.0], 0.5)
        old = compute_hawkes_intensity([30.0], 0.5)
        assert recent > old

    def test_capped_at_one(self):
        # Many very recent events
        ts = [0.1] * 50
        assert compute_hawkes_intensity(ts, 0.01) == 1.0

    def test_zero_mu(self):
        result = compute_hawkes_intensity([1.0], 0.0)
        assert result > 0.0
        assert result <= 1.0


# ---------------------------------------------------------------------------
# Inbound Spike
# ---------------------------------------------------------------------------

class TestComputeInboundSpike:
    def test_unreplied_inbound(self):
        now = datetime(2025, 6, 1, tzinfo=timezone.utc)
        ix = [{"direction": "inbound",
               "timestamp": datetime(2025, 5, 30, tzinfo=timezone.utc)}]
        assert compute_inbound_spike(ix, now) == 0.3

    def test_replied_inbound(self):
        now = datetime(2025, 6, 1, tzinfo=timezone.utc)
        ix = [
            {"direction": "inbound",
             "timestamp": datetime(2025, 5, 30, tzinfo=timezone.utc)},
            {"direction": "outbound",
             "timestamp": datetime(2025, 5, 31, tzinfo=timezone.utc)},
        ]
        # Most recent is outbound (May 31 > May 30), so no spike
        assert compute_inbound_spike(ix, now) == 0.0

    def test_empty(self):
        now = datetime(2025, 6, 1, tzinfo=timezone.utc)
        assert compute_inbound_spike([], now) == 0.0


# ---------------------------------------------------------------------------
# Urgency
# ---------------------------------------------------------------------------

class TestComputeUrgency:
    def test_sum_capped(self):
        assert compute_urgency(0.8, 0.3) == 1.0

    def test_basic(self):
        assert compute_urgency(0.5, 0.0) == 0.5


# ---------------------------------------------------------------------------
# Burstiness
# ---------------------------------------------------------------------------

class TestComputeBurstiness:
    def test_regular_intervals(self):
        # All same → σ=0, μ=7 → B = (0-7)/(0+7) = -1.0
        b = compute_burstiness([7.0, 7.0, 7.0, 7.0])
        assert b < 0

    def test_mixed(self):
        # Mix of short and long → high variance → positive burstiness
        b = compute_burstiness([1.0, 1.0, 30.0, 1.0, 30.0])
        assert b > 0

    def test_insufficient(self):
        assert compute_burstiness([5.0]) == 0.0
        assert compute_burstiness([]) == 0.0


# ---------------------------------------------------------------------------
# Weibull Rescue
# ---------------------------------------------------------------------------

class TestComputeWeibullRescue:
    def test_no_history(self):
        assert compute_weibull_rescue([], 10.0) == 0.0
        assert compute_weibull_rescue([5.0], 10.0) == 0.0

    def test_moderate_gap(self):
        # Regular contact every 7 days, now 10 days since last
        gaps = [7.0, 7.0, 7.0, 7.0]
        r = compute_weibull_rescue(gaps, 10.0)
        assert r > 0.0

    def test_zero_since_last(self):
        assert compute_weibull_rescue([7.0, 7.0], 0.0) == 0.0

    def test_very_long_gap_decays(self):
        gaps = [7.0, 7.0, 7.0, 7.0]
        r_moderate = compute_weibull_rescue(gaps, 10.0)
        r_extreme = compute_weibull_rescue(gaps, 200.0)
        # At extreme gaps, survival S(t) drops so product decays
        assert r_extreme < r_moderate


# ---------------------------------------------------------------------------
# Normalize Min-Max
# ---------------------------------------------------------------------------

class TestNormalizeMinMax:
    def test_uniform(self):
        assert normalize_min_max([5.0, 5.0, 5.0]) == [0.0, 0.0, 0.0]

    def test_range(self):
        result = normalize_min_max([0.0, 5.0, 10.0])
        assert abs(result[0] - 0.0) < 1e-9
        assert abs(result[1] - 0.5) < 1e-9
        assert abs(result[2] - 1.0) < 1e-9

    def test_empty(self):
        assert normalize_min_max([]) == []


# ---------------------------------------------------------------------------
# Dunbar Layer
# ---------------------------------------------------------------------------

class TestAssignDunbarLayer:
    def test_top_5(self):
        assert assign_dunbar_layer(1) == 0
        assert assign_dunbar_layer(5) == 0

    def test_layer_1(self):
        assert assign_dunbar_layer(6) == 1
        assert assign_dunbar_layer(15) == 1

    def test_layer_2(self):
        assert assign_dunbar_layer(16) == 2
        assert assign_dunbar_layer(50) == 2

    def test_layer_3(self):
        assert assign_dunbar_layer(51) == 3
        assert assign_dunbar_layer(150) == 3

    def test_beyond_150(self):
        assert assign_dunbar_layer(200) == 3


# ---------------------------------------------------------------------------
# Attention Deficit
# ---------------------------------------------------------------------------

class TestComputeAttentionDeficit:
    def test_underserved(self):
        # Layer 0, 5 people, actual attention = 0.01
        # Expected: 0.40/5 = 0.08. Deficit = 0.08 - 0.01 = 0.07
        d = compute_attention_deficit(0, 5, 0.01)
        assert abs(d - 0.07) < 1e-6

    def test_overserved(self):
        d = compute_attention_deficit(0, 5, 0.20)
        assert d == 0.0

    def test_zero_in_layer(self):
        assert compute_attention_deficit(0, 0, 0.0) == 0.0


# ---------------------------------------------------------------------------
# Response Modifier
# ---------------------------------------------------------------------------

class TestComputeResponseModifier:
    def test_acted(self):
        assert compute_response_modifier(3, True) == 1.1

    def test_never_recommended(self):
        assert compute_response_modifier(0, False) == 1.0

    def test_twice_not_acted(self):
        assert compute_response_modifier(2, False) == 1.0

    def test_three_times_not_acted(self):
        # 0.7^(3-2) = 0.7
        assert abs(compute_response_modifier(3, False) - 0.7) < 1e-9

    def test_five_times_not_acted(self):
        # 0.7^(5-2) = 0.343
        assert abs(compute_response_modifier(5, False) - 0.343) < 1e-9


# ---------------------------------------------------------------------------
# Final Priority Score
# ---------------------------------------------------------------------------

class TestComputePriority:
    def test_formula(self):
        # I=0.5, U=0.8, R=0.3, D=0.2, modifier=1.0
        # 0.5 * (0.6*0.8 + 0.4*0.3) * 1.0 + 0.15*0.2 = 0.5*(0.48+0.12)+0.03
        # = 0.5*0.6 + 0.03 = 0.33 → ×100 = 33.0
        score = compute_priority(0.5, 0.8, 0.3, 0.2)
        assert abs(score - 33.0) < 0.01

    def test_all_zero(self):
        assert compute_priority(0.0, 0.0, 0.0, 0.0) == 0.0

    def test_clamped_at_100(self):
        score = compute_priority(1.0, 1.0, 1.0, 1.0, modifier=2.0)
        assert score <= 100.0

    def test_modifier_applied(self):
        base = compute_priority(0.5, 0.5, 0.5, 0.0, modifier=1.0)
        boosted = compute_priority(0.5, 0.5, 0.5, 0.0, modifier=1.5)
        assert boosted > base
