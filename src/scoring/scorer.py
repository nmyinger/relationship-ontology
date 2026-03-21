"""
src/scoring/scorer.py — Orchestrator: fetch data, compute scores, write results.

Entry point: score_all(database_url, today) -> int
Also runnable as: python -m src.scoring.scorer
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg2

from src.config import load_config
from src.scoring.components import (
    FREQUENCY_WINDOW,
    OUTBOUND_WEIGHT,
    apply_priority_override,
    assign_dunbar_layer,
    compute_attention_deficit,
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
from src.scoring.features import (
    fetch_active_deals,
    fetch_all_persons,
    fetch_deal_signals,
    fetch_interaction_history,
    fetch_recommendation_history,
)


def _split_window(interactions: list[dict],
                  now: datetime) -> list[dict]:
    """Return interactions within the FREQUENCY_WINDOW."""
    cutoff = now - timedelta(days=FREQUENCY_WINDOW)
    return [ix for ix in interactions if ix["timestamp"] >= cutoff]


def _inter_event_days(interactions: list[dict]) -> list[float]:
    """Compute gaps in days between consecutive interactions."""
    if len(interactions) < 2:
        return []
    gaps = []
    for i in range(1, len(interactions)):
        t0 = interactions[i - 1]["timestamp"]
        t1 = interactions[i]["timestamp"]
        diff = (t1 - t0).total_seconds() / 86400
        if diff > 0:
            gaps.append(diff)
    return gaps


def _compute_p95_weighted_count(
    history: dict[str, list[dict]], now: datetime,
) -> float:
    """Compute p95 of weighted interaction counts across all persons."""
    cutoff = now - timedelta(days=FREQUENCY_WINDOW)
    counts = []
    for interactions in history.values():
        window = [ix for ix in interactions if ix["timestamp"] >= cutoff]
        weighted = sum(
            OUTBOUND_WEIGHT if ix.get("direction") == "outbound" else 1.0
            for ix in window
        )
        counts.append(weighted)
    if not counts:
        return 1.0
    sorted_counts = sorted(counts)
    idx = int(len(sorted_counts) * 0.95)
    idx = min(idx, len(sorted_counts) - 1)
    return max(sorted_counts[idx], 1.0)


def _ensure_tz(ts):
    """Ensure timestamp has UTC tzinfo."""
    if isinstance(ts, datetime) and ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def score_all(database_url: str | None = None,
              today: datetime | None = None) -> int:
    """Score all non-internal persons and write to contact_scores.

    Returns the number of persons scored.
    """
    if database_url is None:
        config = load_config()
        database_url = config["DATABASE_URL"]

    if today is None:
        today = datetime.now(timezone.utc)

    conn = psycopg2.connect(database_url)
    conn.autocommit = False

    try:
        # --- Fetch all data (4-5 queries) ---
        persons = fetch_all_persons(conn)
        if not persons:
            return 0

        history = fetch_interaction_history(conn)
        deal_signals = fetch_deal_signals(conn)
        active_deals = fetch_active_deals(conn)
        rec_history = fetch_recommendation_history(conn)

        p95 = _compute_p95_weighted_count(history, today)

        # --- Per-person: compute I, U, R_raw ---
        scored = []
        rescue_raw_values = []

        for person in persons:
            email = person["email"]
            interactions = history.get(email, [])

            # Ensure all timestamps have tzinfo
            for ix in interactions:
                ix["timestamp"] = _ensure_tz(ix["timestamp"])

            window_ix = _split_window(interactions, today)

            # Importance
            F = compute_frequency(window_ix, p95)
            n_out = sum(1 for ix in interactions
                        if ix.get("direction") == "outbound")
            n_in = sum(1 for ix in interactions
                       if ix.get("direction") == "inbound")
            R_recip = compute_reciprocity(n_out, n_in)
            types_used = {ix.get("type") for ix in interactions if ix.get("type")}
            M = compute_multiplexity(types_used)
            D_rel = compute_deal_relevance(
                deal_signals.get(email, []), active_deals,
            )
            imp = compute_importance(F, R_recip, M, D_rel)
            imp = apply_priority_override(imp, person.get("priority_override"))

            # Urgency
            days_ago = []
            for ix in interactions:
                diff = (today - ix["timestamp"]).total_seconds() / 86400
                if diff >= 0:
                    days_ago.append(diff)
            mu = len(window_ix) / max(FREQUENCY_WINDOW, 1)
            hawkes = compute_hawkes_intensity(days_ago, mu)
            spike = compute_inbound_spike(window_ix, today)
            U = compute_urgency(hawkes, spike)

            # Rescue (raw — normalized later)
            gaps = _inter_event_days(interactions)
            if interactions:
                t_since = (today - interactions[-1]["timestamp"]).total_seconds() / 86400
            else:
                t_since = 0.0
            R_raw = compute_weibull_rescue(gaps, t_since)

            # Attention: weighted count in window / total weighted count
            total_weighted = sum(
                OUTBOUND_WEIGHT if ix.get("direction") == "outbound" else 1.0
                for ixs in history.values() for ix in
                _split_window(ixs, today)
            )
            person_weighted = sum(
                OUTBOUND_WEIGHT if ix.get("direction") == "outbound" else 1.0
                for ix in window_ix
            )
            a_actual = person_weighted / max(total_weighted, 1.0)

            scored.append({
                "person_id": person["person_id"],
                "email": email,
                "full_name": person.get("full_name", ""),
                "I": imp,
                "U": U,
                "R_raw": R_raw,
                "a_actual": a_actual,
            })
            rescue_raw_values.append(R_raw)

        # --- Normalize rescue across all persons ---
        rescue_norm = normalize_min_max(rescue_raw_values)
        for i, entry in enumerate(scored):
            entry["R"] = rescue_norm[i]

        # --- Sort by importance, assign Dunbar layers ---
        scored.sort(key=lambda x: x["I"], reverse=True)
        layer_counts = [0, 0, 0, 0]
        for rank, entry in enumerate(scored, 1):
            layer = assign_dunbar_layer(rank)
            entry["layer"] = layer
            layer_counts[layer] += 1

        # --- Compute deficit and final score ---
        for entry in scored:
            layer = entry["layer"]
            D = compute_attention_deficit(
                layer, layer_counts[layer], entry["a_actual"],
            )
            entry["D"] = D

            rec_info = rec_history.get(str(entry["person_id"]),
                                       {"rec_count": 0, "acted": False})
            modifier = compute_response_modifier(
                rec_info["rec_count"], rec_info["acted"],
            )

            entry["score"] = compute_priority(
                entry["I"], entry["U"], entry["R"], entry["D"], modifier,
            )

        # --- Write to contact_scores ---
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM contact_scores WHERE scored_date = %s",
                (today.date(),),
            )
            for entry in scored:
                cur.execute("""
                    INSERT INTO contact_scores
                        (person_id, importance, urgency, rescue, deficit,
                         total_score, dunbar_layer, scored_at, scored_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    entry["person_id"],
                    round(entry["I"], 6),
                    round(entry["U"], 6),
                    round(entry["R"], 6),
                    round(entry["D"], 6),
                    round(entry["score"], 2),
                    entry["layer"],
                    today,
                    today.date(),
                ))
        conn.commit()

        # --- Print top 10 ---
        scored.sort(key=lambda x: x["score"], reverse=True)
        print(f"\nTop 10 contacts ({len(scored)} scored):")
        print(f"{'Name':<30} {'Score':>6} {'I':>5} {'U':>5} "
              f"{'R':>5} {'D':>5} {'Layer':>5}")
        print("-" * 70)
        for entry in scored[:10]:
            print(f"{entry['full_name']:<30} {entry['score']:6.1f} "
                  f"{entry['I']:5.2f} {entry['U']:5.2f} "
                  f"{entry['R']:5.2f} {entry['D']:5.2f} "
                  f"{entry['layer']:5d}")

        return len(scored)

    finally:
        conn.close()


if __name__ == "__main__":
    print("=== Scoring Engine ===")
    count = score_all()
    print(f"\nScored {count} contacts.")
