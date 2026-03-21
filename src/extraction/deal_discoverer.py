"""
src/extraction/deal_discoverer.py — Discover deals from extracted signals via LLM.

Reads deal_mention signals from interaction_signals, sends them to the LLM to
cluster/deduplicate into canonical deal records, and upserts into the deals table.

Runs after `extract` and before `link` in the pipeline.
Also runnable as: python -m src.extraction.deal_discoverer
"""

import logging

import psycopg2

from src.config import load_config
from src.extraction.prompts import DEAL_DISCOVERY_PROMPT
from src.llm.client import call_llm

logger = logging.getLogger(__name__)


def _fetch_deal_signals(conn) -> list[str]:
    """Return distinct deal_mention signal values."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT DISTINCT signal_value
               FROM interaction_signals
               WHERE signal_type = 'deal_mention'
                 AND signal_value IS NOT NULL"""
        )
        return [row[0] for row in cur.fetchall()]


def _format_signals_message(signals: list[str]) -> str:
    """Format signals as a numbered list for the LLM."""
    return "\n".join(f"{i}. {s}" for i, s in enumerate(signals, 1))


def _upsert_deals(conn, deals: list[dict]) -> int:
    """Upsert discovered deals. Returns count of rows affected."""
    count = 0
    with conn.cursor() as cur:
        for deal in deals:
            cur.execute(
                """INSERT INTO deals (name, market, asset_type, stage, status)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (name) DO UPDATE SET
                     market     = COALESCE(deals.market, EXCLUDED.market),
                     asset_type = COALESCE(deals.asset_type, EXCLUDED.asset_type),
                     stage      = COALESCE(deals.stage, EXCLUDED.stage),
                     updated_at = now()""",
                (
                    deal.get("name"),
                    deal.get("market"),
                    deal.get("asset_type"),
                    deal.get("stage"),
                    deal.get("status", "active"),
                ),
            )
            count += cur.rowcount
    return count


def discover_deals(database_url=None, llm_fn=None) -> dict:
    """
    Discover deals from deal_mention signals via LLM clustering.

    Parameters
    ----------
    database_url : str, optional
        Override DB connection string.
    llm_fn : callable, optional
        Override LLM call (for testing). Signature: (system, user) -> dict.

    Returns
    -------
    dict
        {"signals_found": N, "deals_discovered": N}
    """
    if database_url is None:
        config = load_config()
        database_url = config["DATABASE_URL"]

    if llm_fn is None:
        llm_fn = call_llm

    conn = psycopg2.connect(database_url)
    conn.autocommit = False

    try:
        signals = _fetch_deal_signals(conn)
        if not signals:
            return {"signals_found": 0, "deals_discovered": 0}

        user_msg = _format_signals_message(signals)
        result = llm_fn(DEAL_DISCOVERY_PROMPT, user_msg)
        deals = result.get("deals", [])

        count = _upsert_deals(conn, deals)
        conn.commit()

        return {"signals_found": len(signals), "deals_discovered": count}
    finally:
        conn.close()


if __name__ == "__main__":
    print("=== Deal Discovery ===")
    result = discover_deals()
    print(f"Signals found: {result['signals_found']}")
    print(f"Deals discovered: {result['deals_discovered']}")
