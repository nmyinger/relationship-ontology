"""
tests/test_deal_discoverer.py — Tests for auto deal discovery from signals.
"""

import os
import uuid

import psycopg2
import pytest

from src.db.runner import apply_migrations
from src.extraction.deal_discoverer import (
    _format_signals_message,
    discover_deals,
)

_MIGRATIONS_DIR = "db/migrations"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL and DATABASE_URL are both unset")
    return url


def _make_schema_conn(database_url: str):
    schema_name = f"test_dd_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(database_url)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema_name}")
        cur.execute(f"SET search_path TO {schema_name}, public")
    conn.commit()
    return conn, schema_name


def _drop_schema(database_url: str, conn, schema_name: str) -> None:
    conn.close()
    drop_conn = psycopg2.connect(database_url)
    drop_conn.autocommit = True
    with drop_conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
    drop_conn.close()


@pytest.fixture()
def db():
    url = _get_database_url()
    conn, schema_name = _make_schema_conn(url)
    try:
        apply_migrations(conn, _MIGRATIONS_DIR)
        sep = "&" if "?" in url else "?"
        scoped_url = f"{url}{sep}options=-csearch_path%3D{schema_name},public"
        yield scoped_url, conn
    finally:
        _drop_schema(url, conn, schema_name)


def _seed_signals(conn, signals: list[str]):
    """Insert an interaction and deal_mention signals for testing."""
    interaction_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO interactions (interaction_id, type, timestamp, direction, participants)
               VALUES (%s, %s, now(), %s, %s)""",
            (interaction_id, "email", "inbound", ["test@example.com"]),
        )
        for signal_value in signals:
            cur.execute(
                """INSERT INTO interaction_signals
                   (signal_id, interaction_id, signal_type, signal_value)
                   VALUES (%s, %s, %s, %s)""",
                (str(uuid.uuid4()), interaction_id, "deal_mention", signal_value),
            )
    conn.commit()


def _mock_llm_3_deals(system_prompt, user_message):
    """Mock LLM that returns 3 deals."""
    return {
        "deals": [
            {"name": "Altitude Hedge Fund", "market": None, "asset_type": "fund",
             "stage": "prospecting", "status": "active"},
            {"name": "Cayman Fund Formation", "market": "Cayman Islands", "asset_type": "fund",
             "stage": "due_diligence", "status": "active"},
            {"name": "DeCenLRNing Protocol", "market": None, "asset_type": "crypto",
             "stage": "prospecting", "status": "active"},
        ]
    }


# ===========================================================================
# Unit tests — no DB, no LLM
# ===========================================================================


class TestFormatSignals:
    def test_format_signals_message(self):
        signals = ["Altitude Hedge Fund formation", "Cayman fund setup"]
        result = _format_signals_message(signals)
        assert result == "1. Altitude Hedge Fund formation\n2. Cayman fund setup"

    def test_format_signals_single(self):
        result = _format_signals_message(["one signal"])
        assert result == "1. one signal"


class TestDiscoverNoSignals:
    def test_discover_skips_when_no_signals(self, db):
        scoped_url, conn = db
        result = discover_deals(database_url=scoped_url, llm_fn=_mock_llm_3_deals)
        assert result == {"signals_found": 0, "deals_discovered": 0}


# ===========================================================================
# Integration tests — isolated DB schema + mock LLM
# ===========================================================================


class TestDiscoverIntegration:
    def test_discover_upserts_new_deals(self, db):
        scoped_url, conn = db
        _seed_signals(conn, [
            "Altitude Hedge Fund formation with $2M",
            "Cayman fund structure discussion",
            "DeCenLRNing protocol token allocation",
        ])

        result = discover_deals(database_url=scoped_url, llm_fn=_mock_llm_3_deals)
        assert result["signals_found"] == 3
        assert result["deals_discovered"] == 3

        with conn.cursor() as cur:
            cur.execute("SELECT name FROM deals ORDER BY name")
            names = [row[0] for row in cur.fetchall()]
        assert names == ["Altitude Hedge Fund", "Cayman Fund Formation", "DeCenLRNing Protocol"]

    def test_discover_idempotent(self, db):
        scoped_url, conn = db
        _seed_signals(conn, ["Altitude Hedge Fund", "Cayman fund"])

        discover_deals(database_url=scoped_url, llm_fn=_mock_llm_3_deals)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM deals")
            first_count = cur.fetchone()[0]

        discover_deals(database_url=scoped_url, llm_fn=_mock_llm_3_deals)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM deals")
            second_count = cur.fetchone()[0]

        assert first_count == second_count

    def test_discover_coalesce_preserves_existing(self, db):
        """Existing deal with market='Boston' is not overwritten by LLM null."""
        scoped_url, conn = db
        _seed_signals(conn, ["Altitude Hedge Fund formation"])

        # Pre-insert a deal with market set
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO deals (name, market, asset_type, stage, status)
                   VALUES (%s, %s, %s, %s, %s)""",
                ("Altitude Hedge Fund", "Boston", "fund", "closing", "active"),
            )
        conn.commit()

        # LLM returns market=null for this deal
        discover_deals(database_url=scoped_url, llm_fn=_mock_llm_3_deals)

        with conn.cursor() as cur:
            cur.execute("SELECT market, stage FROM deals WHERE name = 'Altitude Hedge Fund'")
            row = cur.fetchone()
        # COALESCE keeps existing Boston, existing closing stage
        assert row[0] == "Boston"
        assert row[1] == "closing"

    def test_discover_returns_counts(self, db):
        scoped_url, conn = db
        _seed_signals(conn, ["signal A", "signal B"])

        result = discover_deals(database_url=scoped_url, llm_fn=_mock_llm_3_deals)
        assert result["signals_found"] == 2
        assert result["deals_discovered"] == 3
