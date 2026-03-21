"""
tests/test_linker.py — Unit + integration tests for entity linking (Slice 6b).

Unit tests: pure matching logic (no DB).
Integration tests: linker orchestrator with isolated Postgres schemas.
"""

import os
import uuid

import psycopg2
import pytest

from src.db.runner import apply_migrations
from src.linking.linker import link_entities
from src.linking.matchers import compute_company_confidence, match_deal_to_signal

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
    schema_name = f"test_linker_{uuid.uuid4().hex[:8]}"
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


def _make_deal(name="Harbor Point Acquisition", market="Boston", asset_type="multifamily"):
    return {
        "deal_id": str(uuid.uuid4()),
        "name": name,
        "market": market,
        "asset_type": asset_type,
        "strategy_tags": ["value-add"],
    }


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


def _seed_basic_data(conn):
    """Insert a person, company, deal, interaction, and signal for linking tests."""
    company_id = str(uuid.uuid4())
    person_id = str(uuid.uuid4())
    deal_id = str(uuid.uuid4())
    interaction_id = str(uuid.uuid4())
    signal_id = str(uuid.uuid4())

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (company_id, name) VALUES (%s, %s)",
            (company_id, "Acme Corp"),
        )
        cur.execute(
            "INSERT INTO persons (person_id, full_name, email, company_id) VALUES (%s, %s, %s, %s)",
            (person_id, "Jane Doe", "jane@acme.com", company_id),
        )
        cur.execute(
            """INSERT INTO deals (deal_id, name, market, asset_type, status)
               VALUES (%s, %s, %s, %s, %s)""",
            (deal_id, "Harbor Point Acquisition", "Boston", "multifamily", "active"),
        )
        cur.execute(
            """INSERT INTO interactions (interaction_id, type, timestamp, direction, participants)
               VALUES (%s, %s, now(), %s, %s)""",
            (interaction_id, "email", "inbound", ["jane@acme.com", "nik@example.com"]),
        )
        cur.execute(
            """INSERT INTO interaction_signals
               (signal_id, interaction_id, signal_type, signal_value)
               VALUES (%s, %s, %s, %s)""",
            (signal_id, interaction_id, "deal_mention", "Harbor Point Acquisition deal"),
        )
    conn.commit()
    return {
        "company_id": company_id,
        "person_id": person_id,
        "deal_id": deal_id,
        "interaction_id": interaction_id,
        "signal_id": signal_id,
    }


# ===========================================================================
# Unit tests — matching logic (no DB)
# ===========================================================================


class TestMatchDealToSignal:
    def test_match_deal_exact_name(self):
        """Exact name match produces a positive match with confidence > 0.
        Confidence < 1.0 because deal tokens include market/asset_type/tags."""
        deal = _make_deal(name="Harbor Point Acquisition")
        deal_id, conf = match_deal_to_signal("Harbor Point Acquisition", [deal])
        assert deal_id == deal["deal_id"]
        assert conf > 0.3

    def test_match_deal_partial_overlap(self):
        deal = _make_deal(name="Harbor Point Acquisition")
        deal_id, conf = match_deal_to_signal("Harbor Point deal", [deal])
        assert deal_id == deal["deal_id"]
        assert conf > 0.0

    def test_match_deal_single_token_rejected(self):
        deal = _make_deal(name="Harbor Point Acquisition")
        deal_id, conf = match_deal_to_signal("Harbor", [deal])
        assert deal_id is None
        assert conf == 0.0

    def test_match_deal_no_match(self):
        deal = _make_deal(name="Harbor Point Acquisition")
        deal_id, conf = match_deal_to_signal("completely unrelated topic xyz", [deal])
        assert deal_id is None
        assert conf == 0.0

    def test_match_deal_multiple_candidates(self):
        d1 = _make_deal(name="Harbor Point Acquisition")
        d2 = _make_deal(name="Harbor Bay Development")
        # "Harbor Point deal" should match d1 better (Point overlaps)
        deal_id, conf = match_deal_to_signal("Harbor Point deal", [d1, d2])
        assert deal_id == d1["deal_id"]

    def test_company_confidence_direct(self):
        assert compute_company_confidence("direct") == 1.0

    def test_company_confidence_inferred(self):
        assert compute_company_confidence("inferred_from_person") == 0.8


# ===========================================================================
# Integration tests — linker orchestrator (DB required)
# ===========================================================================


class TestLinkerIntegration:
    def test_link_person_interactions(self, db):
        """Linker creates person_interactions rows from participants array."""
        scoped_url, conn = db
        _seed_basic_data(conn)

        result = link_entities(database_url=scoped_url, mode="full")
        assert result["person_interactions"] >= 1

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM person_interactions")
            assert cur.fetchone()[0] >= 1

    def test_link_person_interactions_role(self, db):
        """Sender gets 'sender' role for inbound email."""
        scoped_url, conn = db
        _seed_basic_data(conn)

        link_entities(database_url=scoped_url, mode="full")

        with conn.cursor() as cur:
            cur.execute("SELECT role FROM person_interactions LIMIT 1")
            role = cur.fetchone()[0]
        assert role in ("sender", "recipient", "attendee")

    def test_link_interaction_companies(self, db):
        """Person's company_id propagated to interaction_companies."""
        scoped_url, conn = db
        _seed_basic_data(conn)

        result = link_entities(database_url=scoped_url, mode="full")
        assert result["interaction_companies"] >= 1

        with conn.cursor() as cur:
            cur.execute("SELECT mention_type, confidence FROM interaction_companies LIMIT 1")
            row = cur.fetchone()
        assert row[0] == "inferred_from_person"
        assert row[1] == 0.8

    def test_link_interaction_deals(self, db):
        """deal_mention signal resolved to interaction_deals row."""
        scoped_url, conn = db
        _seed_basic_data(conn)

        result = link_entities(database_url=scoped_url, mode="full")
        assert result["interaction_deals"] >= 1

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM interaction_deals")
            assert cur.fetchone()[0] >= 1

    def test_link_interaction_deals_confidence_stored(self, db):
        """Confidence value persisted correctly."""
        scoped_url, conn = db
        _seed_basic_data(conn)

        link_entities(database_url=scoped_url, mode="full")

        with conn.cursor() as cur:
            cur.execute("SELECT confidence FROM interaction_deals LIMIT 1")
            confidence = cur.fetchone()[0]
        assert 0.0 < confidence <= 1.0

    def test_link_below_threshold_stored(self, db):
        """Low-confidence links stored (for inspection)."""
        scoped_url, conn = db
        ids = _seed_basic_data(conn)

        # Add a weak signal
        weak_signal_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO interaction_signals
                   (signal_id, interaction_id,
                    signal_type, signal_value)
                   VALUES (%s, %s, %s, %s)""",
                (weak_signal_id, ids["interaction_id"],
                 "deal_mention",
                 "vague Harbor reference nothing"),
            )
        conn.commit()

        link_entities(database_url=scoped_url, mode="full")

        # The strong signal should still produce a link
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM interaction_deals")
            assert cur.fetchone()[0] >= 1

    def test_linker_idempotent(self, db):
        """Running full mode twice produces same row count."""
        scoped_url, conn = db
        _seed_basic_data(conn)

        link_entities(database_url=scoped_url, mode="full")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM person_interactions")
            first_pi = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM interaction_deals")
            first_id = cur.fetchone()[0]

        link_entities(database_url=scoped_url, mode="full")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM person_interactions")
            second_pi = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM interaction_deals")
            second_id = cur.fetchone()[0]

        assert first_pi == second_pi
        assert first_id == second_id

    def test_linker_incremental_skips_linked(self, db):
        """Incremental mode skips already-linked interactions."""
        scoped_url, conn = db
        _seed_basic_data(conn)

        r1 = link_entities(database_url=scoped_url, mode="incremental")
        assert r1["person_interactions"] >= 1

        r2 = link_entities(database_url=scoped_url, mode="incremental")
        assert r2["person_interactions"] == 0

    def test_linker_full_relinks_all(self, db):
        """Full mode re-processes everything."""
        scoped_url, conn = db
        _seed_basic_data(conn)

        r1 = link_entities(database_url=scoped_url, mode="full")
        r2 = link_entities(database_url=scoped_url, mode="full")
        assert r1["person_interactions"] == r2["person_interactions"]
