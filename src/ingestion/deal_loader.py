"""
src/ingestion/deal_loader.py — CSV-to-Postgres deal loader.

Reads a CSV file of active deals, validates the schema against the deals table,
and upserts rows. Upsert key is the deal name (UNIQUE constraint from migration 007).

No external API access. No LLM calls. Writes only to the deals table.
"""

import csv
import sys

from src.db.connection import get_connection

# Columns that must be present in the CSV header.
_REQUIRED_COLUMNS = {"name"}

# All columns the deals table accepts (excluding deal_id, which is auto-generated).
_ALLOWED_COLUMNS = {
    "name",
    "market",
    "asset_type",
    "size",
    "stage",
    "strategy_tags",
    "status",
    "owner_user_id",
}

_UPSERT_SQL = """
INSERT INTO deals (name, market, asset_type, size, stage, strategy_tags, status, owner_user_id)
VALUES (
    %(name)s, %(market)s, %(asset_type)s, %(size)s,
    %(stage)s, %(strategy_tags)s, %(status)s, %(owner_user_id)s
)
ON CONFLICT (name) DO UPDATE SET
    market         = EXCLUDED.market,
    asset_type     = EXCLUDED.asset_type,
    size           = EXCLUDED.size,
    stage          = EXCLUDED.stage,
    strategy_tags  = EXCLUDED.strategy_tags,
    status         = EXCLUDED.status,
    owner_user_id  = EXCLUDED.owner_user_id
"""


def _parse_strategy_tags(raw: str | None) -> list[str] | None:
    """Convert a comma-separated string to a Python list for Postgres TEXT[]."""
    if not raw or not raw.strip():
        return None
    return [tag.strip() for tag in raw.split(",") if tag.strip()]


def _parse_size(raw: str | None) -> float | None:
    """Convert a size string to a float, or None if empty."""
    if not raw or not raw.strip():
        return None
    return float(raw.strip())


def _row_to_params(row: dict) -> dict:
    """Normalise a CSV row dict into parameters for the upsert SQL."""
    return {
        "name": row["name"].strip(),
        "market": row.get("market", "").strip() or None,
        "asset_type": row.get("asset_type", "").strip() or None,
        "size": _parse_size(row.get("size")),
        "stage": row.get("stage", "").strip() or None,
        "strategy_tags": _parse_strategy_tags(row.get("strategy_tags")),
        "status": row.get("status", "").strip() or "active",
        "owner_user_id": row.get("owner_user_id", "").strip() or None,
    }


def load_deals(csv_path: str, database_url: str | None = None) -> int:
    """
    Read a CSV file and upsert all rows into the deals table.

    Parameters
    ----------
    csv_path:
        Path to the CSV file. Must have a header row.
    database_url:
        Postgres connection string. Falls back to DATABASE_URL env var.

    Returns
    -------
    int
        Number of rows processed.

    Raises
    ------
    FileNotFoundError
        If csv_path does not exist.
    ValueError
        If a required column is missing from the CSV header.
    """
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        headers = set(reader.fieldnames or [])

        missing = _REQUIRED_COLUMNS - headers
        if missing:
            raise ValueError(
                f"CSV is missing required column(s): {', '.join(sorted(missing))}"
            )

        rows = list(reader)

    if not rows:
        return 0

    conn = get_connection(database_url)
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(_UPSERT_SQL, _row_to_params(row))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return len(rows)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.ingestion.deal_loader <csv_path>", file=sys.stderr)
        sys.exit(1)

    path = sys.argv[1]
    try:
        count = load_deals(path)
        print(f"Loaded {count} deal(s) from {path}")
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
