"""
src/db/runner.py — SQL migration runner for the Deal Flow Engine.

Applies unapplied migration files from db/migrations/ in lexicographic order.
Each migration runs in its own transaction. The schema_versions table tracks
which migrations have been applied.

Idempotent: re-running produces no error and no duplicate application.
"""

import os

from src.config import load_config
from src.db.connection import get_connection

_BOOTSTRAP_SQL = """
CREATE TABLE IF NOT EXISTS schema_versions (
    migration_name TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def apply_migrations(conn, migrations_dir: str = "db/migrations") -> list[str]:
    """
    Apply all unapplied SQL migrations in migrations_dir to the database.

    Parameters
    ----------
    conn:
        An open psycopg2 connection. The caller retains ownership; this
        function does not close it.
    migrations_dir:
        Path to the directory containing numbered .sql files.
        Resolved relative to the current working directory.

    Returns
    -------
    list[str]
        Filenames (not full paths) of migrations applied during this call,
        in the order they were applied. Empty list if nothing was applied.

    Raises
    ------
    Exception
        Any error from a migration is re-raised after rolling back that
        migration's transaction.
    """
    # Step 1: Bootstrap schema_versions. This must succeed before we read
    # migration files so the table exists even if 006_create_schema_versions.sql
    # has not yet run.
    with conn.cursor() as cur:
        cur.execute(_BOOTSTRAP_SQL)
    conn.commit()

    # Step 2: Collect migration files.
    sql_files = sorted(
        f for f in os.listdir(migrations_dir) if f.endswith(".sql")
    )

    # Step 3: Find already-applied migrations.
    with conn.cursor() as cur:
        cur.execute("SELECT migration_name FROM schema_versions")
        applied = {row[0] for row in cur.fetchall()}

    # Step 4: Apply unapplied migrations in order.
    newly_applied: list[str] = []
    for filename in sql_files:
        if filename in applied:
            continue

        filepath = os.path.join(migrations_dir, filename)
        with open(filepath, encoding="utf-8") as fh:
            sql = fh.read()

        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                cur.execute(
                    "INSERT INTO schema_versions (migration_name) VALUES (%s)",
                    (filename,),
                )
            conn.commit()
            newly_applied.append(filename)
        except Exception:
            conn.rollback()
            raise

    return newly_applied


if __name__ == "__main__":
    config = load_config()
    conn = get_connection(config["DATABASE_URL"])
    try:
        applied = apply_migrations(conn)
        if applied:
            print(f"Applied {len(applied)} migration(s):")
            for name in applied:
                print(f"  {name}")
        else:
            print("No new migrations to apply.")
    finally:
        conn.close()
