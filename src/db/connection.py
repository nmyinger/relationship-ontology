"""
src/db/connection.py — psycopg2 connection factory for the Deal Flow Engine.

Rules:
- No connection pooling. Each caller gets a fresh connection.
- database_url=None falls back to config via load_config().
- No abstraction beyond returning a raw psycopg2 connection.
"""

import psycopg2

from src.config import load_config


def get_connection(database_url: str | None = None):
    """
    Return a psycopg2 connection.

    Parameters
    ----------
    database_url:
        A libpq-compatible connection string, e.g.
        ``postgresql://user:pass@host:5432/dbname``.
        If None, DATABASE_URL is read from the environment via load_config().

    Returns
    -------
    psycopg2.extensions.connection
        A live database connection. The caller is responsible for closing it.
    """
    if database_url is None:
        config = load_config()
        database_url = config["DATABASE_URL"]

    return psycopg2.connect(database_url)
