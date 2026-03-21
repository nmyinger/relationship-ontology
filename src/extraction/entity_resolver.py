"""
src/extraction/entity_resolver.py — Upsert persons and companies by email/name.

Uses SELECT-then-INSERT pattern for companies (matched on lower(name)).
Uses INSERT ON CONFLICT for persons (matched on email UNIQUE constraint).
"""

import re
import uuid

_SUFFIX_RE = re.compile(
    r"[,\s]+(LLC|Inc|Corp|Ltd|Co|LP|LLP)\.?\s*$",
    re.IGNORECASE,
)
_LEADING_THE_RE = re.compile(r"^The\s+", re.IGNORECASE)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]")


def _normalize_company_name(name: str) -> str:
    """Strip legal suffixes and leading 'The' for matching purposes."""
    name = _SUFFIX_RE.sub("", name).strip()
    name = _LEADING_THE_RE.sub("", name).strip()
    return name


def _match_key(name: str) -> str:
    """Strip suffixes, punctuation, and whitespace, then lowercase."""
    return _NON_ALNUM_RE.sub("", _normalize_company_name(name).lower())


def resolve_person(
    email: str,
    full_name: str,
    company_id: uuid.UUID | None,
    title: str | None,
    conn,
    is_internal: bool = False,
) -> uuid.UUID:
    """
    Upsert a person by email. Updates name/title/company_id on conflict.

    Returns the person_id (existing or newly created).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO persons (email, full_name, company_id, title, is_internal)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (email) DO UPDATE
                SET full_name   = COALESCE(NULLIF(EXCLUDED.full_name, ''), persons.full_name),
                    title       = COALESCE(EXCLUDED.title, persons.title),
                    company_id  = COALESCE(EXCLUDED.company_id, persons.company_id),
                    is_internal = EXCLUDED.is_internal OR persons.is_internal
            RETURNING person_id
            """,
            (email, full_name, company_id, title, is_internal),
        )
        return cur.fetchone()[0]


def resolve_company(name: str, conn) -> uuid.UUID:
    """
    Find or create a company by name.

    Lookup order:
    1. Exact case-insensitive match on name
    2. Match-key comparison (strips whitespace/punctuation) against existing names
    3. Alias table lookup
    4. Insert new company

    Returns the company_id.
    """
    key = _match_key(name)
    with conn.cursor() as cur:
        # 1. Exact case-insensitive match
        cur.execute(
            "SELECT company_id FROM companies WHERE lower(name) = lower(%s)",
            (name,),
        )
        row = cur.fetchone()
        if row:
            return row[0]

        # 2. Match-key comparison against existing company names
        cur.execute("SELECT company_id, name FROM companies")
        for existing_id, existing_name in cur.fetchall():
            if _match_key(existing_name) == key:
                return existing_id

        # 3. Alias table lookup
        cur.execute(
            "SELECT company_id FROM company_aliases WHERE lower(alias) = lower(%s)",
            (name,),
        )
        row = cur.fetchone()
        if row:
            return row[0]

        # 4. Insert new company
        cur.execute(
            "INSERT INTO companies (name) VALUES (%s) RETURNING company_id",
            (name,),
        )
        return cur.fetchone()[0]
