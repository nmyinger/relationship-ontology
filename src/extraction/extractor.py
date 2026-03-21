"""
src/extraction/extractor.py — Orchestrator: reads raw rows, calls LLM, writes core tables.

Entry point: extract_batch(source, batch_size, database_url, llm_fn)
Also runnable as: python -m src.extraction.extractor
"""

import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import psycopg2

from src.config import load_config
from src.extraction.entity_resolver import resolve_company, resolve_person
from src.extraction.prompts import (
    EXTRACTION_SYSTEM_PROMPT,
    format_calendar_prompt,
    format_email_prompt,
)
from src.llm.client import call_llm

logger = logging.getLogger(__name__)

_NOREPLY_RE = re.compile(
    r"(?:^|<)(?:no[\-_.]?reply|donotreply|mailer[\-_.]?daemon)@"
    r"|@noreply\.",
    re.IGNORECASE,
)
_SERVICE_DOMAIN_RE = re.compile(
    r"[@.](?:"
    r"service|mail|email|notifications?|updates?|ealerts|info"
    r"|calendar|welcome|mailer|contact|alerts?"
    r"|bounce|digest|promo|campaign|transactional"
    r"|receipts?|postmaster|news"
    r")\.",
    re.IGNORECASE,
)
_AUTO_LOCAL_RE = re.compile(
    r"(?:^|<)(?:billing|invoice|receipts?|postmaster|alerts?)@",
    re.IGNORECASE,
)


def _is_service_sender(sender: str | None) -> str | None:
    """Return a skip reason if sender matches a transactional/service pattern."""
    if not sender:
        return None
    if _NOREPLY_RE.search(sender):
        return "noreply_sender"
    if _SERVICE_DOMAIN_RE.search(sender):
        return "service_domain"
    if _AUTO_LOCAL_RE.search(sender):
        return "auto_sender"
    return None


def _fetch_unprocessed_emails(conn, batch_size: int) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, sender, recipients, subject, body_text, direction, timestamp,
                   raw_payload
            FROM email_raw
            WHERE processed_at IS NULL
            ORDER BY timestamp
            LIMIT %s
            """,
            (batch_size,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _get_header(headers: list[dict], name: str) -> str | None:
    """Return the value of the first header matching *name* (case-insensitive)."""
    name_lower = name.lower()
    for h in headers:
        if h.get("name", "").lower() == name_lower:
            return h.get("value")
    return None


def _should_skip(raw_payload) -> str | None:
    """Return a skip reason if the email is a newsletter/bulk mail, else None."""
    if raw_payload is None:
        return None

    if isinstance(raw_payload, str):
        try:
            raw_payload = json.loads(raw_payload)
        except (json.JSONDecodeError, TypeError):
            return None

    headers = raw_payload.get("payload", {}).get("headers", [])
    if not headers:
        return None

    if _get_header(headers, "List-ID") is not None:
        return "mailing_list"

    if _get_header(headers, "List-Unsubscribe") is not None:
        return "newsletter"

    precedence = _get_header(headers, "Precedence")
    if precedence and precedence.lower() in ("bulk", "list"):
        return "bulk_mail"

    from_header = _get_header(headers, "From")
    sender_skip = _is_service_sender(from_header)
    if sender_skip:
        return sender_skip

    return None


def _fetch_unprocessed_calendar(conn, batch_size: int) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, title, participants, timestamp
            FROM calendar_raw
            WHERE processed_at IS NULL
            ORDER BY timestamp
            LIMIT %s
            """,
            (batch_size,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _user_domain(user_email: str | None) -> str | None:
    """Extract the domain from the user's email for internal-contact detection."""
    if not user_email or "@" not in user_email:
        return None
    return user_email.rsplit("@", 1)[1].lower()


def _process_extraction(extracted: dict, interaction_id, conn,
                        user_email: str | None = None):
    """Resolve entities and insert signals from an LLM extraction result."""
    company_cache = {}
    internal_domain = _user_domain(user_email)

    for comp in extracted.get("companies", []):
        comp_name = comp.get("name")
        if comp_name:
            company_cache[comp_name.lower()] = resolve_company(comp_name, conn)

    for person in extracted.get("persons", []):
        email = person.get("email")
        name = person.get("name") or ""
        if not email:
            continue
        if user_email and email.lower() == user_email.lower():
            continue
        if not name:
            name = email.split("@")[0].replace(".", " ").title()
        comp_name = person.get("company")
        company_id = None
        if comp_name:
            if comp_name.lower() in company_cache:
                company_id = company_cache[comp_name.lower()]
            else:
                company_id = resolve_company(comp_name, conn)
                company_cache[comp_name.lower()] = company_id
        person_domain = email.rsplit("@", 1)[1].lower() if "@" in email else ""
        is_internal = internal_domain is not None and person_domain == internal_domain
        resolve_person(email, name, company_id, person.get("title"), conn,
                       is_internal=is_internal)

    for signal in extracted.get("signals", []):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO interaction_signals
                    (interaction_id, signal_type, signal_value, confidence)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    interaction_id,
                    signal.get("type", "unknown"),
                    signal.get("value"),
                    signal.get("confidence"),
                ),
            )


def _insert_interaction(source_type: str, timestamp, direction: str | None,
                        participants: list[str], summary: str | None, conn):
    """Insert a row into the interactions table and return the interaction_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO interactions (type, timestamp, direction, participants, summary)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING interaction_id
            """,
            (source_type, timestamp, direction, participants, summary),
        )
        return cur.fetchone()[0]


def _mark_processed(table: str, row_id, conn, skip_reason: str | None = None):
    """Set processed_at (and optionally skip_reason) on a staging table row."""
    now = datetime.now(timezone.utc)
    if skip_reason and table == "email_raw":
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {table} SET processed_at = %s, skip_reason = %s WHERE id = %s",  # noqa: S608
                (now, skip_reason, row_id),
            )
    else:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {table} SET processed_at = %s WHERE id = %s",  # noqa: S608
                (now, row_id),
            )


def _prepare_row(source: str, row: dict) -> tuple[dict, str, str | None]:
    """Apply skip logic and build the user prompt.

    Returns (row, user_msg, skip_reason).  When skip_reason is not None the
    row should be marked processed without an LLM call (user_msg will be "").
    """
    if source == "email":
        skip_reason = _should_skip(row.get("raw_payload"))
        if not skip_reason:
            skip_reason = _is_service_sender(row.get("sender"))
        if skip_reason:
            logger.info("Skipping email %s: %s", row["id"], skip_reason)
            return (row, "", skip_reason)

        user_msg = format_email_prompt(
            sender=row["sender"] or "",
            recipients=row["recipients"] or [],
            subject=row["subject"] or "",
            body_text=row["body_text"] or "",
            direction=row["direction"] or "unknown",
        )
    else:
        user_msg = format_calendar_prompt(
            title=row["title"] or "",
            participants=row["participants"] or [],
            timestamp=str(row["timestamp"]),
        )
    return (row, user_msg, None)


def _call_llm_for_row(llm_fn, system_prompt: str, row: dict,
                       user_msg: str) -> tuple[dict, dict | Exception]:
    """Call the LLM for a single row. Runs in a worker thread."""
    try:
        extracted = llm_fn(system_prompt, user_msg)
        return (row, extracted)
    except Exception as exc:
        return (row, exc)


def extract_batch(
    source: str = "email",
    batch_size: int = 50,
    database_url: str | None = None,
    llm_fn=None,
    concurrency: int = 10,
) -> int:
    """
    Process a batch of unprocessed raw rows through the extraction pipeline.

    Parameters
    ----------
    source : str
        "email" or "calendar"
    batch_size : int
        Max rows to process in this batch.
    database_url : str, optional
        Override DB connection string.
    llm_fn : callable, optional
        Override LLM call (for testing). Signature: (system, user) -> dict.
    concurrency : int
        Max parallel LLM calls (default 10).

    Returns
    -------
    int
        Number of rows successfully processed.
    """
    if database_url is None:
        config = load_config()
        database_url = config["DATABASE_URL"]

    if llm_fn is None:
        llm_fn = call_llm

    user_email = os.environ.get("USER_EMAIL")

    conn = psycopg2.connect(database_url)
    conn.autocommit = False

    try:
        if source == "email":
            rows = _fetch_unprocessed_emails(conn, batch_size)
        elif source == "calendar":
            rows = _fetch_unprocessed_calendar(conn, batch_size)
        else:
            raise ValueError(f"Unknown source: {source}")

        # Phase 1: Pre-filter — skip newsletters, build prompts
        table = "email_raw" if source == "email" else "calendar_raw"
        to_process = []
        for row in rows:
            row_data, user_msg, skip_reason = _prepare_row(source, row)
            if skip_reason:
                _mark_processed(table, row_data["id"], conn, skip_reason=skip_reason)
                conn.commit()
            else:
                to_process.append((row_data, user_msg))

        if not to_process:
            return 0

        # Phase 2: LLM calls in parallel via thread pool
        futures = {}
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            for row, user_msg in to_process:
                fut = executor.submit(
                    _call_llm_for_row, llm_fn, EXTRACTION_SYSTEM_PROMPT,
                    row, user_msg,
                )
                futures[fut] = row

            # Phase 3: DB writes sequentially on main thread
            processed = 0
            done_count = 0
            for fut in as_completed(futures):
                done_count += 1
                row, result = fut.result()
                if isinstance(result, Exception):
                    label = row.get("subject") or row.get("title") or row["id"]
                    print(f"  [{source}] Error on row {row['id']} ({label}): {result}")
                    continue

                try:
                    extracted = result
                    direction = row.get("direction")
                    participants = (row.get("recipients")
                                    or row.get("participants") or [])
                    summary = extracted.get("summary")

                    interaction_id = _insert_interaction(
                        source_type=source,
                        timestamp=row["timestamp"],
                        direction=direction,
                        participants=participants,
                        summary=summary,
                        conn=conn,
                    )

                    _process_extraction(extracted, interaction_id, conn,
                                        user_email=user_email)

                    _mark_processed(table, row["id"], conn)
                    conn.commit()
                    processed += 1

                    if done_count % 10 == 0:
                        print(f"  [{source}] Processed {done_count}/"
                              f"{len(to_process)} rows...")

                except Exception as exc:
                    conn.rollback()
                    label = row.get("subject") or row.get("title") or row["id"]
                    print(f"  [{source}] Error on row {row['id']} ({label}): {exc}")

        return processed

    finally:
        conn.close()


def extract_all(
    source: str = "email",
    batch_size: int = 100,
    concurrency: int = 50,
    database_url: str | None = None,
    llm_fn=None,
) -> int:
    """Run extract_batch in a loop until no unprocessed rows remain."""
    total = 0
    batch_num = 0
    while True:
        batch_num += 1
        count = extract_batch(
            source=source, batch_size=batch_size, database_url=database_url,
            llm_fn=llm_fn, concurrency=concurrency,
        )
        total += count
        if count == 0:
            break
        print(f"  [{source}] Batch {batch_num} done — {count} extracted "
              f"({total} total)")
    return total


if __name__ == "__main__":
    _concurrency = int(os.environ.get("EXTRACT_CONCURRENCY", "50"))
    _batch = int(os.environ.get("EXTRACT_BATCH_SIZE", "100"))
    print(f"=== Entity Extraction Pipeline "
          f"(batch={_batch}, concurrency={_concurrency}) ===")
    email_count = extract_all(
        source="email", batch_size=_batch, concurrency=_concurrency,
    )
    print(f"Processed {email_count} emails.")
    cal_count = extract_all(
        source="calendar", batch_size=_batch, concurrency=_concurrency,
    )
    print(f"Processed {cal_count} calendar events.")
    print(f"Total: {email_count + cal_count} rows extracted.")
