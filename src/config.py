"""
config.py — Environment variable loading for the Deal Flow Engine.

Rules:
- Required keys raise EnvironmentError immediately on load if absent.
- Optional keys return a default when absent.
- Keys needed by later slices are declared here but NOT validated at startup;
  each slice's entry point is responsible for calling validate_slice_N() or
  simply accessing the relevant key at the point it is first used.
"""

import os

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _require(key: str) -> str:
    """Return the value of an env var or raise EnvironmentError."""
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"See .env.example for documentation."
        )
    return value


def _optional(key: str, default: str) -> str:
    """Return the value of an env var or a default when absent."""
    return os.environ.get(key, default)


# ---------------------------------------------------------------------------
# Config loader — called once at startup by any entry point
# ---------------------------------------------------------------------------


def load_config() -> dict:
    """
    Load and validate environment variables.

    Required keys (validated now — Slice 1 and all later slices need these):
      DATABASE_URL

    Optional keys (with defaults):
      OUTPUT_DIR   — directory for generated PDFs (default: "output")
      LLM_MODEL    — LLM model identifier (default: "gpt-4o")

    Later-slice keys (declared but not validated here):
      GMAIL_CREDENTIALS_PATH          — validated by Slice 4 (Gmail ingestion)
      GOOGLE_CALENDAR_CREDENTIALS_PATH — validated by Slice 5 (Calendar ingestion)
      LLM_API_KEY                     — validated by Slice 6 (extraction / LLM calls)
      LLM_API_BASE_URL                — validated by Slice 6
      EMAIL_FROM                      — validated by Slice 10 (email delivery)
      EMAIL_TO                        — validated by Slice 10
      SMTP_HOST                       — validated by Slice 10
      SMTP_PORT                       — validated by Slice 10
      SMTP_PASSWORD                   — validated by Slice 10

    Returns a plain dict. Callers should treat this as immutable.
    """
    config: dict = {}

    # --- Required now ---
    config["DATABASE_URL"] = _require("DATABASE_URL")

    # --- Optional with defaults ---
    config["OUTPUT_DIR"] = _optional("OUTPUT_DIR", "output")
    config["LLM_MODEL"] = _optional("LLM_MODEL", "gpt-4o")

    # --- Later-slice keys: read if present, None if absent ---
    # These are deliberately not validated here; each slice validates what it needs.
    config["GMAIL_CREDENTIALS_PATH"] = os.environ.get("GMAIL_CREDENTIALS_PATH")
    config["GOOGLE_CALENDAR_CREDENTIALS_PATH"] = os.environ.get(
        "GOOGLE_CALENDAR_CREDENTIALS_PATH"
    )
    config["LLM_API_KEY"] = os.environ.get("LLM_API_KEY")
    config["LLM_API_BASE_URL"] = os.environ.get("LLM_API_BASE_URL")
    config["EMAIL_FROM"] = os.environ.get("EMAIL_FROM")
    config["EMAIL_TO"] = os.environ.get("EMAIL_TO")
    config["SMTP_HOST"] = os.environ.get("SMTP_HOST")
    config["SMTP_PORT"] = os.environ.get("SMTP_PORT")
    config["SMTP_PASSWORD"] = os.environ.get("SMTP_PASSWORD")

    return config
