"""
tests/test_config.py — Tests for src/config.py

Covers:
- Missing DATABASE_URL raises EnvironmentError
- Present DATABASE_URL loads correctly
- Optional keys (OUTPUT_DIR, LLM_MODEL) use their defaults when absent
- Optional keys use provided values when present
- Later-slice keys are None when absent (not validated at startup)
"""

import pytest

from src.config import load_config

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _env_with(**overrides):
    """
    Return an env dict that includes only the supplied overrides.
    Use as the argument to monkeypatch.setenv / os.environ patching.
    """
    return overrides


# ---------------------------------------------------------------------------
# Required key: DATABASE_URL
# ---------------------------------------------------------------------------


def test_missing_database_url_raises(monkeypatch):
    """load_config() must raise EnvironmentError when DATABASE_URL is absent."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(EnvironmentError, match="DATABASE_URL"):
        load_config()


def test_empty_database_url_raises(monkeypatch):
    """An empty string for DATABASE_URL is treated as absent and must raise."""
    monkeypatch.setenv("DATABASE_URL", "")
    with pytest.raises(EnvironmentError, match="DATABASE_URL"):
        load_config()


def test_present_database_url_loads(monkeypatch):
    """A valid DATABASE_URL is returned unchanged in the config dict."""
    url = "postgresql://user:pass@localhost:5432/testdb"
    monkeypatch.setenv("DATABASE_URL", url)
    config = load_config()
    assert config["DATABASE_URL"] == url


# ---------------------------------------------------------------------------
# Optional key: OUTPUT_DIR
# ---------------------------------------------------------------------------


def test_output_dir_default(monkeypatch):
    """OUTPUT_DIR defaults to 'output' when not set."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://x:y@localhost/db")
    monkeypatch.delenv("OUTPUT_DIR", raising=False)
    config = load_config()
    assert config["OUTPUT_DIR"] == "output"


def test_output_dir_override(monkeypatch):
    """OUTPUT_DIR uses the provided value when set."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://x:y@localhost/db")
    monkeypatch.setenv("OUTPUT_DIR", "/tmp/my_output")
    config = load_config()
    assert config["OUTPUT_DIR"] == "/tmp/my_output"


# ---------------------------------------------------------------------------
# Optional key: LLM_MODEL
# ---------------------------------------------------------------------------


def test_llm_model_default(monkeypatch):
    """LLM_MODEL defaults to 'claude-haiku-4-5-20251001' when not set."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://x:y@localhost/db")
    monkeypatch.delenv("LLM_MODEL", raising=False)
    config = load_config()
    assert config["LLM_MODEL"] == "claude-haiku-4-5-20251001"


def test_llm_model_override(monkeypatch):
    """LLM_MODEL uses the provided value when set."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://x:y@localhost/db")
    monkeypatch.setenv("LLM_MODEL", "gpt-4-turbo")
    config = load_config()
    assert config["LLM_MODEL"] == "gpt-4-turbo"


# ---------------------------------------------------------------------------
# Later-slice keys: present but not validated at startup
# ---------------------------------------------------------------------------


def test_later_slice_keys_are_none_when_absent(monkeypatch):
    """Keys needed by later slices must be None (not raise) when absent."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://x:y@localhost/db")
    for key in [
        "GOOGLE_CREDENTIALS_PATH",
        "LLM_API_KEY",
        "LLM_API_BASE_URL",
        "EMAIL_FROM",
        "EMAIL_TO",
        "SMTP_HOST",
        "SMTP_PORT",
        "SMTP_PASSWORD",
    ]:
        monkeypatch.delenv(key, raising=False)

    config = load_config()

    assert config["GOOGLE_CREDENTIALS_PATH"] is None
    assert config["LLM_API_KEY"] is None
    assert config["LLM_API_BASE_URL"] is None
    assert config["EMAIL_FROM"] is None
    assert config["EMAIL_TO"] is None
    assert config["SMTP_HOST"] is None
    assert config["SMTP_PORT"] is None
    assert config["SMTP_PASSWORD"] is None


def test_later_slice_keys_pass_through_when_set(monkeypatch):
    """Keys needed by later slices are returned correctly when present."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://x:y@localhost/db")
    monkeypatch.setenv("LLM_API_KEY", "sk-test-key")
    monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
    config = load_config()
    assert config["LLM_API_KEY"] == "sk-test-key"
    assert config["SMTP_HOST"] == "smtp.example.com"
