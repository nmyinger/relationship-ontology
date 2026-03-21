"""
src/llm/client.py — Thin Anthropic SDK wrapper for the Deal Flow Engine.

Provides a single entry point `call_llm()` that sends a system+user message pair
to the Anthropic Messages API and returns the parsed JSON response.
"""

import json
import os
import re

import anthropic
from dotenv import load_dotenv

load_dotenv()

_DEFAULT_MODEL = "claude-haiku-4-5-20251001"


class LLMError(Exception):
    """Raised when the LLM API call fails or the response is not valid JSON."""


def _parse_json(text: str) -> dict:
    """Parse JSON from LLM output, tolerating fences, surrounding text, and truncation."""
    # Strip code fences
    text = re.sub(r"```(?:json)?\s*", "", text).strip()

    # Find first { and last }
    start = text.find("{")
    end = text.rfind("}")
    if start == -1:
        raise LLMError(f"LLM response is not valid JSON: {text[:200]}")

    if end > start:
        candidate = text[start : end + 1]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

    # Truncation repair: take from first { to end, close unmatched brackets
    candidate = text[start:]
    opens_curly = candidate.count("{") - candidate.count("}")
    opens_square = candidate.count("[") - candidate.count("]")
    repaired = candidate + "]" * max(opens_square, 0) + "}" * max(opens_curly, 0)
    try:
        return json.loads(repaired)
    except json.JSONDecodeError as exc:
        raise LLMError(f"LLM response is not valid JSON: {text[:200]}") from exc


def call_llm(system_prompt: str, user_message: str, model: str | None = None) -> dict:
    """
    Call the Anthropic Messages API and return the parsed JSON response.

    Parameters
    ----------
    system_prompt : str
        The system instruction for the model.
    user_message : str
        The user message content.
    model : str, optional
        Override the model. Defaults to LLM_MODEL env var or claude-haiku-4-5-20251001.

    Returns
    -------
    dict
        Parsed JSON from the model's text response.

    Raises
    ------
    LLMError
        On API failure or if the response is not valid JSON.
    """
    api_key = os.environ.get("LLM_API_KEY")
    if not api_key:
        raise LLMError("LLM_API_KEY environment variable is not set")

    base_url = os.environ.get("LLM_API_BASE_URL")
    resolved_model = model or os.environ.get("LLM_MODEL", _DEFAULT_MODEL)

    client_kwargs = {"api_key": api_key}
    if base_url:
        client_kwargs["base_url"] = base_url

    client = anthropic.Anthropic(**client_kwargs)

    try:
        response = client.messages.create(
            model=resolved_model,
            max_tokens=2048,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )
    except Exception as exc:
        raise LLMError(f"Anthropic API call failed: {exc}") from exc

    text = response.content[0].text
    return _parse_json(text)
