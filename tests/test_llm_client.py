"""
tests/test_llm_client.py — Tests for the Anthropic LLM client wrapper (Slice 6).

All Anthropic API calls are mocked.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.llm.client import LLMError, _parse_json, call_llm


def _mock_response(text: str) -> MagicMock:
    """Build a mock Anthropic Messages response."""
    block = MagicMock()
    block.text = text
    resp = MagicMock()
    resp.content = [block]
    return resp


def test_call_llm_returns_parsed_json():
    payload = {"persons": [], "companies": [], "signals": [], "summary": "test"}
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _mock_response(json.dumps(payload))

    with (
        patch.dict("os.environ", {"LLM_API_KEY": "test-key"}),
        patch("src.llm.client.anthropic") as mock_anthropic,
    ):
        mock_anthropic.Anthropic.return_value = mock_client
        result = call_llm("system prompt", "user message")

    assert result == payload
    mock_client.messages.create.assert_called_once()
    call_kwargs = mock_client.messages.create.call_args
    assert call_kwargs.kwargs["system"] == "system prompt"
    assert call_kwargs.kwargs["messages"] == [{"role": "user", "content": "user message"}]


def test_call_llm_uses_model_override():
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _mock_response('{"ok": true}')

    with (
        patch.dict("os.environ", {"LLM_API_KEY": "test-key"}),
        patch("src.llm.client.anthropic") as mock_anthropic,
    ):
        mock_anthropic.Anthropic.return_value = mock_client
        call_llm("sys", "usr", model="claude-sonnet-4-20250514")

    call_kwargs = mock_client.messages.create.call_args
    assert call_kwargs.kwargs["model"] == "claude-sonnet-4-20250514"


def test_call_llm_raises_on_missing_api_key():
    with (
        patch.dict("os.environ", {}, clear=True),
        pytest.raises(LLMError, match="LLM_API_KEY"),
    ):
        call_llm("sys", "usr")


def test_call_llm_raises_on_api_error():
    mock_client = MagicMock()
    mock_client.messages.create.side_effect = Exception("rate limit exceeded")

    with (
        patch.dict("os.environ", {"LLM_API_KEY": "test-key"}),
        patch("src.llm.client.anthropic") as mock_anthropic,
        pytest.raises(LLMError, match="API call failed"),
    ):
        mock_anthropic.Anthropic.return_value = mock_client
        call_llm("sys", "usr")


def test_call_llm_raises_on_invalid_json():
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _mock_response("not json at all")

    with (
        patch.dict("os.environ", {"LLM_API_KEY": "test-key"}),
        patch("src.llm.client.anthropic") as mock_anthropic,
        pytest.raises(LLMError, match="not valid JSON"),
    ):
        mock_anthropic.Anthropic.return_value = mock_client
        call_llm("sys", "usr")


def test_call_llm_reads_base_url_from_env():
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _mock_response('{"ok": true}')

    with (
        patch.dict(
            "os.environ",
            {"LLM_API_KEY": "test-key", "LLM_API_BASE_URL": "https://custom.api.com"},
        ),
        patch("src.llm.client.anthropic") as mock_anthropic,
    ):
        mock_anthropic.Anthropic.return_value = mock_client
        call_llm("sys", "usr")

    init_kwargs = mock_anthropic.Anthropic.call_args.kwargs
    assert init_kwargs["base_url"] == "https://custom.api.com"


# --- _parse_json tests ---


def test_parse_json_clean():
    assert _parse_json('{"a": 1}') == {"a": 1}


def test_parse_json_with_code_fences():
    text = '```json\n{"a": 1}\n```'
    assert _parse_json(text) == {"a": 1}


def test_parse_json_with_surrounding_text():
    text = 'Here is the result:\n{"a": 1}\nDone!'
    assert _parse_json(text) == {"a": 1}


def test_parse_json_truncated():
    text = '{"persons": [{"name": "Alice"}], "signals": ['
    result = _parse_json(text)
    assert result["persons"] == [{"name": "Alice"}]
    assert result["signals"] == []
