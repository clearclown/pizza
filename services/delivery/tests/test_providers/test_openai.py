"""OpenAI provider coverage."""

from __future__ import annotations

import pytest

from pizza_delivery.providers.openai_provider import OpenAIProvider


def test_openai_name_and_model() -> None:
    p = OpenAIProvider()
    assert p.name == "openai"
    assert p.default_model.startswith("gpt-")


def test_openai_ready_checks_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert OpenAIProvider().ready() is False
    monkeypatch.setenv("OPENAI_API_KEY", "dummy")
    assert OpenAIProvider().ready() is True


def test_openai_make_llm_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        OpenAIProvider().make_llm()


def test_openai_make_llm_returns_chat_openai(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "dummy-for-unit-test")
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    from browser_use.llm import ChatOpenAI

    llm = OpenAIProvider().make_llm()
    assert isinstance(llm, ChatOpenAI)


def test_openai_make_llm_respects_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "dummy")
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    llm = OpenAIProvider().make_llm(model="gpt-4o-mini")
    assert "gpt-4o-mini" in getattr(llm, "model", "")


def test_openai_make_llm_env_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "dummy")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-4o-2024-08-06")
    llm = OpenAIProvider().make_llm()
    assert "gpt-4o-2024-08-06" in getattr(llm, "model", "")
