"""Gemini provider coverage."""

from __future__ import annotations

import pytest

from pizza_delivery.providers.gemini_provider import GeminiProvider


def test_gemini_name_and_model() -> None:
    p = GeminiProvider()
    assert p.name == "gemini"
    assert p.default_model.startswith("gemini-")


def test_gemini_ready_checks_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    assert GeminiProvider().ready() is False
    monkeypatch.setenv("GEMINI_API_KEY", "dummy")
    assert GeminiProvider().ready() is True


def test_gemini_make_llm_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="GEMINI_API_KEY"):
        GeminiProvider().make_llm()


def test_gemini_make_llm_returns_chat_google(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "dummy-for-unit-test")
    monkeypatch.delenv("GEMINI_MODEL", raising=False)
    from browser_use.llm import ChatGoogle

    llm = GeminiProvider().make_llm()
    assert isinstance(llm, ChatGoogle)


def test_gemini_make_llm_respects_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "dummy")
    monkeypatch.delenv("GEMINI_MODEL", raising=False)
    llm = GeminiProvider().make_llm(model="gemini-2.5-flash")
    assert "gemini-2.5-flash" in getattr(llm, "model", "")


def test_gemini_make_llm_env_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "dummy")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-pro")
    llm = GeminiProvider().make_llm()
    assert "gemini-2.5-pro" in getattr(llm, "model", "")
