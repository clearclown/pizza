"""🟢 Green: Phase 2 で make_llm() が実 ChatAnthropic を返す。"""

from __future__ import annotations

import pytest

from pizza_delivery.providers.anthropic_provider import AnthropicProvider


def test_anthropic_name_and_model() -> None:
    p = AnthropicProvider()
    assert p.name == "anthropic"
    assert p.default_model.startswith("claude-")


def test_anthropic_ready_checks_env(monkeypatch: pytest.MonkeyPatch) -> None:
    p = AnthropicProvider()
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert p.ready() is False
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-dummy")
    assert p.ready() is True


def test_anthropic_make_llm_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    p = AnthropicProvider()
    with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
        p.make_llm()


def test_anthropic_make_llm_returns_chat_anthropic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-dummy-for-unit-test")
    monkeypatch.delenv("ANTHROPIC_MODEL", raising=False)  # 外部 env を clean
    from browser_use.llm import ChatAnthropic

    p = AnthropicProvider()
    llm = p.make_llm()
    assert isinstance(llm, ChatAnthropic)
    # デフォルトモデルが使われる
    assert p.default_model in getattr(llm, "model", "")


def test_anthropic_make_llm_respects_model_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-dummy")
    monkeypatch.delenv("ANTHROPIC_MODEL", raising=False)
    p = AnthropicProvider()
    llm = p.make_llm(model="claude-sonnet-4-6")
    assert "claude-sonnet-4-6" in getattr(llm, "model", "")


def test_anthropic_make_llm_env_model_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-dummy")
    monkeypatch.setenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
    p = AnthropicProvider()
    llm = p.make_llm()
    assert "claude-haiku-4-5-20251001" in getattr(llm, "model", "")
