"""🔴 Red-phase test — Phase 3 で Green 化する。"""

from __future__ import annotations

import pytest

from pizza_delivery.providers.anthropic_provider import AnthropicProvider


def test_anthropic_make_llm_raises_until_phase3() -> None:
    """Phase 0 baseline: make_llm() は browser-use ChatAnthropic を返す未来の契約。"""
    p = AnthropicProvider()
    with pytest.raises(NotImplementedError, match="Phase 3"):
        p.make_llm()


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
