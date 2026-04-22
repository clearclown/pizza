"""🔴 Red-phase test — Phase 3 で Green 化する。"""

from __future__ import annotations

import pytest

from pizza_delivery.providers.anthropic_provider import AnthropicProvider


def test_anthropic_chat_raises_until_phase3() -> None:
    """Phase 0 baseline: 未実装であることを明示的に契約する。"""
    p = AnthropicProvider()
    with pytest.raises(NotImplementedError, match="Phase 3"):
        p.chat(system="system prompt", user="hello")


def test_anthropic_name_and_model() -> None:
    p = AnthropicProvider()
    assert p.name == "anthropic"
    assert p.default_model.startswith("claude-")
