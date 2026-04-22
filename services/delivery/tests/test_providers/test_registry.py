"""🔴 Red-phase tests for pizza_delivery.providers.registry.

Phase 0 時点ではスタブ実装だが、registry レベルの構造は最初から正しい。
本テストは Green で通る（registry は実装済）。agent 層以降の Red は別ファイル。
"""

from __future__ import annotations

import pytest

from pizza_delivery.providers import get_provider, register_provider
from pizza_delivery.providers.base import LLMProvider
from pizza_delivery.providers.registry import available_providers


@pytest.mark.parametrize("name", ["anthropic", "openai", "gemini"])
def test_default_providers_instantiate(name: str) -> None:
    provider = get_provider(name)
    assert provider.name == name
    assert isinstance(provider.default_model, str)
    assert provider.default_model  # non-empty


def test_unknown_provider_raises() -> None:
    with pytest.raises(ValueError, match="unknown LLM provider"):
        get_provider("nonexistent")


def test_register_custom_provider() -> None:
    class FakeProvider:
        name = "fake"
        default_model = "fake-1"

        def ready(self) -> bool:
            return True

        def chat(self, *, system: str, user: str, model: str | None = None) -> str:
            return f"fake:{user}"

    register_provider("fake", FakeProvider)
    try:
        p = get_provider("fake")
        assert p.name == "fake"
        assert p.chat(system="", user="hi") == "fake:hi"
    finally:
        # clean up to avoid leaking into other tests
        from pizza_delivery.providers.registry import _PROVIDERS

        _PROVIDERS.pop("fake", None)


def test_provider_protocol_conformance() -> None:
    """各 provider が LLMProvider Protocol を満たすこと。"""
    for name in available_providers():
        p = get_provider(name)
        # Protocol 適合の実質チェック
        assert hasattr(p, "name")
        assert hasattr(p, "default_model")
        assert callable(getattr(p, "ready", None))
        assert callable(getattr(p, "chat", None))
        # 型的にも LLMProvider として使えること
        _: LLMProvider = p  # noqa: F841
