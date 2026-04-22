"""🔴 Red-phase tests for pizza_delivery.providers.registry.

registry レベルの動作は Phase 0 から Green (構造は完成)。
make_llm 実装は各 provider の Phase 3 Red ファイル参照。
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

        def make_llm(self, *, model: str | None = None, **_: object) -> str:
            return f"fake-llm:{model or self.default_model}"

    register_provider("fake", FakeProvider)
    try:
        p = get_provider("fake")
        assert p.name == "fake"
        assert p.make_llm() == "fake-llm:fake-1"
        assert p.make_llm(model="fake-2") == "fake-llm:fake-2"
    finally:
        from pizza_delivery.providers.registry import _PROVIDERS

        _PROVIDERS.pop("fake", None)


def test_provider_protocol_conformance() -> None:
    """各 provider が LLMProvider Protocol を満たすこと (runtime_checkable)。"""
    for name in available_providers():
        p = get_provider(name)
        # runtime_checkable Protocol による構造的型チェック
        assert isinstance(p, LLMProvider), f"{name} does not satisfy LLMProvider"
        assert hasattr(p, "name") and hasattr(p, "default_model")
        assert callable(getattr(p, "ready"))
        assert callable(getattr(p, "make_llm"))
