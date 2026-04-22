"""Provider registry — LLM_PROVIDER 環境変数で選択できるようにする。"""

from __future__ import annotations

from .anthropic_provider import AnthropicProvider
from .base import LLMProvider
from .gemini_provider import GeminiProvider
from .openai_provider import OpenAIProvider

_PROVIDERS: dict[str, type] = {
    "anthropic": AnthropicProvider,
    "openai": OpenAIProvider,
    "gemini": GeminiProvider,
}


def get_provider(name: str) -> LLMProvider:
    """プロバイダ名から instance を返す。未知なら ValueError。"""
    if name not in _PROVIDERS:
        allowed = ", ".join(sorted(_PROVIDERS.keys()))
        raise ValueError(
            f"unknown LLM provider: {name!r} (allowed: {allowed})"
        )
    return _PROVIDERS[name]()  # type: ignore[return-value]


def register_provider(name: str, cls: type) -> None:
    """テスト / 拡張用にカスタムプロバイダを登録する。"""
    _PROVIDERS[name] = cls


def available_providers() -> list[str]:
    return sorted(_PROVIDERS.keys())
