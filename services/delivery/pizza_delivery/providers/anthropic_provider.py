"""Anthropic Claude provider.

Phase 0: スタブ。Phase 3 で anthropic SDK を使って chat を実装する。
"""

from __future__ import annotations

import os


class AnthropicProvider:
    name: str = "anthropic"
    default_model: str = "claude-opus-4-7"

    def ready(self) -> bool:
        return bool(os.getenv("ANTHROPIC_API_KEY"))

    def chat(self, *, system: str, user: str, model: str | None = None) -> str:
        _ = system, user, model
        raise NotImplementedError("AnthropicProvider.chat is Phase 3 target")
