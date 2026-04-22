"""Anthropic Claude provider — browser-use ChatAnthropic をラップ。"""

from __future__ import annotations

import os
from typing import Any


class AnthropicProvider:
    name: str = "anthropic"
    default_model: str = "claude-opus-4-7"

    def ready(self) -> bool:
        return bool(os.getenv("ANTHROPIC_API_KEY"))

    def make_llm(self, *, model: str | None = None, **kwargs: Any) -> Any:
        """browser_use.llm.ChatAnthropic を返す (Phase 3 で活性化)。"""
        _ = model, kwargs
        raise NotImplementedError(
            "AnthropicProvider.make_llm is Phase 3 target; "
            "will return browser_use.llm.ChatAnthropic"
        )
