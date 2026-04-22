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
        """browser_use.llm.ChatAnthropic を返す。

        ANTHROPIC_API_KEY が未設定なら RuntimeError。
        model 指定がない場合は ANTHROPIC_MODEL env var、それも空なら default_model。
        """
        # Lazy import: test 環境で browser_use を避けたい時用
        from browser_use.llm import ChatAnthropic

        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError(
                "AnthropicProvider.make_llm requires ANTHROPIC_API_KEY env var"
            )
        chosen_model = model or os.getenv("ANTHROPIC_MODEL") or self.default_model
        return ChatAnthropic(
            model=chosen_model,
            api_key=api_key,
            **kwargs,
        )
