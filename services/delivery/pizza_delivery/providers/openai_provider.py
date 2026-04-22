"""OpenAI provider — browser-use ChatOpenAI をラップ。"""

from __future__ import annotations

import os
from typing import Any


class OpenAIProvider:
    name: str = "openai"
    default_model: str = "gpt-4o"

    def ready(self) -> bool:
        return bool(os.getenv("OPENAI_API_KEY"))

    def make_llm(self, *, model: str | None = None, **kwargs: Any) -> Any:
        from browser_use.llm import ChatOpenAI

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError(
                "OpenAIProvider.make_llm requires OPENAI_API_KEY env var"
            )
        chosen_model = model or os.getenv("OPENAI_MODEL") or self.default_model
        return ChatOpenAI(
            model=chosen_model,
            api_key=api_key,
            **kwargs,
        )
