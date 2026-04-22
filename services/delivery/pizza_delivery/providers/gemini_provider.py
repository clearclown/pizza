"""Google Gemini provider — browser-use ChatGoogle をラップ。"""

from __future__ import annotations

import os
from typing import Any


class GeminiProvider:
    name: str = "gemini"
    default_model: str = "gemini-2.0-flash"

    def ready(self) -> bool:
        return bool(os.getenv("GEMINI_API_KEY"))

    def make_llm(self, *, model: str | None = None, **kwargs: Any) -> Any:
        from browser_use.llm import ChatGoogle

        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError(
                "GeminiProvider.make_llm requires GEMINI_API_KEY env var"
            )
        chosen_model = model or os.getenv("GEMINI_MODEL") or self.default_model
        return ChatGoogle(
            model=chosen_model,
            api_key=api_key,
            **kwargs,
        )
