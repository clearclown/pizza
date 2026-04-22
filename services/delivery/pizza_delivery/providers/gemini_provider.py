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
        _ = model, kwargs
        raise NotImplementedError(
            "GeminiProvider.make_llm is Phase 3 target; "
            "will return browser_use.llm.ChatGoogle"
        )
