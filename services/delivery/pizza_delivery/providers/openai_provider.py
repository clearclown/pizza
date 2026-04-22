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
        _ = model, kwargs
        raise NotImplementedError(
            "OpenAIProvider.make_llm is Phase 3 target; "
            "will return browser_use.llm.ChatOpenAI"
        )
