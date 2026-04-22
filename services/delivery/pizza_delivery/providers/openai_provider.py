"""OpenAI GPT provider. Phase 0: stub."""

from __future__ import annotations

import os


class OpenAIProvider:
    name: str = "openai"
    default_model: str = "gpt-4o"

    def ready(self) -> bool:
        return bool(os.getenv("OPENAI_API_KEY"))

    def chat(self, *, system: str, user: str, model: str | None = None) -> str:
        _ = system, user, model
        raise NotImplementedError("OpenAIProvider.chat is Phase 3 target")
