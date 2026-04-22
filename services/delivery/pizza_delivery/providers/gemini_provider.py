"""Google Gemini provider. Phase 0: stub."""

from __future__ import annotations

import os


class GeminiProvider:
    name: str = "gemini"
    default_model: str = "gemini-2.0-flash"

    def ready(self) -> bool:
        return bool(os.getenv("GEMINI_API_KEY"))

    def chat(self, *, system: str, user: str, model: str | None = None) -> str:
        _ = system, user, model
        raise NotImplementedError("GeminiProvider.chat is Phase 3 target")
