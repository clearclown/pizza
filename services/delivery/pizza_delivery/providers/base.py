"""LLM provider Protocol — 全プロバイダが満たすべき統一 interface。"""

from __future__ import annotations

from typing import Protocol


class LLMProvider(Protocol):
    """統一 LLM interface。

    本 Protocol はダックタイピングで判定される。各 provider 実装は
    属性 ``name``, ``default_model`` とメソッド ``chat``, ``ready`` を
    持てば自動的にこの型を満たす。
    """

    name: str
    default_model: str

    def ready(self) -> bool:
        """API キーが環境変数にセットされているかを返す。"""
        ...

    def chat(self, *, system: str, user: str, model: str | None = None) -> str:
        """1 回の chat completion を実行し、assistant の文字列応答を返す。

        Phase 0 では未実装。Phase 3 で各 provider が実装する。
        """
        ...
