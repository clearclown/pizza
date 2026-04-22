"""LLM provider Protocol — 全プロバイダが満たすべき統一 interface。

PI-ZZA では Delivery モジュールで browser-use の Agent に LLM を注入する。
browser-use は `Agent(task=..., llm=ChatAnthropic(...))` という API なので、
各 provider は browser_use.llm 互換の LLM インスタンスを返す `make_llm()`
メソッドを提供する。
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class LLMProvider(Protocol):
    """統一 LLM interface。

    browser-use と噛み合う provider 抽象:
      - name / default_model: 識別
      - ready(): 環境変数 (API キー) が整っているか
      - make_llm(model=None): browser_use.llm.Chat* と同じ interface を持つ
        LLM インスタンスを返す。Phase 3 で各 provider が実装する。
    """

    name: str
    default_model: str

    def ready(self) -> bool:
        """API キーが環境変数にセットされているかを返す。"""
        ...

    def make_llm(self, *, model: str | None = None, **kwargs: Any) -> Any:
        """browser-use 互換の LLM インスタンスを生成する。

        Phase 0 では NotImplementedError。Phase 3 で browser_use.llm.Chat*
        (ChatAnthropic / ChatOpenAI / ChatGoogle) を返す。
        """
        ...
