"""llm_cleanser: LLM による operator 名正規化のユニットテスト。

LLM は mock (dict response を返す stub client)。実 LLM は叩かない。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest
from pydantic import BaseModel

from pizza_delivery.llm_cleanser import (
    CleanseResult,
    RerankPick,
    canonicalize_operator_name,
    rerank_candidates,
)


@dataclass
class StubReply:
    completion: Any


class StubLLM:
    def __init__(self, payload: Any) -> None:
        self.payload = payload
        self.calls = 0

    async def ainvoke(self, messages: Any, output_format: Any | None = None) -> StubReply:
        self.calls += 1
        if isinstance(self.payload, BaseModel):
            return StubReply(completion=self.payload)
        return StubReply(completion=self.payload)


# ─── canonicalize_operator_name ────────────────────────────────


def test_canonicalize_returns_llm_result() -> None:
    llm = StubLLM(
        CleanseResult(canonical="株式会社モスストアカンパニー", is_legal_entity=True, confidence=0.95)
    )
    r = asyncio.run(canonicalize_operator_name("㈱モスストアカンパニー", llm))
    assert r.canonical == "株式会社モスストアカンパニー"
    assert r.is_legal_entity is True
    assert r.confidence >= 0.9
    assert llm.calls == 1


def test_canonicalize_empty_name_no_call() -> None:
    llm = StubLLM(CleanseResult())
    r = asyncio.run(canonicalize_operator_name("", llm))
    assert r.canonical == ""
    assert llm.calls == 0


def test_canonicalize_graceful_on_llm_failure() -> None:
    class BrokenLLM:
        async def ainvoke(self, *a, **kw):
            raise RuntimeError("network")

    r = asyncio.run(canonicalize_operator_name("株式会社テスト", BrokenLLM()))
    # graceful: 原文保持 + confidence 0
    assert r.canonical == "株式会社テスト"
    assert r.confidence == 0.0


def test_canonicalize_fills_canonical_if_empty() -> None:
    llm = StubLLM(CleanseResult(canonical="", is_legal_entity=True, confidence=0.3))
    r = asyncio.run(canonicalize_operator_name("株式会社X", llm))
    # LLM が canonical を返さなかった場合は原文を補填
    assert r.canonical == "株式会社X"


# ─── rerank_candidates ─────────────────────────────────────────


def test_rerank_picks_best() -> None:
    llm = StubLLM(RerankPick(best_index=1, confidence=0.8, reason="商号完全一致"))
    r = asyncio.run(
        rerank_candidates(
            "株式会社モスストアカンパニー",
            ["株式会社モスバーガー", "株式会社モスストアカンパニー", "株式会社モスフードサービス"],
            llm,
        )
    )
    assert r.best_index == 1
    assert r.confidence >= 0.8


def test_rerank_out_of_range_clamped() -> None:
    """LLM が範囲外 index を返したら -1 に clamp。"""
    llm = StubLLM(RerankPick(best_index=99))
    r = asyncio.run(rerank_candidates("株式会社X", ["A", "B"], llm))
    assert r.best_index == -1


def test_rerank_empty_candidates_short_circuit() -> None:
    llm = StubLLM(RerankPick(best_index=0))
    r = asyncio.run(rerank_candidates("株式会社X", [], llm))
    assert r.best_index == -1
    assert llm.calls == 0


def test_rerank_graceful_on_llm_failure() -> None:
    class BrokenLLM:
        async def ainvoke(self, *a, **kw):
            raise RuntimeError("500")

    r = asyncio.run(rerank_candidates("株式会社X", ["A", "B"], BrokenLLM()))
    assert r.best_index == -1
