"""browser-use + LLM による FC 判定エージェント。

Phase 2 実装方針:
  - 既に Firecrawl が Markdown を抽出済なので、**通常は browser 起動不要**
  - ChatLLM.ainvoke に structured output (Pydantic) を要求し、JSON を直接パース
  - LLM 単独で confidence が低い / Markdown が空 の場合のみ browser fallback
    (Phase 3 で実装、現在は confidence そのまま返す)

テスト戦略:
  - Unit: mock LLM (ainvoke が事前定義の JudgeJSON を返す) で judge_franchise を検証
  - Live: build tag / env gate で実 ChatAnthropic を叩く
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Protocol

import yaml
from pydantic import BaseModel, Field


# ─── Data classes ──────────────────────────────────────────────────────


@dataclass
class JudgeRequest:
    place_id: str
    brand: str
    name: str
    markdown: str
    address: str = ""
    official_url: str = ""
    candidate_urls: list[str] = field(default_factory=list)
    provider_hint: str = ""


@dataclass
class JudgeReply:
    place_id: str
    is_franchise: bool
    operator_name: str
    store_count_estimate: int
    confidence: float
    llm_provider: str
    llm_model: str
    reasoning: str = ""


class JudgeJSON(BaseModel):
    """LLM から structured output で返してほしい JSON スキーマ。"""

    is_franchise: bool
    operator_name: str = Field(default="")
    store_count_estimate: int = Field(default=0, ge=0)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    reasoning: str = Field(default="")


# ─── LLM Protocol (for mocking) ────────────────────────────────────────


class LLMClient(Protocol):
    """browser_use.llm.Chat* が満たすべき最小 interface。"""

    async def ainvoke(
        self,
        messages: list[Any],
        output_format: Optional[type] = None,
        **kwargs: Any,
    ) -> Any: ...


# ─── Prompt loading ────────────────────────────────────────────────────

_PROMPT_PATH = Path(__file__).parent / "prompts" / "judge.yaml"


def _load_prompt() -> dict[str, str]:
    with _PROMPT_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ─── Core judge_franchise ──────────────────────────────────────────────


async def judge_franchise(
    req: JudgeRequest,
    *,
    llm: LLMClient | None = None,
    provider_name: str = "",
    model_name: str = "",
) -> JudgeReply:
    """browser-use LLM で FC 判定する。

    llm が None なら `LLM_PROVIDER` env から provider を解決して make_llm() で生成。
    llm を注入するとテストで固定応答を返せる。
    """
    if llm is None:
        from pizza_delivery.providers import get_provider

        provider_name = provider_name or os.getenv("LLM_PROVIDER", "anthropic")
        provider = get_provider(provider_name)
        llm = provider.make_llm()
        if not model_name:
            model_name = getattr(llm, "model", "") or getattr(llm, "model_name", "")
    # Lazy import to keep test-light import path
    from browser_use.llm.messages import SystemMessage, UserMessage

    prompt = _load_prompt()
    system_msg = SystemMessage(content=prompt["system"])
    user_msg = UserMessage(
        content=prompt["task"].format(
            brand=req.brand,
            name=req.name,
            address=req.address or "(不明)",
            official_url=req.official_url or "(不明)",
            markdown=req.markdown[:5000] if req.markdown else "(Markdown 未取得)",
            candidate_urls="\n".join(req.candidate_urls) or "(なし)",
        )
    )
    completion = await llm.ainvoke([system_msg, user_msg], output_format=JudgeJSON)
    parsed = _extract_judge_json(completion)
    return JudgeReply(
        place_id=req.place_id,
        is_franchise=parsed.is_franchise,
        operator_name=parsed.operator_name,
        store_count_estimate=parsed.store_count_estimate,
        confidence=parsed.confidence,
        llm_provider=provider_name or "unknown",
        llm_model=model_name or "unknown",
        reasoning=parsed.reasoning,
    )


def _extract_judge_json(completion: Any) -> JudgeJSON:
    """ChatInvokeCompletion から JudgeJSON を抽出する。

    completion には .completion 属性 (モデル応答) があり、output_format 指定時は
    parsed instance 自体 or .parsed 属性に JudgeJSON が入る。互換のため両方試す。
    """
    # output_format を指定した場合、browser-use は instance of output_format を返す
    payload = getattr(completion, "completion", completion)
    if isinstance(payload, JudgeJSON):
        return payload
    # Fallback: 文字列 JSON の場合は自力でパース
    if isinstance(payload, str):
        # LLM が ``` で囲んだり前置きを付ける場合に備えて最初の '{' から最後の '}' を抽出
        s = payload.strip()
        start = s.find("{")
        end = s.rfind("}")
        if start >= 0 and end > start:
            s = s[start : end + 1]
        data = json.loads(s)
        return JudgeJSON.model_validate(data)
    # dict の場合
    if isinstance(payload, dict):
        return JudgeJSON.model_validate(payload)
    raise ValueError(f"cannot extract JudgeJSON from {type(payload).__name__}: {payload!r}")
