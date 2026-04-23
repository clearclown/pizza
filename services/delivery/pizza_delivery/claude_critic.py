"""ClaudeCritic — Claude を Panel の critic 役として駆動する実装。

Claude にクリティカルシンキングを行わせて、Worker 2 基の出力と Layer A KB hit を
総合評価し CriticJudgement を返す。

プロンプトは `prompts/critic_v1.yaml` に切り離し、差し替え可能。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from pizza_delivery.agent import JudgeReply
from pizza_delivery.evidence import Evidence
from pizza_delivery.panel import CriticJudgement


_PROMPT_PATH = Path(__file__).parent / "prompts" / "critic_v1.yaml"


class CriticJudgementJSON(BaseModel):
    """LLM から structured output で返してもらう critic 判断 JSON。"""

    verdict: str = Field(
        description="agree_both | prefer_a | prefer_b | both_wrong | uncertain"
    )
    preferred_side: str = Field(description="a | b | both | neither")
    critique: str = Field(default="", description="200 字以内で根拠")
    confidence_adjustment: float = Field(default=0.0, ge=-1.0, le=0.2)
    kb_conflict_overridden: bool = Field(
        default=False, description="KB hit を overrule したか"
    )


def _load_prompt() -> dict[str, str]:
    with _PROMPT_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _summarize_reply(label: str, r: JudgeReply) -> dict[str, Any]:
    return {
        "worker": label,
        "operation_type": r.operation_type,
        "franchisor_name": r.franchisor_name,
        "franchisee_name": r.franchisee_name,
        "confidence": r.confidence,
        "reasoning": r.reasoning[:300],
    }


def _summarize_evidences(evs: list[Evidence], limit: int = 10) -> list[dict[str, str]]:
    return [
        {
            "source_url": e.source_url,
            "snippet": e.snippet[:400],
            "reason": e.reason,
            "keyword": e.keyword,
        }
        for e in evs[:limit]
    ]


@dataclass
class ClaudeCritic:
    """Anthropic Claude で critic を走らせる。"""

    llm: Any  # browser_use.llm.ChatAnthropic 相当
    model_name: str = ""

    async def critique(
        self,
        *,
        reply_a: JudgeReply,
        reply_b: JudgeReply,
        evidences: list[Evidence],
        kb_conflict_flags: list[str] | None = None,
    ) -> CriticJudgement:
        from browser_use.llm.messages import SystemMessage, UserMessage

        prompt = _load_prompt()
        payload = {
            "worker_a": _summarize_reply("a", reply_a),
            "worker_b": _summarize_reply("b", reply_b),
            "evidences": _summarize_evidences(evidences),
            "kb_conflict_flags": kb_conflict_flags or [],
        }
        system_msg = SystemMessage(content=prompt["system"])
        user_msg = UserMessage(
            content=prompt["task"].format(
                payload_json=json.dumps(payload, ensure_ascii=False, indent=2)
            )
        )
        completion = await self.llm.ainvoke(
            [system_msg, user_msg], output_format=CriticJudgementJSON
        )
        parsed = _extract(completion)
        return CriticJudgement(
            verdict=parsed.verdict,
            preferred_side=parsed.preferred_side,
            critique=parsed.critique,
            confidence_adjustment=parsed.confidence_adjustment,
            kb_conflict_overridden=parsed.kb_conflict_overridden,
        )


def _extract(completion: Any) -> CriticJudgementJSON:
    """LLM completion から CriticJudgementJSON を取り出す (色々なラッパーに耐える)。"""
    # パターン 1: 直接 pydantic 型
    if isinstance(completion, CriticJudgementJSON):
        return completion
    # パターン 2: .completion 属性に入っている
    inner = getattr(completion, "completion", None)
    if isinstance(inner, CriticJudgementJSON):
        return inner
    if isinstance(inner, dict):
        return CriticJudgementJSON.model_validate(inner)
    if isinstance(inner, str):
        return CriticJudgementJSON.model_validate_json(inner)
    # パターン 3: dict
    if isinstance(completion, dict):
        return CriticJudgementJSON.model_validate(completion)
    # パターン 4: JSON 文字列
    if isinstance(completion, str):
        return CriticJudgementJSON.model_validate_json(completion)
    raise RuntimeError(
        f"ClaudeCritic: unexpected completion type {type(completion).__name__}"
    )
