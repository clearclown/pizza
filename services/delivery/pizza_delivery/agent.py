"""browser-use wrapper.

Phase 0: シグネチャのみ。Phase 3 で browser-use + provider 統合を実装する。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class JudgeRequest:
    place_id: str
    brand: str
    name: str
    markdown: str
    provider_hint: str = ""


@dataclass
class JudgeReply:
    is_franchise: bool
    operator_name: str
    store_count_estimate: int
    confidence: float
    llm_provider: str
    llm_model: str


async def judge_franchise(req: JudgeRequest) -> JudgeReply:
    """browser-use エージェントを走らせ、FC か否かを判定する。

    Phase 0: NotImplementedError。
    """
    _ = req
    raise NotImplementedError("agent.judge_franchise is Phase 3 target")
