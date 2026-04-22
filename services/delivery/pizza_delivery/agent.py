"""browser-use wrapper — Phase 0 は骨格、Phase 3 で本実装。

Phase 3 での想定実装:
    from browser_use import Agent
    from pizza_delivery.providers import get_provider

    provider = get_provider(os.getenv("LLM_PROVIDER", "anthropic"))
    llm = provider.make_llm()  # browser_use.llm.Chat* 互換
    agent = Agent(task=prompt_from_store(req), llm=llm)
    result = await agent.run()
    return parse_judgement(result)
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class JudgeRequest:
    place_id: str
    brand: str
    name: str
    markdown: str
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


async def judge_franchise(req: JudgeRequest) -> JudgeReply:
    """browser-use エージェントを走らせ、FC か否かを判定する。

    Phase 0: NotImplementedError (契約のみ)。
    Phase 3: browser_use.Agent + provider.make_llm() で実装。
    """
    _ = req
    raise NotImplementedError("agent.judge_franchise is Phase 3 target")
