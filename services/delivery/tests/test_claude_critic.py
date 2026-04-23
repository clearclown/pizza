"""ClaudeCritic unit tests — LLM を mock して、prompt 構築 + JSON 抽出を検証。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from pizza_delivery.agent import JudgeReply
from pizza_delivery.claude_critic import ClaudeCritic, CriticJudgementJSON, _extract
from pizza_delivery.evidence import Evidence


def _reply(fran: str = "株式会社A", op: str = "franchisee") -> JudgeReply:
    return JudgeReply(
        place_id="p1",
        is_franchise=(op != "direct"),
        operator_name=fran,
        store_count_estimate=0,
        confidence=0.8,
        llm_provider="mock",
        llm_model="mock",
        reasoning="r",
        operation_type=op,
        franchisor_name="",
        franchisee_name=fran,
    )


@dataclass
class FakeCompletion:
    completion: Any


class StubLLM:
    def __init__(self, judgement: CriticJudgementJSON):
        self.judgement = judgement
        self.last_messages: list[Any] | None = None
        self.last_output_format: type | None = None

    async def ainvoke(self, messages, output_format=None, **kw):
        self.last_messages = messages
        self.last_output_format = output_format
        return FakeCompletion(completion=self.judgement)


@pytest.mark.asyncio
async def test_critic_passes_payload_to_llm() -> None:
    stub = StubLLM(
        CriticJudgementJSON(
            verdict="agree_both",
            preferred_side="both",
            critique="ok",
            confidence_adjustment=0.0,
        )
    )
    critic = ClaudeCritic(llm=stub)
    ev = [
        Evidence(
            source_url="https://x/",
            snippet="会社名: 株式会社A",
            reason="operator_keyword",
            keyword="会社名",
        )
    ]
    res = await critic.critique(
        reply_a=_reply(),
        reply_b=_reply(),
        evidences=ev,
        kb_conflict_flags=["fitplace"],
    )
    assert res.verdict == "agree_both"
    assert res.preferred_side == "both"

    # prompt に payload JSON が埋まったことを確認
    user_content = stub.last_messages[-1].content
    assert "worker_a" in user_content
    assert "worker_b" in user_content
    assert "kb_conflict_flags" in user_content
    assert "fitplace" in user_content
    assert "株式会社A" in user_content
    # output_format は CriticJudgementJSON
    assert stub.last_output_format is CriticJudgementJSON


@pytest.mark.asyncio
async def test_critic_handles_kb_override_flag() -> None:
    stub = StubLLM(
        CriticJudgementJSON(
            verdict="agree_both",
            preferred_side="both",
            critique="evidence 明確",
            confidence_adjustment=+0.1,
            kb_conflict_overridden=True,
        )
    )
    critic = ClaudeCritic(llm=stub)
    res = await critic.critique(reply_a=_reply(), reply_b=_reply(), evidences=[])
    assert res.kb_conflict_overridden is True
    assert res.confidence_adjustment == pytest.approx(0.1)


def test_extract_from_pydantic() -> None:
    j = CriticJudgementJSON(
        verdict="prefer_a", preferred_side="a", critique="", confidence_adjustment=0.0
    )
    assert _extract(j) is j


def test_extract_from_wrapper_with_completion_attr() -> None:
    j = CriticJudgementJSON(
        verdict="prefer_b", preferred_side="b", critique="", confidence_adjustment=0.0
    )

    @dataclass
    class Wrap:
        completion: Any

    assert _extract(Wrap(completion=j)) is j


def test_extract_from_dict() -> None:
    d = {
        "verdict": "uncertain",
        "preferred_side": "both",
        "critique": "",
        "confidence_adjustment": -0.1,
    }
    parsed = _extract(d)
    assert parsed.verdict == "uncertain"
    assert parsed.confidence_adjustment == pytest.approx(-0.1)


def test_extract_from_json_string() -> None:
    s = (
        '{"verdict":"agree_both","preferred_side":"both",'
        '"critique":"","confidence_adjustment":0.0}'
    )
    assert _extract(s).verdict == "agree_both"


def test_extract_rejects_unknown_type() -> None:
    with pytest.raises(RuntimeError, match="unexpected completion type"):
        _extract(123)
