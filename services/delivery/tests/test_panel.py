"""Expert Panel — 組織設計テスト。

組織:
  Worker A (Gemini Flash, 設定 A) ┐
                                  ├─→ Critic (Claude) が critical thinking 評価
  Worker B (Gemini Flash, 設定 B) ┘

このテストは決定論パス + Mock Critic で動作検証する。
live テストは test_live_panel.py。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from pizza_delivery.agent import JudgeReply, JudgeRequest
from pizza_delivery.evidence import Evidence
from pizza_delivery.panel import (
    CriticJudgement,
    ExpertPanel,
    PanelVerdict,
)


# ─── helpers ───────────────────────────────────────────────────────────


def _reply(
    *,
    place_id: str = "p1",
    operation_type: str = "franchisee",
    franchisor: str = "",
    franchisee: str = "株式会社ABC",
    confidence: float = 0.8,
    provider: str = "mock",
    model: str = "mock-1",
) -> JudgeReply:
    return JudgeReply(
        place_id=place_id,
        is_franchise=(operation_type != "direct"),
        operator_name=franchisee or franchisor,
        store_count_estimate=0,
        confidence=confidence,
        llm_provider=provider,
        llm_model=model,
        reasoning="mock",
        operation_type=operation_type,
        franchisor_name=franchisor,
        franchisee_name=franchisee,
        judge_mode="evidence",
    )


def _req() -> JudgeRequest:
    return JudgeRequest(
        place_id="p1",
        brand="B",
        name="N",
        markdown="",
        official_url="https://example.com/",
    )


def _scripted_judge(reply_a: JudgeReply, reply_b: JudgeReply):
    """2 worker を provider_name で振り分けて返す scripted judge_by_evidence。"""
    async def _fn(req: JudgeRequest, *, llm=None, provider_name: str = "", **kw) -> JudgeReply:
        if provider_name == "worker-a":
            out = reply_a
        elif provider_name == "worker-b":
            out = reply_b
        else:
            raise AssertionError(f"unexpected provider_name: {provider_name}")
        out.place_id = req.place_id
        return out
    return _fn


class ScriptedCritic:
    """Claude 相当の critic mock: 事前設定した CriticJudgement を返す。"""
    def __init__(self, judgement: CriticJudgement) -> None:
        self.judgement = judgement
        self.called_with: dict[str, Any] = {}

    async def critique(
        self,
        *,
        reply_a: JudgeReply,
        reply_b: JudgeReply,
        evidences: list[Evidence],
        kb_conflict_flags: list[str] | None = None,
    ) -> CriticJudgement:
        self.called_with = {
            "a": reply_a,
            "b": reply_b,
            "evidences": evidences,
            "kb_conflict_flags": kb_conflict_flags,
        }
        return self.judgement


# ─── Tests ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_panel_both_agree_critic_approves() -> None:
    a = _reply(franchisee="株式会社ABC", confidence=0.9)
    b = _reply(franchisee="株式会社ABC", confidence=0.8)
    critic = ScriptedCritic(
        CriticJudgement(
            verdict="agree_both",
            critique="両者が同一の operator を返し、evidence に整合",
            preferred_side="both",
            confidence_adjustment=+0.05,
        )
    )
    panel = ExpertPanel(
        worker_a_llm=object(),
        worker_b_llm=object(),
        critic=critic,
        _judge_fn=_scripted_judge(a, b),
    )
    v = await panel.deliberate(_req())
    assert isinstance(v, PanelVerdict)
    assert v.worker_a is a
    assert v.worker_b is b
    assert v.critic_judgement.verdict == "agree_both"
    assert v.final_operation_type == "franchisee"
    assert v.final_franchisee == "株式会社ABC"
    # 合意 + critic approve 加算: min(0.9, 0.8) + 0.05 = 0.85
    assert v.final_confidence == pytest.approx(0.85)


@pytest.mark.asyncio
async def test_panel_critic_prefers_a_over_b() -> None:
    a = _reply(franchisee="株式会社A", confidence=0.9)
    b = _reply(franchisee="株式会社B", confidence=0.7)
    critic = ScriptedCritic(
        CriticJudgement(
            verdict="prefer_a",
            critique="evidence は A を支持",
            preferred_side="a",
            confidence_adjustment=-0.1,
        )
    )
    panel = ExpertPanel(
        worker_a_llm=object(), worker_b_llm=object(),
        critic=critic, _judge_fn=_scripted_judge(a, b),
    )
    v = await panel.deliberate(_req())
    # Critic が A を preferred としたので A の回答が採用
    assert v.final_franchisee == "株式会社A"
    # 不一致 + critic 減点: a.conf (0.9) + (-0.1) = 0.8
    assert v.final_confidence == pytest.approx(0.8)


@pytest.mark.asyncio
async def test_panel_critic_prefers_b_over_a() -> None:
    a = _reply(franchisee="株式会社A")
    b = _reply(franchisee="株式会社B", confidence=0.75)
    critic = ScriptedCritic(
        CriticJudgement(
            verdict="prefer_b", preferred_side="b",
            critique="", confidence_adjustment=0.0,
        )
    )
    panel = ExpertPanel(
        worker_a_llm=object(), worker_b_llm=object(),
        critic=critic, _judge_fn=_scripted_judge(a, b),
    )
    v = await panel.deliberate(_req())
    assert v.final_franchisee == "株式会社B"
    assert v.final_confidence == pytest.approx(0.75)


@pytest.mark.asyncio
async def test_panel_critic_rejects_both() -> None:
    a = _reply(franchisee="株式会社X", confidence=0.8)
    b = _reply(franchisee="株式会社Y", confidence=0.7)
    critic = ScriptedCritic(
        CriticJudgement(
            verdict="both_wrong", preferred_side="neither",
            critique="evidence に operator 情報なし — 推論されている",
            confidence_adjustment=-0.5,
        )
    )
    panel = ExpertPanel(
        worker_a_llm=object(), worker_b_llm=object(),
        critic=critic, _judge_fn=_scripted_judge(a, b),
    )
    v = await panel.deliberate(_req())
    # 両却下 → operator_name を unknown に
    assert v.final_operation_type == "unknown"
    assert v.final_franchisor == ""
    assert v.final_franchisee == ""
    assert v.final_confidence < 0.5


@pytest.mark.asyncio
async def test_panel_critic_uncertain_keeps_consensus_halved() -> None:
    a = _reply(franchisee="株式会社A", confidence=0.6)
    b = _reply(franchisee="株式会社A", confidence=0.5)
    critic = ScriptedCritic(
        CriticJudgement(
            verdict="uncertain", preferred_side="both",
            critique="evidence 不十分",
            confidence_adjustment=-0.2,
        )
    )
    panel = ExpertPanel(
        worker_a_llm=object(), worker_b_llm=object(),
        critic=critic, _judge_fn=_scripted_judge(a, b),
    )
    v = await panel.deliberate(_req())
    # 合意だが critic 減点 : min(0.6, 0.5) + (-0.2) = 0.3
    assert v.final_franchisee == "株式会社A"
    assert v.final_confidence == pytest.approx(0.3)


@pytest.mark.asyncio
async def test_panel_passes_evidence_to_critic() -> None:
    a = _reply()
    b = _reply()
    critic = ScriptedCritic(
        CriticJudgement(
            verdict="agree_both", preferred_side="both",
            critique="", confidence_adjustment=0.0,
        )
    )
    panel = ExpertPanel(
        worker_a_llm=object(), worker_b_llm=object(),
        critic=critic, _judge_fn=_scripted_judge(a, b),
    )
    evidences = [
        Evidence(
            source_url="https://x/",
            snippet="snippet",
            reason="operator_keyword",
            keyword="運営会社",
        )
    ]

    class StubCollector:
        async def collect(self, **kw):
            return evidences

    # panel 側で evidence_collector が両 worker 共通で呼ばれ、critic にも渡される
    await panel.deliberate(_req(), evidence_collector=StubCollector())
    assert critic.called_with["evidences"] == evidences


@pytest.mark.asyncio
async def test_panel_run_many_aggregates() -> None:
    """複数 request を順次 panel で捌く convenience。"""
    from pizza_delivery.panel import deliberate_many

    a = _reply(franchisee="株式会社A")
    b = _reply(franchisee="株式会社A")
    critic = ScriptedCritic(
        CriticJudgement(
            verdict="agree_both", preferred_side="both",
            critique="", confidence_adjustment=0.0,
        )
    )
    panel = ExpertPanel(
        worker_a_llm=object(), worker_b_llm=object(),
        critic=critic, _judge_fn=_scripted_judge(a, b),
    )
    reqs = [
        JudgeRequest(place_id=f"p{i}", brand="B", name="N", markdown="", official_url="")
        for i in range(3)
    ]
    results = await deliberate_many(panel, reqs)
    assert len(results) == 3
    assert all(r.final_franchisee == "株式会社A" for r in results)
