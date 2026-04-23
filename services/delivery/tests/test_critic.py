"""Cross-LLM critic — 別 LLM を批判役に立てた合意/不一致検証。

このテストは critic.py の決定論的な「2 LLM の結果を比較する層」を検証する。
LLM 自体は MockLLM で、judge_by_evidence をバイパスして直接 JudgeReply を返す
`_judge_fn` を注入する設計に依存している。

テストでは:
  - 完全合意 → consensus が primary と一致
  - operator 名の表記ゆれ (株式会社/㈱) → operators_match により agreement=True
  - 完全不一致 → consensus=unknown, disagreements が記録
  - operation_type のみ不一致 → flag される
  - agreement_rate 集計
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from pizza_delivery.agent import JudgeReply, JudgeRequest
from pizza_delivery.critic import CrossLLMCritic, CritiqueReport, agreement_rate


# ─── helpers ───────────────────────────────────────────────────────────


def _reply(
    *,
    place_id: str = "p1",
    operation_type: str = "franchisee",
    franchisor: str = "",
    franchisee: str = "株式会社FIT PLACE",
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


def _scripted_judge(primary_reply: JudgeReply, critic_reply: JudgeReply):
    """judge_by_evidence の代替: provider_name で primary/critic を振り分けて返す。"""
    async def _fn(req: JudgeRequest, *, llm=None, provider_name: str = "", **kw) -> JudgeReply:
        if provider_name == "primary":
            out = primary_reply
        elif provider_name == "critic":
            out = critic_reply
        else:
            raise AssertionError(f"unexpected provider_name: {provider_name}")
        # place_id を req に合わせる
        out.place_id = req.place_id
        return out

    return _fn


def _req(place_id: str = "p1") -> JudgeRequest:
    return JudgeRequest(
        place_id=place_id,
        brand="エニタイムフィットネス",
        name="エニタイムフィットネス 新宿6丁目店",
        markdown="",
        official_url="https://example.com/",
    )


# ─── tests ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_full_agreement_yields_primary_values() -> None:
    primary = _reply(franchisee="株式会社Fast Fitness Japan", confidence=0.9)
    critic = _reply(franchisee="株式会社Fast Fitness Japan", confidence=0.8)
    cr = CrossLLMCritic(
        primary_llm=object(),
        critic_llm=object(),
        _judge_fn=_scripted_judge(primary, critic),
    )
    report = await cr.critique(_req())
    assert isinstance(report, CritiqueReport)
    assert report.operator_agreement is True
    assert report.operation_type_agreement is True
    assert report.full_agreement is True
    assert report.consensus_operation_type == "franchisee"
    assert report.consensus_franchisee == "株式会社Fast Fitness Japan"
    # 合意時は両 confidence の min
    assert report.consensus_confidence == pytest.approx(0.8)
    assert report.disagreements == []


@pytest.mark.asyncio
async def test_notation_variant_is_still_agreement() -> None:
    # "㈱" と "株式会社" は normalize_operator_name で吸収される
    primary = _reply(franchisee="株式会社Fast Fitness Japan", confidence=0.9)
    critic = _reply(franchisee="㈱Fast Fitness Japan", confidence=0.9)
    cr = CrossLLMCritic(
        primary_llm=object(), critic_llm=object(),
        _judge_fn=_scripted_judge(primary, critic),
    )
    report = await cr.critique(_req())
    assert report.operator_agreement is True
    assert report.full_agreement is True


@pytest.mark.asyncio
async def test_operator_mismatch_halves_confidence_and_unknowns_type() -> None:
    primary = _reply(franchisee="株式会社Fast Fitness Japan", confidence=0.9)
    critic = _reply(franchisee="株式会社別の会社", confidence=0.8)
    cr = CrossLLMCritic(
        primary_llm=object(), critic_llm=object(),
        _judge_fn=_scripted_judge(primary, critic),
    )
    report = await cr.critique(_req())
    assert report.operator_agreement is False
    assert report.consensus_operation_type == "unknown"
    # min(0.9, 0.8) * 0.5 = 0.4
    assert report.consensus_confidence == pytest.approx(0.4)
    assert any("operator mismatch" in d for d in report.disagreements)


@pytest.mark.asyncio
async def test_operation_type_mismatch_flagged() -> None:
    primary = _reply(operation_type="franchisee", franchisee="株式会社同じ")
    critic = _reply(operation_type="direct", franchisor="株式会社同じ", franchisee="")
    cr = CrossLLMCritic(
        primary_llm=object(), critic_llm=object(),
        _judge_fn=_scripted_judge(primary, critic),
    )
    report = await cr.critique(_req())
    assert report.operation_type_agreement is False
    assert report.full_agreement is False
    assert report.consensus_operation_type == "unknown"
    assert any("operation_type mismatch" in d for d in report.disagreements)


@pytest.mark.asyncio
async def test_both_unknown_counts_as_agreement() -> None:
    # 両方 "unknown + 空 operator" → 合意 (unknown) 扱い
    primary = _reply(operation_type="unknown", franchisor="", franchisee="", confidence=0.2)
    critic = _reply(operation_type="unknown", franchisor="", franchisee="", confidence=0.2)
    cr = CrossLLMCritic(
        primary_llm=object(), critic_llm=object(),
        _judge_fn=_scripted_judge(primary, critic),
    )
    report = await cr.critique(_req())
    assert report.full_agreement is True
    assert report.consensus_operation_type == "unknown"


def test_agreement_rate_aggregates() -> None:
    # 手組みで 3 件の report を作って集計を確認
    p_agree = _reply(franchisee="株式会社A")
    reports = [
        CritiqueReport(
            place_id=f"p{i}",
            primary=p_agree,
            critic=p_agree,
            operator_agreement=(i < 2),
            operation_type_agreement=True,
            consensus_operation_type="franchisee",
            consensus_franchisee="株式会社A" if i < 2 else "",
            consensus_confidence=0.8 if i < 2 else 0.4,
        )
        for i in range(3)
    ]
    rate = agreement_rate(reports)
    assert rate["n"] == 3
    assert rate["operator_agreement"] == pytest.approx(2 / 3)
    assert rate["operation_type_agreement"] == pytest.approx(1.0)
    assert rate["full_agreement"] == pytest.approx(2 / 3)


def test_agreement_rate_empty() -> None:
    assert agreement_rate([]) == {"n": 0}
