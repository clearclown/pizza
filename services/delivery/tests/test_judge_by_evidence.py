"""Unit tests for judge_by_evidence — evidence-based Phase 4 primary path."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from pizza_delivery.agent import (
    JudgeJSON,
    JudgeRequest,
    judge_by_evidence,
)
from pizza_delivery.evidence import Evidence


# ─── Mocks ─────────────────────────────────────────────────────────────


@dataclass
class MockLLMCompletion:
    completion: Any


class MockLLM:
    model = "mock-v4"
    provider = "mock"

    def __init__(self, judge: JudgeJSON) -> None:
        self.judge = judge
        self.last_messages: list[Any] | None = None
        self.last_output_format: type | None = None

    async def ainvoke(self, messages, output_format=None, **kw):
        self.last_messages = messages
        self.last_output_format = output_format
        return MockLLMCompletion(completion=self.judge)


class MockCollector:
    def __init__(self, evidences: list[Evidence]) -> None:
        self.evidences = evidences
        self.called_with: dict | None = None

    async def collect(self, *, brand, official_url, extra_urls=None):
        self.called_with = {
            "brand": brand,
            "official_url": official_url,
            "extra_urls": extra_urls,
        }
        return self.evidences


# ─── Tests ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_judge_by_evidence_returns_unknown_when_no_evidence() -> None:
    req = JudgeRequest(
        place_id="p1",
        brand="B",
        name="N",
        markdown="",
        official_url="https://x/",
    )
    collector = MockCollector(evidences=[])
    llm = MockLLM(
        JudgeJSON(operation_type="franchisee", franchisee_name="株式会社XXX", confidence=0.9)
    )
    reply = await judge_by_evidence(
        req,
        llm=llm,
        evidence_collector=collector,
    )
    # evidence なしなら operation_type=unknown 強制、LLM は呼ばれない
    assert reply.operation_type == "unknown"
    assert reply.franchisor_name == ""
    assert reply.franchisee_name == ""
    assert reply.confidence == pytest.approx(0.2)
    assert "evidence を収集できなかった" in reply.reasoning
    assert reply.judge_mode == "evidence"
    assert llm.last_messages is None, "evidence 空なら LLM 呼び出しを避ける"


@pytest.mark.asyncio
async def test_judge_by_evidence_passes_evidence_to_llm() -> None:
    req = JudgeRequest(
        place_id="p1",
        brand="エニタイムフィットネス",
        name="新宿店",
        markdown="",
        official_url="https://anytime.example.com/shinjuku/",
    )
    evidences = [
        Evidence(
            source_url="https://anytime.example.com/shinjuku/",
            snippet="運営会社: 株式会社AFJ Project",
            reason="operator_keyword",
            keyword="運営会社",
        ),
        Evidence(
            source_url="https://anytime.example.com/company",
            snippet="当社はエニタイムフィットネスのフランチャイジー企業です",
            reason="operator_keyword",
            keyword="フランチャイジー",
        ),
    ]
    collector = MockCollector(evidences=evidences)
    llm = MockLLM(
        JudgeJSON(
            operation_type="franchisee",
            franchisor_name="株式会社ファストフィットネスジャパン",
            franchisee_name="株式会社AFJ Project",
            confidence=0.9,
            reasoning="evidence に運営会社明記",
        )
    )
    reply = await judge_by_evidence(req, llm=llm, evidence_collector=collector)

    assert reply.operation_type == "franchisee"
    assert reply.franchisee_name == "株式会社AFJ Project"
    assert reply.franchisor_name == "株式会社ファストフィットネスジャパン"
    assert reply.confidence == pytest.approx(0.9)
    assert "evidence=2 sources" in reply.reasoning
    assert reply.judge_mode == "evidence"

    # Collector が正しい引数で呼ばれた
    assert collector.called_with == {
        "brand": "エニタイムフィットネス",
        "official_url": "https://anytime.example.com/shinjuku/",
        "extra_urls": [],
    }

    # LLM に evidence が含まれた prompt が渡された
    assert llm.last_messages is not None
    user_content = getattr(llm.last_messages[1], "content", "")
    assert "株式会社AFJ Project" in user_content
    assert "フランチャイジー" in user_content
    assert "anytime.example.com" in user_content


@pytest.mark.asyncio
async def test_judge_by_evidence_direct_from_evidence() -> None:
    req = JudgeRequest(
        place_id="p1",
        brand="スターバックス",
        name="新宿店",
        markdown="",
        official_url="https://starbucks.example.com/",
    )
    evidences = [
        Evidence(
            source_url="https://starbucks.example.com/",
            snippet="スターバックス コーヒー ジャパン株式会社の全店直営店舗",
            reason="direct_keyword",
            keyword="全店直営",
        ),
    ]
    collector = MockCollector(evidences=evidences)
    llm = MockLLM(
        JudgeJSON(
            operation_type="direct",
            franchisor_name="スターバックス コーヒー ジャパン株式会社",
            franchisee_name="",
            confidence=0.9,
            reasoning="全店直営と明記",
        )
    )
    reply = await judge_by_evidence(req, llm=llm, evidence_collector=collector)
    assert reply.operation_type == "direct"
    assert reply.franchisee_name == ""
    assert reply.franchisor_name == "スターバックス コーヒー ジャパン株式会社"
    assert reply.is_franchise is False


@pytest.mark.asyncio
async def test_judge_by_evidence_passes_candidate_urls_to_collector() -> None:
    req = JudgeRequest(
        place_id="p1",
        brand="B",
        name="N",
        markdown="",
        official_url="https://x/",
        candidate_urls=["https://y/", "https://z/"],
    )
    collector = MockCollector(evidences=[])
    llm = MockLLM(
        JudgeJSON(operation_type="unknown", confidence=0.1)
    )
    await judge_by_evidence(req, llm=llm, evidence_collector=collector)
    assert collector.called_with["extra_urls"] == ["https://y/", "https://z/"]
