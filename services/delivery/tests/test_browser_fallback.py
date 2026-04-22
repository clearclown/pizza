"""🟢 Browser fallback のユニットテスト。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from pizza_delivery.agent import JudgeJSON, JudgeRequest, judge_franchise


# ─── Mock LLM (browser_use.llm.Chat* 互換) ─────────────────────────────


@dataclass
class _Completion:
    completion: Any


class MockLLM:
    model = "mock"
    provider = "mock"

    def __init__(self, judge: Any) -> None:
        self.judge = judge

    async def ainvoke(self, messages, output_format=None, **kw):
        return _Completion(completion=self.judge)


# ─── Browser agent mocks ───────────────────────────────────────────────


async def _ok_browser_agent(task: str, llm: Any, url: str) -> JudgeJSON:
    assert "公式" in task or "訪問" in task
    assert url
    return JudgeJSON(
        is_franchise=True,
        operator_name="株式会社ブラウザ調査済",
        store_count_estimate=42,
        confidence=0.85,
        reasoning="会社概要ページに明記されていた",
    )


async def _failing_browser_agent(task: str, llm: Any, url: str) -> JudgeJSON:
    raise RuntimeError("playwright timeout")


# ─── Tests ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_browser_fallback_triggered_on_low_confidence() -> None:
    low_conf = JudgeJSON(
        is_franchise=False,
        operator_name="",
        store_count_estimate=0,
        confidence=0.2,
        reasoning="Markdown が貧弱で確信できない",
    )
    req = JudgeRequest(
        place_id="p1",
        brand="テスト",
        name="店舗",
        markdown="",
        official_url="https://example.com/",
    )
    reply = await judge_franchise(
        req,
        llm=MockLLM(low_conf),
        browser_agent=_ok_browser_agent,
        enable_browser_fallback=True,
    )
    assert reply.used_browser_fallback is True
    assert reply.is_franchise is True
    assert reply.operator_name == "株式会社ブラウザ調査済"
    assert reply.store_count_estimate == 42
    # 0.85 + 0.1 = 0.95 (cap 1.0)
    assert reply.confidence == pytest.approx(0.95)
    assert "[browser]" in reply.reasoning


@pytest.mark.asyncio
async def test_browser_fallback_skipped_on_high_confidence() -> None:
    high_conf = JudgeJSON(
        is_franchise=True,
        operator_name="株式会社自信あり",
        store_count_estimate=30,
        confidence=0.8,
        reasoning="明確",
    )
    req = JudgeRequest(
        place_id="p1",
        brand="B",
        name="N",
        markdown="# 会社概要\n\n株式会社自信あり",
        official_url="https://example.com/",
    )
    reply = await judge_franchise(
        req,
        llm=MockLLM(high_conf),
        browser_agent=_ok_browser_agent,
        enable_browser_fallback=True,
    )
    assert reply.used_browser_fallback is False
    assert reply.operator_name == "株式会社自信あり"
    assert reply.confidence == pytest.approx(0.8)


@pytest.mark.asyncio
async def test_browser_fallback_skipped_without_official_url() -> None:
    low = JudgeJSON(is_franchise=False, operator_name="", store_count_estimate=0, confidence=0.1, reasoning="")
    req = JudgeRequest(place_id="p", brand="B", name="N", markdown="", official_url="")
    reply = await judge_franchise(
        req,
        llm=MockLLM(low),
        browser_agent=_ok_browser_agent,
        enable_browser_fallback=True,
    )
    assert reply.used_browser_fallback is False


@pytest.mark.asyncio
async def test_browser_fallback_disabled_by_flag() -> None:
    low = JudgeJSON(is_franchise=False, operator_name="", store_count_estimate=0, confidence=0.1, reasoning="")
    req = JudgeRequest(place_id="p", brand="B", name="N", markdown="", official_url="https://x")
    reply = await judge_franchise(
        req,
        llm=MockLLM(low),
        browser_agent=_ok_browser_agent,
        enable_browser_fallback=False,
    )
    assert reply.used_browser_fallback is False


@pytest.mark.asyncio
async def test_browser_fallback_failure_is_graceful() -> None:
    low = JudgeJSON(
        is_franchise=False,
        operator_name="",
        store_count_estimate=0,
        confidence=0.3,
        reasoning="unsure",
    )
    req = JudgeRequest(
        place_id="p1",
        brand="B",
        name="N",
        markdown="",
        official_url="https://timeout.example.com/",
    )
    reply = await judge_franchise(
        req,
        llm=MockLLM(low),
        browser_agent=_failing_browser_agent,
        enable_browser_fallback=True,
    )
    # browser 失敗でも LLM 単独結果が残る
    assert reply.used_browser_fallback is False
    assert reply.confidence == pytest.approx(0.3)
    assert "browser fallback failed" in reply.reasoning


@pytest.mark.asyncio
async def test_browser_threshold_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    # threshold を 0.5 に上げると 0.45 は fallback 対象になる
    monkeypatch.setenv("BROWSER_FALLBACK_THRESHOLD", "0.5")
    mid = JudgeJSON(is_franchise=False, operator_name="", store_count_estimate=0, confidence=0.45, reasoning="")
    req = JudgeRequest(place_id="p", brand="B", name="N", markdown="", official_url="https://x")
    reply = await judge_franchise(
        req,
        llm=MockLLM(mid),
        browser_agent=_ok_browser_agent,
        enable_browser_fallback=True,
    )
    assert reply.used_browser_fallback is True
