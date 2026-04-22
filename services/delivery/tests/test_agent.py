"""🟢 Phase 2 Green: agent.judge_franchise を mock LLM で検証。"""

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
    """ainvoke(messages, output_format) で固定 JudgeJSON を返す mock。"""

    model = "mock-model-2026"
    provider = "mock"

    def __init__(self, judge: Any) -> None:
        self.judge = judge
        self.last_messages: list[Any] | None = None
        self.last_output_format: type | None = None

    async def ainvoke(
        self,
        messages: list[Any],
        output_format: type | None = None,
        **kwargs: Any,
    ) -> _Completion:
        self.last_messages = messages
        self.last_output_format = output_format
        return _Completion(completion=self.judge)


@pytest.mark.asyncio
async def test_judge_franchise_extracts_fc_operator_with_mock_llm() -> None:
    mock = MockLLM(
        JudgeJSON(
            is_franchise=True,
            operator_name="株式会社メガ・スポーツ",
            store_count_estimate=35,
            confidence=0.9,
            reasoning="会社概要に「運営: 株式会社メガ・スポーツ」と明記",
        )
    )
    req = JudgeRequest(
        place_id="p-anytime-shinjuku",
        brand="エニタイムフィットネス",
        name="エニタイムフィットネス新宿店",
        markdown="# 会社概要\n\n運営: 株式会社メガ・スポーツ\n運営店舗数: 35 店舗",
        official_url="https://www.anytimefitness.co.jp/shinjuku/",
    )
    reply = await judge_franchise(req, llm=mock, provider_name="anthropic", model_name="mm")
    assert reply.place_id == "p-anytime-shinjuku"
    assert reply.is_franchise is True
    assert reply.operator_name == "株式会社メガ・スポーツ"
    assert reply.store_count_estimate == 35
    assert reply.confidence == pytest.approx(0.9)
    assert reply.llm_provider == "anthropic"
    assert reply.llm_model == "mm"
    assert "運営" in reply.reasoning
    # messages: system + user の 2 件
    assert mock.last_messages is not None
    assert len(mock.last_messages) == 2
    user_content = getattr(mock.last_messages[1], "content", "")
    assert "エニタイムフィットネス" in user_content
    assert "エニタイムフィットネス新宿店" in user_content


@pytest.mark.asyncio
async def test_judge_franchise_direct_operated_returns_is_franchise_false() -> None:
    mock = MockLLM(
        JudgeJSON(
            is_franchise=False,
            operator_name="",
            store_count_estimate=0,
            confidence=0.8,
            reasoning="スターバックスは全店直営",
        )
    )
    req = JudgeRequest(
        place_id="p-sbux-shinjuku",
        brand="スターバックス コーヒー",
        name="スターバックス コーヒー 新宿駅東口店",
        markdown="# 会社概要\n\nスターバックス コーヒー ジャパン株式会社\n全店直営",
    )
    reply = await judge_franchise(req, llm=mock)
    assert reply.is_franchise is False
    assert reply.operator_name == ""


@pytest.mark.asyncio
async def test_judge_franchise_accepts_json_string_response() -> None:
    """LLM が structured output 未対応で生の JSON 文字列を返した場合もパースできる。"""

    class RawJSONLLM(MockLLM):
        async def ainvoke(self, messages, output_format=None, **kw):
            return _Completion(completion=self.judge)

    raw_json = (
        '{"is_franchise": true, "operator_name": "株式会社テスト", '
        '"store_count_estimate": 22, "confidence": 0.7, "reasoning": "OK"}'
    )
    mock = RawJSONLLM(raw_json)
    req = JudgeRequest(place_id="p1", brand="B", name="N", markdown="md")
    reply = await judge_franchise(req, llm=mock)
    assert reply.is_franchise is True
    assert reply.operator_name == "株式会社テスト"
    assert reply.store_count_estimate == 22


@pytest.mark.asyncio
async def test_judge_franchise_handles_json_with_wrapping_text() -> None:
    """LLM が JSON の前後に説明文を付けた場合、'{...}' だけ抽出する。"""

    class WrappedLLM:
        model = "x"
        provider = "x"

        async def ainvoke(self, messages, output_format=None, **kw):
            return _Completion(
                completion='承知しました。判定結果は以下の通りです:\n\n```json\n'
                '{"is_franchise": true, "operator_name": "株式会社A", '
                '"store_count_estimate": 30, "confidence": 0.88, "reasoning": "..."}\n'
                '```'
            )

    req = JudgeRequest(place_id="p1", brand="B", name="N", markdown="md")
    reply = await judge_franchise(req, llm=WrappedLLM())
    assert reply.is_franchise is True
    assert reply.operator_name == "株式会社A"
    assert reply.store_count_estimate == 30


@pytest.mark.asyncio
async def test_judge_franchise_truncates_long_markdown() -> None:
    """5000 文字以上の markdown は切り詰められて prompt に渡る。"""
    big = "x" * 10000
    mock = MockLLM(
        JudgeJSON(is_franchise=False, operator_name="", store_count_estimate=0, confidence=0.1, reasoning="empty")
    )
    req = JudgeRequest(place_id="p1", brand="B", name="N", markdown=big)
    await judge_franchise(req, llm=mock)
    user_content = getattr(mock.last_messages[1], "content", "")
    # truncation を goal にしている (5000 文字)
    assert big not in user_content
    assert "xxxxx" in user_content  # 部分的には含まれる


@pytest.mark.asyncio
async def test_judge_franchise_passes_output_format_to_llm() -> None:
    mock = MockLLM(
        JudgeJSON(is_franchise=True, operator_name="X", store_count_estimate=1, confidence=0.5, reasoning="r")
    )
    req = JudgeRequest(place_id="p", brand="b", name="n", markdown="m")
    await judge_franchise(req, llm=mock)
    # output_format=JudgeJSON が LLM に伝わっていること (structured output 要求)
    assert mock.last_output_format is JudgeJSON
