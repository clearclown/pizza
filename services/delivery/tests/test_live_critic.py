"""Cross-LLM critic live smoke test (Gemini primary × Claude critic)。

このテストは `RUN_LIVE_CRITIC=1` でのみ有効。実 Gemini + 実 Claude API を
叩くのでコストが発生する (最小 prompt で 1 req/provider = 数セント)。

設計意図:
  - 決定論的 mock では測れない「同じ入力を別モデルが本当に独立解釈するか」を
    実データで測る
  - 合意ケース / 不一致ケースそれぞれで CrossLLMCritic の挙動を観察

実行方法:
  RUN_LIVE_CRITIC=1 uv run pytest tests/test_live_critic.py -s

前提 env:
  ANTHROPIC_API_KEY, GEMINI_API_KEY (.env から読む)
"""

from __future__ import annotations

import os

import pytest

from pizza_delivery.agent import JudgeRequest
from pizza_delivery.critic import CrossLLMCritic
from pizza_delivery.evidence import Evidence


class StubCollector:
    """固定 evidence を返すテスト用 collector。"""

    def __init__(self, evidences: list[Evidence]) -> None:
        self.evidences = evidences

    async def collect(self, *, brand, official_url, extra_urls=None):
        return self.evidences


@pytest.mark.asyncio
async def test_live_critic_starbucks_consensus() -> None:
    if os.getenv("RUN_LIVE_CRITIC") != "1":
        pytest.skip("set RUN_LIVE_CRITIC=1 to enable (hits Gemini + Anthropic APIs)")

    from pizza_delivery.providers import get_provider

    primary = get_provider("gemini")
    critic = get_provider("anthropic")
    if not primary.ready():
        pytest.skip("GEMINI_API_KEY not set")
    if not critic.ready():
        pytest.skip("ANTHROPIC_API_KEY not set")

    p_llm = primary.make_llm()  # env の GEMINI_MODEL を使用
    c_llm = critic.make_llm()   # env の ANTHROPIC_MODEL を使用

    cr = CrossLLMCritic(
        primary_llm=p_llm, critic_llm=c_llm,
        primary_name="gemini", critic_name="anthropic",
    )

    # Starbucks Japan の会社概要相当 evidence (固定) を与えて、
    # 2 モデルが "スターバックス コーヒー ジャパン 株式会社" を franchisor として抽出するか
    ev_list = [
        Evidence(
            source_url="https://www.starbucks.co.jp/company/summary/",
            snippet=(
                "会社概要\n"
                "会社名: スターバックス コーヒー ジャパン 株式会社\n"
                "代表者: 水口 貴文\n"
                "所在地: 東京都品川区上大崎2-25-2\n"
                "全店舗直営 (加盟店方式は採用していない)"
            ),
            reason="operator_keyword",
            keyword="会社名",
        )
    ]
    stub = StubCollector(ev_list)

    req = JudgeRequest(
        place_id="test_sbux_001",
        brand="スターバックス コーヒー",
        name="スターバックス コーヒー 新宿東口店",
        markdown="",
        official_url="https://www.starbucks.co.jp/",
    )
    report = await cr.critique(req, evidence_collector=stub)

    # 2 LLM 両方が反応している
    assert report.primary is not None
    assert report.critic is not None

    # 観察ログ (pytest -s でヒューマンリーダブルに出力)
    print("\n=== Live Critic Result ===")
    print(f"primary(gemini):   op={report.primary.operation_type!r}")
    print(f"                   franchisor={report.primary.franchisor_name!r}")
    print(f"                   franchisee={report.primary.franchisee_name!r}")
    print(f"                   confidence={report.primary.confidence:.2f}")
    print(f"critic(anthropic): op={report.critic.operation_type!r}")
    print(f"                   franchisor={report.critic.franchisor_name!r}")
    print(f"                   franchisee={report.critic.franchisee_name!r}")
    print(f"                   confidence={report.critic.confidence:.2f}")
    print(f"full_agreement: {report.full_agreement}")
    print(f"consensus:      op={report.consensus_operation_type!r}")
    print(f"                confidence={report.consensus_confidence:.2f}")
    if report.disagreements:
        for d in report.disagreements:
            print(f"  - {d}")

    # 期待: evidence に "直営" と明記されているので両モデルとも operation_type="direct"、
    # franchisor_name にスターバックス コーヒー ジャパン を含む。
    # (厳密 assert はしない — モデルの差は許容、合意率の観察が目的)


@pytest.mark.asyncio
async def test_live_critic_claude_only_smoke() -> None:
    """Claude が単独で正常動作するか (残高 / モデル名 確認)。"""
    if os.getenv("RUN_LIVE_CRITIC") != "1":
        pytest.skip("set RUN_LIVE_CRITIC=1 to enable")

    from pizza_delivery.providers import get_provider

    anthropic = get_provider("anthropic")
    if not anthropic.ready():
        pytest.skip("ANTHROPIC_API_KEY not set")

    llm = anthropic.make_llm()
    from browser_use.llm.messages import SystemMessage, UserMessage

    # 最小 prompt で Claude に何か言わせる
    resp = await llm.ainvoke(
        [
            SystemMessage(content="You are a concise assistant."),
            UserMessage(content="Reply with just 'PI-ZZA'."),
        ]
    )
    completion_text = getattr(resp, "completion", resp)
    print(f"\n=== Claude smoke ({getattr(llm, 'model', 'unknown')}) ===")
    print(f"response: {completion_text}")
    assert completion_text is not None
