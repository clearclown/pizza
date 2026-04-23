"""Live smoke: Gemini Flash × Gemini Flash (別設定) + Claude critic の組織動作確認。

実行:
  RUN_LIVE_PANEL=1 uv run pytest tests/test_live_panel.py -s

必要 env: GEMINI_API_KEY, ANTHROPIC_API_KEY (+ .env)
"""

from __future__ import annotations

import os

import pytest

from pizza_delivery.agent import JudgeRequest
from pizza_delivery.evidence import Evidence
from pizza_delivery.panel import ExpertPanel


class StubCollector:
    def __init__(self, evidences: list[Evidence]):
        self.evidences = evidences

    async def collect(self, **kw):
        return self.evidences


@pytest.mark.asyncio
async def test_live_panel_starbucks_direct() -> None:
    if os.getenv("RUN_LIVE_PANEL") != "1":
        pytest.skip("set RUN_LIVE_PANEL=1 to enable (hits Gemini + Anthropic APIs)")

    from pizza_delivery.claude_critic import ClaudeCritic
    from pizza_delivery.providers import get_provider

    gemini = get_provider("gemini")
    anthropic = get_provider("anthropic")
    if not gemini.ready():
        pytest.skip("GEMINI_API_KEY not set")
    if not anthropic.ready():
        pytest.skip("ANTHROPIC_API_KEY not set")

    # Gemini を 2 箇所別々で: Flash (worker A) / Flash (worker B) を別モデル設定で
    worker_a_llm = gemini.make_llm(model="gemini-2.5-flash")
    worker_b_llm = gemini.make_llm(model="gemini-2.5-flash")  # 同モデル別 run (seed 差で独立性)

    # Critic は Claude
    critic_llm = anthropic.make_llm()  # env の ANTHROPIC_MODEL
    critic = ClaudeCritic(
        llm=critic_llm, model_name=getattr(critic_llm, "model", "claude")
    )

    panel = ExpertPanel(
        worker_a_llm=worker_a_llm,
        worker_b_llm=worker_b_llm,
        critic=critic,
        worker_a_name="gemini-flash-a",
        worker_b_name="gemini-flash-b",
    )

    evidences = [
        Evidence(
            source_url="https://www.starbucks.co.jp/company/summary/",
            snippet=(
                "会社名: スターバックス コーヒー ジャパン 株式会社\n"
                "本社: 東京都品川区上大崎2-25-2\n"
                "全店舗を直営で運営 (加盟店制度は採用していない)"
            ),
            reason="operator_keyword",
            keyword="会社名",
        )
    ]

    req = JudgeRequest(
        place_id="test_sbux_panel",
        brand="スターバックス コーヒー",
        name="スターバックス コーヒー 新宿東口店",
        markdown="",
        official_url="https://www.starbucks.co.jp/",
    )
    verdict = await panel.deliberate(
        req,
        evidence_collector=StubCollector(evidences),
        kb_conflict_flags=[],
    )

    print("\n=== Live Panel Verdict ===")
    print(f"worker_a: op={verdict.worker_a.operation_type!r}")
    print(f"          franchisor={verdict.worker_a.franchisor_name!r}")
    print(f"          conf={verdict.worker_a.confidence:.2f}")
    print(f"worker_b: op={verdict.worker_b.operation_type!r}")
    print(f"          franchisor={verdict.worker_b.franchisor_name!r}")
    print(f"          conf={verdict.worker_b.confidence:.2f}")
    print(f"critic verdict: {verdict.critic_judgement.verdict}")
    print(f"       preferred: {verdict.critic_judgement.preferred_side}")
    print(f"       critique:  {verdict.critic_judgement.critique}")
    print(f"       adjust:    {verdict.critic_judgement.confidence_adjustment:+.2f}")
    print(f"final: op={verdict.final_operation_type!r}  conf={verdict.final_confidence:.2f}")
    print(f"       franchisor={verdict.final_franchisor!r}")
    print(f"       franchisee={verdict.final_franchisee!r}")

    # 期待: evidence に "直営" と明記 → 両 worker とも direct を返し critic が approve
    assert verdict.final_operation_type in ("direct", "unknown")
    assert verdict.critic_judgement.verdict in (
        "agree_both", "prefer_a", "prefer_b", "uncertain"
    )


@pytest.mark.asyncio
async def test_live_panel_kb_override() -> None:
    """KB conflict flag が出ているが、evidence 明確 → Claude が overrule するか。"""
    if os.getenv("RUN_LIVE_PANEL") != "1":
        pytest.skip("set RUN_LIVE_PANEL=1 to enable")

    from pizza_delivery.claude_critic import ClaudeCritic
    from pizza_delivery.providers import get_provider

    gemini = get_provider("gemini")
    anthropic = get_provider("anthropic")
    if not gemini.ready() or not anthropic.ready():
        pytest.skip("require GEMINI + ANTHROPIC keys")

    worker_llm = gemini.make_llm(model="gemini-2.5-flash")
    critic = ClaudeCritic(llm=anthropic.make_llm())
    panel = ExpertPanel(
        worker_a_llm=worker_llm,
        worker_b_llm=worker_llm,
        critic=critic,
    )

    # "スターバックス コーヒー 新宿ドトール ビル 店" (架空) — 場所名に "ドトール" が
    # 含まれるので KB が false positive で conflict flag を上げる想定
    evidences = [
        Evidence(
            source_url="https://www.starbucks.co.jp/company/summary/",
            snippet="会社名: スターバックス コーヒー ジャパン 株式会社 (全店直営)",
            reason="operator_keyword",
            keyword="会社名",
        )
    ]
    req = JudgeRequest(
        place_id="test_sbux_kb_override",
        brand="スターバックス コーヒー",
        name="スターバックス コーヒー 新宿ドトール ビル 店",
        markdown="",
        official_url="https://www.starbucks.co.jp/",
    )
    verdict = await panel.deliberate(
        req,
        evidence_collector=StubCollector(evidences),
        kb_conflict_flags=["ドトール"],  # KB が誤 flag を上げたと仮定
    )

    print("\n=== KB override test ===")
    print(f"critic verdict: {verdict.critic_judgement.verdict}")
    print(f"kb_overridden: {verdict.critic_judgement.kb_conflict_overridden}")
    print(f"critique: {verdict.critic_judgement.critique}")
    print(f"final op: {verdict.final_operation_type}")

    # Claude が evidence を見て KB flag を overrule するのが期待挙動
    # (厳密 assert はしない。観察用 log のみ)
    assert verdict.critic_judgement.verdict is not None
