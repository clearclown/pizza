"""Expert Panel — 3 者構成の組織設計で operator 判定を行う。

組織設計 (docs/phase5-panel.md 参照):

  ┌────────────────────────────────┐
  │ Worker A (Gemini Flash, 設定 A) │──╮
  ├────────────────────────────────┤  │
  │ Worker B (Gemini Flash, 設定 B) │──┼─→ ┌──────────────────────────┐
  └────────────────────────────────┘  │   │ Critic (Claude Haiku 4.5) │
                                      │   │ クリティカルシンキング     │
  Layer A KB conflict flags ──────────┤   │ - 両 Worker の批判         │
                                      ╰─→ │ - KB hit の overrule 判断 │
                                          │ - 最終 verdict             │
                                          └──────────────────────────┘

設計意図:
  - 抽出は 2 つの Gemini Flash で独立実行 (cost / speed 重視)
  - 評価は Claude が critical thinking で行う (reasoning quality 重視)
  - KB (blocklist) は情報として提示、絶対視せず Claude が override できる

この構成は 2026-04-23 ユーザー要請に基づく:
  「geminiに関しては flashの方が良かったり、flashをツールとして使用したり、
   claudeがクリティカルシンキングを用いて評価をしたりと組織設計を」
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol

from pizza_delivery.agent import JudgeReply, JudgeRequest, judge_by_evidence
from pizza_delivery.evidence import Evidence


# ─── Data types ────────────────────────────────────────────────────────


# Critic verdict enum
VERDICT_AGREE_BOTH = "agree_both"
VERDICT_PREFER_A = "prefer_a"
VERDICT_PREFER_B = "prefer_b"
VERDICT_BOTH_WRONG = "both_wrong"
VERDICT_UNCERTAIN = "uncertain"


@dataclass
class CriticJudgement:
    """Critic (Claude) が両 Worker の出力を critical thinking で評価した結果。"""

    verdict: str  # agree_both | prefer_a | prefer_b | both_wrong | uncertain
    preferred_side: str  # a | b | both | neither
    critique: str
    confidence_adjustment: float  # -1.0 ~ +0.2
    # Layer A KB hit を critic が overrule したか (True なら KB reject を覆す)
    kb_conflict_overridden: bool = False


@dataclass
class PanelVerdict:
    """Panel 最終判断。"""

    place_id: str
    worker_a: JudgeReply
    worker_b: JudgeReply
    critic_judgement: CriticJudgement
    final_operation_type: str
    final_franchisor: str
    final_franchisee: str
    final_confidence: float
    reasoning: str = ""


# ─── Critic Protocol ───────────────────────────────────────────────────


class Critic(Protocol):
    """Claude を想定した critic interface。"""

    async def critique(
        self,
        *,
        reply_a: JudgeReply,
        reply_b: JudgeReply,
        evidences: list[Evidence],
        kb_conflict_flags: list[str] | None = None,
    ) -> CriticJudgement:
        ...


# judge_by_evidence 互換の signature (テストで差し替え可)
JudgeFn = Callable[..., Awaitable[JudgeReply]]


# ─── ExpertPanel ───────────────────────────────────────────────────────


@dataclass
class ExpertPanel:
    """2 Worker + 1 Critic の組織。

    Attributes:
        worker_a_llm / worker_b_llm: 抽出 LLM (Gemini Flash 推奨、温度違いで独立性を稼ぐ)
        critic: Critic interface (Claude 推奨)
        worker_a_name / worker_b_name: provider tag
        _judge_fn: judge_by_evidence を差し替えるテストフック
    """

    worker_a_llm: Any
    worker_b_llm: Any
    critic: Critic
    worker_a_name: str = "worker-a"
    worker_b_name: str = "worker-b"
    _judge_fn: JudgeFn | None = None

    async def deliberate(
        self,
        req: JudgeRequest,
        *,
        evidence_collector: Any | None = None,
        kb_conflict_flags: list[str] | None = None,
    ) -> PanelVerdict:
        """両 Worker 独立抽出 → Critic 評価 → Final verdict 組み立て。"""
        judge_fn: JudgeFn = self._judge_fn or judge_by_evidence

        # Step 1: 2 Worker 独立抽出 (sequential; 並列化は将来 optimization)
        reply_a = await judge_fn(
            req, llm=self.worker_a_llm, provider_name=self.worker_a_name,
            evidence_collector=evidence_collector,
        )
        reply_b = await judge_fn(
            req, llm=self.worker_b_llm, provider_name=self.worker_b_name,
            evidence_collector=evidence_collector,
        )

        # Step 2: evidence を critic に渡すため取り直す (collector が None なら空)
        evidences: list[Evidence] = []
        if evidence_collector is not None:
            evidences = await evidence_collector.collect(
                brand=req.brand,
                official_url=req.official_url,
                extra_urls=req.candidate_urls or [],
            )

        # Step 3: Critic が critical thinking で評価
        judgement = await self.critic.critique(
            reply_a=reply_a,
            reply_b=reply_b,
            evidences=evidences,
            kb_conflict_flags=kb_conflict_flags,
        )

        # Step 4: Final verdict 構築
        return _aggregate(req.place_id, reply_a, reply_b, judgement)


def _aggregate(
    place_id: str,
    a: JudgeReply,
    b: JudgeReply,
    j: CriticJudgement,
) -> PanelVerdict:
    """critic の判断に応じて final operator/type/confidence を決定。"""
    if j.verdict == VERDICT_BOTH_WRONG:
        # 両却下 → unknown 扱い
        base_conf = min(a.confidence, b.confidence) * 0.5
        final_conf = max(0.0, base_conf + j.confidence_adjustment)
        return PanelVerdict(
            place_id=place_id,
            worker_a=a,
            worker_b=b,
            critic_judgement=j,
            final_operation_type="unknown",
            final_franchisor="",
            final_franchisee="",
            final_confidence=final_conf,
            reasoning=j.critique,
        )

    # どちらを採用するか
    if j.verdict == VERDICT_PREFER_A or j.preferred_side == "a":
        picked = a
        base_conf = a.confidence
    elif j.verdict == VERDICT_PREFER_B or j.preferred_side == "b":
        picked = b
        base_conf = b.confidence
    else:
        # agree_both / uncertain with both → 両者が同じ内容なら a を採用
        picked = a
        base_conf = min(a.confidence, b.confidence)

    final_conf = max(0.0, min(1.0, base_conf + j.confidence_adjustment))
    return PanelVerdict(
        place_id=place_id,
        worker_a=a,
        worker_b=b,
        critic_judgement=j,
        final_operation_type=picked.operation_type,
        final_franchisor=picked.franchisor_name,
        final_franchisee=picked.franchisee_name,
        final_confidence=final_conf,
        reasoning=j.critique,
    )


async def deliberate_many(
    panel: ExpertPanel,
    reqs: list[JudgeRequest],
    *,
    evidence_collector: Any | None = None,
    kb_conflict_flags: list[str] | None = None,
) -> list[PanelVerdict]:
    """複数 request を順次 deliberate (ordered)。"""
    out: list[PanelVerdict] = []
    for r in reqs:
        out.append(
            await panel.deliberate(
                r,
                evidence_collector=evidence_collector,
                kb_conflict_flags=kb_conflict_flags,
            )
        )
    return out
