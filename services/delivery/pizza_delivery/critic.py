"""Cross-LLM critic layer — 批判役による検証レイヤー。

2 つの LLM プロバイダを独立に走らせ、抽出結果が一致するかを測定する。
単一 LLM の判断を信じず、「別の頭脳 (別プロバイダ・別モデル)」でも
同じ answer が出るときに初めて高信頼として採用する設計。

役割:
  - 合意判定 (operator 名は normalize 後比較、operation_type は exact 比較)
  - 不一致 flag (後段の人間レビュー用 disagreements list)
  - confidence 集計 (合意時は min、不一致時は half)

本ファイル自体に LLM 呼び出しはない。`judge_by_evidence` を呼ぶだけで、
推論は LLM に委ねているが、推論が "extraction only" であることは
prompts/judge_v4_extraction.yaml 側の責務。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from pizza_delivery.agent import (
    JudgeReply,
    JudgeRequest,
    LLMClient,
    judge_by_evidence,
)
from pizza_delivery.normalize import operators_match


# judge function signature (for injection in tests)
JudgeFn = Callable[..., Awaitable[JudgeReply]]


@dataclass
class CritiqueReport:
    """2 つの LLM の独立判定と、その合意レベル。"""

    place_id: str
    primary: JudgeReply
    critic: JudgeReply
    operator_agreement: bool
    operation_type_agreement: bool
    consensus_operation_type: str = "unknown"
    consensus_franchisor: str = ""
    consensus_franchisee: str = ""
    consensus_confidence: float = 0.0
    disagreements: list[str] = field(default_factory=list)

    @property
    def full_agreement(self) -> bool:
        return self.operator_agreement and self.operation_type_agreement


@dataclass
class CrossLLMCritic:
    """primary と critic 2 つの LLM で同じ evidence を独立に判定する。

    両方に同じ evidence_collector を渡すため、データ差は生まれず
    「同じ入力に対する 2 つの頭脳の出力差」だけが測定される。

    Attributes:
        primary_llm / critic_llm: ainvoke() を持つ LLM 互換オブジェクト。
        primary_name / critic_name: provider 識別用タグ。
        _judge_fn: judge_by_evidence を差し替えるテスト用フック。
    """

    primary_llm: LLMClient
    critic_llm: LLMClient
    primary_name: str = "primary"
    critic_name: str = "critic"
    _judge_fn: JudgeFn | None = None

    async def critique(
        self,
        req: JudgeRequest,
        *,
        evidence_collector: Any | None = None,
    ) -> CritiqueReport:
        judge_fn: JudgeFn = self._judge_fn or judge_by_evidence
        primary_reply = await judge_fn(
            req,
            llm=self.primary_llm,
            provider_name=self.primary_name,
            evidence_collector=evidence_collector,
        )
        critic_reply = await judge_fn(
            req,
            llm=self.critic_llm,
            provider_name=self.critic_name,
            evidence_collector=evidence_collector,
        )
        return _compare(req.place_id, primary_reply, critic_reply)


def _both_empty(reply: JudgeReply) -> bool:
    return not reply.franchisor_name and not reply.franchisee_name


def _field_match(a: str, b: str) -> bool:
    """operator name 単位の比較: 両方空は match、片方空は mismatch、
    両方値ありなら operators_match (正規化後比較) に委譲。"""
    if not a and not b:
        return True
    if not a or not b:
        return False
    return operators_match(a, b)


def _compare(place_id: str, primary: JudgeReply, critic: JudgeReply) -> CritiqueReport:
    # operator 比較: 両方空なら agreement (unknown 合意)
    if _both_empty(primary) and _both_empty(critic):
        op_agree = True
    else:
        op_agree = (
            _field_match(primary.franchisor_name, critic.franchisor_name)
            and _field_match(primary.franchisee_name, critic.franchisee_name)
        )

    type_agree = primary.operation_type == critic.operation_type

    report = CritiqueReport(
        place_id=place_id,
        primary=primary,
        critic=critic,
        operator_agreement=op_agree,
        operation_type_agreement=type_agree,
    )

    if op_agree and type_agree:
        report.consensus_operation_type = primary.operation_type
        report.consensus_franchisor = primary.franchisor_name or critic.franchisor_name
        report.consensus_franchisee = primary.franchisee_name or critic.franchisee_name
        report.consensus_confidence = min(primary.confidence, critic.confidence)
    else:
        report.consensus_operation_type = "unknown"
        report.consensus_confidence = min(primary.confidence, critic.confidence) * 0.5
        if not op_agree:
            report.disagreements.append(
                "operator mismatch: "
                f"primary={primary.franchisor_name!r}/{primary.franchisee_name!r} "
                f"vs critic={critic.franchisor_name!r}/{critic.franchisee_name!r}"
            )
        if not type_agree:
            report.disagreements.append(
                "operation_type mismatch: "
                f"primary={primary.operation_type} vs critic={critic.operation_type}"
            )

    return report


def agreement_rate(reports: list[CritiqueReport]) -> dict[str, float]:
    """合意率メトリクス集計。"""
    if not reports:
        return {"n": 0}
    n = len(reports)
    op = sum(1 for r in reports if r.operator_agreement) / n
    tp = sum(1 for r in reports if r.operation_type_agreement) / n
    full = sum(1 for r in reports if r.full_agreement) / n
    return {
        "n": n,
        "operator_agreement": op,
        "operation_type_agreement": tp,
        "full_agreement": full,
    }


async def critique_many(
    critic: CrossLLMCritic,
    reqs: list[JudgeRequest],
    *,
    evidence_collector: Any | None = None,
) -> list[CritiqueReport]:
    """複数 request を順次 critique (ordered)。"""
    out: list[CritiqueReport] = []
    for r in reqs:
        out.append(await critic.critique(r, evidence_collector=evidence_collector))
    return out
