"""gRPC DeliveryServicer — mock / live の切替対応。

DELIVERY_MODE 環境変数で選択:
  - mock (default): 固定値を返す。Pipeline 疎通テストや CI 用
  - live:           真の browser-use + LLM 判定。ANTHROPIC_API_KEY 等が必要

実行:
    cd services/delivery
    DELIVERY_MODE=mock uv run python -m pizza_delivery    # → mock サーバ
    DELIVERY_MODE=live uv run python -m pizza_delivery    # → 実 LLM サーバ
"""

from __future__ import annotations

import asyncio
import os
from concurrent import futures
from typing import Any

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from pizza.v1 import delivery_pb2, delivery_pb2_grpc


# ─── MOCK Servicer ─────────────────────────────────────────────────────


class MockDeliveryServicer(delivery_pb2_grpc.DeliveryServiceServicer):
    """固定値を返す Phase 1 mock。"""

    def JudgeFranchiseType(  # noqa: N802
        self,
        request: delivery_pb2.JudgeFranchiseTypeRequest,
        context: Any,
    ) -> delivery_pb2.JudgeFranchiseTypeResponse:
        store = request.context.store
        result = delivery_pb2.JudgeResult(
            place_id=store.place_id,
            is_franchise=True,
            operator_name="(mock) 株式会社モック運営",
            store_count_estimate=25,
            confidence=0.5,
            llm_provider="mock",
            llm_model="none",
        )
        result.evidence.append(
            delivery_pb2.Evidence(
                source_url=store.official_url or "(none)",
                snippet="Phase 1 mock — DELIVERY_MODE=live で真の LLM 判定",
                reason="mock baseline",
            )
        )
        return delivery_pb2.JudgeFranchiseTypeResponse(result=result)

    def BatchJudge(  # noqa: N802
        self,
        request_iterator,
        context: Any,
    ):
        for req in request_iterator:
            store = req.context.store
            yield delivery_pb2.BatchJudgeResponse(
                result=delivery_pb2.JudgeResult(
                    place_id=store.place_id,
                    is_franchise=True,
                    operator_name="(mock) 株式会社モック運営",
                    store_count_estimate=25,
                    confidence=0.5,
                    llm_provider="mock",
                )
            )


# ─── LIVE Servicer ─────────────────────────────────────────────────────


class RealDeliveryServicer(delivery_pb2_grpc.DeliveryServiceServicer):
    """browser-use + LLM で実判定する live サーバ。

    LLM_PROVIDER (anthropic/openai/gemini) を env から読み、
    provider.make_llm() で browser_use.llm インスタンスを得る。
    """

    def __init__(self) -> None:
        # 起動時に provider を解決 (env チェック)
        from pizza_delivery.providers import get_provider

        self.provider_name = os.getenv("LLM_PROVIDER", "anthropic")
        self.provider = get_provider(self.provider_name)
        if not self.provider.ready():
            raise RuntimeError(
                f"RealDeliveryServicer: provider {self.provider_name} is not ready "
                f"(API キーを env にセットしてください)"
            )
        # llm は毎回作り直さず reuse (session/connection 再利用)
        self._llm = self.provider.make_llm()
        self.model_name = getattr(self._llm, "model", "") or getattr(
            self._llm, "model_name", ""
        )

    def JudgeFranchiseType(  # noqa: N802
        self,
        request: delivery_pb2.JudgeFranchiseTypeRequest,
        context: Any,
    ) -> delivery_pb2.JudgeFranchiseTypeResponse:
        from pizza_delivery.agent import JudgeRequest, judge_franchise

        store = request.context.store
        judge_req = JudgeRequest(
            place_id=store.place_id,
            brand=store.brand,
            name=store.name,
            address=store.address,
            markdown=request.context.markdown,
            official_url=store.official_url,
            candidate_urls=list(request.context.candidate_urls),
            provider_hint=request.context.provider_hint,
        )
        try:
            reply = asyncio.run(
                judge_franchise(
                    judge_req,
                    llm=self._llm,
                    provider_name=self.provider_name,
                    model_name=self.model_name,
                )
            )
        except Exception as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"judge failed: {exc}")
            raise  # unreachable

        result = delivery_pb2.JudgeResult(
            place_id=reply.place_id,
            is_franchise=reply.is_franchise,
            operator_name=reply.operator_name,
            store_count_estimate=reply.store_count_estimate,
            confidence=reply.confidence,
            llm_provider=reply.llm_provider,
            llm_model=reply.llm_model,
        )
        if reply.reasoning:
            result.evidence.append(
                delivery_pb2.Evidence(
                    source_url=store.official_url or "",
                    snippet=(reply.reasoning[:180] + "…") if len(reply.reasoning) > 180 else reply.reasoning,
                    reason="llm",
                )
            )
        return delivery_pb2.JudgeFranchiseTypeResponse(result=result)

    def BatchJudge(self, request_iterator, context):  # noqa: N802
        for req in request_iterator:
            single = delivery_pb2.JudgeFranchiseTypeRequest(context=req.context)
            resp = self.JudgeFranchiseType(single, context)
            yield delivery_pb2.BatchJudgeResponse(result=resp.result)


# ─── PANEL Servicer (Phase 5.1: 組織設計) ─────────────────────────────


class PanelDeliveryServicer(delivery_pb2_grpc.DeliveryServiceServicer):
    """Expert Panel (Gemini Flash×2 + Claude critic) で判定する組織モード。

    env:
      GEMINI_API_KEY           — Worker (2 基)
      ANTHROPIC_API_KEY        — Critic
      PANEL_WORKER_A_MODEL     — default gemini-2.5-flash
      PANEL_WORKER_B_MODEL     — default gemini-2.5-flash
      PANEL_CRITIC_MODEL       — default ANTHROPIC_MODEL (claude-haiku-4-5)
    """

    def __init__(self) -> None:
        from pizza_delivery.claude_critic import ClaudeCritic
        from pizza_delivery.providers import get_provider

        gemini = get_provider("gemini")
        anthropic = get_provider("anthropic")
        if not gemini.ready():
            raise RuntimeError("PanelDeliveryServicer requires GEMINI_API_KEY")
        if not anthropic.ready():
            raise RuntimeError("PanelDeliveryServicer requires ANTHROPIC_API_KEY")

        worker_a_model = os.getenv("PANEL_WORKER_A_MODEL", "gemini-2.5-flash")
        worker_b_model = os.getenv("PANEL_WORKER_B_MODEL", "gemini-2.5-flash")
        critic_model = os.getenv("PANEL_CRITIC_MODEL") or None

        self._worker_a = gemini.make_llm(model=worker_a_model)
        self._worker_b = gemini.make_llm(model=worker_b_model)
        self._critic_llm = anthropic.make_llm(model=critic_model)
        self._critic = ClaudeCritic(
            llm=self._critic_llm,
            model_name=getattr(self._critic_llm, "model", "") or "claude",
        )
        self._worker_a_model = worker_a_model
        self._worker_b_model = worker_b_model
        self._critic_model = getattr(self._critic_llm, "model", "") or "claude"

    def JudgeFranchiseType(  # noqa: N802
        self,
        request: delivery_pb2.JudgeFranchiseTypeRequest,
        context: Any,
    ) -> delivery_pb2.JudgeFranchiseTypeResponse:
        from pizza_delivery.agent import JudgeRequest
        from pizza_delivery.evidence import EvidenceCollector
        from pizza_delivery.panel import ExpertPanel

        store = request.context.store
        judge_req = JudgeRequest(
            place_id=store.place_id,
            brand=store.brand,
            name=store.name,
            address=store.address,
            markdown=request.context.markdown,
            official_url=store.official_url,
            candidate_urls=list(request.context.candidate_urls),
            provider_hint=request.context.provider_hint,
        )
        panel = ExpertPanel(
            worker_a_llm=self._worker_a,
            worker_b_llm=self._worker_b,
            critic=self._critic,
            worker_a_name="gemini-flash-a",
            worker_b_name="gemini-flash-b",
        )
        try:
            verdict = asyncio.run(
                panel.deliberate(judge_req, evidence_collector=EvidenceCollector())
            )
        except Exception as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"panel failed: {exc}")
            raise

        operator_display = verdict.final_franchisee or verdict.final_franchisor
        is_franchise = verdict.final_operation_type not in ("direct", "")
        result = delivery_pb2.JudgeResult(
            place_id=verdict.place_id,
            is_franchise=is_franchise,
            operator_name=operator_display,
            store_count_estimate=0,
            confidence=verdict.final_confidence,
            llm_provider="panel:gemini×2+claude",
            llm_model=f"{self._worker_a_model}/{self._worker_b_model}→{self._critic_model}",
        )
        if verdict.reasoning:
            snippet = verdict.reasoning[:400]
            result.evidence.append(
                delivery_pb2.Evidence(
                    source_url=store.official_url or "",
                    snippet=snippet,
                    reason="panel_critic",
                )
            )
        return delivery_pb2.JudgeFranchiseTypeResponse(result=result)

    def BatchJudge(self, request_iterator, context):  # noqa: N802
        for req in request_iterator:
            single = delivery_pb2.JudgeFranchiseTypeRequest(context=req.context)
            resp = self.JudgeFranchiseType(single, context)
            yield delivery_pb2.BatchJudgeResponse(result=resp.result)


# ─── Factory ───────────────────────────────────────────────────────────


def pick_servicer(mode: str | None = None) -> delivery_pb2_grpc.DeliveryServiceServicer:
    """DELIVERY_MODE から Servicer を選択する。mock | live | panel。"""
    if mode is None:
        mode = os.getenv("DELIVERY_MODE", "mock").lower()
    if mode == "live":
        return RealDeliveryServicer()
    if mode == "panel":
        return PanelDeliveryServicer()
    if mode == "mock":
        return MockDeliveryServicer()
    raise ValueError(f"unknown DELIVERY_MODE {mode!r} (want mock|live|panel)")


def build_server(
    addr: str | None = None,
    mode: str | None = None,
) -> grpc.Server:
    """gRPC サーバを組み立てる (起動はしない)。

    reflection と health check 付き。mode=mock|live で Servicer を切替。
    """
    if addr is None:
        addr = os.getenv("DELIVERY_LISTEN_ADDR", "0.0.0.0:50053")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    servicer = pick_servicer(mode)
    delivery_pb2_grpc.add_DeliveryServiceServicer_to_server(servicer, server)

    # Health check
    health_servicer = health.HealthServicer()
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    health_servicer.set(
        "pizza.v1.DeliveryService",
        health_pb2.HealthCheckResponse.SERVING,
    )
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    # Reflection
    service_names = (
        delivery_pb2.DESCRIPTOR.services_by_name["DeliveryService"].full_name,
        health_pb2.DESCRIPTOR.services_by_name["Health"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(service_names, server)

    server.add_insecure_port(addr)
    return server


def serve() -> None:
    mode = os.getenv("DELIVERY_MODE", "mock").lower()
    server = build_server(mode=mode)
    server.start()
    print(
        f"🛵 delivery-service ({mode.upper()}) listening on 0.0.0.0:50053",
        flush=True,
    )
    server.wait_for_termination()


# 後方互換
DeliveryServicer = MockDeliveryServicer
