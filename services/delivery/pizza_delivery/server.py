"""gRPC DeliveryServicer — Phase 1 は固定値の mock 実装。

Phase 3 (browser-use 統合) までの間、Oven Pipeline の Judge 呼び出しが
疎通する状態を維持するためのダミーサーバ。全リクエストに対し
is_franchise=true / operator="(mock) 株式会社モック運営" / count=25 を返す。

実行:
    cd services/delivery
    uv run python -m pizza_delivery
    # => 0.0.0.0:50053 で gRPC serve
"""

from __future__ import annotations

import os
from concurrent import futures
from typing import Any

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from pizza.v1 import delivery_pb2, delivery_pb2_grpc


class MockDeliveryServicer(delivery_pb2_grpc.DeliveryServiceServicer):
    """固定値を返す Phase 1 mock。Phase 3 で本実装に差し替える。"""

    def JudgeFranchiseType(  # noqa: N802 — gRPC convention
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
                snippet="Phase 1 mock — Phase 3 で browser-use で実判定",
                reason="Phase 1 baseline",
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


def build_server(addr: str | None = None) -> grpc.Server:
    """gRPC サーバを組み立てる (起動はしない)。テスト/実行共通の factory。

    reflection と health check を登録済 (`grpcurl list` / `grpc_health_probe` 可)。
    """
    if addr is None:
        addr = os.getenv("DELIVERY_LISTEN_ADDR", "0.0.0.0:50053")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    delivery_pb2_grpc.add_DeliveryServiceServicer_to_server(
        MockDeliveryServicer(), server
    )
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
    server = build_server()
    server.start()
    print("🛵 delivery-service (MOCK) listening on 0.0.0.0:50053", flush=True)
    server.wait_for_termination()


# 後方互換: 既存コードが DeliveryServicer 名を参照しているため alias
DeliveryServicer = MockDeliveryServicer
