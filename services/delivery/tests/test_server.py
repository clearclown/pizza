"""🟢 Phase 1 Green test — mock DeliveryServicer の契約検証。

実 gRPC サーバを ephemeral port で立て、client から JudgeFranchiseType と
BatchJudge を呼ぶことで end-to-end の疎通を確認する。
"""

from __future__ import annotations

import pytest

import grpc

from pizza.v1 import delivery_pb2, delivery_pb2_grpc, seed_pb2
from pizza_delivery.server import build_server


@pytest.fixture()
def running_server_addr():
    server = build_server(addr="127.0.0.1:0")
    # build_server 側で既に add_insecure_port(0) 済。port は内部で握られている。
    # 追加の空 port を取り直すため、もう 1 つ add_insecure_port して返す。
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        yield f"127.0.0.1:{port}"
    finally:
        server.stop(grace=None)


def test_mock_returns_fixed_judgement(running_server_addr: str) -> None:
    with grpc.insecure_channel(running_server_addr) as ch:
        stub = delivery_pb2_grpc.DeliveryServiceStub(ch)
        req = delivery_pb2.JudgeFranchiseTypeRequest(
            context=delivery_pb2.StoreContext(
                store=seed_pb2.Store(
                    place_id="ChIJ_test",
                    brand="エニタイムフィットネス",
                    name="テスト店舗",
                    official_url="https://example.com/test",
                ),
                markdown="# 会社概要\n\n運営: 株式会社テスト",
            ),
        )
        resp = stub.JudgeFranchiseType(req, timeout=5)

    assert resp.result.place_id == "ChIJ_test"
    assert resp.result.is_franchise is True
    assert "モック" in resp.result.operator_name
    assert resp.result.store_count_estimate == 25
    assert 0.0 <= resp.result.confidence <= 1.0
    assert resp.result.llm_provider == "mock"
    assert len(resp.result.evidence) >= 1


def test_mock_batch_judge_streams_results(running_server_addr: str) -> None:
    with grpc.insecure_channel(running_server_addr) as ch:
        stub = delivery_pb2_grpc.DeliveryServiceStub(ch)
        requests = [
            delivery_pb2.BatchJudgeRequest(
                context=delivery_pb2.StoreContext(
                    store=seed_pb2.Store(place_id=f"p{i}", name=f"S{i}"),
                ),
            )
            for i in range(3)
        ]
        responses = list(stub.BatchJudge(iter(requests), timeout=5))
    assert len(responses) == 3
    assert {r.result.place_id for r in responses} == {"p0", "p1", "p2"}
