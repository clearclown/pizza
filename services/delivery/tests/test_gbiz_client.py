"""gbiz_client: gBizINFO REST API クライアントのユニットテスト。

httpx.MockTransport で API を mock して、トークン無し/あり、検索/詳細、
verify_operator_via_gbiz 動作を検証。
"""

from __future__ import annotations

import json

import httpx
import pytest

from pizza_delivery.gbiz_client import (
    GBizClient,
    GBizRecord,
    verify_operator_via_gbiz,
)


# ─── ready() ───────────────────────────────────────────────────


def test_ready_false_without_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GBIZ_API_TOKEN", raising=False)
    assert GBizClient().ready() is False


def test_ready_true_with_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GBIZ_API_TOKEN", "dummy-token")
    assert GBizClient().ready() is True


# ─── get_by_corporate_number ───────────────────────────────────


def test_get_by_corporate_number_skips_when_no_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GBIZ_API_TOKEN", raising=False)
    import asyncio
    r = asyncio.run(GBizClient().get_by_corporate_number("1234567890123"))
    assert r is None


def test_get_by_corporate_number_parses_hojin_info() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "1234567890123" in str(request.url)
        assert request.headers.get("X-hojinInfo-api-token") == "tok"
        return httpx.Response(
            200,
            json={
                "hojin-infos": [
                    {
                        "corporate_number": "1234567890123",
                        "name": "株式会社テスト商事",
                        "postal_code": "1000001",
                        "location": "東京都千代田区千代田1-1",
                        "representative_name": "山田太郎",
                        "representative_title": "代表取締役",
                        "capital_stock": "10000000",
                        "employee_number": "50",
                        "kind": "株式会社",
                        "update_date": "2025-01-01",
                    }
                ]
            },
        )

    transport = httpx.MockTransport(handler)
    client = GBizClient(token="tok", transport=transport)
    import asyncio
    r = asyncio.run(client.get_by_corporate_number("1234567890123"))
    assert r is not None
    assert r.corporate_number == "1234567890123"
    assert r.name == "株式会社テスト商事"
    assert r.address == "東京都千代田区千代田1-1"
    assert r.capital_stock == "10000000"


def test_get_by_corporate_number_returns_none_on_404() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    import asyncio
    r = asyncio.run(
        GBizClient(token="tok", transport=transport).get_by_corporate_number(
            "0000000000000"
        )
    )
    assert r is None


def test_get_by_corporate_number_rejects_non_13_digit() -> None:
    import asyncio
    c = GBizClient(token="tok")
    assert asyncio.run(c.get_by_corporate_number("12345")) is None
    assert asyncio.run(c.get_by_corporate_number("abcdefghijklm")) is None
    assert asyncio.run(c.get_by_corporate_number("")) is None


# ─── search_by_name ────────────────────────────────────────────


def test_search_by_name_returns_records() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "hojin-infos": [
                    {
                        "corporate_number": "1111111111111",
                        "name": "株式会社A",
                        "location": "東京都港区",
                    },
                    {
                        "corporate_number": "2222222222222",
                        "name": "株式会社B",
                        "location": "東京都中央区",
                    },
                ]
            },
        )

    import asyncio
    res = asyncio.run(
        GBizClient(token="tok", transport=httpx.MockTransport(handler)).search_by_name(
            "株式会社"
        )
    )
    assert res.found is True
    assert len(res.records) == 2


def test_search_by_name_empty_without_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GBIZ_API_TOKEN", raising=False)
    import asyncio
    res = asyncio.run(GBizClient().search_by_name("株式会社"))
    assert res.found is False


# ─── verify_operator_via_gbiz ─────────────────────────────────


def test_verify_operator_gbiz_skipped_when_no_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GBIZ_API_TOKEN", raising=False)
    import asyncio
    r = asyncio.run(verify_operator_via_gbiz("株式会社テスト"))
    assert r["exists"] is False
    assert r["source"] == "gbiz_skipped"


def test_verify_operator_gbiz_hit() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "hojin-infos": [
                    {
                        "corporate_number": "5010401089998",
                        "name": "大和フーヅ株式会社",
                        "location": "埼玉県熊谷市",
                    }
                ]
            },
        )

    import asyncio
    client = GBizClient(token="tok", transport=httpx.MockTransport(handler))
    r = asyncio.run(verify_operator_via_gbiz("大和フーヅ株式会社", client=client))
    assert r["exists"] is True
    assert r["best_match_number"] == "5010401089998"
    assert r["source"] == "gbiz"
    assert r["name_similarity"] >= 0.9
