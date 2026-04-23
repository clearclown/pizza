"""PlacesClient.get_place_details のテスト (httpx MockTransport で)。"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from pizza_delivery.places_client import PlacesClient, PlacesAPIError


def test_get_place_details_parses_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "places/" in str(request.url)
        assert request.headers.get("X-Goog-FieldMask") is not None
        return httpx.Response(
            200,
            json={
                "id": "ChIJXXX",
                "displayName": {"text": "モスバーガー秋葉原末広町店"},
                "formattedAddress": "東京都千代田区外神田3-16-14",
                "nationalPhoneNumber": "03-1111-2222",
                "internationalPhoneNumber": "+81 3-1111-2222",
                "websiteUri": "https://www.mos.jp/shop/detail/?shop_cd=02232",
                "location": {"latitude": 35.702, "longitude": 139.771},
            },
        )

    client = PlacesClient(api_key="tok")

    # Transport を一時 monkey patch するため、client 経由の httpx.AsyncClient を
    # transport= で作るより、get_place_details 内部を直接 hook する方が素直。
    # ここでは httpx の global mock 風に: PlacesClient は httpx.AsyncClient を
    # context で作るため、module 側 patch が必要。
    # 代替: client.base_url を MockTransport 用 mock server に向ける別テスト路線。

    # 簡易: transport を notreachable にしない単純 test はこの形で可能。
    # 実際の monkey は以下のように httpx.AsyncClient を patch する。
    import httpx as _httpx

    original = _httpx.AsyncClient

    def _factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original(*args, **kwargs)

    _httpx.AsyncClient = _factory  # type: ignore
    try:
        r = asyncio.run(client.get_place_details("ChIJXXX"))
    finally:
        _httpx.AsyncClient = original  # type: ignore

    assert r is not None
    assert r.name == "モスバーガー秋葉原末広町店"
    assert r.phone == "03-1111-2222"
    assert r.website_uri.startswith("https://www.mos.jp/")


def test_get_place_details_returns_none_on_404() -> None:
    import httpx as _httpx

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    original = _httpx.AsyncClient

    def _factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original(*args, **kwargs)

    _httpx.AsyncClient = _factory  # type: ignore
    try:
        client = PlacesClient(api_key="tok")
        r = asyncio.run(client.get_place_details("MISSING"))
    finally:
        _httpx.AsyncClient = original  # type: ignore

    assert r is None


def test_get_place_details_empty_id() -> None:
    client = PlacesClient(api_key="tok")
    r = asyncio.run(client.get_place_details(""))
    assert r is None


def test_get_place_details_raises_on_5xx() -> None:
    import httpx as _httpx

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    original = _httpx.AsyncClient

    def _factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original(*args, **kwargs)

    _httpx.AsyncClient = _factory  # type: ignore
    try:
        client = PlacesClient(api_key="tok")
        with pytest.raises(PlacesAPIError):
            asyncio.run(client.get_place_details("X"))
    finally:
        _httpx.AsyncClient = original  # type: ignore
