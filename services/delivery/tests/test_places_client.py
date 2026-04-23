"""Unit tests for PlacesClient (Phase 5 Places API integration)."""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest
import httpx

from pizza_delivery.places_client import (
    DEFAULT_FIELD_MASK,
    PlaceRaw,
    PlacesAPIError,
    PlacesClient,
)


# ─── httpx MockTransport helpers ───────────────────────────────────────


def _mock_text_search(request: httpx.Request) -> httpx.Response:
    assert request.method == "POST"
    assert request.url.path.endswith("/places:searchText")
    assert request.headers.get("X-Goog-Api-Key") == "test-key"
    assert request.headers.get("X-Goog-FieldMask") == DEFAULT_FIELD_MASK
    payload = json.loads(request.content.decode())
    assert "textQuery" in payload
    return httpx.Response(
        200,
        json={
            "places": [
                {
                    "id": "ChIJ_A",
                    "displayName": {"text": "A 新宿店", "languageCode": "ja"},
                    "formattedAddress": "東京都新宿区",
                    "location": {"latitude": 35.69, "longitude": 139.70},
                    "websiteUri": "https://a.example.com/shinjuku",
                    "nationalPhoneNumber": "03-0000-0001",
                },
                {
                    "id": "ChIJ_B",
                    "displayName": {"text": "A 渋谷店", "languageCode": "ja"},
                    "formattedAddress": "東京都渋谷区",
                    "location": {"latitude": 35.66, "longitude": 139.70},
                },
            ],
            "nextPageToken": "",
        },
    )


def _mock_error_response(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        403,
        json={"error": {"code": 403, "message": "PERMISSION_DENIED"}},
    )


# ─── Tests ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_search_text_parses_places(monkeypatch: pytest.MonkeyPatch) -> None:
    _orig_client = httpx.AsyncClient

    def patched_async_client(*a, **kw):
        kw.pop("transport", None)
        return _orig_client(
            transport=httpx.MockTransport(_mock_text_search), **kw
        )

    monkeypatch.setattr(httpx, "AsyncClient", patched_async_client)

    client = PlacesClient(api_key="test-key")
    result = await client.search_text("A 東京都")
    assert len(result.places) == 2
    assert result.places[0].place_id == "ChIJ_A"
    assert result.places[0].name == "A 新宿店"
    assert result.places[0].address == "東京都新宿区"
    assert result.places[0].lat == pytest.approx(35.69)
    assert result.places[0].website_uri == "https://a.example.com/shinjuku"
    assert result.places[0].phone == "03-0000-0001"
    assert result.next_page_token == ""


@pytest.mark.asyncio
async def test_search_text_requires_query() -> None:
    client = PlacesClient(api_key="test-key")
    with pytest.raises(ValueError):
        await client.search_text("")


@pytest.mark.asyncio
async def test_search_text_errors_without_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GOOGLE_MAPS_API_KEY", raising=False)
    client = PlacesClient(api_key="")
    with pytest.raises(PlacesAPIError):
        await client.search_text("anything")


@pytest.mark.asyncio
async def test_search_text_propagates_api_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _orig_client = httpx.AsyncClient

    def patched_async_client(*a, **kw):
        kw.pop("transport", None)
        return _orig_client(
            transport=httpx.MockTransport(_mock_error_response), **kw
        )

    monkeypatch.setattr(httpx, "AsyncClient", patched_async_client)
    client = PlacesClient(api_key="test-key")
    with pytest.raises(PlacesAPIError) as ei:
        await client.search_text("anything")
    assert ei.value.status == 403


@pytest.mark.asyncio
async def test_search_by_operator_builds_query(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_payload = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        nonlocal captured_payload
        captured_payload = json.loads(request.content.decode())
        return httpx.Response(200, json={"places": [], "nextPageToken": ""})

    _orig_client = httpx.AsyncClient

    def patched_async_client(*a, **kw):
        kw.pop("transport", None)
        return _orig_client(transport=httpx.MockTransport(_capture), **kw)

    monkeypatch.setattr(httpx, "AsyncClient", patched_async_client)

    client = PlacesClient(api_key="test-key")
    await client.search_by_operator("株式会社AFJ Project", area_hint="東京都")
    assert "textQuery" in captured_payload
    q = captured_payload["textQuery"]
    assert "株式会社AFJ Project" in q
    assert "店舗" in q
    assert "東京都" in q


@pytest.mark.asyncio
async def test_search_by_operator_empty_returns_empty() -> None:
    client = PlacesClient(api_key="test-key")
    got = await client.search_by_operator("")
    assert got == []


@pytest.mark.asyncio
async def test_client_uses_env_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "env-key-xyz")
    client = PlacesClient(api_key="")
    assert client.api_key == "env-key-xyz"
