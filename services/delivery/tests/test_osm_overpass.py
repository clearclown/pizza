"""Phase 17.1: OSM Overpass API client のテスト (mock transport)。"""

from __future__ import annotations

import httpx
import pytest

from pizza_delivery.osm_overpass import (
    OSMPlace,
    OverpassClient,
    brand_to_osm_tags,
)


# ─── brand → OSM tag mapping ──────────────────────────────────────────


@pytest.mark.parametrize(
    "brand, expected_tag",
    [
        ("エニタイムフィットネス", "leisure=fitness_centre"),
        ("chocoZAP", "leisure=fitness_centre"),
        ("ゴールドジム", "leisure=fitness_centre"),
        ("セブン-イレブン", "shop=convenience"),
        ("ファミリーマート", "shop=convenience"),
        ("マクドナルド", "amenity=fast_food"),
        ("モスバーガー", "amenity=fast_food"),
        ("スターバックス", "amenity=cafe"),
        ("TSUTAYA", "shop=books"),
        ("ブックオフ", "shop=second_hand"),
    ],
)
def test_brand_to_osm_tags(brand: str, expected_tag: str) -> None:
    tags = brand_to_osm_tags(brand)
    assert expected_tag in tags


def test_brand_to_osm_tags_unknown_returns_empty() -> None:
    assert brand_to_osm_tags("完全に未知のブランド") == []


# ─── OverpassClient (MockTransport で閉じて test) ────────────────────


_SAMPLE_RESPONSE = {
    "version": 0.6,
    "generator": "Overpass API",
    "elements": [
        {
            "type": "node",
            "id": 1,
            "lat": 35.6812,
            "lon": 139.7671,
            "tags": {
                "name": "エニタイムフィットネス 東京駅前店",
                "leisure": "fitness_centre",
                "addr:full": "東京都千代田区丸の内1-1-1",
            },
        },
        {
            "type": "node",
            "id": 2,
            "lat": 35.6895,
            "lon": 139.7006,
            "tags": {
                "name": "エニタイムフィットネス 新宿店",
                "leisure": "fitness_centre",
                "addr:street": "西新宿",
                "addr:city": "新宿区",
                "addr:postcode": "160-0023",
            },
        },
    ],
}


@pytest.mark.asyncio
async def test_overpass_query_by_tag_parses_response() -> None:
    called_with: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        called_with["url"] = str(request.url)
        called_with["body"] = request.content.decode() if request.content else ""
        import json as json_mod
        return httpx.Response(200, text=json_mod.dumps(_SAMPLE_RESPONSE))

    client = OverpassClient(transport=httpx.MockTransport(handler))
    places = await client.query_by_tag(
        tag="leisure=fitness_centre",
        bbox=(35.5, 139.5, 35.9, 140.0),
    )
    assert len(places) == 2
    assert all(isinstance(p, OSMPlace) for p in places)
    assert places[0].name == "エニタイムフィットネス 東京駅前店"
    assert places[0].lat == pytest.approx(35.6812)
    assert places[0].lng == pytest.approx(139.7671)
    # address が組み立てられる
    assert "丸の内" in places[0].address


@pytest.mark.asyncio
async def test_overpass_handles_empty_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        import json as json_mod
        return httpx.Response(200, text=json_mod.dumps({"elements": []}))

    client = OverpassClient(transport=httpx.MockTransport(handler))
    places = await client.query_by_tag(
        tag="leisure=fitness_centre", bbox=(0, 0, 1, 1)
    )
    assert places == []


@pytest.mark.asyncio
async def test_overpass_handles_api_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, text="Too many requests")

    client = OverpassClient(transport=httpx.MockTransport(handler))
    # rate limit 時は空 list を返し crash しない (落とす選択肢もあるが gap filling 用途)
    places = await client.query_by_tag(
        tag="leisure=fitness_centre", bbox=(0, 0, 1, 1)
    )
    assert places == []


def test_osm_place_address_assembly_from_parts() -> None:
    p = OSMPlace(
        osm_id=1, name="X", lat=0, lng=0, tags={
            "addr:postcode": "100-0005",
            "addr:state": "東京都",
            "addr:city": "千代田区",
            "addr:street": "丸の内",
            "addr:housenumber": "1-1-1",
        }
    )
    # assemble_address が pref + city + street + housenumber を連結
    assert "東京都千代田区丸の内1-1-1" in p.address or "千代田区丸の内" in p.address


def test_osm_place_address_prefers_addr_full() -> None:
    p = OSMPlace(
        osm_id=1, name="X", lat=0, lng=0, tags={
            "addr:full": "東京都渋谷区道玄坂1-1-1",
            "addr:city": "新宿区",  # addr:full 優先
        }
    )
    assert "渋谷区" in p.address


# ─── recall metrics helper ─────────────────────────────────────────────


def test_compute_recall_ratio() -> None:
    from pizza_delivery.osm_overpass import compute_recall_ratio

    # 10 店舗 Places 取得、OSM では 12 件 → recall = 10/12 ≈ 0.833
    r = compute_recall_ratio(places_count=10, reference_count=12)
    assert r == pytest.approx(0.833, abs=0.001)


def test_compute_recall_ratio_zero_reference() -> None:
    from pizza_delivery.osm_overpass import compute_recall_ratio

    # reference が 0 なら recall 不明 → None
    assert compute_recall_ratio(places_count=5, reference_count=0) is None
