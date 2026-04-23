"""Phase 16 Territory check の TDD。

業種別 territory 半径を yaml から load し、
- brand → (dominant_min_m, dominant_typical_m, territory_max_m) lookup
- 2 店舗 pair 判定 (duplicate / dominant / territory_independent)
- 既知店舗群の cover map (bottom-up scan 省略に使う)
"""

from __future__ import annotations

import pytest

from pizza_delivery.territory import (
    TerritoryClass,
    check_pair,
    compute_cover_map,
    load_territory_knowledge,
    territory_radius,
)


# ─── load_territory_knowledge ────────────────────────────────────────


def test_load_territory_knowledge_returns_industries() -> None:
    kb = load_territory_knowledge()
    assert "convenience_dominant" in kb.industries
    assert "gym_24h_territory" in kb.industries
    assert "fastfood_urban" in kb.industries


def test_load_territory_knowledge_includes_known_brands() -> None:
    kb = load_territory_knowledge()
    assert "セブン-イレブン" in kb.brands
    assert "エニタイムフィットネス" in kb.brands
    assert "TSUTAYA" in kb.brands
    assert "モスバーガー" in kb.brands


# ─── territory_radius ────────────────────────────────────────────────


@pytest.mark.parametrize(
    "brand, expected_min_m",
    [
        ("セブン-イレブン", 80),         # convenience_dominant
        ("セイコーマート", 150),         # brand override
        ("エニタイムフィットネス", 800), # brand override
        ("chocoZAP", 100),               # gym_24h_dominant
        ("スターバックス", 50),          # brand override
        ("コメダ珈琲", 1000),            # fastfood_suburban default
    ],
)
def test_territory_radius_known_brands(brand, expected_min_m) -> None:
    r = territory_radius(brand)
    assert r is not None
    assert r.dominant_min_m == expected_min_m


def test_territory_radius_unknown_brand_returns_none() -> None:
    r = territory_radius("未知のブランドXYZ")
    assert r is None


# ─── check_pair (2 店舗 pair 判定) ────────────────────────────────────


def test_check_pair_too_close_is_duplicate_suspect() -> None:
    # セブン-イレブン 2 店舗 が 50m 以内 = dominant_min_m (80m) 未満
    cls = check_pair(
        brand="セブン-イレブン",
        lat1=35.6812, lng1=139.7671,
        lat2=35.68125, lng2=139.76715,  # 約 7m
    )
    assert cls == TerritoryClass.DUPLICATE_SUSPECT


def test_check_pair_dominant_typical() -> None:
    # セブン 200m 離れ (80-1000m 内) → dominant 範囲
    cls = check_pair(
        brand="セブン-イレブン",
        lat1=35.6812, lng1=139.7671,
        lat2=35.6820, lng2=139.7700,  # 約 260m
    )
    assert cls == TerritoryClass.DOMINANT_CLUSTER


def test_check_pair_independent_territory() -> None:
    # エニタイムフィットネス 10km 離れ → 別 territory
    cls = check_pair(
        brand="エニタイムフィットネス",
        lat1=35.6812, lng1=139.7671,
        lat2=35.7700, lng2=139.8500,  # ~12km
    )
    assert cls == TerritoryClass.INDEPENDENT


def test_check_pair_unknown_brand_fallsback_to_generic() -> None:
    # 未登録ブランドは generic default (ある程度の距離閾値) を使う
    cls = check_pair(
        brand="未知ブランド", lat1=0, lng1=0, lat2=0.0001, lng2=0.0001,
    )
    # classify だけ、正確な class は不問、crash しない
    assert cls in (
        TerritoryClass.DUPLICATE_SUSPECT,
        TerritoryClass.DOMINANT_CLUSTER,
        TerritoryClass.INDEPENDENT,
        TerritoryClass.UNKNOWN,
    )


# ─── compute_cover_map (既知店舗から カバー範囲) ─────────────────────


def test_compute_cover_map_simple() -> None:
    """既知 store が存在する領域を territory 半径でカバー判定。"""
    stores = [
        {"place_id": "A", "lat": 35.6812, "lng": 139.7671},
        {"place_id": "B", "lat": 35.7000, "lng": 139.7700},
    ]
    cover = compute_cover_map("エニタイムフィットネス", stores)
    # 既知店舗から territory_typical_m (1600m) 以内の点は covered
    # 店舗 A の近く (100m 以内) は covered
    assert cover.is_covered(35.6813, 139.7672) is True
    # 10km 離れた地点は covered 外
    assert cover.is_covered(36.0, 140.0) is False


def test_compute_cover_map_handles_empty_stores() -> None:
    cover = compute_cover_map("エニタイムフィットネス", [])
    # 空なら何処も covered 外
    assert cover.is_covered(35.0, 139.0) is False
