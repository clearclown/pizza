"""match.py — 決定論的突合エンジンの TDD テスト。

Phase 8 BrandAuditor の核になる、Places API 結果 (Top-down) と
SQLite stores (Bottom-up) を突合する機能。

3 段階:
  1. place_id 完全一致 (最強)
  2. 住所 normalize + bi-gram Jaccard
  3. 緯度経度 Haversine 距離
"""

from __future__ import annotations

import pytest

from pizza_delivery.match import (
    MatchCandidate,
    MergeResult,
    haversine_m,
    match_by_address,
    match_by_place_id,
    match_by_proximity,
    merge_all,
    normalize_address,
)


# ─── normalize_address ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("〒160-0023 東京都新宿区西新宿6-3-1", "東京都新宿区西新宿6-3-1"),
        ("東京都 新宿区 西新宿 6-3-1", "東京都新宿区西新宿6-3-1"),
        ("東京都新宿区西新宿6丁目3番1号", "東京都新宿区西新宿6-3-1"),
        ("大阪府大阪市福島区玉川1-8-10 UGビル2F", "大阪府大阪市福島区玉川1-8-10"),
        ("", ""),
    ],
)
def test_normalize_address(raw: str, expected: str) -> None:
    got = normalize_address(raw)
    assert got == expected


# ─── place_id 完全一致 ────────────────────────────────────────────────


def test_match_by_place_id_exact() -> None:
    top = [
        {"place_id": "ChIJ_A", "name": "N1"},
        {"place_id": "ChIJ_B", "name": "N2"},
        {"place_id": "ChIJ_X", "name": "X"},
    ]
    bottom = [
        {"place_id": "ChIJ_B", "address": "東京"},
        {"place_id": "ChIJ_A", "address": "大阪"},
        {"place_id": "ChIJ_Y", "address": "京都"},
    ]
    result = match_by_place_id(top, bottom)
    assert len(result) == 2
    ids = {m.top_id for m in result}
    assert ids == {"ChIJ_A", "ChIJ_B"}


# ─── 住所 bi-gram 突合 ──────────────────────────────────────────────────


def test_match_by_address_normalize_variants() -> None:
    top = [{"place_id": "T1", "address": "東京都新宿区西新宿6-3-1"}]
    bottom = [
        {"place_id": "B1", "address": "〒160-0023 東京都新宿区西新宿6丁目3番1号"},
    ]
    res = match_by_address(top, bottom, threshold=0.7)
    assert len(res) == 1
    assert res[0].top_id == "T1" and res[0].bottom_id == "B1"
    assert res[0].score >= 0.7


def test_match_by_address_rejects_low_similarity() -> None:
    top = [{"place_id": "T1", "address": "東京都渋谷区"}]
    bottom = [{"place_id": "B1", "address": "北海道札幌市"}]
    res = match_by_address(top, bottom, threshold=0.7)
    assert res == []


# ─── 緯度経度 Haversine ────────────────────────────────────────────────


def test_haversine_m_known_pair() -> None:
    # 東京駅 (35.6812, 139.7671) → 新宿駅 (35.6896, 139.7006)
    # 直線距離 ~6.2 km
    d = haversine_m(35.6812, 139.7671, 35.6896, 139.7006)
    assert 6000 < d < 6400


def test_haversine_m_same_point() -> None:
    assert haversine_m(35.0, 139.0, 35.0, 139.0) < 1.0


def test_match_by_proximity_50m() -> None:
    top = [{"place_id": "T1", "lat": 35.6812, "lng": 139.7671}]
    # 0.0003 deg ~ 33m (lat 1deg ≈ 111km)
    bottom = [
        {"place_id": "B1", "lat": 35.68123, "lng": 139.76713},  # 数m
        {"place_id": "B2", "lat": 35.7000, "lng": 139.8000},     # 遠い
    ]
    res = match_by_proximity(top, bottom, radius_m=100)
    assert len(res) == 1
    assert res[0].bottom_id == "B1"


# ─── merge_all 集約 ──────────────────────────────────────────────────


def test_merge_all_combines_strategies() -> None:
    top = [
        {"place_id": "P1", "address": "東京都渋谷区道玄坂1-1", "lat": 35.659, "lng": 139.700},
        {"place_id": "P2", "address": "大阪府大阪市北区梅田1-1", "lat": 34.700, "lng": 135.500},
        {"place_id": "P3", "address": "名古屋市中区栄", "lat": 35.170, "lng": 136.900},
    ]
    bottom = [
        # P1 と place_id 一致
        {"place_id": "P1", "address": "渋谷道玄坂", "lat": 35.659, "lng": 139.700},
        # P2 と住所が normalize で一致
        {"place_id": "B2", "address": "〒530-0001 大阪府大阪市北区梅田1丁目1-1", "lat": 34.701, "lng": 135.501},
        # P3 に近接 50m
        {"place_id": "B3", "address": "全然違う文字列", "lat": 35.17005, "lng": 136.90005},
    ]
    merged = merge_all(top, bottom, addr_threshold=0.6, radius_m=100)
    assert isinstance(merged, MergeResult)
    # 3 件全てマッチするはず
    assert len(merged.matches) == 3
    # unmatched は 0
    assert merged.unmatched_top == []
    assert merged.unmatched_bottom == []


def test_merge_all_keeps_unmatched() -> None:
    top = [{"place_id": "T1", "address": "東京", "lat": 35.0, "lng": 139.0}]
    bottom = [
        {"place_id": "B1", "address": "全然違う", "lat": 40.0, "lng": 130.0},
    ]
    merged = merge_all(top, bottom)
    assert merged.matches == []
    assert len(merged.unmatched_top) == 1
    assert len(merged.unmatched_bottom) == 1


def test_match_candidate_carries_strategy() -> None:
    top = [{"place_id": "P1", "address": "", "lat": 0.0, "lng": 0.0}]
    bottom = [{"place_id": "P1", "address": "", "lat": 0.0, "lng": 0.0}]
    merged = merge_all(top, bottom)
    assert merged.matches[0].strategy == "place_id"
    assert merged.matches[0].score == 1.0
