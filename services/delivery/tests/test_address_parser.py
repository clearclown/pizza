"""Phase 10.1 — 住所 parser テスト。

日本住所を pref + city + rest に分割、pref+city 完全一致を match gate に使う。
"""

from __future__ import annotations

import pytest

from pizza_delivery.match import (
    MergeResult,
    ParsedAddress,
    match_by_address,
    merge_all,
    parse_address,
)


# ─── pref + city parsing ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw, pref, city",
    [
        ("東京都新宿区西新宿6-3-1", "東京都", "新宿区"),
        ("〒160-0023 東京都新宿区西新宿6丁目3番1号", "東京都", "新宿区"),
        ("大阪府大阪市福島区玉川1-8-10", "大阪府", "大阪市福島区"),
        ("大阪市福島区玉川1-8-10", "", "大阪市福島区"),  # pref 省略
        ("北海道小樽市新光1-11-1", "北海道", "小樽市"),
        ("愛知県名古屋市中区栄3-18-1", "愛知県", "名古屋市中区"),
        ("京都府京都市左京区北白川", "京都府", "京都市左京区"),
        ("群馬県前橋市本町2-12-1", "群馬県", "前橋市"),
        ("沖縄県那覇市久茂地3-1-1", "沖縄県", "那覇市"),
        ("鹿児島県鹿児島市中央町1-1", "鹿児島県", "鹿児島市"),
        ("", "", ""),
        ("abc", "", ""),
    ],
)
def test_parse_address_splits_pref_city(raw, pref, city) -> None:
    p = parse_address(raw)
    assert isinstance(p, ParsedAddress)
    assert p.pref == pref, f"pref mismatch: {raw!r} → {p.pref!r}"
    assert p.city == city, f"city mismatch: {raw!r} → {p.city!r}"


def test_parse_address_rest_contains_street() -> None:
    p = parse_address("〒160-0023 東京都新宿区西新宿6丁目3番1号")
    # rest は「西新宿...」以降
    assert "西新宿" in p.rest


def test_parse_address_handles_special_wards() -> None:
    # 東京 23 区はそれぞれ単独で city
    for ward in ["千代田区", "渋谷区", "港区", "新宿区", "世田谷区"]:
        p = parse_address(f"東京都{ward}神宮前1-1-1")
        assert p.pref == "東京都"
        assert p.city == ward


# ─── 政令市の区 (〇〇市〇〇区) は市区ごとの扱い ────────────────────


@pytest.mark.parametrize(
    "raw, city",
    [
        ("大阪府大阪市中央区", "大阪市中央区"),
        ("京都府京都市下京区", "京都市下京区"),
        ("神奈川県横浜市中区", "横浜市中区"),
        ("愛知県名古屋市東区", "名古屋市東区"),
    ],
)
def test_parse_address_seirei_city_ward(raw, city) -> None:
    p = parse_address(raw)
    assert p.city == city


# ─── match_by_address strict: pref+city 一致必須 ─────────────────


def test_match_by_address_requires_pref_city_match() -> None:
    """同住所文字列でも pref が違えば match しない。"""
    top = [{"place_id": "T1", "address": "東京都新宿区西新宿6-3-1"}]
    # pref 違い + city 偶然同じ "新宿区"
    bottom = [{"place_id": "B1", "address": "北海道新宿区西新宿6-3-1"}]
    # 現行: bi-gram で同一判定されてしまう
    # 新実装: pref が違うと match 不可
    res = match_by_address(top, bottom, threshold=0.7)
    assert res == [], "pref 違いは reject されるべき"


def test_match_by_address_same_pref_city_bigram_matches() -> None:
    """同 pref+city なら bi-gram で番地違いでも match。"""
    top = [{"place_id": "T1", "address": "東京都新宿区西新宿6-3-1"}]
    bottom = [{"place_id": "B1", "address": "〒160-0023 東京都新宿区西新宿6丁目3番1号"}]
    res = match_by_address(top, bottom, threshold=0.7)
    assert len(res) == 1
    assert res[0].top_id == "T1"
