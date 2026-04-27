"""osm_fetch_all: OSM operator tag capture."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from pizza_delivery.commands.osm_fetch_all import (
    _is_japan_place,
    _matches_brand,
    _operator_from_osm_tags,
    _osm_place_id,
    _split_brands,
    _upsert_operator_from_osm,
    _upsert_store,
)


def _setup_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE stores (
            place_id TEXT PRIMARY KEY,
            brand TEXT,
            name TEXT,
            address TEXT,
            lat REAL,
            lng REAL,
            official_url TEXT,
            phone TEXT,
            grid_cell_id TEXT
        );
        CREATE TABLE operator_stores (
            operator_name TEXT NOT NULL,
            place_id TEXT NOT NULL,
            brand TEXT,
            operator_type TEXT,
            confidence REAL DEFAULT 0.0,
            discovered_via TEXT DEFAULT 'per_store',
            corporate_number TEXT,
            verification_source TEXT,
            PRIMARY KEY (operator_name, place_id)
        );
        """
    )
    return conn


def test_operator_from_osm_tags_accepts_legal_entity() -> None:
    op = _operator_from_osm_tags(
        {"operator:ja": "株式会社テスト運営"},
        brand="モスバーガー",
        franchisor_blocklist=set(),
    )
    assert op == "株式会社テスト運営"


def test_operator_from_osm_tags_rejects_brand_and_non_company() -> None:
    assert _operator_from_osm_tags(
        {"operator:ja": "モスバーガー"},
        brand="モスバーガー",
        franchisor_blocklist=set(),
    ) == ""
    assert _operator_from_osm_tags(
        {"operator": "個人オーナー"},
        brand="モスバーガー",
        franchisor_blocklist=set(),
    ) == ""


def test_operator_from_osm_tags_rejects_franchisor_blocklist() -> None:
    assert _operator_from_osm_tags(
        {"operator": "株式会社モスフードサービス"},
        brand="モスバーガー",
        franchisor_blocklist={"モスフードサービス"},
    ) == ""


def test_operator_from_osm_tags_rejects_franchisor_group_prefix() -> None:
    assert _operator_from_osm_tags(
        {"operator": "カルチュア・コンビニエンス・クラブ株式会社"},
        brand="TSUTAYA",
        franchisor_blocklist={"カルチュア・エクスペリエンス株式会社"},
    ) == ""


def test_matches_brand_accepts_osm_aliases() -> None:
    assert _matches_brand(
        "MOS Burger",
        {"brand:en": "MOS Burger"},
        "モスバーガー",
    )
    assert _matches_brand("コメダ珈琲店", {}, "コメダ珈琲")


def test_matches_brand_rejects_itto_substring_noise() -> None:
    assert not _matches_brand("NITTO CAMERA", {}, "Itto個別指導学院")
    assert not _matches_brand("PITTORE SQUARE", {}, "Itto個別指導学院")
    assert _matches_brand("ITTO個別指導学院", {}, "Itto個別指導学院")


def test_matches_brand_rejects_not_brand_tags() -> None:
    assert not _matches_brand(
        "ハネルカフェ 鵜沼店",
        {"brand": "モスバーガー", "not:brand:wikidata": "Q1204169"},
        "モスバーガー",
    )


def test_is_japan_place_rejects_korea_inside_old_bbox() -> None:
    assert _is_japan_place(35.6, 139.7, {})
    assert _is_japan_place(26.2, 127.7, {"addr:country": "JP"})
    assert not _is_japan_place(37.5, 126.7, {})
    assert not _is_japan_place(35.6, 139.7, {"addr:country": "KR"})


def test_osm_place_id_keeps_node_backward_compatibility() -> None:
    assert _osm_place_id(123, "node") == "osm:123"
    assert _osm_place_id(123, "way") == "osm:way:123"


def test_split_brands_defaults_to_14_target_brands() -> None:
    assert len(_split_brands("")) == 14
    assert "モスバーガー" in _split_brands("")
    assert _split_brands("モスバーガー, TSUTAYA ") == ["モスバーガー", "TSUTAYA"]


def test_upsert_store_and_operator_from_osm(tmp_path: Path) -> None:
    conn = _setup_db(tmp_path / "p.sqlite")
    try:
        assert _upsert_store(
            conn,
            brand="モスバーガー",
            name="モスバーガー テスト店",
            address="東京都千代田区丸の内1-1-1",
            lat=35.0,
            lng=139.0,
            osm_id=123,
            osm_type="node",
        )
        assert _upsert_operator_from_osm(
            conn,
            brand="モスバーガー",
            operator_name="株式会社テスト運営",
            osm_id=123,
            osm_type="node",
        )
        row = conn.execute(
            "SELECT operator_name, place_id, brand, operator_type, discovered_via, verification_source "
            "FROM operator_stores"
        ).fetchone()
        assert row == (
            "株式会社テスト運営",
            "osm:123",
            "モスバーガー",
            "franchisee",
            "osm_operator_tag_unverified",
            "osm_operator_tag",
        )
    finally:
        conn.close()


def test_upsert_store_skips_nearby_same_brand_duplicate(tmp_path: Path) -> None:
    conn = _setup_db(tmp_path / "p.sqlite")
    try:
        assert _upsert_store(
            conn,
            brand="モスバーガー",
            name="モスバーガー 既存店",
            address="東京都千代田区丸の内1-1-1",
            lat=35.0000,
            lng=139.0000,
            osm_id=123,
            osm_type="node",
        )
        assert not _upsert_store(
            conn,
            brand="モスバーガー",
            name="モスバーガー OSM重複候補",
            address="東京都千代田区丸の内1-1-2",
            lat=35.0005,
            lng=139.0005,
            osm_id=124,
            osm_type="node",
        )
        row = conn.execute("SELECT COUNT(*) FROM stores").fetchone()
        assert row == (1,)
    finally:
        conn.close()


def test_operator_from_osm_is_not_inserted_without_matching_store(tmp_path: Path) -> None:
    conn = _setup_db(tmp_path / "p.sqlite")
    try:
        assert not _upsert_operator_from_osm(
            conn,
            brand="モスバーガー",
            operator_name="株式会社テスト運営",
            osm_id=999,
            osm_type="node",
        )
        row = conn.execute("SELECT COUNT(*) FROM operator_stores").fetchone()
        assert row == (0,)
    finally:
        conn.close()
