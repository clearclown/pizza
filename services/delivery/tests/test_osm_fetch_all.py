"""osm_fetch_all: OSM operator tag capture."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from pizza_delivery.commands.osm_fetch_all import (
    _operator_from_osm_tags,
    _osm_place_id,
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


def test_osm_place_id_keeps_node_backward_compatibility() -> None:
    assert _osm_place_id(123, "node") == "osm:123"
    assert _osm_place_id(123, "way") == "osm:way:123"


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
