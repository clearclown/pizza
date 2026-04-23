"""Step 6.2 — Places API 広域芋づる式 (operator 発見後の他店舗探索) テスト。"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from pizza_delivery.chain_discovery import ChainDiscovery
from pizza_delivery.cross_verifier import CrossVerifier
from pizza_delivery.per_store import StoreExtractionResult
from pizza_delivery.places_client import PlaceRaw
from pizza_delivery.research_pipeline import ResearchPipeline, ResearchRequest


# ─── shared schema (research_pipeline tests と共通形式) ─────────────────

_MIGRATION_SQL = """
CREATE TABLE IF NOT EXISTS stores (
  place_id      TEXT PRIMARY KEY,
  brand         TEXT NOT NULL,
  name          TEXT NOT NULL,
  address       TEXT,
  lat           REAL NOT NULL DEFAULT 0,
  lng           REAL NOT NULL DEFAULT 0,
  official_url  TEXT,
  phone         TEXT,
  grid_cell_id  TEXT,
  extracted_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS operator_stores (
  operator_name        TEXT NOT NULL,
  place_id             TEXT NOT NULL,
  brand                TEXT,
  operator_type        TEXT,
  confidence           REAL DEFAULT 0.0,
  discovered_via       TEXT DEFAULT 'per_store',
  verification_score   REAL DEFAULT 0.0,
  corporate_number     TEXT,
  verification_source  TEXT,
  confirmed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (operator_name, place_id)
);
CREATE TABLE IF NOT EXISTS store_evidence (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  place_id      TEXT NOT NULL,
  evidence_url  TEXT NOT NULL,
  snippet       TEXT NOT NULL,
  reason        TEXT,
  keyword       TEXT,
  collected_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def _seed_db(path: str, rows: list[tuple]) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_MIGRATION_SQL)
        conn.executemany(
            "INSERT INTO stores (place_id, brand, name, official_url) VALUES (?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


# ─── Stubs ─────────────────────────────────────────────────────────────


@dataclass
class StubExtractor:
    results_by_place: dict[str, StoreExtractionResult]

    async def extract(self, *, place_id, brand, name, official_url, extra_urls=None):
        return self.results_by_place.get(
            place_id,
            StoreExtractionResult(place_id=place_id, brand=brand, name=name),
        )


def _result(place_id: str, op: str, brand: str = "B") -> StoreExtractionResult:
    return StoreExtractionResult(
        place_id=place_id,
        brand=brand,
        name=f"N{place_id}",
        operator_name=op,
        operator_type="franchisee" if op else "unknown",
        confidence=0.8,
    )


@dataclass
class StubPlacesClient:
    """PlacesClient の search_by_operator interface を持つ in-memory fake。

    responses は operator 名 → PlaceRaw list の dict。
    """

    responses: dict[str, list[PlaceRaw]]
    called_with: list[dict] = field(default_factory=list)

    async def search_by_operator(
        self, operator_name: str, *, area_hint: str = "", max_result_count: int = 20
    ) -> list[PlaceRaw]:
        self.called_with.append(
            {
                "operator": operator_name,
                "area_hint": area_hint,
                "max_result_count": max_result_count,
            }
        )
        places = list(self.responses.get(operator_name, []))
        return places[:max_result_count]


def _place(place_id: str, name: str) -> PlaceRaw:
    return PlaceRaw(
        place_id=place_id,
        name=name,
        address="東京都",
        lat=35.7,
        lng=139.7,
        website_uri=f"https://brand.example/{place_id}",
        phone="",
    )


# ─── Tests ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_expand_via_places_inserts_new_stores(tmp_path: Path) -> None:
    db = tmp_path / "test.db"
    _seed_db(str(db), [("p1", "B", "seed 店舗", "https://brand.example/p1")])

    # seed 店舗 → operator A
    ext = StubExtractor(results_by_place={"p1": _result("p1", "株式会社A")})
    # operator A で Places 検索 → 4 店舗ヒット (そのうち p1 は既存)
    places_stub = StubPlacesClient(
        responses={
            "株式会社A": [
                _place("p1", "既存店舗"),
                _place("pX", "新宿店"),
                _place("pY", "渋谷店"),
                _place("pZ", "池袋店"),
            ]
        }
    )
    pipeline = ResearchPipeline(
        chain=ChainDiscovery(extractor=ext),
        verifier=CrossVerifier(extractor=ext),
    )
    req = ResearchRequest(brand="B", db_path=str(db), verify=False)
    req.expand_via_places = True
    req.places_client = places_stub
    req.max_expansion_per_operator = 10

    await pipeline.run(req)

    # Places 検索が 1 operator について 1 回呼ばれる
    assert len(places_stub.called_with) == 1
    assert places_stub.called_with[0]["operator"] == "株式会社A"

    # stores テーブルに pX, pY, pZ が insert された (p1 は既存で重複なし)
    conn = sqlite3.connect(str(db))
    rows = conn.execute(
        "SELECT place_id FROM stores ORDER BY place_id"
    ).fetchall()
    conn.close()
    assert {r[0] for r in rows} == {"p1", "pX", "pY", "pZ"}


@pytest.mark.asyncio
async def test_expand_disabled_by_default(tmp_path: Path) -> None:
    db = tmp_path / "test.db"
    _seed_db(str(db), [("p1", "B", "seed", "https://brand.example/p1")])

    ext = StubExtractor(results_by_place={"p1": _result("p1", "株式会社A")})
    places_stub = StubPlacesClient(responses={})
    pipeline = ResearchPipeline(
        chain=ChainDiscovery(extractor=ext),
        verifier=CrossVerifier(extractor=ext),
    )
    # expand_via_places は default False
    req = ResearchRequest(brand="B", db_path=str(db), verify=False)
    req.places_client = places_stub
    await pipeline.run(req)
    # Places API は呼ばれない
    assert places_stub.called_with == []


@pytest.mark.asyncio
async def test_expand_respects_max_expansion_per_operator(tmp_path: Path) -> None:
    db = tmp_path / "test.db"
    _seed_db(str(db), [("p1", "B", "seed", "https://brand.example/p1")])

    ext = StubExtractor(results_by_place={"p1": _result("p1", "株式会社A")})
    places_stub = StubPlacesClient(
        responses={
            "株式会社A": [_place(f"p{i}", f"N{i}") for i in range(10)],
        }
    )
    pipeline = ResearchPipeline(
        chain=ChainDiscovery(extractor=ext),
        verifier=CrossVerifier(extractor=ext),
    )
    req = ResearchRequest(brand="B", db_path=str(db), verify=False)
    req.expand_via_places = True
    req.places_client = places_stub
    req.max_expansion_per_operator = 3

    await pipeline.run(req)
    assert places_stub.called_with[0]["max_result_count"] == 3


@pytest.mark.asyncio
async def test_expand_bfs_depth_1(tmp_path: Path) -> None:
    """拡張で追加された店舗から発見された新 operator で再拡張しない (depth=1 fixed)。"""
    db = tmp_path / "test.db"
    _seed_db(str(db), [("p1", "B", "seed", "https://brand.example/p1")])

    ext = StubExtractor(
        results_by_place={
            "p1": _result("p1", "株式会社A"),
            # 拡張で追加された pX から operator B が見つかるとしても…
            "pX": _result("pX", "株式会社B"),
        }
    )
    places_stub = StubPlacesClient(
        responses={
            "株式会社A": [_place("pX", "newshop")],
            # …operator B で再検索はしない (depth=1 fixed)
            "株式会社B": [_place("pXXX", "should_not_be_seen")],
        }
    )
    pipeline = ResearchPipeline(
        chain=ChainDiscovery(extractor=ext),
        verifier=CrossVerifier(extractor=ext),
    )
    req = ResearchRequest(brand="B", db_path=str(db), verify=False)
    req.expand_via_places = True
    req.places_client = places_stub
    req.max_expansion_per_operator = 20

    await pipeline.run(req)
    # 株式会社B に対する search_by_operator は呼ばれない
    operators_queried = [c["operator"] for c in places_stub.called_with]
    assert operators_queried == ["株式会社A"]
    assert "株式会社B" not in operators_queried
