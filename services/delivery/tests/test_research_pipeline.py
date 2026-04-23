"""Unit tests for ResearchPipeline — Phase 5 Step E."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from pizza_delivery.chain_discovery import ChainDiscovery, StoreInput
from pizza_delivery.cross_verifier import CrossVerifier
from pizza_delivery.evidence import Evidence
from pizza_delivery.per_store import StoreExtractionResult
from pizza_delivery.research_pipeline import (
    MegaFranchiseeCandidate,
    ResearchPipeline,
    ResearchRequest,
    _load_stores_from_sqlite,
)


# ─── DB fixtures ───────────────────────────────────────────────────────


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
  operator_name    TEXT NOT NULL,
  place_id         TEXT NOT NULL,
  brand            TEXT,
  operator_type    TEXT,
  confidence       REAL DEFAULT 0.0,
  discovered_via   TEXT DEFAULT 'per_store',
  confirmed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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


def _seed_db(path: str, rows: list[tuple]):
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


# ─── Mocks ─────────────────────────────────────────────────────────────


@dataclass
class StubExtractor:
    results_by_place: dict[str, StoreExtractionResult]

    async def extract(self, *, place_id, brand, name, official_url, extra_urls=None):
        return self.results_by_place.get(
            place_id,
            StoreExtractionResult(place_id=place_id, brand=brand, name=name),
        )


def _mk_result(place_id, op, evidences=None):
    return StoreExtractionResult(
        place_id=place_id,
        brand="エニタイム",
        name=f"店舗{place_id}",
        operator_name=op,
        operator_type="franchisee" if op else "unknown",
        confidence=0.8,
        evidences=evidences or [],
    )


# ─── Tests ─────────────────────────────────────────────────────────────


def test_load_stores_from_sqlite_filters_by_brand(tmp_path: Path):
    db = tmp_path / "test.db"
    _seed_db(
        str(db),
        [
            ("p1", "BrandA", "N1", "https://x/1"),
            ("p2", "BrandB", "N2", "https://x/2"),
            ("p3", "BrandA", "N3", "https://x/3"),
            ("p4", "BrandA", "N4", ""),  # no url, excluded
        ],
    )
    # by brand
    got = _load_stores_from_sqlite(str(db), "BrandA", 0)
    assert len(got) == 2
    assert {s.place_id for s in got} == {"p1", "p3"}

    # all brands
    got_all = _load_stores_from_sqlite(str(db), None, 0)
    assert len(got_all) == 3

    # max_stores
    got_lim = _load_stores_from_sqlite(str(db), None, 2)
    assert len(got_lim) == 2


@pytest.mark.asyncio
async def test_pipeline_run_persists_operator_stores(tmp_path: Path) -> None:
    db = tmp_path / "test.db"
    _seed_db(
        str(db),
        [
            ("p1", "エニタイム", "新宿店", "https://x/1"),
            ("p2", "エニタイム", "渋谷店", "https://x/2"),
            ("p3", "エニタイム", "池袋店", "https://x/3"),
        ],
    )

    stub = StubExtractor(
        results_by_place={
            "p1": _mk_result(
                "p1",
                "株式会社MEGA",
                evidences=[
                    Evidence(
                        source_url="https://x/1/company",
                        snippet="運営会社: 株式会社MEGA",
                        reason="operator_keyword",
                        keyword="運営会社",
                    )
                ],
            ),
            "p2": _mk_result("p2", "株式会社MEGA"),
            "p3": _mk_result("p3", ""),  # operator 不明
        }
    )
    pipeline = ResearchPipeline(
        chain=ChainDiscovery(extractor=stub),
        verifier=CrossVerifier(extractor=stub),
    )
    report = await pipeline.run(
        ResearchRequest(brand="エニタイム", db_path=str(db), verify=False),
    )
    assert report.total_stores == 3
    assert report.stores_with_operator == 2
    assert report.stores_unknown == 1
    assert len(report.operators) == 1
    assert report.operators[0].operator_name == "株式会社MEGA"
    assert report.operators[0].store_count == 2
    assert set(report.operators[0].place_ids) == {"p1", "p2"}
    assert report.operators[0].brands == ["エニタイム"]

    # SQLite に書かれたか
    conn = sqlite3.connect(str(db))
    rows = conn.execute("SELECT place_id FROM operator_stores WHERE operator_name=?",
                        ("株式会社MEGA",)).fetchall()
    assert {r[0] for r in rows} == {"p1", "p2"}

    # store_evidence に p1 の 1 件が記録されている
    ev = conn.execute("SELECT snippet FROM store_evidence WHERE place_id=?", ("p1",)).fetchone()
    assert ev is not None
    assert "株式会社MEGA" in ev[0]
    conn.close()


@pytest.mark.asyncio
async def test_pipeline_run_with_verify_boosts_confidence(tmp_path: Path) -> None:
    db = tmp_path / "test.db"
    _seed_db(str(db), [("p1", "B", "N1", "https://x/1")])

    # primary 抽出と verify 抽出で同じ operator が返る (match → verified)
    stub = StubExtractor(
        results_by_place={"p1": _mk_result("p1", "株式会社VERIFY")}
    )
    pipeline = ResearchPipeline(
        chain=ChainDiscovery(extractor=stub),
        verifier=CrossVerifier(extractor=stub),
    )
    report = await pipeline.run(
        ResearchRequest(brand="B", db_path=str(db), verify=True),
    )
    assert len(report.operators) == 1
    assert report.operators[0].verified_count == 1

    # SQLite に discovered_via=chain_verified で書かれる
    conn = sqlite3.connect(str(db))
    row = conn.execute(
        "SELECT discovered_via, confidence FROM operator_stores WHERE place_id=?", ("p1",)
    ).fetchone()
    assert row is not None
    assert row[0] == "chain_verified"
    # 元 confidence=0.8 に verify boost +0.1 → 0.9 付近
    assert row[1] >= 0.85
    conn.close()


@pytest.mark.asyncio
async def test_pipeline_empty_stores(tmp_path: Path) -> None:
    db = tmp_path / "test.db"
    _seed_db(str(db), [])
    stub = StubExtractor(results_by_place={})
    pipeline = ResearchPipeline(
        chain=ChainDiscovery(extractor=stub),
        verifier=CrossVerifier(extractor=stub),
    )
    report = await pipeline.run(
        ResearchRequest(brand="X", db_path=str(db), verify=False),
    )
    assert report.total_stores == 0
    assert report.operators == []


@pytest.mark.asyncio
async def test_pipeline_rejects_missing_db(tmp_path: Path) -> None:
    stub = StubExtractor(results_by_place={})
    pipeline = ResearchPipeline(
        chain=ChainDiscovery(extractor=stub),
        verifier=CrossVerifier(extractor=stub),
    )
    with pytest.raises(FileNotFoundError):
        await pipeline.run(
            ResearchRequest(brand="X", db_path=str(tmp_path / "nonexistent.db")),
        )


@pytest.mark.asyncio
async def test_pipeline_rejects_empty_db_path() -> None:
    stub = StubExtractor(results_by_place={})
    pipeline = ResearchPipeline(
        chain=ChainDiscovery(extractor=stub),
        verifier=CrossVerifier(extractor=stub),
    )
    with pytest.raises(ValueError):
        await pipeline.run(ResearchRequest(db_path=""))
