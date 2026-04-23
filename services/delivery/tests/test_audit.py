"""BrandAuditor の TDD テスト (Phase 8.2)。

Top-down (registry) × Bottom-up (SQLite stores) の突合結果を
FranchiseeCoverage / unknown_stores / missing_operators に分類する。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from pizza_delivery.audit import (
    AuditReport,
    BrandAuditor,
    FranchiseeCoverage,
    run_audit,
)
from pizza_delivery.franchisee_registry import (
    BrandRegistry,
    KnownFranchisee,
    Registry,
)
from pizza_delivery.places_client import PlaceRaw


_SCHEMA = """
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
"""


def _seed(db: str, rows: list[tuple]) -> None:
    conn = sqlite3.connect(db)
    try:
        conn.executescript(_SCHEMA)
        conn.executemany(
            "INSERT INTO stores (place_id, brand, name, address, lat, lng, official_url) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


@dataclass
class StubPlacesClient:
    """operator 名 → PlaceRaw list の scripted fake。"""

    responses: dict[str, list[PlaceRaw]]
    called: list[dict] = field(default_factory=list)

    async def search_by_operator(
        self, operator_name: str, *, area_hint: str = "", max_result_count: int = 20
    ) -> list[PlaceRaw]:
        self.called.append({"op": operator_name, "area": area_hint})
        return list(self.responses.get(operator_name, []))[:max_result_count]


def _place(pid: str, addr: str, lat: float = 35.0, lng: float = 139.0) -> PlaceRaw:
    return PlaceRaw(
        place_id=pid,
        name="N",
        address=addr,
        lat=lat,
        lng=lng,
        website_uri="",
        phone="",
    )


def _registry_with_3_franchisees() -> Registry:
    reg = Registry(version=1, updated_at="")
    reg.brands["X"] = BrandRegistry(
        brand="X",
        known_franchisees=[
            KnownFranchisee(name="株式会社A", corporate_number="1", estimated_store_count=3),
            KnownFranchisee(name="株式会社B", corporate_number="2", estimated_store_count=2),
            KnownFranchisee(name="株式会社C", corporate_number="3", estimated_store_count=1),
        ],
    )
    return reg


# ─── Tests ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_audit_coverage_per_operator(tmp_path: Path) -> None:
    """registry 3 社 + bottom-up 店舗で coverage を計算。"""
    db = tmp_path / "a.db"
    _seed(
        str(db),
        [
            # Bottom-up 店舗 5 件 (すべて brand=X)
            ("p1", "X", "N1", "東京都渋谷区1-1", 35.0, 139.0, ""),
            ("p2", "X", "N2", "大阪府大阪市2-2", 34.7, 135.5, ""),
            ("p3", "X", "N3", "愛知県名古屋市3", 35.1, 136.9, ""),
            ("p_unknown", "X", "N4", "千葉県4", 35.6, 140.1, ""),
            ("p_unknown2", "X", "N5", "福岡県5", 33.6, 130.4, ""),
        ],
    )
    stub = StubPlacesClient(
        responses={
            "株式会社A": [_place("p1", "東京都渋谷区1-1", 35.0, 139.0)],       # 1/3
            "株式会社B": [
                _place("p2", "大阪府大阪市2-2", 34.7, 135.5),
                # もう 1 件 Places にあるが DB に無い
                _place("z1", "埼玉県XX", 36.0, 139.5),
            ],  # 1/2
            "株式会社C": [],  # 0/1
        }
    )
    reg = _registry_with_3_franchisees()

    auditor = BrandAuditor(registry=reg, places_client=stub, db_path=str(db))
    report = await auditor.run(brand="X", areas=["東京都"])
    assert isinstance(report, AuditReport)
    assert report.brand == "X"
    assert report.bottom_up_total == 5

    cov_by_op = {c.operator_name: c for c in report.franchisees}
    assert cov_by_op["株式会社A"].bottom_up_matched_count == 1
    assert cov_by_op["株式会社A"].coverage_pct == pytest.approx(33.33, abs=0.01)
    assert cov_by_op["株式会社B"].bottom_up_matched_count == 1
    assert cov_by_op["株式会社C"].bottom_up_matched_count == 0

    # unknown_stores: bottom-up にあるが registry と突合できなかった = 2 件
    unknown_ids = {u["place_id"] for u in report.unknown_stores}
    assert unknown_ids == {"p_unknown", "p_unknown2", "p3"}
    # 株式会社C は missing (Places で 0 件)
    assert "株式会社C" in report.missing_operators


@pytest.mark.asyncio
async def test_audit_address_match_fallback(tmp_path: Path) -> None:
    """place_id が違っても住所類似で突合される。"""
    db = tmp_path / "a.db"
    _seed(
        str(db),
        [
            ("stores_p1", "X", "N1", "〒160-0023 東京都新宿区西新宿6丁目3番1号", 0, 0, ""),
        ],
    )
    stub = StubPlacesClient(
        responses={"株式会社A": [_place("places_id_x", "東京都新宿区西新宿6-3-1", 0, 0)]}
    )
    reg = Registry(version=1, updated_at="")
    reg.brands["X"] = BrandRegistry(
        brand="X",
        known_franchisees=[
            KnownFranchisee(name="株式会社A", corporate_number="1", estimated_store_count=1),
        ],
    )
    auditor = BrandAuditor(registry=reg, places_client=stub, db_path=str(db))
    report = await auditor.run(brand="X", areas=[""])
    cov = report.franchisees[0]
    assert cov.bottom_up_matched_count == 1


@pytest.mark.asyncio
async def test_audit_missing_brand_raises(tmp_path: Path) -> None:
    db = tmp_path / "a.db"
    _seed(str(db), [])
    reg = Registry(version=1, updated_at="")  # 空 registry
    stub = StubPlacesClient(responses={})
    auditor = BrandAuditor(registry=reg, places_client=stub, db_path=str(db))
    # brand が registry にないとき: error or 空 report?  ここでは空 report を期待
    report = await auditor.run(brand="UNKNOWN", areas=["東京都"])
    assert report.franchisees == []
    assert report.missing_operators == []


@pytest.mark.asyncio
async def test_run_audit_helper_writes_csv(tmp_path: Path) -> None:
    """run_audit トップレベルが CSV を出力できる。"""
    db = tmp_path / "a.db"
    _seed(str(db), [("p1", "X", "N1", "東京都", 0, 0, "")])
    stub = StubPlacesClient(responses={"株式会社A": [_place("p1", "東京都", 0, 0)]})
    reg = Registry(version=1, updated_at="")
    reg.brands["X"] = BrandRegistry(
        brand="X",
        known_franchisees=[
            KnownFranchisee(
                name="株式会社A",
                corporate_number="1",
                head_office="東京",
                source_urls=["https://example.com/"],
                estimated_store_count=1,
            ),
        ],
    )
    out = tmp_path / "audit.csv"
    report = await run_audit(
        registry=reg, places_client=stub, db_path=str(db),
        brand="X", areas=[""], out_csv=str(out),
    )
    assert out.exists()
    text = out.read_text(encoding="utf-8")
    # ヘッダ + 株式会社A の行
    assert "企業名" in text
    assert "株式会社A" in text
    # サブレポート
    assert (tmp_path / "audit-unknown-stores.csv").exists()
    assert (tmp_path / "audit-missing-operators.csv").exists()


def test_franchisee_coverage_dataclass_defaults() -> None:
    c = FranchiseeCoverage(
        operator_name="A", corporate_number="", head_office="", website="",
        registered_count=0, found_count=0, bottom_up_matched_count=0,
        coverage_pct=0.0,
    )
    assert c.coverage_pct == 0.0
