"""Phase 11: E2E 統合テスト — audit 全パス (registry → places → match → CSV)。

決定論 MockPlacesClient を使い、実 API コストなしで audit 全フローを回す。
- 複数 franchisee × 複数 area
- 突合 3 段 (place_id / address / proximity)
- CSV + unknown-stores + missing-operators の 3 ファイル生成
- 下位互換: pref/city parse が失敗する住所でも落ちない
"""

from __future__ import annotations

import csv as csv_mod
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from pizza_delivery.audit import run_audit
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


def _seed_stores(db: str, rows: list[tuple]) -> None:
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
class MockPlacesClient:
    """operator 名 → PlaceRaw list の scripted fake。"""

    responses: dict[str, list[PlaceRaw]]
    called: list[dict] = field(default_factory=list)

    async def search_by_operator(
        self, operator_name: str, *, area_hint: str = "", max_result_count: int = 20
    ) -> list[PlaceRaw]:
        self.called.append({"op": operator_name, "area": area_hint})
        return list(self.responses.get(operator_name, []))[:max_result_count]


def _pl(pid: str, addr: str, lat: float = 35.0, lng: float = 139.0) -> PlaceRaw:
    return PlaceRaw(
        place_id=pid, name="N", address=addr, lat=lat, lng=lng,
        website_uri="", phone="",
    )


# ─── 大規模 E2E シナリオ ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_e2e_audit_multi_franchisee_multi_area(tmp_path: Path) -> None:
    """ブランド B / 4 franchisee / 2 area でフル audit。
    正解 coverage 値、unknown_stores、missing_operators が期待通り出ること。"""
    db = tmp_path / "e2e.db"
    # 10 店舗 (bottom-up)
    _seed_stores(
        str(db),
        [
            # 株式会社A 系 3 店舗 (東京都新宿区)
            ("A1", "B", "東京本店", "東京都新宿区西新宿1-1-1", 35.69, 139.69, ""),
            ("A2", "B", "東京2", "東京都新宿区西新宿2-2-2", 35.691, 139.691, ""),
            ("A3", "B", "東京3", "東京都新宿区西新宿3-3-3", 35.692, 139.692, ""),
            # 株式会社B 系 2 店舗 (大阪府)
            ("B1", "B", "大阪1", "大阪府大阪市北区梅田1-1", 34.70, 135.50, ""),
            ("B2", "B", "大阪2", "大阪府大阪市北区梅田2-2", 34.701, 135.501, ""),
            # 未知 operator 運営の 5 店舗
            ("U1", "B", "unknown1", "東京都渋谷区1", 35.66, 139.70, ""),
            ("U2", "B", "unknown2", "東京都渋谷区2", 35.661, 139.701, ""),
            ("U3", "B", "unknown3", "東京都渋谷区3", 35.662, 139.702, ""),
            ("U4", "B", "unknown4", "東京都渋谷区4", 35.663, 139.703, ""),
            ("U5", "B", "unknown5", "東京都渋谷区5", 35.664, 139.704, ""),
        ],
    )

    # Registry: 4 社
    reg = Registry(version=1, updated_at="2026")
    reg.brands["B"] = BrandRegistry(
        brand="B",
        known_franchisees=[
            KnownFranchisee(
                name="株式会社A", corporate_number="1111111111111",
                head_office="東京都新宿区", estimated_store_count=3,
            ),
            KnownFranchisee(
                name="株式会社B", corporate_number="2222222222222",
                head_office="大阪府大阪市北区", estimated_store_count=2,
            ),
            # C: Places で 0 件 (missing_operator)
            KnownFranchisee(
                name="株式会社C", corporate_number="3333333333333",
                head_office="愛知県", estimated_store_count=5,
            ),
            # D: 登録店舗数多いが、bottom-up に該当なし
            KnownFranchisee(
                name="株式会社D", corporate_number="4444444444444",
                head_office="北海道", estimated_store_count=10,
            ),
        ],
    )

    # MockPlacesClient
    places = MockPlacesClient(
        responses={
            # A: 3 件返る (全て stores と place_id / 住所一致)
            "株式会社A": [
                _pl("A1", "東京都新宿区西新宿1-1-1", 35.69, 139.69),
                _pl("A2", "東京都新宿区西新宿2-2-2", 35.691, 139.691),
                _pl("A3", "東京都新宿区西新宿3-3-3", 35.692, 139.692),
            ],
            # B: 2 件、うち 1 件は住所 normalize fallback で突合、もう 1 件は proximity
            "株式会社B": [
                _pl("X1", "〒530-0001 大阪府大阪市北区梅田1丁目1", 34.70, 135.50),
                _pl("X2", "ちがう文字列", 34.7010001, 135.5010001),
            ],
            # C: 0 件
            "株式会社C": [],
            # D: Places から 2 件返るが bottom-up に無い
            "株式会社D": [
                _pl("Y1", "北海道札幌市中央区", 43.0, 141.0),
                _pl("Y2", "北海道札幌市北区", 43.1, 141.1),
            ],
        },
    )

    out = tmp_path / "audit.csv"
    report = await run_audit(
        registry=reg, places_client=places, db_path=str(db),
        brand="B", areas=["東京都", "大阪府"],
        out_csv=str(out),
    )

    # franchisees
    cov = {c.operator_name: c for c in report.franchisees}
    assert cov["株式会社A"].bottom_up_matched_count == 3  # place_id 完全一致
    assert cov["株式会社A"].coverage_pct == pytest.approx(100.0, abs=0.01)
    assert cov["株式会社B"].bottom_up_matched_count == 2  # address + proximity
    assert cov["株式会社C"].bottom_up_matched_count == 0
    assert cov["株式会社D"].bottom_up_matched_count == 0

    # missing_operators に C のみ (C は 0 件 Places)
    assert "株式会社C" in report.missing_operators
    assert "株式会社D" not in report.missing_operators  # D は 2 件 Places で見つけた

    # unknown_stores: U1-U5 (5 店舗) が registry 突合 せず
    unk_ids = {u["place_id"] for u in report.unknown_stores}
    assert unk_ids == {"U1", "U2", "U3", "U4", "U5"}

    # CSV 3 ファイル
    assert out.exists()
    unk_csv = tmp_path / "audit-unknown-stores.csv"
    miss_csv = tmp_path / "audit-missing-operators.csv"
    assert unk_csv.exists()
    assert miss_csv.exists()

    # メイン CSV の中身を parse して全社分の行があること
    with out.open(encoding="utf-8") as f:
        reader = csv_mod.reader(f)
        rows = list(reader)
    header = rows[0]
    assert "企業名" in header
    body = rows[1:]
    names = {r[0] for r in body}
    assert {"株式会社A", "株式会社B", "株式会社C", "株式会社D"} == names

    # unknown-stores CSV に 5 件
    with unk_csv.open(encoding="utf-8") as f:
        unk_rows = list(csv_mod.reader(f))
    assert len(unk_rows) == 6  # header + 5


@pytest.mark.asyncio
async def test_e2e_audit_handles_address_without_pref(tmp_path: Path) -> None:
    """住所に pref がなくても (parse_address が pref='' 返す) 突合可能。"""
    db = tmp_path / "e.db"
    # pref 省略店舗と正規住所店舗を混ぜる
    _seed_stores(
        str(db),
        [
            ("P1", "B", "N", "新宿区西新宿1-1", 35.69, 139.69, ""),
        ],
    )
    reg = Registry(version=1, updated_at="")
    reg.brands["B"] = BrandRegistry(
        brand="B",
        known_franchisees=[
            KnownFranchisee(
                name="株式会社NP", corporate_number="5555555555555",
                estimated_store_count=1,
            ),
        ],
    )
    # Places は pref ありで返す
    places = MockPlacesClient(
        responses={
            "株式会社NP": [_pl("P1", "東京都新宿区西新宿1-1", 35.69, 139.69)],
        }
    )
    report = await run_audit(
        registry=reg, places_client=places, db_path=str(db),
        brand="B", areas=[""], out_csv=str(tmp_path / "o.csv"),
    )
    # place_id 一致で match するので pref の有無は無関係
    assert report.franchisees[0].bottom_up_matched_count == 1


@pytest.mark.asyncio
async def test_e2e_audit_proximity_close_stores_same_chain(tmp_path: Path) -> None:
    """同一ビル複数店 (proximity 150m 以内) で 1 対 1 マッチを保証。"""
    db = tmp_path / "prox.db"
    _seed_stores(
        str(db),
        [
            # ほぼ同座標の 2 店舗 (同じビル、place_id は違う)
            ("S1", "B", "1F", "東京都新宿区", 35.690, 139.690, ""),
            ("S2", "B", "2F", "東京都新宿区", 35.6901, 139.6901, ""),
        ],
    )
    reg = Registry(version=1, updated_at="")
    reg.brands["B"] = BrandRegistry(
        brand="B",
        known_franchisees=[
            KnownFranchisee(
                name="株式会社P", corporate_number="6666666666666",
                estimated_store_count=2,
            ),
        ],
    )
    places = MockPlacesClient(
        responses={
            "株式会社P": [
                # Places からは同じ座標近辺 2 件 (別 place_id)
                _pl("X1", "未知住所1", 35.6900, 139.6900),
                _pl("X2", "未知住所2", 35.6901, 139.6901),
            ]
        }
    )
    report = await run_audit(
        registry=reg, places_client=places, db_path=str(db),
        brand="B", areas=[""], out_csv=str(tmp_path / "p.csv"),
    )
    # 1:1 で 2 件両方 match (proximity)
    assert report.franchisees[0].bottom_up_matched_count == 2


# ─── 境界ケース単体テスト ──────────────────────────────────────────


def test_parse_address_handles_empty_and_noise() -> None:
    from pizza_delivery.match import parse_address
    for raw in ["", "   ", "\n\t", "1234567890"]:
        p = parse_address(raw)
        assert p.pref in ("", )  # 解析できない
        # crash しない
    p = parse_address("〒100-0005 東京都千代田区丸の内1-1-1")
    assert p.pref == "東京都"
    assert p.city == "千代田区"


def test_merge_all_handles_empty_inputs() -> None:
    from pizza_delivery.match import merge_all

    r = merge_all([], [])
    assert r.matches == []
    assert r.unmatched_top == []
    assert r.unmatched_bottom == []

    r2 = merge_all([{"place_id": "A", "address": "", "lat": 0, "lng": 0}], [])
    assert r2.matches == []
    assert len(r2.unmatched_top) == 1


def test_audit_empty_bottom_up(tmp_path: Path) -> None:
    """stores テーブル空でも audit が crash しない。"""
    import asyncio

    db = tmp_path / "empty.db"
    _seed_stores(str(db), [])
    reg = Registry(version=1, updated_at="")
    reg.brands["B"] = BrandRegistry(
        brand="B",
        known_franchisees=[
            KnownFranchisee(name="株式会社X", corporate_number="7777777777777", estimated_store_count=5),
        ],
    )
    places = MockPlacesClient(responses={"株式会社X": [_pl("P1", "東京都", 35, 139)]})
    report = asyncio.run(
        run_audit(
            registry=reg, places_client=places, db_path=str(db),
            brand="B", areas=["東京都"], out_csv=str(tmp_path / "o.csv"),
        )
    )
    assert report.bottom_up_total == 0
    assert report.franchisees[0].bottom_up_matched_count == 0
    assert report.unknown_stores == []
