from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from pizza_delivery.coverage_exports import export_coverage


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def test_export_coverage_outputs_47_prefecture_matrix(tmp_path: Path) -> None:
    db = tmp_path / "pizza.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE stores (
          place_id TEXT PRIMARY KEY,
          brand TEXT NOT NULL,
          name TEXT NOT NULL,
          address TEXT,
          lat REAL NOT NULL,
          lng REAL NOT NULL,
          official_url TEXT,
          phone TEXT,
          grid_cell_id TEXT,
          extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE operator_stores (
          operator_name TEXT NOT NULL,
          place_id TEXT NOT NULL,
          brand TEXT,
          operator_type TEXT,
          confidence REAL DEFAULT 0.0,
          discovered_via TEXT DEFAULT 'per_store',
          verification_score REAL DEFAULT 0.0,
          corporate_number TEXT,
          verification_source TEXT,
          confirmed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (operator_name, place_id)
        );
        """
    )
    conn.executemany(
        """
        INSERT INTO stores
          (place_id, brand, name, address, lat, lng, official_url, phone, grid_cell_id)
        VALUES (?, ?, ?, ?, ?, ?, '', '', '')
        """,
        [
            ("s1", "モスバーガー", "モス東京", "東京都千代田区丸の内1", 35.6812, 139.7671),
            ("s2", "モスバーガー", "モス住所なし", "", 0.0, 0.0),
            ("s3", "カーブス", "カーブス大阪", "大阪府大阪市北区梅田1", 34.7025, 135.4959),
            ("s4", "モスバーガー", "モス座標推定", "", 35.6820, 139.7680),
        ],
    )
    conn.executemany(
        """
        INSERT INTO operator_stores
          (operator_name, place_id, brand, operator_type, corporate_number, verification_source)
        VALUES (?, ?, ?, 'franchisee', ?, ?)
        """,
        [
            ("株式会社A", "s1", "モスバーガー", "1234567890123", "houjin_csv"),
            ("株式会社B", "s3", "カーブス", "", "osm_operator_tag"),
        ],
    )
    conn.commit()
    conn.close()

    out = tmp_path / "out"
    stats = export_coverage(db, brands=["モスバーガー", "カーブス"], out_dir=out)

    assert stats["stores"] == 4
    assert stats["brand_prefecture_rows"] == 94
    assert stats["stores_missing_prefecture"] == 1

    brand_rows = {r["brand"]: r for r in _read_csv(out / "brand-operator-coverage.csv")}
    assert brand_rows["モスバーガー"]["stores"] == "3"
    assert brand_rows["モスバーガー"]["stores_with_prefecture"] == "2"
    assert brand_rows["モスバーガー"]["known_store_coverage"] == "0.3333"
    assert brand_rows["モスバーガー"]["verified_store_coverage"] == "0.3333"
    assert brand_rows["カーブス"]["known_store_coverage"] == "1.0000"
    assert brand_rows["カーブス"]["verified_store_coverage"] == "0.0000"

    pref_rows = _read_csv(out / "brand-prefecture-coverage.csv")
    assert len(pref_rows) == 94
    tokyo = next(r for r in pref_rows if r["brand"] == "モスバーガー" and r["prefecture"] == "東京都")
    assert tokyo["coverage_status"] == "observed"
    assert tokyo["stores"] == "2"
    assert tokyo["verified_operator_stores"] == "1"
    hokkaido = next(r for r in pref_rows if r["brand"] == "モスバーガー" and r["prefecture"] == "北海道")
    assert hokkaido["coverage_status"] == "no_store_observed"

    missing_store_rows = _read_csv(out / "stores-missing-prefecture-14brand.csv")
    assert [r["place_id"] for r in missing_store_rows] == ["s2"]

    unknown_rows = {r["place_id"]: r for r in _read_csv(out / "unknown-stores-14brand.csv")}
    assert unknown_rows["s4"]["prefecture"] == "東京都"
    assert unknown_rows["s4"]["prefecture_source"] == "nearest_coordinate"
