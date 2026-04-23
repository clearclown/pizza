"""franchisee_registry の load + seed テスト。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pizza_delivery.franchisee_registry import (
    BrandRegistry,
    KnownFranchisee,
    Registry,
    load_registry,
    seed_registry_to_sqlite,
)


_SCHEMA = """
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
"""


def test_load_registry_reads_default_yaml() -> None:
    reg = load_registry()
    assert reg.version >= 1
    assert "エニタイムフィットネス" in reg.brands
    anytime = reg.brands["エニタイムフィットネス"]
    # Phase 10.3: 既存 5 社 + 追加 9 社 = 14 社
    assert len(anytime.known_franchisees) >= 14
    names = {f.name for f in anytime.known_franchisees}
    # 初期 5 社
    assert "株式会社エムデジ" in names
    assert "株式会社トピーレック" in names
    assert "川勝商事株式会社" in names
    assert "株式会社アズ" in names
    assert "株式会社アトラクト" in names
    # Phase 10.3 追加
    assert "株式会社KOHATAホールディングス" in names
    assert "株式会社タカ・コーポレーション" in names
    # 法人番号は 13 桁 or 要確認 ("")
    for fr in anytime.known_franchisees:
        if fr.corporate_number:
            assert len(fr.corporate_number) == 13, f"{fr.name}: {fr.corporate_number!r}"
            assert fr.corporate_number.isdigit()


def test_seed_registry_to_sqlite_inserts_all_estimated_stores(tmp_path: Path) -> None:
    db = tmp_path / "t.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(_SCHEMA)
    conn.commit()
    conn.close()

    # 小さな Registry を手組み
    reg = Registry(
        version=1,
        updated_at="2026-04-23",
        brands={
            "X": BrandRegistry(
                brand="X",
                known_franchisees=[
                    KnownFranchisee(
                        name="株式会社A",
                        corporate_number="1234567890123",
                        estimated_store_count=3,
                    ),
                    KnownFranchisee(
                        name="株式会社B",
                        corporate_number="9876543210987",
                        estimated_store_count=2,
                    ),
                ],
            )
        },
    )
    inserted = seed_registry_to_sqlite(str(db), reg)
    assert inserted == 5

    conn = sqlite3.connect(str(db))
    try:
        rows = conn.execute(
            "SELECT operator_name, COUNT(*) "
            "FROM operator_stores "
            "WHERE discovered_via='registry' "
            "GROUP BY operator_name"
        ).fetchall()
    finally:
        conn.close()
    counts = {r[0]: r[1] for r in rows}
    assert counts["株式会社A"] == 3
    assert counts["株式会社B"] == 2


def test_seed_is_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "t.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(_SCHEMA)
    conn.commit()
    conn.close()

    reg = Registry(
        version=1, updated_at="",
        brands={
            "X": BrandRegistry(
                brand="X",
                known_franchisees=[
                    KnownFranchisee(
                        name="株式会社A", corporate_number="1234567890123",
                        estimated_store_count=2,
                    )
                ],
            )
        },
    )
    # 2 回実行しても合計 2 件
    n1 = seed_registry_to_sqlite(str(db), reg)
    n2 = seed_registry_to_sqlite(str(db), reg)
    assert n1 == 2
    assert n2 == 0  # 2 回目は重複 skip

    conn = sqlite3.connect(str(db))
    cnt = conn.execute("SELECT COUNT(*) FROM operator_stores").fetchone()[0]
    conn.close()
    assert cnt == 2
