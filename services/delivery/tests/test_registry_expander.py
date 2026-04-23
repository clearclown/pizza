"""Phase 17.4: Registry 自動拡充 loop のテスト。

unknown_stores (registry 未突合の bottom-up 店舗) の operator_name を集計し、
頻度 N 以上の social を registry 追加候補として YAML-ready に書き出す。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pizza_delivery.registry_expander import (
    CandidateFranchisee,
    aggregate_unknown_operators,
    export_candidates_to_yaml,
    load_unknown_stores_csv,
)


_SCHEMA = """
CREATE TABLE operator_stores (
  operator_name       TEXT NOT NULL,
  place_id            TEXT NOT NULL,
  brand               TEXT,
  operator_type       TEXT,
  confidence          REAL DEFAULT 0.0,
  discovered_via      TEXT DEFAULT 'per_store',
  verification_score  REAL DEFAULT 0.0,
  corporate_number    TEXT,
  verification_source TEXT,
  confirmed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (operator_name, place_id)
);
"""


def _seed(db: str, rows: list[tuple]) -> None:
    conn = sqlite3.connect(db)
    conn.executescript(_SCHEMA)
    conn.executemany(
        "INSERT INTO operator_stores (operator_name, place_id, brand, operator_type) "
        "VALUES (?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def test_aggregate_unknown_operators(tmp_path: Path) -> None:
    """operator_stores から frequency >= threshold の未登録 operator を集計。"""
    db = tmp_path / "t.db"
    _seed(
        str(db),
        [
            # per_store で発見された未登録 operator (discovered_via=chain_discovery など)
            ("株式会社フィットベイト", "p1", "エニタイムフィットネス", "unknown"),
            ("株式会社フィットベイト", "p2", "エニタイムフィットネス", "unknown"),
            ("株式会社フィットベイト", "p3", "エニタイムフィットネス", "unknown"),
            ("株式会社フィットベイト", "p4", "エニタイムフィットネス", "unknown"),
            ("株式会社フィットベイト", "p5", "エニタイムフィットネス", "unknown"),
            ("株式会社アーバンフィット", "q1", "エニタイムフィットネス", "unknown"),
            ("株式会社アーバンフィット", "q2", "エニタイムフィットネス", "unknown"),
            ("株式会社単発社", "r1", "エニタイムフィットネス", "unknown"),
            # 既登録 (registry seed) は除外
            ("株式会社エムデジ", "REG:XXX:1", "エニタイムフィットネス", "franchisee"),
        ],
    )
    cands = aggregate_unknown_operators(
        db_path=str(db), brand="エニタイムフィットネス", min_stores=2,
    )
    # フィットベイト(5) / アーバンフィット(2) は候補に入る、単発社(1) は入らない
    names = {c.name for c in cands}
    assert "株式会社フィットベイト" in names
    assert "株式会社アーバンフィット" in names
    assert "株式会社単発社" not in names
    # 既登録の franchisee (discovered_via='registry' or operator_type='franchisee') は除外
    assert "株式会社エムデジ" not in names
    # store_count が正しく集計される
    cand_map = {c.name: c for c in cands}
    assert cand_map["株式会社フィットベイト"].estimated_store_count == 5
    assert cand_map["株式会社アーバンフィット"].estimated_store_count == 2


def test_aggregate_respects_min_stores(tmp_path: Path) -> None:
    db = tmp_path / "t.db"
    _seed(
        str(db),
        [
            ("株式会社A", "p1", "B", "unknown"),
            ("株式会社A", "p2", "B", "unknown"),
            ("株式会社A", "p3", "B", "unknown"),
            ("株式会社B", "q1", "B", "unknown"),
            ("株式会社B", "q2", "B", "unknown"),
        ],
    )
    # min_stores=3 なら A だけ
    cands = aggregate_unknown_operators(
        db_path=str(db), brand="B", min_stores=3,
    )
    names = {c.name for c in cands}
    assert "株式会社A" in names
    assert "株式会社B" not in names


def test_export_candidates_to_yaml_appends_block(tmp_path: Path) -> None:
    """candidates を YAML ready 形式で書き出す。"""
    out = tmp_path / "candidates.yaml"
    cands = [
        CandidateFranchisee(
            name="株式会社テスト1", brand="エニタイムフィットネス",
            estimated_store_count=5, source="per_store extraction",
        ),
        CandidateFranchisee(
            name="株式会社テスト2", brand="エニタイムフィットネス",
            estimated_store_count=3, source="per_store extraction",
        ),
    ]
    export_candidates_to_yaml(cands, out_path=str(out))
    assert out.exists()
    text = out.read_text(encoding="utf-8")
    # YAML 構造のチェック (registry yaml に pipe で追記できる形)
    assert "株式会社テスト1" in text
    assert "estimated_store_count: 5" in text
    assert "エニタイムフィットネス" in text
    # 法人番号は "" 初期値 (手動確認前提)
    assert 'corporate_number: ""' in text


def test_load_unknown_stores_csv(tmp_path: Path) -> None:
    """audit の -unknown-stores.csv から place_id/住所を読める。"""
    csv_path = tmp_path / "unknown.csv"
    csv_path.write_text(
        "place_id,name,address,lat,lng,official_url\n"
        "p1,N1,東京都新宿区,35.6,139.6,https://x/1\n"
        "p2,N2,東京都渋谷区,35.66,139.7,https://x/2\n",
        encoding="utf-8",
    )
    rows = load_unknown_stores_csv(str(csv_path))
    assert len(rows) == 2
    assert rows[0]["place_id"] == "p1"
    assert rows[1]["address"] == "東京都渋谷区"
