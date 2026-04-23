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
    CrossBrandOperator,
    aggregate_cross_brand_operators,
    aggregate_unknown_operators,
    export_candidates_to_yaml,
    export_cross_brand_to_csv,
    export_cross_brand_to_yaml,
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


# ─── Phase 19: cross-brand メガジー集計 ─────────────────────────────


def _seed_with_corp(db: str, rows: list[tuple]) -> None:
    """rows = (operator_name, place_id, brand, operator_type, corporate_number, discovered_via)."""
    conn = sqlite3.connect(db)
    conn.executescript(_SCHEMA)
    conn.executemany(
        "INSERT INTO operator_stores "
        "(operator_name, place_id, brand, operator_type, "
        " corporate_number, discovered_via) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def test_aggregate_cross_brand_groups_by_operator(tmp_path: Path) -> None:
    """1 事業会社が複数ブランド運営 → 1 行に集約される。"""
    db = tmp_path / "t.db"
    _seed_with_corp(
        str(db),
        [
            # 大和フーヅ: モス 3 + ミスド 5 = 8 店 (多業態)
            ("大和フーヅ株式会社", "m1", "モスバーガー", "franchisee", "5010401089998", "per_store"),
            ("大和フーヅ株式会社", "m2", "モスバーガー", "franchisee", "5010401089998", "per_store"),
            ("大和フーヅ株式会社", "m3", "モスバーガー", "franchisee", "5010401089998", "per_store"),
            ("大和フーヅ株式会社", "d1", "ミスタードーナツ", "franchisee", "5010401089998", "per_store"),
            ("大和フーヅ株式会社", "d2", "ミスタードーナツ", "franchisee", "5010401089998", "per_store"),
            ("大和フーヅ株式会社", "d3", "ミスタードーナツ", "franchisee", "5010401089998", "per_store"),
            ("大和フーヅ株式会社", "d4", "ミスタードーナツ", "franchisee", "5010401089998", "per_store"),
            ("大和フーヅ株式会社", "d5", "ミスタードーナツ", "franchisee", "5010401089998", "per_store"),
            # 単一ブランドの中堅
            ("株式会社単独社", "x1", "エニタイムフィットネス", "franchisee", "", "per_store"),
            ("株式会社単独社", "x2", "エニタイムフィットネス", "franchisee", "", "per_store"),
            # 本部 (除外されるはず)
            ("株式会社モスフードサービス", "h1", "モスバーガー", "franchisor", "", "per_store"),
        ],
    )
    ops = aggregate_cross_brand_operators(db_path=str(db), min_total_stores=1)
    names = {o.name: o for o in ops}
    # 大和フーヅが 2 ブランド合計で 1 行に
    assert "大和フーヅ株式会社" in names
    y = names["大和フーヅ株式会社"]
    assert y.total_stores == 8
    assert y.brand_count == 2
    assert y.brand_counts["モスバーガー"] == 3
    assert y.brand_counts["ミスタードーナツ"] == 5
    assert y.corporate_number == "5010401089998"
    # 単独社も入る
    assert "株式会社単独社" in names
    assert names["株式会社単独社"].total_stores == 2
    # 本部 (franchisor) は除外
    assert "株式会社モスフードサービス" not in names
    # 合計降順: 大和フーヅ (8) が単独社 (2) より先
    assert ops[0].name == "大和フーヅ株式会社"
    assert ops[1].name == "株式会社単独社"


def test_aggregate_cross_brand_min_brands_filter(tmp_path: Path) -> None:
    """min_brands=2 なら多業態のみ残る。"""
    db = tmp_path / "t.db"
    _seed_with_corp(
        str(db),
        [
            ("多業態社", "p1", "A", "franchisee", "", "per_store"),
            ("多業態社", "p2", "B", "franchisee", "", "per_store"),
            ("単一社", "q1", "A", "franchisee", "", "per_store"),
            ("単一社", "q2", "A", "franchisee", "", "per_store"),
            ("単一社", "q3", "A", "franchisee", "", "per_store"),
        ],
    )
    ops = aggregate_cross_brand_operators(
        db_path=str(db), min_total_stores=1, min_brands=2,
    )
    names = {o.name for o in ops}
    assert "多業態社" in names
    assert "単一社" not in names


def test_export_cross_brand_csv(tmp_path: Path) -> None:
    out = tmp_path / "mj.csv"
    ops = [
        CrossBrandOperator(
            name="大和フーヅ株式会社",
            total_stores=66,
            brand_counts={"ミスタードーナツ": 48, "モスバーガー": 18},
            corporate_number="5010401089998",
        ),
    ]
    export_cross_brand_to_csv(ops, out_path=str(out))
    text = out.read_text(encoding="utf-8")
    assert "大和フーヅ株式会社" in text
    assert "ミスタードーナツ:48" in text
    assert "モスバーガー:18" in text
    assert "5010401089998" in text
    # ヘッダー
    assert "brand_count" in text


def test_export_cross_brand_yaml(tmp_path: Path) -> None:
    out = tmp_path / "mj.yaml"
    ops = [
        CrossBrandOperator(
            name="大和フーヅ株式会社",
            total_stores=66,
            brand_counts={"ミスタードーナツ": 48, "モスバーガー": 18},
            corporate_number="5010401089998",
        ),
    ]
    export_cross_brand_to_yaml(ops, out_path=str(out))
    text = out.read_text(encoding="utf-8")
    assert "operators:" in text
    assert "大和フーヅ株式会社:" in text
    assert "total_stores: 66" in text
    assert "brand_count: 2" in text
    assert "ミスタードーナツ: 48" in text
    assert "モスバーガー: 18" in text


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
