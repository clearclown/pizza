"""Phase 26: fc_directory の stores_in_prefecture filter のテスト。

pipeline DB に operator_stores + stores があり、operator 本社所在地に
関係なく「当該都道府県に店舗を持つ operator」を検出できることを確認。
"""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import sqlalchemy as sa

from pizza_delivery.fc_directory import (
    _operators_with_stores_in_prefecture,
    build_directory,
    export_component_csv,
)
from pizza_delivery.orm import (
    link_brand_operator,
    make_session,
    upsert_brand,
    upsert_operator,
)


def _setup_pipeline_db(db_path: Path) -> None:
    """pipeline SQLite に stores + operator_stores の最小 schema を作成。"""
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE stores (
            place_id TEXT PRIMARY KEY,
            name TEXT,
            address TEXT,
            brand TEXT
        );
        CREATE TABLE operator_stores (
            operator_name TEXT,
            place_id TEXT,
            brand TEXT,
            operator_type TEXT DEFAULT 'franchisee'
        );
        """
    )
    conn.executemany(
        "INSERT INTO stores VALUES (?, ?, ?, ?)",
        [
            ("p1", "モス 新宿店", "東京都新宿区西新宿1-1", "モスバーガー"),
            ("p2", "モス 池袋店", "東京都豊島区南池袋2-1", "モスバーガー"),
            ("p3", "モス 松山店", "愛媛県松山市千舟町1", "モスバーガー"),
            ("p4", "ハードオフ 渋谷", "東京都渋谷区宇田川町1", "ハードオフ"),
        ],
    )
    conn.executemany(
        "INSERT INTO operator_stores VALUES (?, ?, ?, ?)",
        [
            ("株式会社ありがとうサービス", "p1", "モスバーガー", "franchisee"),
            ("株式会社ありがとうサービス", "p3", "モスバーガー", "franchisee"),
            ("株式会社ありがとうサービス", "p4", "ハードオフ", "franchisee"),
            ("株式会社モスストアカンパニー", "p2", "モスバーガー", "franchisee"),
        ],
    )
    conn.commit()
    conn.close()


def test_operators_with_stores_in_prefecture(tmp_path: Path) -> None:
    db = tmp_path / "pipeline.sqlite"
    _setup_pipeline_db(db)
    m = _operators_with_stores_in_prefecture(str(db), "東京都")
    # ありがとう: 東京 2 店 (p1 Mos + p4 ハードオフ)
    # モスストア: 東京 1 店 (p2)
    assert m == {
        "株式会社ありがとうサービス": 2,
        "株式会社モスストアカンパニー": 1,
    }


def test_operators_with_stores_in_prefecture_no_match(tmp_path: Path) -> None:
    db = tmp_path / "pipeline.sqlite"
    _setup_pipeline_db(db)
    # 大阪府には 1 店も無い
    assert _operators_with_stores_in_prefecture(str(db), "大阪府") == {}


def test_operators_with_stores_in_prefecture_missing_db(tmp_path: Path) -> None:
    """DB 不在なら graceful に 空 dict。"""
    assert _operators_with_stores_in_prefecture(
        str(tmp_path / "no_such.db"), "東京都"
    ) == {}


def test_operators_with_stores_in_prefecture_empty_args() -> None:
    assert _operators_with_stores_in_prefecture("", "東京都") == {}
    assert _operators_with_stores_in_prefecture("/tmp/x.db", "") == {}


def test_component_export_keeps_small_side_brand_for_total_qualified(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """合計10+ operator なら、1店舗だけの side brand も component に残す。"""
    engine = sa.create_engine(f"sqlite:///{tmp_path}/registry.sqlite", future=True)
    original_make_session = make_session

    def temp_session(engine_arg=None):
        return original_make_session(engine)

    monkeypatch.setattr("pizza_delivery.orm.make_session", temp_session)

    sess = original_make_session(engine)
    try:
        mos = upsert_brand(sess, "モスバーガー")
        anytime = upsert_brand(sess, "エニタイムフィットネス")
        curves = upsert_brand(sess, "カーブス")
        cross = upsert_operator(
            sess,
            name="株式会社クロスフィット",
            corporate_number="1234567890123",
            head_office="東京都千代田区丸の内1-1",
            prefecture="東京都",
            kind="franchisee",
        )
        small = upsert_operator(
            sess,
            name="株式会社小規模",
            head_office="東京都千代田区丸の内2-2",
            prefecture="東京都",
            kind="franchisee",
        )
        sess.flush()
        link_brand_operator(
            sess,
            brand=anytime,
            operator=cross,
            estimated_store_count=30,
            source="manual_test",
        )
        link_brand_operator(
            sess,
            brand=mos,
            operator=cross,
            estimated_store_count=1,
            source="manual_test",
        )
        link_brand_operator(
            sess,
            brand=curves,
            operator=small,
            estimated_store_count=9,
            source="manual_test",
        )
        sess.commit()
    finally:
        sess.close()

    entries = build_directory(
        brands_filter={"モスバーガー", "エニタイムフィットネス", "カーブス"},
        include_zero_stores=False,
    )
    qualified = [e for e in entries if e.total_stores_est >= 10]
    out = tmp_path / "components.csv"
    export_component_csv(
        qualified,
        out,
        brands_filter={"モスバーガー", "エニタイムフィットネス", "カーブス"},
        qualified_total_threshold=10,
    )

    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert {r["brand"] for r in rows} == {"モスバーガー", "エニタイムフィットネス"}
    mos_row = next(r for r in rows if r["brand"] == "モスバーガー")
    assert mos_row["operator_name"] == "株式会社クロスフィット"
    assert mos_row["brand_estimated_store_count"] == "1"
    assert mos_row["operator_total_stores_est"] == "31"
    assert all(r["operator_name"] != "株式会社小規模" for r in rows)
