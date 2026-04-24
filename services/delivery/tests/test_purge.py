"""pizza purge の structural / cross-brand-pollution ロジック回帰テスト。

国税庁 CSV lookup は本物の HoujinCSVIndex を使うと環境依存になるので、
該当経路を個別に呼び出さず、cross-brand pollution 検出 / structural garbage の
純粋関数だけを対象にする。

DB fixture は tmp_path で作った SQLite に operator_stores を直接挿入。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pizza_delivery.purge import (
    _detect_cross_brand_pollution,
    _is_structural_garbage,
)


def _mk_db(p: Path, rows: list[tuple[str, str, str, str]]) -> Path:
    """(operator_name, place_id, brand, corp) のリストで DB を作る。"""
    db = p / "test.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE operator_stores ("
        "operator_name TEXT, place_id TEXT, brand TEXT, "
        "corporate_number TEXT DEFAULT '')"
    )
    conn.executemany(
        "INSERT INTO operator_stores (operator_name, place_id, brand, corporate_number) "
        "VALUES (?,?,?,?)",
        rows,
    )
    conn.commit()
    return db


class TestStructuralGarbage:
    def test_address_prefix_is_garbage(self):
        assert _is_structural_garbage("〒100-0001東京都千代田区1-2-3")

    def test_pref_city_is_garbage(self):
        assert _is_structural_garbage("株式会社XXX東京都千代田区本社")

    def test_id_suffix_is_garbage(self):
        assert _is_structural_garbage("株式会社STAYGOLD第303311408号")

    def test_ntt_portal_is_garbage(self):
        assert _is_structural_garbage("NTTタウンページ株式会社")

    def test_real_company_is_not_garbage(self):
        assert not _is_structural_garbage("株式会社モスストアカンパニー")

    def test_empty_is_garbage(self):
        assert _is_structural_garbage("")


class TestCrossBrandPollution:
    def test_corp_empty_multi_brand_detected(self, tmp_path):
        db = _mk_db(tmp_path, [
            # 広告文由来のゴミ: 3 brand に corp 空でまたがる
            ("株式会社新鮮組本部", "p1", "ファミリーマート", ""),
            ("株式会社新鮮組本部", "p2", "ローソン", ""),
            ("株式会社新鮮組本部", "p3", "マクドナルド", ""),
        ])
        conn = sqlite3.connect(db)
        try:
            out = _detect_cross_brand_pollution(conn, brand="", threshold=3)
        finally:
            conn.close()
        assert out == ["株式会社新鮮組本部"]

    def test_corp_present_multi_brand_preserved(self, tmp_path):
        """corp 付き多業態 (本物メガジー) は検出しない。"""
        db = _mk_db(tmp_path, [
            ("株式会社ハードオフコーポレーション", "p1", "ハードオフ", "6110001012853"),
            ("株式会社ハードオフコーポレーション", "p2", "オフハウス", "6110001012853"),
            ("株式会社ハードオフコーポレーション", "p3", "アップガレージ", "6110001012853"),
        ])
        conn = sqlite3.connect(db)
        try:
            out = _detect_cross_brand_pollution(conn, brand="", threshold=3)
        finally:
            conn.close()
        assert out == []

    def test_below_threshold_preserved(self, tmp_path):
        """閾値未満は削除対象外。"""
        db = _mk_db(tmp_path, [
            ("株式会社大宮電化", "p1", "ハードオフ", ""),
            ("株式会社大宮電化", "p2", "オフハウス", ""),
        ])
        conn = sqlite3.connect(db)
        try:
            out = _detect_cross_brand_pollution(conn, brand="", threshold=3)
        finally:
            conn.close()
        assert out == []

    def test_brand_filter_disables_detection(self, tmp_path):
        """brand filter 指定時は検出しない (単一 brand では意味なし)。"""
        db = _mk_db(tmp_path, [
            ("株式会社X", "p1", "A", ""),
            ("株式会社X", "p2", "B", ""),
            ("株式会社X", "p3", "C", ""),
        ])
        conn = sqlite3.connect(db)
        try:
            out = _detect_cross_brand_pollution(conn, brand="A", threshold=2)
        finally:
            conn.close()
        assert out == []

    def test_threshold_zero_disabled(self, tmp_path):
        db = _mk_db(tmp_path, [
            ("株式会社X", "p1", "A", ""),
            ("株式会社X", "p2", "B", ""),
            ("株式会社X", "p3", "C", ""),
        ])
        conn = sqlite3.connect(db)
        try:
            out = _detect_cross_brand_pollution(conn, brand="", threshold=0)
        finally:
            conn.close()
        assert out == []
