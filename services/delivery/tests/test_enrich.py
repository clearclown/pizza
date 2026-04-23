"""enrich: Places Details + browser-use 逆引きの一括 orchestrator テスト。

全て stub client で、実ネットワーク・Playwright は起動しない。
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from pizza_delivery.browser_scraper import OperatorInfo
from pizza_delivery.enrich import (
    EnrichStats,
    Enricher,
    _candidate_stores,
    _insert_operator_store,
    _upsert_phone,
)
from pizza_delivery.places_client import PlaceRaw


# ─── DB fixture ──────────────────────────────────────


def _setup_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE stores (
          place_id TEXT PRIMARY KEY, name TEXT, brand TEXT,
          address TEXT, lat REAL, lng REAL,
          official_url TEXT, phone TEXT
        );
        CREATE TABLE operator_stores (
          operator_name TEXT, place_id TEXT, brand TEXT,
          operator_type TEXT, confidence REAL,
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
        "INSERT INTO stores VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("p1", "モス秋葉原店", "モスバーガー", "東京都千代田区", 0, 0, "https://mos.jp/1", ""),
            ("p2", "モス新宿店", "モスバーガー", "東京都新宿区", 0, 0, "https://mos.jp/2", "03-1234-5678"),
            ("p3", "モス渋谷店", "モスバーガー", "東京都渋谷区", 0, 0, "https://mos.jp/3", ""),
        ],
    )
    conn.executemany(
        "INSERT INTO operator_stores (operator_name, place_id, brand, discovered_via) VALUES (?, ?, ?, ?)",
        [
            # p1 は既に per_store で処理済 → enrich 対象から除外
            ("株式会社既知", "p1", "モスバーガー", "per_store"),
        ],
    )
    conn.commit()
    conn.close()


# ─── _candidate_stores ─────────────────────────────


def test_candidate_stores_excludes_known(tmp_path: Path) -> None:
    db = tmp_path / "p.sqlite"
    _setup_db(db)
    cands = _candidate_stores(db, brand="モスバーガー")
    # p1 は既に operator 付き → 除外、p2 + p3 の 2 件
    ids = {c[0] for c in cands}
    assert ids == {"p2", "p3"}


def test_candidate_stores_respects_max_stores(tmp_path: Path) -> None:
    db = tmp_path / "p.sqlite"
    _setup_db(db)
    cands = _candidate_stores(db, brand="モスバーガー", max_stores=1)
    assert len(cands) == 1


def test_candidate_stores_no_brand_returns_all(tmp_path: Path) -> None:
    db = tmp_path / "p.sqlite"
    _setup_db(db)
    cands = _candidate_stores(db)
    assert len(cands) == 2


# ─── _upsert_phone ─────────────────────────────────


def test_upsert_phone_fills_empty(tmp_path: Path) -> None:
    db = tmp_path / "p.sqlite"
    _setup_db(db)
    _upsert_phone(db, "p1", "03-9999-0000")
    conn = sqlite3.connect(db)
    phone = conn.execute("SELECT phone FROM stores WHERE place_id='p1'").fetchone()[0]
    conn.close()
    assert phone == "03-9999-0000"


def test_upsert_phone_preserves_existing(tmp_path: Path) -> None:
    db = tmp_path / "p.sqlite"
    _setup_db(db)
    _upsert_phone(db, "p2", "03-0000-0000")  # p2 は既存 phone あり
    conn = sqlite3.connect(db)
    phone = conn.execute("SELECT phone FROM stores WHERE place_id='p2'").fetchone()[0]
    conn.close()
    assert phone == "03-1234-5678"  # 上書きされない


def test_upsert_phone_ignores_empty_input(tmp_path: Path) -> None:
    db = tmp_path / "p.sqlite"
    _setup_db(db)
    _upsert_phone(db, "p1", "")
    # 空文字列を渡しても no-op


# ─── _insert_operator_store ─────────────────────────


def test_insert_operator_store(tmp_path: Path) -> None:
    db = tmp_path / "p.sqlite"
    _setup_db(db)
    _insert_operator_store(
        db,
        place_id="p2",
        brand="モスバーガー",
        operator_name="株式会社加盟店A",
        corporate_number="1234567890123",
        confidence=0.7,
    )
    conn = sqlite3.connect(db)
    row = conn.execute(
        "SELECT operator_name, discovered_via, corporate_number FROM operator_stores "
        "WHERE place_id='p2' AND operator_name='株式会社加盟店A'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "株式会社加盟店A"
    assert row[1] == "enrich_phone_lookup"
    assert row[2] == "1234567890123"


def test_insert_operator_store_empty_skip(tmp_path: Path) -> None:
    db = tmp_path / "p.sqlite"
    _setup_db(db)
    _insert_operator_store(db, place_id="p2", brand="モスバーガー", operator_name="")
    # 空 operator_name は noop


# ─── Enricher end-to-end with stubs ─────────────────


class _StubPlaces:
    def __init__(self, phone_by_id: dict[str, str]) -> None:
        self.phone_by_id = phone_by_id
        self.calls = 0

    async def get_place_details(self, pid: str):
        self.calls += 1
        phone = self.phone_by_id.get(pid, "")
        if not phone:
            return None
        return PlaceRaw(place_id=pid, name="", address="", lat=0, lng=0, phone=phone)


class _StubScraper:
    def __init__(self, op_by_phone: dict[str, OperatorInfo | None]) -> None:
        self.op_by_phone = op_by_phone
        self.calls = 0

    async def lookup_operator_by_phone(self, phone: str, brand_hint: str = ""):
        self.calls += 1
        return self.op_by_phone.get(phone)


def test_enricher_full_flow(tmp_path: Path) -> None:
    db = tmp_path / "p.sqlite"
    _setup_db(db)
    # p2=既存 phone, p3=Places Details で取る
    places = _StubPlaces({"p3": "03-3333-3333"})
    scraper = _StubScraper({
        "03-1234-5678": OperatorInfo(name="株式会社新宿運営", corporate_number="1111111111111", confidence=0.9),
        "03-3333-3333": OperatorInfo(name="株式会社渋谷運営", corporate_number="", confidence=0.6),
    })
    enr = Enricher(
        places_client=places, browser_scraper=scraper,
        details_concurrency=1, lookup_concurrency=1,
    )
    stats = asyncio.run(enr.enrich(db_path=db, brand="モスバーガー", max_stores=10))

    assert stats.total_candidates == 2
    assert stats.details_fetched == 1   # p3 のみ詳細 fetch (p2 は既存 phone)
    assert stats.phones_obtained == 2   # p2 既存 + p3 取得 = 2
    assert stats.operators_found == 2

    # DB 確認
    conn = sqlite3.connect(db)
    rows = conn.execute(
        "SELECT place_id, operator_name FROM operator_stores "
        "WHERE discovered_via='enrich_phone_lookup' ORDER BY place_id"
    ).fetchall()
    conn.close()
    assert rows == [
        ("p2", "株式会社新宿運営"),
        ("p3", "株式会社渋谷運営"),
    ]


def test_enricher_handles_scraper_exception(tmp_path: Path) -> None:
    db = tmp_path / "p.sqlite"
    _setup_db(db)
    places = _StubPlaces({})

    class _BrokenScraper:
        async def lookup_operator_by_phone(self, phone, brand_hint=""):
            raise RuntimeError("playwright boom")

    enr = Enricher(
        places_client=places, browser_scraper=_BrokenScraper(),
        details_concurrency=1, lookup_concurrency=1,
    )
    stats = asyncio.run(enr.enrich(db_path=db, brand="モスバーガー", max_stores=10))
    # 例外は errors に記録され pipeline は継続
    assert stats.operators_found == 0
    assert len(stats.errors) >= 1


def test_enricher_no_candidates_short_circuit(tmp_path: Path) -> None:
    """operator 未確定 store が 0 件 → 即 return。"""
    db = tmp_path / "p.sqlite"
    _setup_db(db)
    # 全 store を per_store 処理済にしておく
    conn = sqlite3.connect(db)
    conn.executemany(
        "INSERT OR IGNORE INTO operator_stores (operator_name, place_id, brand, discovered_via) VALUES (?,?,?,?)",
        [("株式会社X", "p2", "モスバーガー", "per_store"),
         ("株式会社Y", "p3", "モスバーガー", "per_store")],
    )
    conn.commit()
    conn.close()

    enr = Enricher(places_client=_StubPlaces({}), browser_scraper=_StubScraper({}))
    stats = asyncio.run(enr.enrich(db_path=db, brand="モスバーガー"))
    assert stats.total_candidates == 0
    assert stats.details_fetched == 0
