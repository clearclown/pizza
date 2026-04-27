"""brand_profiler.py orchestrator のテスト (Phase 25)。"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from pizza_delivery.brand_profiler import (
    BrandProfile,
    BrandProfiler,
    CSV_COLUMNS,
    _fetch_affiliate_brands,
    _merge_profile,
    export_csv,
    export_json,
)


# ─── _merge_profile ────────────────────────────────────────


def test_merge_all_sources() -> None:
    jfa = {
        "source": "jfa",
        "franchisor_name": "株式会社モスフードサービス",
        "website_url": "https://www.mos.co.jp/",
        "fc_recruitment_url": "",
        "corporate_number": "",
        "head_office": "東京都品川区",
    }
    gbiz = {
        "source": "gbiz",
        "corporate_number": "5010701019713",
        "representative_name": "中村 栄輔",
        "representative_title": "代表取締役社長",
        "headquarters_address": "東京都品川区大崎2-1-1",
    }
    official = {
        "source": "official",
        "company_name": "株式会社モスフードサービス",
        "fc_store_count": 1260,
        "representative_name": "中村 栄輔",
        "representative_title": "代表取締役社長",
        "headquarters_address": "東京都品川区大崎2-1-1",
        "revenue_current_jpy": 84_600_000_000,
        "revenue_previous_jpy": 79_800_000_000,
        "revenue_observed_at": "2024年3月期",
        "fc_recruitment_url": "https://www.mos.co.jp/franchise/",
        "visited_urls": ["https://www.mos.co.jp/"],
        "website_url": "https://www.mos.co.jp/",
    }
    affiliate = ["ミスタードーナツ", "コメダ珈琲"]

    p = _merge_profile("モスバーガー", jfa, gbiz, official, affiliate)
    assert p.brand_name == "モスバーガー"
    assert p.franchisor_name == "株式会社モスフードサービス"
    assert p.corporate_number == "5010701019713"
    assert p.fc_store_count == 1260
    assert p.representative_name == "中村 栄輔"
    assert p.revenue_current_jpy == 84_600_000_000
    assert p.affiliate_brands == affiliate
    assert p.confidence == 1.0  # 10/10 filled
    assert "jfa" in p.sources
    assert "gbiz" in p.sources
    assert "official" in p.sources
    assert "orm_crossbrand" in p.sources


def test_merge_priority_official_over_gbiz_for_company_name() -> None:
    """JFA も gBiz も franchisor_name 空で、official の company_name で埋める。"""
    jfa = {"source": "jfa"}
    gbiz = {"source": "gbiz"}
    official = {
        "source": "official",
        "company_name": "株式会社ハードオフコーポレーション",
    }
    p = _merge_profile("ハードオフ", jfa, gbiz, official, [])
    assert p.franchisor_name == "株式会社ハードオフコーポレーション"


def test_merge_revenue_from_official_only() -> None:
    """gBiz / JFA は売上を持たないため、official のみで埋まる (ハルシネ禁止)。"""
    p = _merge_profile(
        "X",
        {"source": "jfa"},
        {"source": "gbiz"},
        {"source": "official", "revenue_current_jpy": 10_000_000_000},
        [],
    )
    assert p.revenue_current_jpy == 10_000_000_000
    assert p.revenue_previous_jpy == 0


def test_merge_all_empty_returns_zero_confidence() -> None:
    p = _merge_profile(
        "Unknown", {"source": "jfa"}, {"source": "gbiz_skipped"},
        {"source": "official_skipped"}, [],
    )
    assert p.confidence <= 0.1  # brand_name だけ "埋まってる"
    assert p.errors == []


# ─── _fetch_affiliate_brands ───────────────────────────────


def test_affiliate_brands_no_db(tmp_path: Path) -> None:
    """pipeline DB が存在しなければ空 list を返す。"""
    result = _fetch_affiliate_brands("モスバーガー", "株式会社モスフードサービス",
                                      str(tmp_path / "no_such.db"))
    assert result == []


def test_affiliate_brands_with_db(tmp_path: Path) -> None:
    """pipeline DB に operator_stores があれば cross-brand を返す。"""
    db_path = tmp_path / "pipeline.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE operator_stores (
            place_id TEXT,
            brand TEXT,
            operator_name TEXT,
            operator_type TEXT
        )
    """)
    # Op-A は モス と ドトール を運営
    conn.executemany(
        "INSERT INTO operator_stores VALUES (?, ?, ?, ?)",
        [
            ("p1", "モスバーガー", "株式会社OpA", "franchisee"),
            ("p2", "ドトール", "株式会社OpA", "franchisee"),
            ("p3", "モスバーガー", "株式会社OpA", "franchisee"),  # dup
        ],
    )
    conn.commit()
    conn.close()

    result = _fetch_affiliate_brands("モスバーガー", "", str(db_path))
    assert "ドトール" in result
    assert "モスバーガー" not in result  # 自ブランド除外


# ─── CSV / JSON export ────────────────────────────────────


def test_export_csv_format(tmp_path: Path) -> None:
    p = BrandProfile(
        brand_name="テスト",
        franchisor_name="株式会社テスト",
        corporate_number="1234567890123",
        fc_store_count=100,
        representative_name="山田太郎",
        representative_title="代表取締役社長",
        headquarters_address="東京都千代田区",
        revenue_current_jpy=5_000_000_000,
        revenue_previous_jpy=4_500_000_000,
        revenue_observed_at="2024年3月期",
        website_url="https://example.co.jp/",
        affiliate_brands=["ブランドA", "ブランドB"],
        fc_recruitment_url="https://example.co.jp/fc/",
        sources=["jfa", "gbiz", "official"],
        confidence=1.0,
    )
    out = tmp_path / "out.csv"
    export_csv([p], out)
    text = out.read_text(encoding="utf-8")
    assert text.startswith(",".join(CSV_COLUMNS))
    assert "株式会社テスト" in text
    assert "ブランドA;ブランドB" in text  # list → ';' 連結


def test_export_json_roundtrip(tmp_path: Path) -> None:
    import json

    p = BrandProfile(brand_name="X", sources=["jfa"], confidence=0.1)
    out = tmp_path / "out.json"
    export_json([p], out)
    data = json.loads(out.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert data[0]["brand_name"] == "X"


# ─── BrandProfiler orchestrator (mock) ─────────────────────


@pytest.mark.asyncio
async def test_profiler_all_sources_fail_returns_empty(monkeypatch) -> None:
    """全 source が失敗しても profile は brand_name 付きで返る (graceful)。"""
    from pizza_delivery import brand_profiler as bp

    async def mock_jfa(b, **kw): return {"source": "jfa"}
    async def mock_places(b): return {"source": "places"}
    async def mock_gbiz(n, **kw): return {"source": "gbiz"}
    async def mock_official(u): return {"source": "official_skipped"}

    monkeypatch.setattr(bp, "_fetch_jfa", mock_jfa)
    monkeypatch.setattr(bp, "_fetch_places_fallback", mock_places)
    monkeypatch.setattr(bp, "_fetch_gbiz", mock_gbiz)
    monkeypatch.setattr(bp, "_fetch_official", mock_official)
    monkeypatch.setattr(bp, "_fetch_affiliate_brands", lambda *a, **k: [])

    profiler = BrandProfiler(brand_concurrency=2, intra_concurrency=2)
    res = await profiler.profile_many(["A", "B"])
    assert len(res) == 2
    assert all(r.brand_name for r in res)
    assert all(r.confidence <= 0.2 for r in res)


@pytest.mark.asyncio
async def test_profiler_concurrency_semaphore(monkeypatch) -> None:
    """brand_concurrency=1 のときは逐次 (intra は並列許可)。"""
    from pizza_delivery import brand_profiler as bp

    active = [0]
    peak = [0]

    async def mock_jfa(b, **kw):
        active[0] += 1
        peak[0] = max(peak[0], active[0])
        await asyncio.sleep(0.01)
        active[0] -= 1
        return {"source": "jfa"}

    async def mock_simple(*a, **k):
        return {"source": "x"}

    monkeypatch.setattr(bp, "_fetch_jfa", mock_jfa)
    monkeypatch.setattr(bp, "_fetch_places_fallback", mock_simple)
    monkeypatch.setattr(bp, "_fetch_gbiz", mock_simple)
    monkeypatch.setattr(bp, "_fetch_official", mock_simple)
    monkeypatch.setattr(bp, "_fetch_affiliate_brands", lambda *a, **k: [])

    profiler = BrandProfiler(brand_concurrency=1, intra_concurrency=3)
    await profiler.profile_many(["A", "B", "C"])
    assert peak[0] == 1  # brand_concurrency=1 なので 1 つずつ
