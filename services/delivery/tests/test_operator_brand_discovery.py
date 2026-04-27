from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pizza_delivery.operator_brand_discovery import (
    OperatorSeed,
    apply_discoveries,
    discover_for_operator,
    find_business_links,
    load_candidate_operators,
)


class FakeFetcher:
    def __init__(self, pages: dict[str, str]) -> None:
        self.pages = pages

    def fetch_with_mode(self, url: str, mode: str = "auto") -> str | None:
        return self.pages.get(url)


def _setup_orm(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE operator_company (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            corporate_number TEXT NOT NULL DEFAULT '',
            head_office TEXT NOT NULL DEFAULT '',
            prefecture TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            website_url TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE franchise_brand (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            industry TEXT NOT NULL DEFAULT '',
            master_franchisor_name TEXT NOT NULL DEFAULT '',
            master_franchisor_corp TEXT NOT NULL DEFAULT '',
            jfa_member BOOLEAN NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT '',
            fc_recruitment_url TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE brand_operator_link (
            id INTEGER PRIMARY KEY,
            brand_id INTEGER NOT NULL,
            operator_id INTEGER NOT NULL,
            estimated_store_count INTEGER NOT NULL DEFAULT 0,
            observed_at TEXT NOT NULL DEFAULT '',
            operator_type TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT '',
            source_url TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            UNIQUE (brand_id, operator_id, source)
        );
        """
    )
    conn.execute(
        """
        INSERT INTO operator_company
          (id, name, corporate_number, website_url, source)
        VALUES (1, '株式会社テストFC', '1234567890123', 'https://op.example/', 'manual')
        """
    )
    conn.executemany(
        "INSERT INTO franchise_brand (id, name, source) VALUES (?, ?, ?)",
        [
            (10, "モスバーガー", "seed"),
            (11, "エニタイムフィットネス", "seed"),
        ],
    )
    conn.execute(
        """
        INSERT INTO brand_operator_link
          (brand_id, operator_id, estimated_store_count, operator_type, source)
        VALUES (10, 1, 30, 'franchisee', 'manual')
        """
    )
    conn.commit()
    conn.close()


def test_find_business_links_same_site_only() -> None:
    html = """
    <a href="/business/">事業内容</a>
    <a href="/business/#food">事業内容 食品</a>
    <a href="/news/info.pdf">事業説明PDF</a>
    <a href="https://external.example/brand">ブランド</a>
    <a href="/contact/">お問い合わせ</a>
    """
    links = find_business_links("https://op.example/", html, max_links=10)
    assert links == ["https://op.example/business/"]


def test_load_candidate_operators_filters_by_total_and_website(tmp_path: Path) -> None:
    orm = tmp_path / "orm.sqlite"
    _setup_orm(orm)
    seeds = load_candidate_operators(orm, min_total=20)
    assert seeds == [
        OperatorSeed(
            operator_id=1,
            operator_name="株式会社テストFC",
            corporate_number="1234567890123",
            website_url="https://op.example/",
            operator_total_stores_est=30,
        )
    ]


@pytest.mark.asyncio
async def test_discover_for_operator_accepts_new_internal_brand_link(tmp_path: Path) -> None:
    orm = tmp_path / "orm.sqlite"
    _setup_orm(orm)
    pages = {
        "https://op.example/": '<a href="/business/">事業内容</a>',
        "https://op.example/business/": '<a href="/business/anytime">Anytime Fitness</a>',
    }
    rows, pages_fetched, failed = await discover_for_operator(
        OperatorSeed(
            operator_id=1,
            operator_name="株式会社テストFC",
            corporate_number="1234567890123",
            website_url="https://op.example/",
            operator_total_stores_est=30,
        ),
        orm_db=orm,
        fetcher=FakeFetcher(pages),
        fetcher_mode="static",
    )

    assert not failed
    assert pages_fetched == 2
    accepted = [r for r in rows if r.status == "accepted"]
    assert len(accepted) == 1
    assert accepted[0].brand_name == "エニタイムフィットネス"

    applied = apply_discoveries(orm, rows)
    assert applied == 1
    conn = sqlite3.connect(orm)
    try:
        stored = conn.execute(
            """
            SELECT b.name, l.estimated_store_count, l.operator_type, l.source_url
            FROM brand_operator_link l
            JOIN franchise_brand b ON b.id = l.brand_id
            WHERE l.source = 'operator_official_brand_link'
            """
        ).fetchone()
    finally:
        conn.close()
    assert stored == (
        "エニタイムフィットネス",
        0,
        "unknown",
        "https://op.example/business/",
    )


@pytest.mark.asyncio
async def test_discover_for_operator_marks_existing_and_external_review(tmp_path: Path) -> None:
    orm = tmp_path / "orm.sqlite"
    _setup_orm(orm)
    pages = {
        "https://op.example/": """
          <a href="/business/mos">モスバーガー</a>
          <a href="https://brand.example/anytime">Anytime Fitness</a>
        """,
    }
    rows, _, _ = await discover_for_operator(
        OperatorSeed(
            operator_id=1,
            operator_name="株式会社テストFC",
            corporate_number="1234567890123",
            website_url="https://op.example/",
            operator_total_stores_est=30,
        ),
        orm_db=orm,
        fetcher=FakeFetcher(pages),
        fetcher_mode="static",
    )
    by_brand = {r.brand_name: r.status for r in rows}
    assert by_brand["モスバーガー"] == "existing_link"
    assert by_brand["エニタイムフィットネス"] == "external_link_review"
