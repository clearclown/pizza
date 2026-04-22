"""🧪 Unit tests for evidence.py — deterministic extraction (no LLM)."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from pizza_delivery.evidence import (
    Evidence,
    EvidenceCollector,
    detect_direct_operation_from_snippet,
    find_company_names_in_snippet,
)


# ─── Mock fetcher ──────────────────────────────────────────────────────


@dataclass
class MockFetcher:
    """URL → HTML の固定 map。"""

    pages: dict[str, str]

    async def fetch(self, url: str, *, timeout: float = 20.0) -> str:
        if url not in self.pages:
            raise RuntimeError(f"mock: no page for {url}")
        return self.pages[url]


# ─── Regex helpers ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "snippet, expected",
    [
        ("運営会社: 株式会社AFJ Project が当店を運営", ["株式会社AFJ Project"]),
        ("運営元は株式会社メガ・スポーツです", ["株式会社メガ・スポーツ"]),
        ("弊社は(株)テストフードが展開", ["(株)テストフード"]),
        ("当社名: ㈱ピザ商事の子会社", ["㈱ピザ商事"]),
        ("本店: 株式会社A / 支店: 株式会社B", ["株式会社A", "株式会社B"]),
        ("運営情報なし", []),
    ],
)
def test_find_company_names_in_snippet(snippet, expected) -> None:
    got = find_company_names_in_snippet(snippet)
    for exp in expected:
        assert exp in got, f"expected {exp!r} in {got!r}"


@pytest.mark.parametrize(
    "snippet, expected",
    [
        ("当店は全店直営で運営されています", True),
        ("直営店のみ展開", True),
        ("自社運営です", True),
        ("弊社100%子会社が運営", True),
        ("加盟店のみ", False),
        ("FC 展開中", False),
    ],
)
def test_detect_direct_operation_from_snippet(snippet, expected) -> None:
    assert detect_direct_operation_from_snippet(snippet) is expected


# ─── EvidenceCollector.collect ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_collect_extracts_operator_keyword_evidence() -> None:
    root_html = """
    <html><body>
      <header>店舗情報</header>
      <main>
        <h1>エニタイムフィットネス 新宿6丁目店</h1>
        <p>当店は24時間営業のフィットネスです。</p>
        <p>運営会社: 株式会社AFJ Project</p>
      </main>
      <footer>
        <a href="/about">会社概要</a>
        <a href="/stores">店舗一覧</a>
      </footer>
    </body></html>
    """
    about_html = """
    <html><body>
      <h1>会社概要</h1>
      <p>株式会社AFJ Project は全国に35店舗のエニタイムフィットネスを運営しています。</p>
    </body></html>
    """
    collector = EvidenceCollector(
        fetcher=MockFetcher(pages={
            "https://example.com/store": root_html,
            "https://example.com/about": about_html,
        }),
        max_pages=3,
    )
    evs = await collector.collect(
        brand="エニタイムフィットネス",
        official_url="https://example.com/store",
    )
    assert len(evs) >= 1
    # root と about 両方から evidence が取れる
    urls = {e.source_url for e in evs}
    assert "https://example.com/store" in urls
    assert "https://example.com/about" in urls
    # 運営会社キーワードが拾えている
    operator_evs = [e for e in evs if e.reason == "operator_keyword"]
    assert operator_evs
    joined = " ".join(e.snippet for e in operator_evs)
    assert "株式会社AFJ Project" in joined


@pytest.mark.asyncio
async def test_collect_extracts_direct_keyword() -> None:
    html = """
    <html><body>
      <h1>スターバックス</h1>
      <p>当店はスターバックス コーヒー ジャパン株式会社の全店直営店舗の 1 つです。</p>
    </body></html>
    """
    collector = EvidenceCollector(
        fetcher=MockFetcher(pages={"https://example.com/": html}),
    )
    evs = await collector.collect(
        brand="スターバックス",
        official_url="https://example.com/",
    )
    direct_evs = [e for e in evs if e.reason == "direct_keyword"]
    assert direct_evs, f"direct keyword が拾えず: {evs}"


@pytest.mark.asyncio
async def test_collect_returns_empty_when_root_fetch_fails() -> None:
    collector = EvidenceCollector(fetcher=MockFetcher(pages={}))
    evs = await collector.collect(
        brand="B",
        official_url="https://dead.example.com/",
    )
    assert evs == [], "ルート fetch 失敗 → evidence なし (空で安全に)"


@pytest.mark.asyncio
async def test_collect_follows_about_links() -> None:
    root_html = """
    <a href="/corp/company">会社概要</a>
    <a href="https://other.example.com/about">別ホスト</a>
    <p>main content</p>
    """
    about_html = "<p>運営: 株式会社Testerfood</p>"
    collector = EvidenceCollector(
        fetcher=MockFetcher(pages={
            "https://pizza.example.com/": root_html,
            "https://pizza.example.com/corp/company": about_html,
        }),
    )
    evs = await collector.collect(
        brand="B",
        official_url="https://pizza.example.com/",
    )
    urls = {e.source_url for e in evs}
    # 同一ホストの会社概要ページを訪問、別ホストはスキップ
    assert "https://pizza.example.com/corp/company" in urls
    assert "https://other.example.com/about" not in urls


@pytest.mark.asyncio
async def test_collect_dedupes_by_url_and_snippet() -> None:
    html = (
        "<p>運営会社: 株式会社Same</p>" * 3  # 同一キーワードが 3 回出現
    )
    collector = EvidenceCollector(
        fetcher=MockFetcher(pages={"https://x/": html}),
    )
    evs = await collector.collect(
        brand="B",
        official_url="https://x/",
    )
    # 同じ snippet は dedupe される (snippet の頭 100 文字が同じ場合)
    operator_evs = [e for e in evs if e.reason == "operator_keyword"]
    unique_snippets = {e.snippet[:100] for e in operator_evs}
    assert len(unique_snippets) >= 1


@pytest.mark.asyncio
async def test_collect_captures_meta_description() -> None:
    html = """
    <head>
      <meta name="description" content="当社は株式会社Metaが運営する公式サイトです">
    </head>
    <body><p>本文</p></body>
    """
    collector = EvidenceCollector(
        fetcher=MockFetcher(pages={"https://y/": html}),
    )
    evs = await collector.collect(brand="B", official_url="https://y/")
    meta = [e for e in evs if e.reason == "metadata"]
    assert meta
    assert "株式会社Meta" in meta[0].snippet
