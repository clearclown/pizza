"""Phase 14: OperatorSpider — operator 公式 URL から店舗一覧を scrape するテスト。

Top-down の核: 「1 operator 判明 → 公式サイトから全運営店舗取得」。
Google Places API を 20+ 回叩く代わりに、operator 公式を 1 回 fetch するだけで
多数の店舗候補を取得できる (API コスト削減 + Ground Truth 品質向上)。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from pizza_delivery.operator_spider import (
    OperatorSpider,
    ScraplingOperatorFetcher,
    StoreCandidate,
    extract_store_candidates_from_html,
)


# ─── Pure extractor 単体 ──────────────────────────────────────────────


def test_extract_store_candidates_from_company_page() -> None:
    """公式サイトの「店舗一覧」ページから住所 + 店舗名を抽出できる。"""
    html = """
    <html><body>
    <h2>店舗一覧</h2>
    <ul>
      <li class="store">
        <h3>エニタイム 新宿東口店</h3>
        <p>〒160-0022 東京都新宿区新宿3-30-11</p>
      </li>
      <li class="store">
        <h3>エニタイム 渋谷店</h3>
        <p>東京都渋谷区道玄坂1-12-1</p>
      </li>
      <li class="store">
        <h3>エニタイム 池袋店</h3>
        <p>東京都豊島区南池袋1-28-1</p>
      </li>
    </ul>
    </body></html>
    """
    cands = extract_store_candidates_from_html(html)
    # 3 店舗全て抽出
    assert len(cands) >= 3
    addrs = {c.address for c in cands}
    assert any("新宿区" in a for a in addrs)
    assert any("渋谷区" in a for a in addrs)
    assert any("豊島区" in a for a in addrs)


def test_extract_store_candidates_handles_table_layout() -> None:
    """table レイアウトでも住所を検出できる。"""
    html = """
    <table>
      <tr><th>店名</th><th>住所</th></tr>
      <tr><td>横浜店</td><td>神奈川県横浜市中区本町1-1-1</td></tr>
      <tr><td>川崎店</td><td>神奈川県川崎市川崎区駅前1-1</td></tr>
    </table>
    """
    cands = extract_store_candidates_from_html(html)
    assert any("横浜市" in c.address for c in cands)
    assert any("川崎市" in c.address for c in cands)


def test_extract_store_candidates_empty_html() -> None:
    """空/無関係な HTML でも crash しない。"""
    assert extract_store_candidates_from_html("") == []
    assert extract_store_candidates_from_html("<html>no stores here</html>") == []


def test_extract_store_candidates_rejects_non_japanese_addresses() -> None:
    """日本住所 (都道府県+市区町村) の形でない文字列は候補に入れない。"""
    html = """
    <div>東京は素敵だ。</div>
    <div>We have stores in Tokyo, Osaka.</div>
    """
    cands = extract_store_candidates_from_html(html)
    # 具体的な住所パターン (都道府県 + 市区町村 + 番地) がないので空
    assert cands == []


# ─── OperatorSpider with mock fetcher ────────────────────────────────


def test_default_spider_uses_async_scrapling_adapter() -> None:
    """default fetcher は OperatorSpider の async fetch protocol を満たす。"""
    spider = OperatorSpider()
    assert isinstance(spider.fetcher, ScraplingOperatorFetcher)
    assert hasattr(spider.fetcher, "fetch")


@dataclass
class MockFetcher:
    pages: dict[str, str]

    async def fetch(self, url: str, *, timeout: float = 20.0) -> str:
        if url not in self.pages:
            raise RuntimeError(f"mock: no page for {url}")
        return self.pages[url]


@pytest.mark.asyncio
async def test_spider_discovers_stores_from_official_url() -> None:
    """公式サイト URL → 店舗一覧ページ link 追跡 → 住所抽出のフロー。"""
    company_html = """
    <html><body>
      <a href="/stores">店舗一覧</a>
      <a href="/company">会社情報</a>
    </body></html>
    """
    stores_html = """
    <h2>店舗一覧</h2>
    <ul>
      <li><h3>A店</h3><p>東京都新宿区1-1</p></li>
      <li><h3>B店</h3><p>東京都渋谷区2-2</p></li>
    </ul>
    """
    fetcher = MockFetcher(
        pages={
            "https://example.com/": company_html,
            "https://example.com/stores": stores_html,
        }
    )
    spider = OperatorSpider(fetcher=fetcher)
    cands = await spider.discover(
        operator_name="株式会社テスト",
        official_url="https://example.com/",
    )
    # 2 店舗検出
    assert len(cands) >= 2
    assert any("新宿区" in c.address for c in cands)
    assert any("渋谷区" in c.address for c in cands)
    # 各候補に operator 情報が紐づく
    for c in cands:
        assert c.operator_name == "株式会社テスト"


@pytest.mark.asyncio
async def test_spider_handles_fetch_error() -> None:
    """fetch 失敗時は空リストを返し、crash しない。"""
    fetcher = MockFetcher(pages={})
    spider = OperatorSpider(fetcher=fetcher)
    cands = await spider.discover(
        operator_name="株式会社X",
        official_url="https://notfound.example/",
    )
    assert cands == []


@pytest.mark.asyncio
async def test_spider_finds_stores_on_entry_page() -> None:
    """店舗一覧 link がなくても、entry page に直接住所があれば拾う。"""
    entry_html = """
    <h1>株式会社AAA</h1>
    <div>
      店舗:
      東京都港区六本木1-1 (本店)
      東京都中央区銀座2-2 (支店)
    </div>
    """
    fetcher = MockFetcher(pages={"https://a.example/": entry_html})
    spider = OperatorSpider(fetcher=fetcher)
    cands = await spider.discover(
        operator_name="株式会社AAA",
        official_url="https://a.example/",
    )
    assert any("港区" in c.address for c in cands)
    assert any("中央区" in c.address for c in cands)


# ─── Multi-brand discovery tests ──────────────────────────────────────


def test_extract_brand_candidates_from_nav_menu() -> None:
    """navigation menu に並ぶブランド名を検出 (1 operator が複数 FC 運営)。"""
    from pizza_delivery.operator_spider import extract_brand_candidates_from_html

    html = """
    <nav>
      <a href="/fitness/">エニタイムフィットネス</a>
      <a href="/sauna/">ONEPERSON</a>
      <a href="/pilates/">CLUB PILATES</a>
      <a href="/cycle/">CYCLEBAR</a>
      <a href="/about">会社概要</a>  <!-- ブランドではない -->
    </nav>
    """
    out = extract_brand_candidates_from_html(html, base_url="https://a.example/")
    brands = {c.brand_name for c in out}
    assert "エニタイムフィットネス" in brands
    assert "ONEPERSON" in brands
    assert "CLUB PILATES" in brands
    assert "CYCLEBAR" in brands


def test_extract_brand_candidates_ignores_non_brand_links() -> None:
    """無関係 link は無視。"""
    from pizza_delivery.operator_spider import extract_brand_candidates_from_html

    html = """
    <a href="/ir">IR情報</a>
    <a href="/contact">お問い合わせ</a>
    <a href="/news">ニュース</a>
    """
    out = extract_brand_candidates_from_html(html)
    assert out == []


def test_extract_brand_candidates_handles_empty() -> None:
    from pizza_delivery.operator_spider import extract_brand_candidates_from_html

    assert extract_brand_candidates_from_html("") == []


def test_extract_brand_candidates_canonicalizes_target_aliases_and_image_alt() -> None:
    from pizza_delivery.operator_spider import extract_brand_candidates_from_html

    html = """
    <a href="/brands/anytime"><img alt="Anytime Fitness"></a>
    <a href="/brands/brand-off">BRAND OFF</a>
    <a href="/brands/itto">ITTO個別指導学院</a>
    """
    out = extract_brand_candidates_from_html(html, base_url="https://a.example/")
    brands = {c.brand_name for c in out}
    assert "エニタイムフィットネス" in brands
    assert "Brand off" in brands
    assert "Itto個別指導学院" in brands


def test_extract_brand_candidates_does_not_match_itto_inside_kitto() -> None:
    from pizza_delivery.operator_spider import extract_brand_candidates_from_html

    html = '<a href="/kitto">KITTO</a>'
    assert extract_brand_candidates_from_html(html, base_url="https://a.example/") == []


@pytest.mark.asyncio
async def test_discover_multi_brand_fetches_and_extracts() -> None:
    """discover_multi_brand が fetcher 経由で HTML を取り、ブランド候補を返す。"""
    from pizza_delivery.operator_spider import discover_multi_brand

    html = """
    <html>
      <nav>
        <a href="/a">マクドナルド</a>
        <a href="/b">TSUTAYA</a>
      </nav>
    </html>
    """
    fetcher = MockFetcher(pages={"https://x.example/": html})
    cands = await discover_multi_brand(
        fetcher=fetcher, official_url="https://x.example/"
    )
    brand_names = {c.brand_name for c in cands}
    assert "マクドナルド" in brand_names
    assert "TSUTAYA" in brand_names
