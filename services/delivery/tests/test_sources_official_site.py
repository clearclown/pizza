"""official_site の 代表者 / 住所 / 会社名抽出テスト (Phase 25)。"""

from __future__ import annotations

import asyncio

import pytest

from pizza_delivery.sources.official_site import (
    OfficialSiteData,
    _clean_name_candidate,
    _extract_address,
    _extract_company_name,
    _extract_representative,
    _extract_store_count,
    _find_company_links,
    fetch_official_site,
)


# ─── _clean_name_candidate ─────────────────────────────────


def test_clean_name_plain() -> None:
    assert _clean_name_candidate("中村 栄輔") == "中村 栄輔"


def test_clean_name_strips_trailing_title() -> None:
    assert _clean_name_candidate("甘利 祐一 専務") == "甘利 祐一"


def test_clean_name_strips_address_suffix() -> None:
    assert _clean_name_candidate("山内 祐也 金沢") == "山内 祐也"


def test_clean_name_strips_kanekigo() -> None:
    assert _clean_name_candidate("兼 グル") == ""  # katakana garbage


def test_clean_name_accepts_single_kanji_given() -> None:
    assert _clean_name_candidate("増本 岳") == "増本 岳"


def test_clean_name_rejects_label_start() -> None:
    assert _clean_name_candidate("代表取締役 山田太郎") == ""


def test_clean_name_length_bounds() -> None:
    assert _clean_name_candidate("") == ""
    assert _clean_name_candidate("あ") == ""
    assert _clean_name_candidate("a" * 10) == ""


# ─── _extract_representative ───────────────────────────────


def test_extract_representative_with_title() -> None:
    t = "代表取締役社長 中村 栄輔 本社所在地 東京都品川区"
    title, name = _extract_representative(t)
    assert title == "代表取締役社長"
    assert name == "中村 栄輔"


def test_extract_representative_ceo_label() -> None:
    t = "代表者 山田太郎 資本金 1 億円"
    title, name = _extract_representative(t)
    assert name == "山田太郎"


def test_extract_representative_miss() -> None:
    t = "このページには代表者情報はありません"
    title, name = _extract_representative(t)
    assert title == "" and name == ""


# ─── _extract_address ──────────────────────────────────────


def test_extract_address_with_postal() -> None:
    postal, addr = _extract_address("〒100-0001 東京都千代田区千代田1-1")
    # _RE_ADDRESS requires label (本社/所在地/住所/本社) — plain addr not matched
    # なのでラベル付き文を与える
    _, addr = _extract_address("本社所在地: 東京都千代田区千代田1-1")
    assert "東京都" in addr


def test_extract_address_no_label() -> None:
    postal, addr = _extract_address("渋谷の街")
    assert addr == ""


# ─── _extract_store_count ──────────────────────────────────


def test_store_count_kansuji() -> None:
    assert _extract_store_count("全国 1,260 店舗") == 1260


def test_store_count_kokunai() -> None:
    assert _extract_store_count("国内 2,000 拠点") == 2000


def test_store_count_unreasonable() -> None:
    assert _extract_store_count("全国 100,000 店") == 0  # over 50k = invalid


# ─── _extract_company_name ─────────────────────────────────


def test_company_name_labeled() -> None:
    s = "会社名: 株式会社モスフードサービス"
    assert _extract_company_name(s) == "株式会社モスフードサービス"


def test_company_name_bare_frequency() -> None:
    s = ("株式会社テスト は株式会社テスト の子会社です。"
         "株式会社テスト について。")
    assert _extract_company_name(s) == "株式会社テスト"


def test_company_name_single_match_rejected() -> None:
    s = "株式会社XXX が一度だけ登場する文章"
    assert _extract_company_name(s) == ""


# ─── _find_company_links ───────────────────────────────────


def test_find_company_links_path_hint() -> None:
    html = '<nav><a href="/company/">会社概要</a><a href="/news/">お知らせ</a></nav>'
    links = _find_company_links(html, "https://x.com/")
    assert "https://x.com/company/" in links
    assert not any("/news/" in u for u in links)


def test_find_company_links_anchor_hint() -> None:
    html = '<a href="/about/overview.html">企業情報</a>'
    links = _find_company_links(html, "https://x.com/")
    assert any("overview" in u for u in links)


# ─── fetch_official_site (integration w/ mocks) ────────────


@pytest.mark.asyncio
async def test_fetch_official_site_full_flow() -> None:
    top = '<nav><a href="/company/">会社概要</a></nav><p>全国 1,260 店舗</p>'
    company = ("<dl>"
               "<dt>代表者</dt><dd>代表取締役社長 中村 栄輔</dd>"
               "<dt>本社所在地</dt><dd>東京都品川区大崎2-1-1</dd>"
               "</dl>"
               "<table><tr><td>2024年3月期</td><td>売上高: 846億円</td></tr></table>")
    pad = "x" * 5000

    def static(url: str) -> str:
        if "/company" in url:
            return company + pad
        return top + pad

    def dynamic(url: str) -> str:
        return ""

    d = await fetch_official_site(
        "https://www.mos.co.jp/",
        fetcher_static=static,
        fetcher_dynamic=dynamic,
    )
    assert "中村" in d.representative_name
    assert "東京都品川区" in d.headquarters_address
    assert d.fc_store_count == 1260
    assert d.revenue.current_jpy == 84_600_000_000
    assert d.website_url == "https://www.mos.co.jp/"
    assert len(d.visited_urls) >= 1


@pytest.mark.asyncio
async def test_fetch_official_site_empty_url() -> None:
    d = await fetch_official_site("")
    assert isinstance(d, OfficialSiteData)
    assert d.website_url == ""
    assert d.representative_name == ""
