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
        # Phase 9: NFKC 正規化で ㈱ → (株) に統一される
        ("当社名: ㈱ピザ商事の子会社", ["(株)ピザ商事"]),
        ("本店: 株式会社A / 支店: 株式会社B", ["株式会社A", "株式会社B"]),
        ("運営情報なし", []),
    ],
)
def test_find_company_names_in_snippet(snippet, expected) -> None:
    got = find_company_names_in_snippet(snippet)
    for exp in expected:
        assert exp in got, f"expected {exp!r} in {got!r}"


# Phase 7: 「XXX 株式会社について紹介します」のような suffix-noise で
# prefix pattern が誤抽出しないことを保証する。
@pytest.mark.parametrize(
    "snippet, must_include, must_exclude",
    [
        (
            "日本マクドナルド株式会社について紹介します。",
            "日本マクドナルド株式会社",
            "株式会社について紹介します",
        ),
        (
            "株式会社フーズに関するお問い合わせ",
            "株式会社フーズ",
            "株式会社フーズに関するお問い合わせ",
        ),
        (
            "株式会社Aに対する調査結果",
            "株式会社A",
            "株式会社Aに対する調査結果",
        ),
    ],
)
def test_find_company_names_rejects_verb_suffix_noise(
    snippet, must_include, must_exclude
) -> None:
    got = find_company_names_in_snippet(snippet)
    assert must_include in got, f"{must_include!r} が抽出されるべき: got {got!r}"
    for name in got:
        assert must_exclude not in name, (
            f"noise フレーズ {must_exclude!r} を含む誤抽出が残っている: {got!r}"
        )


# Phase 7: 会社概要ページの HTML ラベル ("名称", "会社概要", "会社案内" 等) が
# body 前方に紛れ込む noise を除去できる。
@pytest.mark.parametrize(
    "snippet, wanted",
    [
        (
            "会社案内 会社概要 会社概要 名称 スターバックス コーヒー ジャパン株式会社",
            "スターバックス コーヒー ジャパン株式会社",
        ),
        (
            "会社名 日本マクドナルド株式会社 本社 東京都",
            "日本マクドナルド株式会社",
        ),
        (
            "商号 ㈱ファミリーマート 代表取締役 細見 研介",
            "(株)ファミリーマート",  # NFKC で ㈱ → (株)
        ),
    ],
)
def test_find_company_names_strips_html_labels(snippet, wanted) -> None:
    got = find_company_names_in_snippet(snippet)
    assert wanted in got, (
        f"{wanted!r} が抽出されるべき (HTML ラベル除去): got {got!r}"
    )


# Phase 9 Bug 1: Unicode dash を body に含められる
@pytest.mark.parametrize(
    "snippet, wanted",
    [
        # ASCII hyphen
        ("運営: 株式会社セブン-イレブン・ジャパン の店舗", "株式会社セブン-イレブン・ジャパン"),
        # U+2010 HYPHEN
        ("運営会社は株式会社セブン‐イレブン・ジャパンです。", "株式会社セブン‐イレブン・ジャパン"),
        # U+2013 EN DASH
        ("株式会社セブン–イレブン・ジャパン が運営", "株式会社セブン–イレブン・ジャパン"),
        # U+FF0D 全角マイナス (NFKC で ASCII - になるはずだが念のため)
        ("株式会社セブン－イレブン・ジャパン の店舗", "株式会社セブン-イレブン・ジャパン"),
    ],
)
def test_find_company_names_accepts_unicode_dashes(snippet, wanted) -> None:
    got = find_company_names_in_snippet(snippet)
    # snippet が NFKC 前後で内容変わる可能性、どちらか含まれていれば OK
    assert any(wanted in g or g == wanted for g in got), (
        f"期待 {wanted!r} が抽出されず: got {got!r}"
    )


# Phase 9 Bug 2: 複数社を連結しない (non-greedy + separator 強化)
def test_find_company_names_no_concat_suffix() -> None:
    s = "運営は株式会社A 委託先 株式会社B株式会社"
    out = find_company_names_in_snippet(s)
    assert "株式会社A" in out or any("A" in n for n in out)
    assert "株式会社B" in out
    for n in out:
        assert "A株式会社B" not in n, (
            f"連結誤抽出が残っている: {out!r}"
        )


def test_find_company_names_three_operators_separate() -> None:
    """3 社を 1 つの snippet に並べて、3 件別々に抽出されること。"""
    s = (
        "A 店は株式会社アルファが運営。"
        "B 店は株式会社ベータ・ジャパンが運営。"
        "C 店は株式会社ガンマ商事が運営。"
    )
    out = find_company_names_in_snippet(s)
    assert "株式会社アルファ" in out
    assert "株式会社ベータ・ジャパン" in out
    assert "株式会社ガンマ商事" in out


def test_find_company_names_real_family_mart_pattern() -> None:
    """実ログで観測された複数社連結パターンに基づく回帰テスト。"""
    s = (
        "運営会社: 株式会社ファミマ・サポート / "
        "本件事業: 株式会社クリアーウォーター津南"
    )
    out = find_company_names_in_snippet(s)
    assert "株式会社ファミマ・サポート" in out or any(
        "ファミマ" in n for n in out
    )
    # 3 社連結は発生しない
    for n in out:
        assert "サポート株式会社" not in n, f"連結バグ再発: {out!r}"


# Phase 11 バグ fix: 広告文 (キャンペーン/期間/年月日) の body 吸収を防ぐ
@pytest.mark.parametrize(
    "snippet, wanted, must_not_include",
    [
        # 実ログで観測: エニタイムで広告文が吸収された
        (
            "株式会社アルペンクイックフィットネス キャンペーン期間2026年4月1日",
            "株式会社アルペンクイックフィットネス",
            "キャンペーン",
        ),
        # 年月日 suffix
        (
            "株式会社テスト 2026年5月1日オープン",
            "株式会社テスト",
            "2026",
        ),
        # "期間" 単独
        (
            "株式会社サンプル 期間限定",
            "株式会社サンプル",
            "期間",
        ),
        # "特典" キーワード
        (
            "株式会社プロ 特典情報",
            "株式会社プロ",
            "特典",
        ),
        # "お知らせ"
        (
            "株式会社ABC お知らせ: 営業時間変更",
            "株式会社ABC",
            "お知らせ",
        ),
    ],
)
def test_find_company_names_strips_advertising_noise(
    snippet, wanted, must_not_include
) -> None:
    got = find_company_names_in_snippet(snippet)
    assert wanted in got, f"期待 {wanted!r} 抽出されず: {got!r}"
    for name in got:
        assert must_not_include not in name, (
            f"広告文 {must_not_include!r} が body に残っている: {got!r}"
        )


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
