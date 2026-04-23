"""Evidence Collector — 公式サイトから実データを取得する。

Phase 4 Evidence-based architecture の核心モジュール:
  - LLM の推論に依存せず、実際の URL から HTML/text を取得
  - フッター・ヘッダから「会社概要」「運営会社」「About」リンクを探す
  - 運営関連キーワード近傍の snippet を抽出して pb.Evidence を生成

使い方:
    collector = EvidenceCollector()
    evidences = await collector.collect(
        brand="エニタイムフィットネス",
        official_url="https://www.anytimefitness.co.jp/shinjuku6/",
    )
    # => [Evidence(source_url=..., snippet="運営会社: 株式会社...", reason="operator_clue")]

設計原則:
  1. fetch 層は差し替え可能 (httpx or browser-use Agent for JS-heavy sites)
  2. 抽出は決定的 — LLM に推論させない
  3. 見つからなければ **空リストを返す** (推測で空文字 evidence を作らない)
"""

from __future__ import annotations

import asyncio
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit

import httpx
from bs4 import BeautifulSoup


# ─── Data classes ──────────────────────────────────────────────────────


@dataclass
class Evidence:
    """1 件の証拠 = URL + テキスト断片 + 抽出ルール名。"""

    source_url: str
    snippet: str
    reason: str  # 'operator_keyword' | 'direct_keyword' | 'about_page' | 'metadata'
    keyword: str = ""  # 一致したキーワード (operator_keyword 時)


# ─── Fetcher protocol ─────────────────────────────────────────────────


class Fetcher(Protocol):
    """URL から HTML を取得する抽象。"""

    async def fetch(self, url: str, *, timeout: float = 20.0) -> str: ...


class HttpxFetcher:
    """httpx による軽量な HTML fetch。JS 実行なし。"""

    def __init__(self, user_agent: str | None = None) -> None:
        self.user_agent = user_agent or (
            "Mozilla/5.0 (compatible; PI-ZZA-Collector/1.0; "
            "+https://github.com/clearclown/pizza)"
        )

    async def fetch(self, url: str, *, timeout: float = 20.0) -> str:
        async with httpx.AsyncClient(
            headers={"User-Agent": self.user_agent},
            follow_redirects=True,
            timeout=timeout,
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.text


# ─── Keyword libraries ─────────────────────────────────────────────────


# 運営会社を示唆するキーワード (日本語 + 英語)
OPERATOR_KEYWORDS = [
    "運営会社",
    "運営法人",
    "運営元",
    "運営:",
    "運営：",
    "加盟店",
    "フランチャイジー",
    "株式会社",
    "Operated by",
    "Franchisee",
    "Operator:",
]

# 直営を示唆するキーワード
DIRECT_KEYWORDS = [
    "直営店",
    "自社運営",
    "全店直営",
    "子会社が運営",
    "Directly operated",
    "全店舗直営",
]

# 会社概要ページへのリンクに付くテキスト
ABOUT_LINK_TEXTS = [
    "会社概要",
    "企業情報",
    "会社情報",
    "運営会社",
    "運営元",
    "About",
    "About us",
    "Corporate",
    "Company",
]

# 日本語法人名 — 助詞 (が,を,は,の,で,と,も) で body を切る
# body に含めてよい文字: ASCII letters/digits, hiragana ぁ-ゖ, katakana ァ-ヿ, kanji 一-龠,
#   空白, 中黒・ ー, & - etc.
# Phase 9: Unicode dash 類 (U+2010..U+2015, U+2212) を追加して
# 「株式会社セブン‐イレブン・ジャパン」等を取りこぼさない。
_COMPANY_BODY_CHARS = r"A-Za-z0-9ぁ-ゖァ-ヿ一-龠々ー・\s&\-‐-―−"
_COMPANY_BODY_STOP = r"、。,.がをはのでとも"

# body を 1 文字ずつ、"株式会社"/"㈱"/"(株)" を含まない atom として定義する。
# これで body は次の「株式会社」の直前で必ず止まり、複数社の連結誤抽出を防ぐ。
_COMPANY_BODY_ATOM = rf"(?:(?!株式会社|㈱|\(株\))[{_COMPANY_BODY_CHARS}])"

# prefix ("株式会社", "㈱", "(株)") + body
_COMPANY_RE_PREFIX = re.compile(
    rf"(株式会社|㈱|\(株\))({_COMPANY_BODY_ATOM}{{1,40}})",
    re.MULTILINE,
)
# body + suffix ("株式会社")
_COMPANY_RE_SUFFIX = re.compile(
    rf"({_COMPANY_BODY_ATOM}{{1,40}})(株式会社)",
    re.MULTILINE,
)


def _trim_at_particles(body: str) -> str:
    """body の先頭から、助詞・句読点・英語 boilerplate・会社概要ラベルの直前までを返す。"""
    body = body.rstrip()
    # Japanese particles
    jp_stops = ["、", "。", "の", "が", "を", "は", "で", "と", "も"]
    # 会社概要ラベル (所在地、住所、電話など後続の情報が混ざるのを防ぐ)
    jp_labels = [
        "所在地", "住所", "電話", "電話番号", "FAX", "Fax", "ファックス",
        "〒", "Tel", "TEL", "Phone",
        "設立", "代表", "代表者", "代表取締役", "事業内容", "本社", "本店",
        "資本金", "従業員", "創業", "公式サイト", "URL", "ウェブサイト",
        "業種", "営業時間", "定休日",
    ]
    # English boilerplate in footer/copyright
    en_stops = [
        " All", " all",
        "©", " ©", "(c)", "(C)",
        " Copyright", " copyright",
        " Rights", " rights",
        " Inc", " Inc.",
        " Ltd", " Ltd.",
        " Co.", " Co ",
    ]
    # Verb/preposition suffix noise — 法人名の直後に来がちな日本語 verbe-ish
    # 表現。例: "株式会社について紹介します" / "株式会社フーズに関する"
    verb_stops = [
        "について",
        "に関する",
        "に関し",
        "に対する",
        "に対し",
        "によって",
        "による",
        "として",
        "への",
        "からの",
    ]
    # Phase 11: 広告/案内文の吸収を防ぐ。法人名直後に来がちな商業文言。
    # 例: "株式会社アルペンクイックフィットネス キャンペーン期間2026年4月1日"
    ad_stops = [
        "キャンペーン",
        "期間",
        "特典",
        "限定",
        "お得",
        "お知らせ",
        "セール",
        "イベント",
        "新着",
        "最新",
        "情報",
        "募集",
        "求人",
    ]
    for p in jp_stops + jp_labels + en_stops + verb_stops + ad_stops:
        i = body.find(p)
        if i >= 0:
            body = body[:i]
    # 年月日表記が body に混入 ("2026年5月1日オープン") した場合の trim
    date_re = re.compile(r"(20\d{2}|令和\d+|平成\d+)")
    m = date_re.search(body)
    if m:
        body = body[: m.start()]
    return body.rstrip()


# ─── Collector ────────────────────────────────────────────────────────


@dataclass
class EvidenceCollector:
    """公式 URL から始めて、evidence を集める。"""

    fetcher: Fetcher = field(default_factory=HttpxFetcher)
    max_pages: int = 3  # 最大訪問ページ数 (root + about links)
    snippet_context_chars: int = 200  # キーワード前後の文字数

    async def collect(
        self,
        *,
        brand: str,
        official_url: str,
        extra_urls: list[str] | None = None,
    ) -> list[Evidence]:
        """evidence を収集する。

        Strategy:
          1. official_url を fetch
          2. そのページから about-like リンクを抽出
          3. ルートページ + about ページ (最大 max_pages) を走査
          4. 各ページから運営・直営キーワード近傍の snippet を抽出
          5. Evidence[] を重複除去して返す
        """
        visited: set[str] = set()
        out: list[Evidence] = []
        urls_to_visit: list[str] = []
        if official_url:
            urls_to_visit.append(official_url)
        for u in extra_urls or []:
            if u and u not in urls_to_visit:
                urls_to_visit.append(u)

        # Step 1: root fetch + about link discovery
        root_html: str | None = None
        if official_url:
            try:
                root_html = await self.fetcher.fetch(official_url)
                visited.add(official_url)
            except Exception as exc:  # noqa: BLE001
                return []  # ルート fetch できなければ evidence なし (安全側)
            about_links = self._find_about_links(root_html, official_url)
            for link in about_links:
                if link not in urls_to_visit:
                    urls_to_visit.append(link)

        # Step 2: root page 自体から evidence 抽出
        if root_html and official_url:
            out.extend(self._extract_evidence(root_html, official_url))

        # Step 3: about / extra ページ訪問
        remaining = self.max_pages - 1 if root_html else self.max_pages
        for url in urls_to_visit:
            if remaining <= 0:
                break
            if url in visited:
                continue
            visited.add(url)
            try:
                html = await self.fetcher.fetch(url)
            except Exception:  # noqa: BLE001
                continue
            out.extend(self._extract_evidence(html, url))
            remaining -= 1

        # Step 4: dedupe (source_url + snippet 同一は排除)
        return _dedupe_evidence(out)

    def _find_about_links(self, html: str, base_url: str) -> list[str]:
        """HTML から会社概要ページへのリンク URL を抽出する。"""
        soup = BeautifulSoup(html, "lxml")
        out: list[str] = []
        for a in soup.find_all("a", href=True):
            text = (a.get_text() or "").strip()
            if not text:
                continue
            for kw in ABOUT_LINK_TEXTS:
                if kw in text:
                    href = a["href"]
                    abs_url = urljoin(base_url, href)
                    # 同一ホストに限定
                    if _same_host(abs_url, base_url):
                        if abs_url not in out:
                            out.append(abs_url)
                    break
        return out

    def _extract_evidence(self, html: str, source_url: str) -> list[Evidence]:
        """HTML から運営・直営キーワード近傍の snippet を evidence として抽出。"""
        # テキスト本文を取得 (script/style を除去)
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
        text = re.sub(r"\n{3,}", "\n\n", text)  # 過剰改行を圧縮

        out: list[Evidence] = []

        # 運営キーワード周辺
        for kw in OPERATOR_KEYWORDS:
            for m in re.finditer(re.escape(kw), text):
                snippet = _extract_snippet(text, m.start(), self.snippet_context_chars)
                out.append(
                    Evidence(
                        source_url=source_url,
                        snippet=snippet,
                        reason="operator_keyword",
                        keyword=kw,
                    )
                )

        # 直営キーワード周辺
        for kw in DIRECT_KEYWORDS:
            for m in re.finditer(re.escape(kw), text):
                snippet = _extract_snippet(text, m.start(), self.snippet_context_chars)
                out.append(
                    Evidence(
                        source_url=source_url,
                        snippet=snippet,
                        reason="direct_keyword",
                        keyword=kw,
                    )
                )

        # meta description / copyright も取得 (補助 evidence)
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            out.append(
                Evidence(
                    source_url=source_url,
                    snippet=str(meta_desc.get("content", ""))[:300],
                    reason="metadata",
                    keyword="description",
                )
            )

        return out


# ─── Helpers ──────────────────────────────────────────────────────────


def _same_host(a: str, b: str) -> bool:
    try:
        ha = urlparse(a).hostname or ""
        hb = urlparse(b).hostname or ""
        return ha == hb and ha != ""
    except Exception:  # noqa: BLE001
        return False


def _extract_snippet(text: str, pos: int, context: int) -> str:
    """text の pos を中心に ±context 文字を切り出し、改行・空白を整形。"""
    start = max(0, pos - context)
    end = min(len(text), pos + context)
    raw = text[start:end]
    # 改行連続を 1 つに
    cleaned = re.sub(r"\s+", " ", raw).strip()
    return cleaned


def _dedupe_evidence(items: list[Evidence]) -> list[Evidence]:
    # reason も key に含める: 同じ snippet でも operator_keyword と direct_keyword は
    # 別の evidence として扱う (後段の判定で両方必要になる)
    seen: set[tuple[str, str, str]] = set()
    out: list[Evidence] = []
    for e in items:
        key = (e.source_url, e.snippet[:100], e.reason)
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


# ─── Deterministic helpers (no LLM needed) ────────────────────────────


def find_company_names_in_snippet(snippet: str) -> list[str]:
    """snippet から法人名 (株式会社..., ..株式会社, (株).., ㈱..) を正規表現で抽出。

    Phase 9:
      - 入力を NFKC 正規化 (全角記号 → 半角、全角マイナス → ASCII hyphen 等)
      - body に「株式会社」を含めない atom で複数社連結誤抽出を防止
      - body charset に Unicode dash U+2010..U+2015 / U+2212 を追加

    助詞 (が/を/は/の/で/と/も) や句読点で body を切る。
    """
    # NFKC 正規化で全角→半角の差異を吸収
    snippet = unicodedata.normalize("NFKC", snippet)
    out: list[str] = []

    # prefix pattern: "株式会社" + body
    for m in _COMPANY_RE_PREFIX.finditer(snippet):
        prefix = m.group(1)
        body = _trim_at_particles(m.group(2))
        if not body:
            continue
        name = prefix + body
        if name not in out:
            out.append(name)

    # suffix pattern: body + "株式会社"
    for m in _COMPANY_RE_SUFFIX.finditer(snippet):
        body_raw = m.group(1)
        suffix = m.group(2)
        # body が助詞列や空で始まる場合は skip 気味
        body = body_raw.strip()
        # body の末尾トリム (「本店: 株式会社A 支店: 」のような挟み込みを避けるため)
        # 単純に直近の助詞/句読点/label より後ろを取る。
        # Phase 7: 会社概要ページの HTML ラベルも含める。
        separators = [
            "、", "。",
            "   ", "  ",  # 連続半角スペース
            "　",          # 全角スペース
            "\n", "：", ":",
            # HTML ラベル (会社概要ページでよく見る)
            "名称", "会社名", "社名", "商号", "事業者名", "法人名",
            "会社概要", "企業情報", "会社案内", "会社情報", "Home", "HOME",
        ]
        for sep in separators:
            i = body.rfind(sep)
            if i >= 0:
                body = body[i + len(sep):].strip()
        if not body or body in ("の", "が", "を", "は", "で", "と"):
            continue
        name = body + suffix
        if name not in out:
            out.append(name)

    return out


def detect_direct_operation_from_snippet(snippet: str) -> bool:
    """snippet に直営を示すキーワードがあるか。"""
    return any(kw in snippet for kw in DIRECT_KEYWORDS)
