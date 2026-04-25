"""公式 HP の 会社概要 / IR ページから代表者 / 住所 / 売上を抽出 (Phase 25)。

処理:
  1. 起点 URL (JFA 由来の HP URL 等) を Scrapling で fetch
  2. /company/ /corporate/ /about/ /profile/ /ir/ 等の candidate link を発見
  3. 各 page を fetch (最大 max_pages 枚) し、正規表現抽出
  4. 合流した結果を OfficialSiteData にまとめて返す

決定論抽出のみ。
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urljoin

from pizza_delivery.sources.revenue_extractor import (
    RevenueFinding,
    extract_revenue_from_html,
)
from pizza_delivery.sources.fc_recruitment import (
    FCRecruitmentFinding,
    extract_fc_recruitment_url,
)

logger = logging.getLogger(__name__)


@dataclass
class OfficialSiteData:
    """公式 HP 一式から抽出したデータ。"""

    website_url: str = ""
    company_name: str = ""  # 会社概要ページで見つけた『株式会社XXX』
    representative_name: str = ""
    representative_title: str = ""
    headquarters_address: str = ""
    headquarters_postal: str = ""
    fc_store_count: int = 0  # 『全国○店』表記
    revenue: RevenueFinding = field(default_factory=RevenueFinding)
    fc_recruitment: FCRecruitmentFinding = field(default_factory=FCRecruitmentFinding)
    visited_urls: list[str] = field(default_factory=list)


# ─── 抽出 regex ────────────────────────────────────────────────

# 「代表取締役社長  山田 太郎」 型 (社長 の前に役職、後ろに氏名)
# 氏名は 2〜8 chars (姓 + 名)、半角/全角空白 OK、役職や住所キーワードで始まらない。
_RE_REPRESENTATIVE = re.compile(
    r"(代表取締役(?:社長|会長|CEO)?|取締役社長|代表執行役(?:社長)?|社長)"
    r"[:：\s　]+"
    r"([一-龥ぁ-んァ-ヶ][一-龥ぁ-んァ-ヶ・\s　]{1,10})"
)
_RE_CEO_NAME_ONLY = re.compile(
    r"代表者[:：\s　]+"
    r"([一-龥ぁ-んァ-ヶ][一-龥ぁ-んァ-ヶ・\s　]{1,10})"
)

# 役職/住所ラベル語: これらで始まる / 含むなら 人名ではなく後続ラベル混入とみなす
_NAME_REJECT_PREFIXES = (
    "代表", "取締", "社長", "会長", "本社", "所在", "住所", "資本",
    "設立", "創業", "業種", "事業",
)

# 氏名の後ろに付く役職語: 氏名抽出時にこれらで切る
_NAME_TRAILING_TITLES = (
    "会長", "社長", "専務", "常務", "取締役", "理事長",
    "CEO", "COO", "CFO",
)

# 本社住所 「東京都港区南青山3-1-30」 等
_RE_ADDRESS = re.compile(
    r"(?:本社[\s　]*所在地|所在地|住所|本社)[:：\s　]*"
    r"(?:〒?(\d{3}[-‐]?\d{4})[\s　]*)?"
    r"((?:北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
    r"[^\s<>,。]{3,60}\d[^\s<>,。]{0,40})"
)

# 「全国 1,234 店」 型
_RE_STORE_COUNT = re.compile(
    r"(?:全国|日本全国|国内)[\s　]*"
    r"([0-9][0-9,]*)"
    r"[\s　]*(?:店舗|店|拠点)"
)

# 「会社名: 株式会社XXX」「商号: 株式会社XXX」
_RE_COMPANY_NAME_LABELED = re.compile(
    r"(?:会社名|商号|社\s*名|会社)[:：\s　]+"
    r"((?:株式会社|有限会社|合同会社)[一-龥ぁ-んァ-ヶA-Za-z0-9・\-ー]{2,30})"
)
# 「株式会社XXX」「XXX株式会社」単発 (ブロックリスト適用)
_RE_COMPANY_NAME_BARE = re.compile(
    r"((?:株式会社|有限会社|合同会社)[一-龥ぁ-んァ-ヶA-Za-z0-9・\-ー]{2,25})"
)


_COMPANY_PATH_KEYWORDS = (
    "/company/", "/corporate/", "/about/", "/profile/",
    "/ir/", "/ir-information/", "/overview/", "/outline/",
    "/philosophy/", "/greeting/", "/message/",
)
_COMPANY_ANCHOR_KEYWORDS = (
    "会社概要", "企業情報", "会社案内", "会社情報",
    "コーポレート", "IR情報", "IR 情報", "企業情報",
    "代表挨拶", "会社沿革",
)


def _find_company_links(html: str, base_url: str) -> list[str]:
    """top page から 会社概要 / IR 系の link を列挙する。"""
    link_re = re.compile(
        r'<a\s+[^>]*href="([^"]+)"[^>]*>([^<]+)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    out: list[str] = []
    seen: set[str] = set()
    for m in link_re.finditer(html):
        href = m.group(1).strip()
        anchor = re.sub(r"\s+", " ", m.group(2).strip())
        if not href or href.startswith("#") or href.startswith("mailto:"):
            continue
        href_l = href.lower()
        match = (
            any(k in href_l for k in _COMPANY_PATH_KEYWORDS)
            or any(k in anchor for k in _COMPANY_ANCHOR_KEYWORDS)
        )
        if not match:
            continue
        abs_url = urljoin(base_url, href)
        if abs_url in seen or abs_url == base_url:
            continue
        seen.add(abs_url)
        out.append(abs_url)
    return out


def _clean_name_candidate(raw: str) -> str:
    """候補文字列を人名 (姓 + 名) に絞り込む。ラベル混入時は空文字を返す。"""
    s = re.sub(r"[\s　]+", " ", raw).strip()
    s = re.sub(r"[（(].*?[)）]$", "", s).strip()
    # 続く「本社所在地」等のラベルで切る
    for prefix in _NAME_REJECT_PREFIXES:
        idx = s.find(prefix)
        if idx > 0:
            s = s[:idx].strip()
            break
    # 氏名の後ろに来る役職語 (専務/常務/取締役等) で切る
    for title in _NAME_TRAILING_TITLES:
        idx = s.find(title)
        if idx > 0:
            s = s[:idx].strip()
            break
    # 「兼」で切る (「兼CEO」等)
    if "兼" in s:
        s = s.split("兼", 1)[0].strip()
    # 都道府県・市で切る (住所混入時)
    addr_pat = re.compile(
        r"(北海道|東京都|大阪府|京都府|.{2,3}県|.{2,3}市|.{2,3}区)"
    )
    m_addr = addr_pat.search(s)
    if m_addr and m_addr.start() > 0:
        s = s[: m_addr.start()].strip()
    # 日本人名は通常 姓 + 名 の 2 パート、3 パート以上なら余分を落とす
    parts = s.split()
    if len(parts) > 2:
        s = " ".join(parts[:2])
        parts = s.split()
    # 名前の妥当な長さ: 2-8 chars (姓+名、空白 1 個含む)
    if len(s) < 2 or len(s) > 8:
        return ""
    if any(s.startswith(p) for p in _NAME_REJECT_PREFIXES):
        return ""
    # 「兼 グル」「田 社」 のような 1 文字カタカナ/英字ゴミは除外
    # ただし「増本 岳」「田中 誠」のような 1 漢字名は有効
    if len(parts) >= 2:
        tail = parts[1]
        # 2 語目が 1 文字で、カタカナ/英字なら garbage
        is_kata = bool(tail) and all("ァ" <= c <= "ヿ" for c in tail)
        is_en = bool(tail) and all(c.isascii() and c.isalpha() for c in tail)
        if len(tail) == 1 and (is_kata or is_en):
            first = parts[0]
            return first if len(first) >= 2 else ""
    return s


def _extract_representative(text: str) -> tuple[str, str]:
    """(title, name) を返す。hit 無しなら ("", "")。"""
    for m in _RE_REPRESENTATIVE.finditer(text):
        name = _clean_name_candidate(m.group(2))
        if name:
            return m.group(1).strip(), name
    for m in _RE_CEO_NAME_ONLY.finditer(text):
        name = _clean_name_candidate(m.group(1))
        if name:
            return "代表者", name
    return "", ""


def _extract_address(text: str) -> tuple[str, str]:
    """(postal_code, address) を返す。hit 無しなら ("", "")。"""
    m = _RE_ADDRESS.search(text)
    if not m:
        return "", ""
    postal = m.group(1) or ""
    addr = re.sub(r"[\s　]+", "", m.group(2)).strip()
    addr = addr.rstrip("。、,.").strip()
    return postal, addr[:120]


def _extract_company_name(text: str) -> str:
    """会社概要ページから『株式会社XXX』の運営会社名を抽出。"""
    m = _RE_COMPANY_NAME_LABELED.search(text)
    if m:
        return m.group(1).strip()[:80]
    # ラベル無し fallback: 最も頻出する「株式会社XXX」を採用
    counts: dict[str, int] = {}
    for m2 in _RE_COMPANY_NAME_BARE.finditer(text):
        name = m2.group(1).strip()
        if len(name) >= 5:
            counts[name] = counts.get(name, 0) + 1
    if not counts:
        return ""
    best = max(counts.items(), key=lambda kv: (kv[1], -len(kv[0])))
    # 1 回だけなら無効 (ノイズ可能性)
    if best[1] < 2:
        return ""
    return best[0][:80]


def _extract_store_count(text: str) -> int:
    m = _RE_STORE_COUNT.search(text)
    if not m:
        return 0
    s = m.group(1).replace(",", "")
    try:
        n = int(s)
    except ValueError:
        return 0
    # 妥当性: 1 〜 50,000 (フランチャイズ最大規模でもこの range 内)
    if 1 <= n <= 50000:
        return n
    return 0


# ─── Orchestrator ──────────────────────────────────────────────


async def fetch_official_site(
    website_url: str,
    *,
    fetcher_static: Callable[[str], Any] | None = None,
    fetcher_dynamic: Callable[[str], Any] | None = None,
    max_pages: int = 4,
) -> OfficialSiteData:
    """公式 HP top + 会社概要/IR 複数ページを走査し OfficialSiteData を返す。

    fetcher_static / fetcher_dynamic は Scrapling の `fetch_static` /
    `fetch_dynamic` を受け取る (DI でテストしやすく)。
    デフォルトは ScraplingFetcher を使う (sync → asyncio.to_thread 越し)。
    """
    if not website_url:
        return OfficialSiteData()

    if fetcher_static is None or fetcher_dynamic is None:
        from pizza_delivery.scrapling_fetcher import ScraplingFetcher

        sf = ScraplingFetcher()
        if fetcher_static is None:
            fetcher_static = sf.fetch_static
        if fetcher_dynamic is None:
            fetcher_dynamic = sf.fetch_dynamic

    import asyncio

    data = OfficialSiteData(website_url=website_url)

    async def _fetch(url: str) -> str:
        # Scrapling は sync なので to_thread 化
        try:
            html = await asyncio.to_thread(fetcher_static, url)
            if html and len(html) >= 2000:
                return html
            # 静的が薄すぎる → dynamic
            html2 = await asyncio.to_thread(fetcher_dynamic, url)
            return html2 or html or ""
        except Exception as e:
            logger.debug("fetch failed: %s %s", url, e)
            return ""

    top_html = await _fetch(website_url)
    if top_html:
        data.visited_urls.append(website_url)
        # top page で fc 募集 LP を探索 (nav menu にあることが多い)
        rec = extract_fc_recruitment_url(top_html, base_url=website_url)
        if not rec.empty:
            data.fc_recruitment = rec
        # 『全国○店』型 は top で拾える
        c = _extract_store_count(top_html)
        if c:
            data.fc_store_count = c

    # 会社概要 / IR candidate links を fetch
    candidate_links = _find_company_links(top_html, website_url)[:max_pages]
    htmls: list[str] = [top_html] if top_html else []
    for link in candidate_links:
        h = await _fetch(link)
        if h:
            data.visited_urls.append(link)
            htmls.append(h)

    # 統合: 各 page から representative / address / revenue を探す
    # 最初に見つかった妥当な値を採用 (会社概要ページに書いてある方が信頼性高)
    joined_text_for_meta = ""
    for h in htmls:
        # HTML から text を抽出 (BeautifulSoup で text 化)
        try:
            from bs4 import BeautifulSoup

            joined_text_for_meta += (
                BeautifulSoup(h, "lxml").get_text(" ", strip=True) + " "
            )
        except Exception:
            joined_text_for_meta += re.sub(r"<[^>]+>", " ", h) + " "

    if joined_text_for_meta:
        title, name = _extract_representative(joined_text_for_meta)
        if name:
            data.representative_name = name
            data.representative_title = title
        postal, addr = _extract_address(joined_text_for_meta)
        if addr:
            data.headquarters_address = addr
            data.headquarters_postal = postal
        if not data.fc_store_count:
            c = _extract_store_count(joined_text_for_meta)
            if c:
                data.fc_store_count = c
        company = _extract_company_name(joined_text_for_meta)
        if company:
            data.company_name = company

    # 売上は元の HTML (年度表記の regex が HTML 構造を頼る) で抽出
    rev = RevenueFinding()
    for h in htmls:
        r = extract_revenue_from_html(h, source_url=website_url)
        if not r.empty:
            rev = r
            break
    data.revenue = rev

    return data
