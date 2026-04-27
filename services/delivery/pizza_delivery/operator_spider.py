"""OperatorSpider (Phase 14) — 事業会社公式 URL から店舗一覧 を決定論的に抽出。

目的: 「1 operator 判明 → その公式サイトから運営店舗全件を発見」。
Google Places API を大量に叩く代わりに、operator 公式を 1-3 回 fetch する
だけで多数の StoreCandidate を取得できる (top-down 主導探索の核)。

処理:
  1. operator の official_url を fetch
  2. HTML から「店舗一覧」系の link を検出 (あれば追従)
  3. 店舗一覧ページ (or entry page) から (店舗名, 住所) を正規表現で抽出
  4. StoreCandidate のリストを返す (call 側が Places API で place_id 逆引き)

LLM 不使用。正規表現 + BeautifulSoup (必要なら lxml) で決定論。
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urljoin


# ─── 住所パターン (日本住所の決定論 regex) ────────────────────────


_PREFS = (
    "北海道",
    "青森県|岩手県|宮城県|秋田県|山形県|福島県",
    "茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県",
    "新潟県|富山県|石川県|福井県|山梨県|長野県",
    "岐阜県|静岡県|愛知県|三重県",
    "滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県",
    "鳥取県|島根県|岡山県|広島県|山口県",
    "徳島県|香川県|愛媛県|高知県",
    "福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県",
)
_PREF_UNION = "|".join(_PREFS)

# 「都道府県 + 市区町村 + 町名 or 番地」
_ADDRESS_RE = re.compile(
    rf"(?:〒?\d{{3}}-?\d{{4}}\s*)?"
    rf"((?:{_PREF_UNION})[^\s<>,。]{{2,80}}\d[^\s<>,。]{{0,40}})"
)


_STORE_LINK_HINTS = (
    "店舗",
    "店舗一覧",
    "店舗検索",
    "ショップ",
    "SHOPS",
    "STORES",
    "location",
    "Location",
    "Locations",
    "Store",
    "shop",
)


# ─── Data types ────────────────────────────────────────────────────────


@dataclass
class StoreCandidate:
    """operator の公式サイトから発見した店舗候補。

    place_id は後段 (Places API 住所逆引き) で解決される想定で、
    spider が返す時点では空文字。
    """

    operator_name: str
    name: str           # 店舗名 (見出しやリスト項目テキスト)
    address: str        # 検出した住所
    source_url: str = ""  # どのページで見つけたか
    place_id: str = ""
    lat: float = 0.0
    lng: float = 0.0


# ─── HTML → candidate extraction ──────────────────────────────────────


def _strip_tags(html: str) -> str:
    # BeautifulSoup があればそれを使う、無ければ regex で <tag> 除去
    try:
        from bs4 import BeautifulSoup  # type: ignore

        return BeautifulSoup(html, "html.parser").get_text(" ")
    except ImportError:
        return re.sub(r"<[^>]+>", " ", html)


def _extract_headings(html: str) -> list[str]:
    """h2/h3/li の直下テキストなどを店舗名候補として抽出。"""
    patterns = [
        re.compile(r"<h[1-6][^>]*>([^<]+)</h[1-6]>", re.IGNORECASE),
        re.compile(r"<li[^>]*>\s*<[^>]+>([^<]+)<", re.IGNORECASE),
    ]
    out: list[str] = []
    for pat in patterns:
        for m in pat.finditer(html):
            text = m.group(1).strip()
            if text and len(text) < 100:
                out.append(text)
    return out


def extract_store_candidates_from_html(html: str) -> list[StoreCandidate]:
    """HTML から StoreCandidate を抽出する pure 関数。

    - 住所 regex で日本住所を検出
    - 住所の前後 100 字から店舗名候補 (heading) を紐付ける
    - LLM は使わず、pattern matching のみ
    """
    if not html or len(html.strip()) == 0:
        return []

    text = _strip_tags(html)
    # 住所を全て検出
    matches = list(_ADDRESS_RE.finditer(text))
    if not matches:
        return []

    headings = _extract_headings(html)
    out: list[StoreCandidate] = []
    seen_addresses: set[str] = set()
    for m in matches:
        addr = m.group(1).strip().rstrip("。、,.")
        if addr in seen_addresses:
            continue
        seen_addresses.add(addr)
        # 近傍 (-200 +0 chars) の heading を store name として採用
        start = max(0, m.start() - 200)
        snippet = text[start : m.start()]
        name = ""
        # snippet 内で見出しっぽいものがあれば採用
        for h in headings:
            if h in snippet:
                name = h
        if not name:
            name = "(名称不明)"
        out.append(
            StoreCandidate(operator_name="", name=name, address=addr)
        )
    return out


# ─── Fetcher Protocol ────────────────────────────────────────────────


class Fetcher(Protocol):
    async def fetch(self, url: str, *, timeout: float = 20.0) -> str: ...


@dataclass
class ScraplingOperatorFetcher:
    """OperatorSpider 用 Scrapling adapter。

    `ScraplingFetcher` は同期 API (`fetch_auto`) なので、OperatorSpider の
    async fetcher protocol に合わせて thread 経由で実行する。
    """

    def fetch_auto(self, url: str, *, timeout: float = 20.0) -> str:
        from pizza_delivery.scrapling_fetcher import ScraplingFetcher

        fetcher = ScraplingFetcher(
            timeout_static_sec=timeout,
            timeout_dynamic_sec=max(timeout, 8.0),
        )
        return fetcher.fetch_auto(url) or ""

    async def fetch(self, url: str, *, timeout: float = 20.0) -> str:
        html = await asyncio.to_thread(self.fetch_auto, url, timeout=timeout)
        if not html:
            raise RuntimeError(f"empty response: {url}")
        return html


# ─── Spider ───────────────────────────────────────────────────────────


@dataclass
class OperatorSpider:
    """operator の official_url を起点に店舗一覧を discover する。

    fetcher 未指定時は Phase 26 で default にした ScraplingEvidenceFetcher を
    遅延 import で組み立てる (SPA 対応 / StealthyFetcher 自動 fallback)。
    """

    fetcher: Fetcher | None = None
    max_follow_links: int = 3  # /stores や /locations など店舗一覧 link を何個辿るか
    timeout: float = 20.0

    def __post_init__(self) -> None:
        if self.fetcher is None:
            self.fetcher = ScraplingOperatorFetcher()

    async def discover(
        self, *, operator_name: str, official_url: str
    ) -> list[StoreCandidate]:
        """公式 URL から店舗候補を取得する。

        処理:
          1. official_url を fetch
          2. 店舗一覧 link を検出 (_find_store_list_links)
          3. 追従可能な link を fetch (max_follow_links 件まで)
          4. 各 HTML で住所抽出
          5. 重複除去 + operator_name 紐付けして返す
        """
        html_pages: list[tuple[str, str]] = []

        # Step 1: entry page
        try:
            entry_html = await self.fetcher.fetch(official_url, timeout=self.timeout)
            html_pages.append((official_url, entry_html))
        except Exception:
            return []

        # Step 2: 店舗一覧 link 検出
        links = _find_store_list_links(official_url, entry_html)[: self.max_follow_links]
        for link in links:
            try:
                h = await self.fetcher.fetch(link, timeout=self.timeout)
                html_pages.append((link, h))
            except Exception:
                continue

        # Step 3: 各 HTML から candidate 抽出 + dedup
        all_candidates: list[StoreCandidate] = []
        seen_addrs: set[str] = set()
        for url, html in html_pages:
            for c in extract_store_candidates_from_html(html):
                if c.address in seen_addrs:
                    continue
                seen_addrs.add(c.address)
                c.operator_name = operator_name
                c.source_url = url
                all_candidates.append(c)
        return all_candidates


def _find_store_list_links(base_url: str, html: str) -> list[str]:
    """「店舗一覧」系の <a> link を検出する (相対 → 絶対 URL 変換)。"""
    out: list[str] = []
    seen: set[str] = set()
    # <a href="..." >店舗一覧</a> 形式を広く拾う
    link_re = re.compile(
        r'<a\s+[^>]*href="([^"]+)"[^>]*>([^<]+)</a>',
        re.IGNORECASE,
    )
    for m in link_re.finditer(html):
        href = m.group(1)
        anchor = m.group(2).strip()
        if not any(h in anchor for h in _STORE_LINK_HINTS):
            continue
        if not any(h in href.lower() for h in [
            "store", "shop", "location", "dealer", "find", "list"
        ]) and not any(h in anchor for h in _STORE_LINK_HINTS):
            # href にも anchor にも hint がない → skip
            continue
        abs_url = urljoin(base_url, href)
        if abs_url in seen or abs_url == base_url:
            continue
        seen.add(abs_url)
        out.append(abs_url)
    return out


# ─── Multi-brand discovery (Phase 15) ────────────────────────────────


@dataclass
class BrandCandidate:
    """operator 公式サイトから発見した、この operator が運営している可能性のある
    他ブランド名。"""

    brand_name: str
    source_url: str = ""
    anchor_text: str = ""
    href: str = ""


# 既知 FC ブランド名辞書。
# anchor text / 画像 alt に出てきたら「この operator が運営している可能性」と判断。
# 値は ORM に入れる canonical brand 名。14 対象ブランドは表記ゆれをここで吸収する。
_FC_BRAND_ALIAS_TO_CANONICAL: dict[str, str] = {
    # 14 対象ブランド
    "カーブス": "カーブス",
    "Curves": "カーブス",
    "モスバーガー": "モスバーガー",
    "MOS BURGER": "モスバーガー",
    "業務スーパー": "業務スーパー",
    "Itto個別指導学院": "Itto個別指導学院",
    "ITTO個別指導学院": "Itto個別指導学院",
    "ITTO": "Itto個別指導学院",
    "エニタイムフィットネス": "エニタイムフィットネス",
    "Anytime Fitness": "エニタイムフィットネス",
    "ANYTIME FITNESS": "エニタイムフィットネス",
    "コメダ珈琲店": "コメダ珈琲",
    "コメダ珈琲": "コメダ珈琲",
    "シャトレーゼ": "シャトレーゼ",
    "ハードオフ": "ハードオフ",
    "HARD OFF": "ハードオフ",
    "オフハウス": "オフハウス",
    "OFF HOUSE": "オフハウス",
    "Kids Duo": "Kids Duo",
    "KIDS DUO": "Kids Duo",
    "キッズデュオ": "Kids Duo",
    "アップガレージ": "アップガレージ",
    "UP GARAGE": "アップガレージ",
    "カルビ丼とスン豆腐専門店韓丼": "カルビ丼とスン豆腐専門店韓丼",
    "韓丼": "カルビ丼とスン豆腐専門店韓丼",
    "Brand off": "Brand off",
    "BRAND OFF": "Brand off",
    "ブランドオフ": "Brand off",
    "TSUTAYA": "TSUTAYA",
    # その他の大口 FC / 複数業態検出用
    "chocoZAP": "chocoZAP",
    "チョコザップ": "chocoZAP",
    "FIT PLACE24": "FIT PLACE24",
    "24GYM": "24GYM",
    "JOYFIT": "JOYFIT",
    "ファストジム24": "ファストジム24",
    "ゴールドジム": "ゴールドジム",
    "CLUB PILATES": "CLUB PILATES",
    "CYCLEBAR": "CYCLEBAR",
    "ONEPERSON": "ONEPERSON",
    "セブン-イレブン": "セブン-イレブン",
    "セブンイレブン": "セブン-イレブン",
    "ファミリーマート": "ファミリーマート",
    "ローソン": "ローソン",
    "ミニストップ": "ミニストップ",
    "デイリーヤマザキ": "デイリーヤマザキ",
    "マクドナルド": "マクドナルド",
    "KFC": "ケンタッキー",
    "ケンタッキー": "ケンタッキー",
    "ドトール": "ドトール",
    "スターバックス": "スターバックス",
    "タリーズ": "タリーズ",
    "すき家": "すき家",
    "吉野家": "吉野家",
    "ガスト": "ガスト",
    "サイゼリヤ": "サイゼリヤ",
    "ブックオフ": "ブックオフ",
    "BOOKOFF": "ブックオフ",
    "ゲオ": "ゲオ",
    "GEO": "ゲオ",
    "セカンドストリート": "セカンドストリート",
    "2nd STREET": "セカンドストリート",
    "洋服の青山": "洋服の青山",
    "ゴンチャ": "ゴンチャ",
    "餃子の王将": "餃子の王将",
}

_KNOWN_FC_BRANDS: tuple[str, ...] = tuple(
    sorted(_FC_BRAND_ALIAS_TO_CANONICAL, key=len, reverse=True)
)


def canonical_fc_brand_name(name: str) -> str:
    """operator 公式サイト上の表記ゆれを ORM canonical brand 名へ寄せる。"""
    raw = (name or "").strip()
    return _FC_BRAND_ALIAS_TO_CANONICAL.get(raw, raw)


def _alias_in_anchor(alias: str, anchor_fold: str) -> bool:
    """英数字 alias は単語境界で見る。例: ITTO が KITTO に誤ヒットしないようにする。"""
    alias_fold = alias.casefold()
    if not re.search(r"[A-Za-z0-9]", alias):
        return alias_fold in anchor_fold
    return bool(
        re.search(
            rf"(?<![A-Za-z0-9]){re.escape(alias_fold)}(?![A-Za-z0-9])",
            anchor_fold,
            flags=re.IGNORECASE,
        )
    )


def extract_brand_candidates_from_html(
    html: str, *, base_url: str = ""
) -> list[BrandCandidate]:
    """HTML から、既知 FC ブランド名が anchor text に含まれる link を検出。

    この operator が当該ブランドを運営している可能性 (navigation menu の
    「事業一覧」「ブランド一覧」等に並ぶブランド名が典型)。
    """
    if not html:
        return []
    links: list[tuple[str, str]] = []
    try:
        from bs4 import BeautifulSoup  # type: ignore

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = str(a.get("href") or "")
            anchor = a.get_text(" ", strip=True)
            if not anchor:
                alt_parts: list[str] = []
                for img in a.find_all("img"):
                    alt = str(img.get("alt") or img.get("title") or "").strip()
                    if alt:
                        alt_parts.append(alt)
                anchor = " ".join(alt_parts)
            links.append((href, anchor))
    except Exception:
        link_re = re.compile(
            r'<a\s+[^>]*href="([^"]+)"[^>]*>([^<]+)</a>',
            re.IGNORECASE | re.DOTALL,
        )
        links = [(m.group(1), m.group(2)) for m in link_re.finditer(html)]

    out: list[BrandCandidate] = []
    seen: set[str] = set()
    for href, raw_anchor in links:
        anchor = re.sub(r"\s+", " ", raw_anchor.strip())
        if not anchor:
            continue
        anchor_fold = anchor.casefold()
        for brand in _KNOWN_FC_BRANDS:
            if _alias_in_anchor(brand, anchor_fold):
                canonical = canonical_fc_brand_name(brand)
                key = f"{canonical}|{href}"
                if key in seen:
                    continue
                seen.add(key)
                abs_url = urljoin(base_url, href) if base_url else href
                out.append(
                    BrandCandidate(
                        brand_name=canonical,
                        source_url=base_url,
                        anchor_text=anchor,
                        href=abs_url,
                    )
                )
    return out


async def discover_multi_brand(
    *,
    fetcher: Fetcher,
    official_url: str,
    timeout: float = 20.0,
) -> list[BrandCandidate]:
    """operator 公式サイトの entry を fetch し、他ブランド link を検出する。"""
    try:
        html = await fetcher.fetch(official_url, timeout=timeout)
    except Exception:
        return []
    return extract_brand_candidates_from_html(html, base_url=official_url)
