"""Scrapling による電話番号 → 営業主 (FC 会社名) 逆引き。

browser-use 経路が deprecated のため、Phase 26 で D4Vinci/Scrapling に全面移行。

探索経路 (順序):
  1. iタウンページ直叩き (https://itp.ne.jp/keyword/?keyword=<phone>)
  2. Google 検索 "<phone>" via StealthyFetcher (Camoufox で bot block 回避)

抽出は決定論 regex (brand_profiler の company name extractor 流用)。
LLM は evidence が取れた後に canonicalize / rerank だけに使う (ground truth 源でない)。

出力は browser_scraper.OperatorInfo 互換。
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

logger = logging.getLogger(__name__)


@dataclass
class PhoneLookupResult:
    operator_name: str = ""
    corporate_number: str = ""
    address: str = ""
    phone: str = ""
    source_url: str = ""
    confidence: float = 0.0
    reasoning: str = ""

    @property
    def empty(self) -> bool:
        return not self.operator_name


# 「株式会社 XXX」「XXX 株式会社」の抽出 (brand_profiler の _RE_COMPANY_NAME_BARE 類似)
_RE_COMPANY_BARE = re.compile(
    r"((?:株式会社|有限会社|合同会社)[一-龥ぁ-んァ-ヶA-Za-z0-9・\-ー]{2,25}"
    r"|"
    r"[一-龥ぁ-んァ-ヶA-Za-z0-9・\-ー]{2,25}(?:株式会社|有限会社|合同会社))"
)

# 住所抽出
_RE_ADDRESS = re.compile(
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


# 本部 / チェーン名ブロックリスト (営業主判定から除外)
_REJECT_NAMES_BASE = (
    # FC 本部 (precision 対策)
    "株式会社モスフードサービス", "日本マクドナルド株式会社", "株式会社壱番屋",
    "株式会社ダスキン", "株式会社大戸屋", "株式会社アレフ",
    # 電話インフラ系 (phone search で上位に出がち、営業主ではない)
    "東日本電信電話株式会社", "西日本電信電話株式会社", "日本電信電話株式会社",
    "KDDI株式会社", "ソフトバンク株式会社", "株式会社NTTドコモ",
    "楽天モバイル株式会社", "株式会社エヌ・ティ・ティ・ドコモ",
    # 検索 site / ポータル (phone 検索結果に出現)
    "株式会社リクルート", "株式会社ぐるなび", "株式会社カカクコム",
    "株式会社リクルートホールディングス", "ヤフー株式会社",
    "LINEヤフー株式会社", "株式会社Z Holdings",
    # 地図・広告系
    "株式会社ゼンリン", "株式会社マピオン",
    # iタウンページ / 電話帳系 (enrich で block page から誤抽出)
    "NTTタウンページ株式会社", "NTT タウンページ株式会社",
    "株式会社タウンページ",
    # 文字列断片由来の誤抽出 (snippet 切断で出来る虚構)
    "モスバーガーを展開する株式会社", "展開する株式会社",
)

# 文字列断片を示す「含まれたら reject」パターン (部分一致)
_REJECT_SUBSTRINGS = (
    "を展開する株式会社",  # 「XXX を展開する株式会社」は切断 fragment
    "タウンページ",        # NTT タウンページ広告
    "食べログ",            # 食べログ cookie notice
    "Retty", "レッティ",
    "ぐるなび",
)


def _extract_companies_from_html(html: str) -> list[str]:
    """HTML から 株式会社 XXX / XXX 株式会社 形式を全て抽出 (dedup + ordered)."""
    if not html:
        return []
    # HTML → text 化
    try:
        from bs4 import BeautifulSoup

        text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html)
    seen: set[str] = set()
    out: list[str] = []
    for m in _RE_COMPANY_BARE.finditer(text):
        name = m.group(1).strip()
        if not (5 <= len(name) <= 40) or name in seen:
            continue
        # 文字列断片 / 広告由来の reject substring チェック
        if any(sub in name for sub in _REJECT_SUBSTRINGS):
            continue
        seen.add(name)
        out.append(name)
    return out


def _extract_address(html: str) -> str:
    try:
        from bs4 import BeautifulSoup

        text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html)
    m = _RE_ADDRESS.search(text)
    return m.group(1).strip() if m else ""


async def lookup_phone_itownpage(
    phone: str,
    *,
    brand_hint: str = "",
    reject_headquarters: bool = True,
    fetcher: Any = None,
) -> PhoneLookupResult:
    """iタウンページ で電話番号 → 営業主検索。

    URL: https://itp.ne.jp/keyword/?keyword=<phone>
    iタウンページは bot block (403) が厳しいので StealthyFetcher (Camoufox) 必須。
    """
    if not phone:
        return PhoneLookupResult()
    url = f"https://itp.ne.jp/keyword/?keyword={quote(phone)}"
    if fetcher is None:
        # iタウンページは anti-bot が厳しい → StealthyFetcher (Camoufox) 経路
        try:
            from scrapling.fetchers import StealthyFetcher

            r = await asyncio.to_thread(StealthyFetcher.fetch, url)
            if r is None:
                html = ""
            else:
                body = r.body
                html = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else str(body or "")
        except ImportError:
            logger.debug("StealthyFetcher unavailable, fallback to DynamicFetcher")
            from pizza_delivery.scrapling_fetcher import ScraplingFetcher

            sf = ScraplingFetcher()
            html = await asyncio.to_thread(sf.fetch_dynamic, url)
        except Exception as e:
            logger.debug("itownpage stealth fetch failed for %s: %s", phone, e)
            html = ""
    else:
        html = await fetcher.fetch(url)
    if not html:
        return PhoneLookupResult(phone=phone, source_url=url)

    companies = _extract_companies_from_html(html)
    blocklist = set(_REJECT_NAMES_BASE)
    if reject_headquarters:
        # brand_hint 自身が本部名の場合も block
        if brand_hint:
            for n in companies:
                if brand_hint in n and any(
                    kw in n for kw in ("フード", "サービス", "コーポレーション", "ホールディングス")
                ):
                    blocklist.add(n)
    for name in companies:
        if name in blocklist:
            continue
        address = _extract_address(html)
        return PhoneLookupResult(
            operator_name=name,
            address=address,
            phone=phone,
            source_url=url,
            confidence=0.7,
            reasoning="iタウンページ",
        )
    return PhoneLookupResult(phone=phone, source_url=url)


async def lookup_phone_duckduckgo(
    phone: str,
    *,
    brand_hint: str = "",
    reject_headquarters: bool = True,
) -> PhoneLookupResult:
    """DuckDuckGo HTML endpoint で電話番号検索。

    Google 429 対策: https://html.duckduckgo.com/html/?q=<phone>
    anti-bot が緩いので Scrapling static Fetcher で OK。
    Instant Answer API (api.duckduckgo.com) は電話番号に弱いので HTML 経路優先。
    """
    if not phone:
        return PhoneLookupResult()
    # phone 単体検索だと 店舗 list しか出ないため「運営会社 株式会社」を足す
    query = f'"{phone}" 運営会社 OR 株式会社'
    if brand_hint:
        query += f" {brand_hint}"
    url = f"https://html.duckduckgo.com/html/?q={quote(query)}&kl=jp-jp"
    try:
        from pizza_delivery.scrapling_fetcher import ScraplingFetcher

        sf = ScraplingFetcher()
        html = await asyncio.to_thread(sf.fetch_static, url)
    except Exception as e:
        logger.debug("ddg fetch failed for %s: %s", phone, e)
        return PhoneLookupResult(phone=phone, source_url=url,
                                 reasoning=f"error: {e}")
    if not html:
        return PhoneLookupResult(phone=phone, source_url=url)

    companies = _extract_companies_from_html(html)
    blocklist = set(_REJECT_NAMES_BASE)
    if reject_headquarters and brand_hint:
        for n in companies:
            if brand_hint in n and any(
                kw in n for kw in ("フード", "サービス", "コーポレーション", "ホールディングス")
            ):
                blocklist.add(n)
    for name in companies:
        if name in blocklist:
            continue
        address = _extract_address(html)
        return PhoneLookupResult(
            operator_name=name,
            address=address,
            phone=phone,
            source_url=url,
            confidence=0.6,
            reasoning="duckduckgo_html",
        )
    return PhoneLookupResult(phone=phone, source_url=url)


async def lookup_phone_google_stealth(
    phone: str,
    *,
    brand_hint: str = "",
    reject_headquarters: bool = True,
) -> PhoneLookupResult:
    """Google 検索 "<phone>" を StealthyFetcher (Camoufox) で叩く。

    bot block 回避のため StealthyFetcher 必須。
    複数検索結果から最も confident な 株式会社名を抽出。
    """
    if not phone:
        return PhoneLookupResult()
    query = f'"{phone}"'
    if brand_hint:
        query += f" {brand_hint}"
    url = f"https://www.google.com/search?q={quote(query)}&hl=ja"
    try:
        from scrapling.fetchers import StealthyFetcher
    except ImportError:
        return PhoneLookupResult(phone=phone, source_url=url,
                                 reasoning="stealthy_unavailable")
    try:
        r = await asyncio.to_thread(StealthyFetcher.fetch, url)
        html = r.body.decode("utf-8", errors="replace") if isinstance(r.body, bytes) else str(r.body or "")
    except Exception as e:
        logger.debug("google stealth fetch failed for %s: %s", phone, e)
        return PhoneLookupResult(phone=phone, source_url=url, reasoning=f"error: {e}")

    if not html:
        return PhoneLookupResult(phone=phone, source_url=url)
    companies = _extract_companies_from_html(html)
    blocklist = set(_REJECT_NAMES_BASE)
    if reject_headquarters and brand_hint:
        for n in companies:
            if brand_hint in n and any(
                kw in n for kw in ("フード", "サービス", "コーポレーション", "ホールディングス")
            ):
                blocklist.add(n)
    for name in companies:
        if name in blocklist:
            continue
        return PhoneLookupResult(
            operator_name=name,
            phone=phone,
            source_url=url,
            confidence=0.5,
            reasoning="google_stealth",
        )
    return PhoneLookupResult(phone=phone, source_url=url)


async def lookup_operator_by_phone(
    phone: str,
    *,
    brand_hint: str = "",
    try_ddg: bool = True,
    try_google_fallback: bool = True,
) -> PhoneLookupResult:
    """3 段 fallback: iタウンページ → DuckDuckGo HTML → Google StealthyFetcher。

    - iタウンページ: 構造化データあり、最精度だが anti-bot 厳しい
    - DuckDuckGo: anti-bot 緩く、rate-limit も緩い (Google 429 代替)
    - Google: 最終手段、StealthyFetcher (Camoufox) でも 429 に注意
    """
    r = await lookup_phone_itownpage(phone, brand_hint=brand_hint)
    if not r.empty:
        return r
    if try_ddg:
        r2 = await lookup_phone_duckduckgo(phone, brand_hint=brand_hint)
        if not r2.empty:
            return r2
    if try_google_fallback:
        return await lookup_phone_google_stealth(phone, brand_hint=brand_hint)
    return r
