"""Scrapling ベースの高速 web fetcher。

browser-use が 1 店舗 30-60s 取るのに対し、Scrapling Fetcher は静的 HTML で
0.1-1s、動的 (SPA) でも 3-6s。LLM 推論不要、決定論的 CSS/regex 抽出。

3 段階 fallback:
  1. Fetcher          (静的 HTTP、~0.1s)       ← 多くの法人サイト、iタウンページ
  2. DynamicFetcher   (Playwright JS、~5s)      ← SPA (Mos shop detail 等)
  3. StealthyFetcher  (Camoufox 擬装、~10s)     ← bot block 対策 (Google 検索等)

主な使用目的:
  - JFA 会員企業 489 社の公式サイトから「FC 加盟店一覧」「会社概要」を取得
  - 電話番号 / 店舗名で Google / iタウンページ逆引き
  - Mos SPA 店舗ページから phone / address / (運営会社があれば) 抽出
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# ─── 正規表現: 運営会社名 / 法人番号 / 電話 / 住所 ────────────


# 社格込みの法人名 1 つ (前置 or 後置)。character class は日本語+英数記号。
_CORP_NAME_RE = (
    r"(?:(?:株式会社|有限会社|合同会社|㈱|㈲|\(株\)|（株）)"
    r"[一-龥ぁ-んァ-ヶA-Za-z0-9・\-ー]{2,25}"
    r"|"
    r"[一-龥ぁ-んァ-ヶA-Za-z0-9・\-ー]{2,25}"
    r"(?:株式会社|有限会社|合同会社|㈱|㈲|\(株\)|（株）))"
)

# (context keyword, raw regex template, confidence) の順で priority 高→低
_OPERATOR_PATTERN_SPECS = [
    ("運営会社", r"運営会社[:：\s]*(" + _CORP_NAME_RE + r")", 0.9),
    ("運営", r"運営[:：\s]*(" + _CORP_NAME_RE + r")", 0.85),
    ("店舗運営者", r"店舗運営(?:者|会社)[:：\s]*(" + _CORP_NAME_RE + r")", 0.85),
    ("事業主", r"事業主[:：\s]*(" + _CORP_NAME_RE + r")", 0.75),
    ("加盟店", r"加盟店[:：\s]*(" + _CORP_NAME_RE + r")", 0.75),
    ("社名", r"(?:社\s?名|商\s?号|会社名)[:：\s]*(" + _CORP_NAME_RE + r")", 0.7),
    ("bare-株式会社", r"(" + _CORP_NAME_RE + r")", 0.4),
]

_CORPORATE_NUMBER = re.compile(r"法人番号[:：\s]*(\d{13})")
_PHONE = re.compile(r"(0\d{1,4}-\d{1,4}-\d{4})")
_ADDRESS = re.compile(r"〒?\d{3}[-‐]\d{4}[^<\n]{5,60}")

# 明らかな本部社名を除外するブロックリスト (PI-ZZA として本部は既知)
_FRANCHISOR_BLOCKLIST = {
    "株式会社モスフードサービス",
    "株式会社モスストアカンパニー",
    "日本マクドナルド株式会社",
    "株式会社ダスキン",
    "株式会社壱番屋",
    "株式会社大戸屋",
    "株式会社アレフ",
    "株式会社ファーストフーズ",
}


@dataclass
class ExtractedOperator:
    """抽出結果 (LLM 不使用、決定論で取得)。"""

    name: str
    corporate_number: str = ""
    phone: str = ""
    address: str = ""
    source_url: str = ""
    pattern: str = ""      # ヒットしたパターンのラベル
    confidence: float = 0.0

    @property
    def empty(self) -> bool:
        return not self.name


# ─── Fetcher wrapper ─────────────────────────────────────────


@dataclass
class ScraplingFetcher:
    """3 段階 fallback の web fetcher。"""

    timeout_static_sec: float = 15.0
    timeout_dynamic_sec: float = 30.0
    prefer_stealthy: bool = False
    camofox_base_url: str = ""
    camofox_user: str = "pizza"
    camofox_preset: str = "japan"

    def fetch_static(self, url: str) -> str | None:
        """Fetcher で静的 HTML を取得。成功で html str、失敗で None。"""
        try:
            from scrapling.fetchers import Fetcher

            r = Fetcher.get(url, timeout=self.timeout_static_sec)
            if r.status >= 200 and r.status < 300:
                return _body_to_text(r.body)
            return None
        except Exception as e:
            logger.debug("fetch_static failed: %s %s", url, e)
            return None

    def fetch_dynamic(self, url: str) -> str | None:
        """DynamicFetcher で JS レンダリング後の HTML を取得。"""
        try:
            from scrapling.fetchers import DynamicFetcher

            r = DynamicFetcher.fetch(
                url,
                network_idle=True,
                timeout=int(self.timeout_dynamic_sec * 1000),
            )
            if r.status >= 200 and r.status < 300:
                return _body_to_text(r.body)
            return None
        except Exception as e:
            logger.debug("fetch_dynamic failed: %s %s", url, e)
            return None

    def fetch_camofox(self, url: str) -> str | None:
        """camofox-browser REST API で JS レンダリング後の HTML を取得する。

        `camofox-browser` server は別プロセスで起動済みの前提。未起動なら
        例外を握って None を返し、呼び出し側の fallback を邪魔しない。
        """
        base_url = (self.camofox_base_url or os.getenv("CAMOFOX_BASE_URL") or "http://localhost:9377").rstrip("/")
        api_key = os.getenv("CAMOFOX_API_KEY", "")
        user_id = os.getenv("CAMOFOX_USER", self.camofox_user)
        preset = os.getenv("CAMOFOX_PRESET", self.camofox_preset)
        session_key = os.getenv("CAMOFOX_SESSION_KEY", "pizza-fetch")
        timeout = max(self.timeout_dynamic_sec, self.timeout_static_sec, 5.0)
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        tab_id = ""
        try:
            import httpx

            with httpx.Client(timeout=timeout, headers=headers) as client:
                created = client.post(
                    f"{base_url}/tabs",
                    json={
                        "userId": user_id,
                        "sessionKey": session_key,
                        "preset": preset,
                        "url": url,
                    },
                )
                created.raise_for_status()
                data = created.json()
                tab_id = str(data.get("tabId") or data.get("targetId") or "")
                if not tab_id:
                    return None
                client.post(
                    f"{base_url}/tabs/{tab_id}/wait",
                    json={"userId": user_id, "state": "domcontentloaded", "timeout": int(timeout * 1000)},
                )
                evaluated = client.post(
                    f"{base_url}/tabs/{tab_id}/evaluate",
                    json={"userId": user_id, "expression": "document.documentElement.outerHTML"},
                )
                evaluated.raise_for_status()
                result = evaluated.json().get("result")
                return str(result) if result else None
        except Exception as e:
            logger.debug("fetch_camofox failed: %s %s", url, e)
            return None
        finally:
            if tab_id:
                try:
                    import httpx

                    httpx.request(
                        "DELETE",
                        f"{base_url}/tabs/{tab_id}",
                        headers=headers,
                        json={"userId": user_id},
                        timeout=3.0,
                    )
                except Exception:
                    pass

    def fetch_auto(self, url: str) -> str | None:
        """静的 → 動的 の順に試行 (高速優先)。"""
        if os.getenv("PIZZA_FETCHER") == "camofox" or os.getenv("PIZZA_USE_CAMOFOX") == "1":
            html = self.fetch_camofox(url)
            if html:
                return html
        html = self.fetch_static(url)
        if html and _looks_rendered(html):
            return html
        # SPA の場合 静的取得は skeleton のみなので dynamic へ
        return self.fetch_dynamic(url) or html

    def fetch_with_mode(self, url: str, mode: str = "auto") -> str | None:
        """CLI から指定された fetcher mode で HTML を取得する。"""
        normalized = (mode or "auto").strip().lower()
        if normalized == "static":
            return self.fetch_static(url)
        if normalized == "dynamic":
            return self.fetch_dynamic(url)
        if normalized == "camofox":
            return self.fetch_camofox(url)
        if normalized == "auto":
            return self.fetch_auto(url)
        raise ValueError(f"unknown fetcher mode: {mode}")


def _body_to_text(body: Any) -> str:
    """Scrapling Response の body (bytes|str) を str に統一。"""
    if body is None:
        return ""
    if isinstance(body, bytes):
        try:
            return body.decode("utf-8", errors="replace")
        except Exception:
            return body.decode("cp932", errors="replace")
    return str(body)


def _looks_rendered(html: str) -> bool:
    """SPA の skeleton じゃなく実コンテンツがあるか雑に判定。"""
    if not html or len(html) < 2000:
        return False
    # body 内のテキスト要素 (JP 文字) が 500 以上あれば rendered とみなす
    jp_count = sum(1 for c in html[:50000] if "぀" <= c <= "ヿ" or "一" <= c <= "鿿")
    return jp_count > 500


# ─── 決定論的 operator 抽出 ─────────────────────────────────


def extract_operator_from_html(
    html: str,
    *,
    source_url: str = "",
    brand_hint: str = "",
) -> ExtractedOperator:
    """HTML から運営会社名を決定論で抽出。

    優先順位:
      1. 『運営会社: 株式会社XXX』形式 (confidence=0.9)
      2. 『社名/商号: 株式会社XXX』 (0.75)
      3. 『加盟店/事業主: 株式会社XXX』 (0.7)
      4. 単発 『株式会社XXX』 in context (0.4)
    本部ブロックリスト 該当は即 reject (cross-brand 誤認防止)。
    """
    if not html:
        return ExtractedOperator(name="", source_url=source_url)

    for label, pat, conf in _OPERATOR_PATTERN_SPECS:
        m = re.search(pat, html)
        if not m:
            continue
        name = _clean_operator_name(m.group(1))
        if not name:
            continue
        if name in _FRANCHISOR_BLOCKLIST:
            continue
        # 法人番号 / phone / 住所 もまとめて抽出
        cn_m = _CORPORATE_NUMBER.search(html)
        cn = cn_m.group(1) if cn_m else ""
        phone_m = _PHONE.search(html)
        phone = phone_m.group(1) if phone_m else ""
        addr_m = _ADDRESS.search(html)
        addr = addr_m.group(0).strip() if addr_m else ""
        return ExtractedOperator(
            name=name,
            corporate_number=cn,
            phone=phone,
            address=addr,
            source_url=source_url,
            pattern=label,
            confidence=conf,
        )

    return ExtractedOperator(name="", source_url=source_url)


_NAME_STRIP_CHARS = "「」『』【】\"“”'’・,，。、　 \t\n\r"


def _clean_operator_name(s: str) -> str:
    """正規化: 前後空白・記号削除、連続空白 1 化。"""
    if not s:
        return ""
    s = s.strip(_NAME_STRIP_CHARS)
    s = re.sub(r"\s+", "", s)
    # 長すぎる名称は誤 match、除外
    if len(s) > 50:
        return ""
    return s


# ─── Google 検索 逆引き ─────────────────────────────────────


def build_google_lookup_url(*, phone: str = "", name: str = "", brand: str = "") -> str:
    """Google 検索 の URL を組み立てる (Scrapling Fetcher で叩く用)。

    example: `"03-1234-5678" "運営" site:*.jp`
    """
    from urllib.parse import quote_plus

    parts: list[str] = []
    if phone:
        parts.append(f'"{phone}"')
    if name:
        parts.append(f'"{name}"')
    if brand:
        parts.append(brand)
    parts.append("運営 株式会社")
    q = " ".join(parts)
    return f"https://www.google.com/search?q={quote_plus(q)}&hl=ja"
