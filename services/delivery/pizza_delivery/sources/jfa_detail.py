"""JFA 会員個別 detail page を scrape (Phase 25)。

JFA 会員一覧 (`jfa_fetcher.py`) は会員の URL を取得するのみで、個別
detail page (`/particle/XXXX.html` 等) を開かない。本モジュールは:

  - 個別 URL を入力に
  - 代表者 / 本社所在地 / 資本金 / 加盟店募集サイト などを追加抽出
  - 公式 HP URL (会員自身が JFA に登録している URL) も確認

決定論 regex のみ。失敗 graceful (空 JFADetail)。
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass
class JFADetail:
    member_url: str = ""
    company_name: str = ""
    website_url: str = ""
    representative: str = ""
    representative_title: str = ""
    headquarters_address: str = ""
    capital_stock: str = ""
    established: str = ""
    industry: str = ""
    note: str = ""

    @property
    def empty(self) -> bool:
        return not (
            self.representative or self.headquarters_address
            or self.website_url or self.capital_stock
        )


# JFA 会員 detail page で使われる dt/dd 風のレイアウトを緩く拾う
_LABEL_PAIRS = [
    ("representative", ("代表者", "代表取締役", "代表")),
    ("headquarters_address", ("本社所在地", "本社", "所在地", "住所")),
    ("capital_stock", ("資本金",)),
    ("established", ("設立", "創業")),
    ("website_url", ("ホームページ", "URL", "Web サイト", "Webサイト", "公式 URL")),
    ("industry", ("業種", "事業内容")),
]


def _extract_pairs(text: str) -> dict[str, str]:
    """ラベル群を横断的に発見し dict 化。"""
    out: dict[str, str] = {}
    for field_name, labels in _LABEL_PAIRS:
        for lbl in labels:
            # ラベル : 値 の行 or ラベル + 空白 + 値 の行
            pat = re.compile(
                re.escape(lbl) + r"[\s　:：]*([^\n<]{2,200})"
            )
            m = pat.search(text)
            if m:
                val = re.sub(r"[\s　]+", " ", m.group(1)).strip()
                val = val.rstrip("。、,. ").strip()
                if val and field_name not in out:
                    out[field_name] = val[:200]
                break
    return out


def parse_jfa_detail_html(html: str, *, member_url: str = "") -> JFADetail:
    """JFA detail page の HTML から JFADetail を構築する pure 関数。"""
    if not html:
        return JFADetail(member_url=member_url)

    # HTML → text
    try:
        from bs4 import BeautifulSoup

        text = BeautifulSoup(html, "lxml").get_text("\n", strip=True)
    except Exception:
        text = re.sub(r"<[^>]+>", "\n", html)

    pairs = _extract_pairs(text)
    detail = JFADetail(member_url=member_url)
    # 代表者: 役職 と 氏名 を分離
    rep = pairs.get("representative", "")
    if rep:
        m = re.match(
            r"(代表取締役(?:社長|会長|)|取締役社長|社長)[\s　:：]*(.+)",
            rep,
        )
        if m:
            detail.representative_title = m.group(1)
            detail.representative = re.sub(r"[\s　]+", " ", m.group(2)).strip()
        else:
            detail.representative = rep
    detail.headquarters_address = pairs.get("headquarters_address", "")
    detail.capital_stock = pairs.get("capital_stock", "")
    detail.established = pairs.get("established", "")
    detail.industry = pairs.get("industry", "")

    # website_url は "ホームページ: https://..." 以外に <a href> でも拾う
    website = pairs.get("website_url", "")
    if not website or not website.startswith("http"):
        url_re = re.compile(r'href="(https?://[^"]+)"')
        for m in url_re.finditer(html):
            href = m.group(1)
            if "jfa-fc.or.jp" in href:
                continue  # JFA 自身の link は skip
            website = href
            break
    # "https://www.example.com/" の末尾に ) 等が混じってたら削除
    website = re.sub(r'[\)\s"]+$', "", website)
    detail.website_url = website

    # 会社名らしきもの (h1 / title)
    name = ""
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        h1 = soup.find("h1")
        if h1:
            name = re.sub(r"[\s　]+", "", h1.get_text(strip=True))[:100]
    except Exception:
        pass
    detail.company_name = name
    return detail


async def fetch_jfa_detail(
    member_url: str,
    *,
    fetcher_static: Callable[[str], Any] | None = None,
) -> JFADetail:
    """JFA 個別 detail page を fetch → parse。"""
    if not member_url:
        return JFADetail()

    if fetcher_static is None:
        from pizza_delivery.scrapling_fetcher import ScraplingFetcher

        fetcher_static = ScraplingFetcher().fetch_static

    import asyncio

    try:
        html = await asyncio.to_thread(fetcher_static, member_url)
    except Exception as e:
        logger.debug("jfa detail fetch failed: %s %s", member_url, e)
        return JFADetail(member_url=member_url)
    if not html:
        return JFADetail(member_url=member_url)
    return parse_jfa_detail_html(html, member_url=member_url)
