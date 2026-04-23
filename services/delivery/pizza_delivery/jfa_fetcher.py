"""日本フランチャイズチェーン協会 (JFA) 会員企業一覧を pipeline 経由で取込。

ソース: https://jfa-fc.or.jp/ (協会サイト、会員企業ページ)
会員企業一覧は協会公開ページで一般閲覧可能。HTML から ブランド名 / 運営会社 /
業種 を抽出して ORM に upsert する。

使い方:
    fetcher = JFAFetcher()
    members = await fetcher.fetch_members()       # list[JFAMember]
    # または 1 shot:
    count = await JFAFetcher().sync_to_orm(session)

設計原則:
- **LLM 推論は使わない** (正規化は canonical_key / regex のみ)
- 失敗はログして継続 (partial 取込でも価値あり)
- スクレイピング対象 URL が変更されたら graceful に 0 件返して落ちない
- レート制限: 1 req / 秒 (協会サーバへの配慮)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import httpx
from bs4 import BeautifulSoup

from pizza_delivery.orm import (
    OperatorCompany,
    link_brand_operator,
    upsert_brand,
    upsert_operator,
)

logger = logging.getLogger(__name__)


DEFAULT_JFA_MEMBER_INDEX = "https://www.jfa-fc.or.jp/particle/38.html"


@dataclass
class JFAMember:
    """JFA 会員 1 社分のパース結果。"""

    operator_name: str   # 会員企業名 (株式会社○○)
    brand_name: str = "" # 主要ブランド (空なら operator = brand)
    industry: str = ""   # 業種区分
    url: str = ""        # 会員企業公式 URL
    source_url: str = "" # JFA 掲載ページ


# ─── Fetcher ─────────────────────────────────────────────────


@dataclass
class JFAFetcher:
    base_url: str = DEFAULT_JFA_MEMBER_INDEX
    timeout: float = 20.0
    # テスト時は httpx.MockTransport を注入
    transport: Any = None
    # 1 リクエスト間隔 (秒)、協会サーバーへの配慮
    rate_limit_sec: float = 1.0

    async def _fetch_html(self, url: str) -> str:
        kwargs: dict[str, Any] = {
            "timeout": self.timeout,
            "follow_redirects": True,
            "headers": {"User-Agent": "PI-ZZA-research/1.0"},
        }
        if self.transport is not None:
            kwargs["transport"] = self.transport
        async with httpx.AsyncClient(**kwargs) as c:
            r = await c.get(url)
        r.raise_for_status()
        return r.text

    async def fetch_members(self) -> list[JFAMember]:
        """JFA 会員企業一覧 HTML を取得して JFAMember の list を返す。"""
        try:
            html = await self._fetch_html(self.base_url)
        except Exception as e:
            logger.warning("jfa fetch failed: %s", e)
            return []
        return _parse_member_index(html, source_url=self.base_url)

    async def sync_to_orm(self, session) -> int:
        """取得した会員一覧を ORM に upsert。戻り値 = 新規/更新件数。

        ブランド名が取れない会員 (セブン銀行等の業種説明のみ) は
        operator のみ登録し brand_operator_link は作成しない。
        truth set の evaluate recall が 業種説明で汚染されるのを防ぐ。
        """
        members = await self.fetch_members()
        n = 0
        for m in members:
            operator = upsert_operator(
                session,
                name=m.operator_name,
                kind="franchisor",
                source="jfa",
            )
            if m.brand_name:
                brand = upsert_brand(
                    session,
                    name=m.brand_name,
                    source="jfa",
                    industry=m.industry,
                    master_franchisor_name=m.operator_name,
                    jfa_member=True,
                )
                session.flush()  # brand.id / operator.id を確定
                link_brand_operator(
                    session,
                    brand=brand,
                    operator=operator,
                    operator_type="franchisor",
                    source="jfa",
                    source_url=m.source_url,
                )
            n += 1
        session.commit()
        return n


# ─── HTML parser ───────────────────────────────────────────


def _parse_member_index(html: str, *, source_url: str = "") -> list[JFAMember]:
    """JFA 会員一覧 HTML から JFAMember を抽出。

    協会サイトは年次で構造が変わることがあるため、緩めの抽出ルール:
    - table 形式: <tr><td>社名</td><td>業種</td><td>URL</td></tr> を優先
    - それ以外: li に 会員企業名らしき部分 (株式会社/有限会社 を含む) があれば採用
    """
    soup = BeautifulSoup(html, "lxml")
    members: list[JFAMember] = []
    seen: set[str] = set()

    # パターン 1: table rows (header <th> は除外し、<td> のみ)
    # JFA 会員一覧の構造: <tr><td>会社名<br><span>英名</span></td>
    #                      <td>ブランド名<br>業種説明</td></tr>
    # ただし一部の会員 (銀行/コンサル等) は col[1] が「業種説明のみ」で
    # ブランド名が空の場合あり → brand を空のままにする。
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        td_texts = [td.get_text(" ", strip=True) for td in tds]
        # 会社名は col[0] を優先 (JFA レイアウト)、無ければ全 cell 探索
        name = _pick_corporate_name(td_texts[:1]) or _pick_corporate_name(td_texts)
        if not name or name in seen:
            continue
        brand = ""
        industry = ""
        if len(tds) >= 2:
            brand, industry = _split_brand_industry(tds[1])
        if not industry:
            industry = _pick_industry(td_texts)
        url = _pick_url(tr)
        members.append(
            JFAMember(
                operator_name=_canonicalize_operator(name),
                brand_name=brand,
                industry=industry,
                url=url,
                source_url=source_url,
            )
        )
        seen.add(name)

    # パターン 2: li items (table が空振りのとき)
    if not members:
        for li in soup.find_all("li"):
            text = li.get_text(" ", strip=True)
            name = _pick_corporate_name([text])
            if not name or name in seen:
                continue
            url = _pick_url(li)
            members.append(
                JFAMember(operator_name=name, url=url, source_url=source_url)
            )
            seen.add(name)

    return members


_CORP_MARKERS = (
    "株式会社", "有限会社", "合同会社", "一般社団法人", "公益社団法人",
    "（株）", "(株)", "㈱", "（有）", "(有)", "㈲",
)


def _canonicalize_corp_abbrev(name: str) -> str:
    """（株）/ (株) / ㈱ を 株式会社 に正規化する (前置/後置両対応)。"""
    s = name.strip()
    abbrev_map = [("（株）", "株式会社"), ("(株)", "株式会社"), ("㈱", "株式会社"),
                  ("（有）", "有限会社"), ("(有)", "有限会社"), ("㈲", "有限会社")]
    for src, dst in abbrev_map:
        s = s.replace(src, dst)
    return s.strip()


def _canonicalize_operator(name: str) -> str:
    """JFA operator の『株式会社モスフードサービス MOS FOOD SERVICES INC.』
    → 英語サフィックスを落とし、法人格を正規化する。
    """
    import re

    s = _canonicalize_corp_abbrev(name)
    # 「株式会社モスフードサービス MOS FOOD SERVICES INC.」のような
    # ASCII-only の単語列を末尾に持つケースを削除
    # 最後に残る日本語/漢字/カナ が含まれる token までで止める
    s = re.sub(r"\s+[A-Za-z0-9&.,'\-\s()]+$", "", s).strip()
    return s


# ─── col[1] から brand 名と industry を慎重に分離 ─────────


# 業種説明と思われるキーワード (brand 名では普通出現しない)
_INDUSTRY_PHRASES = (
    "事業", "サービス", "の提供", "の運営", "の販売", "の開発", "の設置",
    "の展開", "の製造", "の輸出入", "の管理", "コンサル", "ソリューション",
    "等の", "及び", "並びに", "等に係る", "に関する", "マニュアル",
)
# brand 名として妥当な最大長 (経験則)
_BRAND_MAX_LEN = 40


def _split_brand_industry(td) -> tuple[str, str]:
    """col[1] の <td> から (brand, industry) を分離して返す。

    構造前提:
      <td>ブランド名<br>業種説明</td>   または
      <td>業種説明</td> (brand 無し会員)

    行分割して前行 = brand 候補 / 残りを industry として扱う。
    前行が長すぎる / 業種キーワードを含むなら brand は空で industry 扱い。
    """
    lines = [
        ln.strip()
        for ln in td.get_text("\n", strip=True).split("\n")
        if ln.strip()
    ]
    if not lines:
        return "", ""
    first = lines[0]
    rest = " ".join(lines[1:]).strip()

    if _looks_like_industry(first):
        return "", first[:120]
    if len(first) > _BRAND_MAX_LEN:
        # 1 行目が長すぎれば industry とみなす
        return "", first[:120]
    # 1 行目 = brand、残りは industry
    return first[:100], rest[:120]


def _looks_like_industry(s: str) -> bool:
    """業種説明に典型的なキーワードを含むか。"""
    return any(p in s for p in _INDUSTRY_PHRASES)


def _pick_corporate_name(texts: list[str]) -> str:
    """cell 群から『株式会社○○』を 1 件見つけて返す。"""
    for t in texts:
        for m in _CORP_MARKERS:
            if m in t:
                # 前後の記号を除去して最初の 1 社名だけ採用
                s = t.strip()
                # 同 cell に複数社書いてあるケースは最初だけ
                for sep in ["\n", "／", "/", " ／ ", "、"]:
                    if sep in s:
                        parts = [p.strip() for p in s.split(sep) if m in p]
                        if parts:
                            return parts[0][:100]
                return s[:100]
    return ""


def _pick_industry(texts: list[str]) -> str:
    """『飲食』『小売』『外食』等のキーワードを拾う。

    注: 法人名自体が含む文字列と衝突しないよう、会社名らしき cell
    (株式会社 等を含む) は industry 候補から除外する。
    """
    markers = ("飲食", "小売", "宿泊", "教育", "理美容", "外食", "コンビニ", "不動産", "サービス業")
    for t in texts:
        if any(c in t for c in _CORP_MARKERS):
            continue  # 会社名 cell は industry 判定から除外
        for m in markers:
            if m in t:
                return t[:40]
    return ""


def _pick_url(tag) -> str:
    """tag 配下の最初の <a href> を返す。"""
    a = tag.find("a")
    if a is None:
        return ""
    href = a.get("href", "")
    if isinstance(href, str) and href.startswith(("http://", "https://")):
        return href
    return ""


# ─── CLI ────────────────────────────────────────────────────


def _main() -> None:
    import argparse
    import csv as csv_mod
    import sys
    from pathlib import Path

    ap = argparse.ArgumentParser(description="JFA 会員企業一覧の fetch/sync/export")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_sync = sub.add_parser("sync", help="JFA から scrape して ORM DB に upsert")
    p_sync.add_argument("--url", default="", help="会員一覧 URL (default: 公式)")

    p_export = sub.add_parser("export", help="ORM DB のブランド×事業会社を CSV 出力")
    p_export.add_argument("--out", required=True)
    p_export.add_argument("--source", default="", help="source フィルタ (空で全件)")

    args = ap.parse_args()

    if args.cmd == "sync":
        fetcher = JFAFetcher(base_url=args.url or DEFAULT_JFA_MEMBER_INDEX)
        from pizza_delivery.orm import make_session

        sess = make_session()
        try:
            n = asyncio.run(fetcher.sync_to_orm(sess))
            print(f"✅ synced {n} members from {fetcher.base_url}")
        finally:
            sess.close()
        return

    if args.cmd == "export":
        from sqlalchemy.orm import joinedload

        from pizza_delivery.orm import (
            BrandOperatorLink,
            FranchiseBrand,
            OperatorCompany,
            make_session,
        )

        sess = make_session()
        try:
            q = (
                sess.query(BrandOperatorLink)
                .options(
                    joinedload(BrandOperatorLink.brand),
                    joinedload(BrandOperatorLink.operator),
                )
                .join(FranchiseBrand, BrandOperatorLink.brand_id == FranchiseBrand.id)
                .join(OperatorCompany, BrandOperatorLink.operator_id == OperatorCompany.id)
            )
            if args.source:
                q = q.filter(BrandOperatorLink.source == args.source)
            # detach 前に tuple 化 (lazy load 回避)
            rows = [
                (
                    link.brand.name, link.brand.industry,
                    link.operator.name, link.operator.corporate_number,
                    link.operator.head_office, link.operator_type,
                    link.estimated_store_count, link.observed_at,
                    link.source, link.source_url, link.note,
                )
                for link in q.all()
            ]
        finally:
            sess.close()

        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8", newline="") as f:
            w = csv_mod.writer(f)
            w.writerow([
                "brand_name", "industry", "operator_name", "corporate_number",
                "head_office", "operator_type", "estimated_store_count",
                "observed_at", "source", "source_url", "note",
            ])
            w.writerows(rows)
        print(f"✅ exported {len(rows)} rows → {out}")
        return


if __name__ == "__main__":
    _main()
