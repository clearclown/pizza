"""Phase 25: 14 ブランド並列プロファイリング orchestrator。

目的: 14 ブランドについて **ハルシネーション 0** で 10 項目を同時並列で集約:
  企業名 / ブランド名 / FC 店舗数 / 代表者氏名 / 本社住所 /
  当期売上 / 前期売上 / HP URL / 加盟店ブランド / FC 募集サイト

2 階層の並列度:
  - brand_concurrency (default 4): ブランド間 並列 (Mos と TSUTAYA が同時進行)
  - intra_concurrency (default 3): 1 ブランド内 4 source 同時 fetch
                                   (JFA detail / gBiz / 公式 HP / cross-brand)

データ源の優先順位 (融合):
  franchisor_name          ← JFA → ORM (FranchiseBrand.master_franchisor_name)
  corporate_number         ← gBizINFO → Houjin CSV
  fc_store_count           ← 公式 HP regex → pipeline operator_stores COUNT
  representative_name      ← gBizINFO → 公式 HP 会社概要
  headquarters_address     ← gBizINFO → Houjin CSV → 公式 HP
  revenue_{current,prev}   ← 公式 IR regex (出典無しなら空)
  website_url              ← JFA member URL → 公式 HP root
  affiliate_brands         ← ORM cross-brand (BrandOperatorLink)
  fc_recruitment_url       ← 公式 HP auto-discover

取得できなかった項目は空文字で返す (ハルシネーション禁止ルール)。
"""

from __future__ import annotations

import asyncio
import csv as csv_mod
import json
import logging
import os
import sqlite3
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


# ─── データ型 ─────────────────────────────────────────────────


@dataclass
class BrandProfile:
    """1 ブランドの 10 項目サマリ + 出典情報。"""

    brand_name: str
    franchisor_name: str = ""
    corporate_number: str = ""
    fc_store_count: int = 0
    representative_name: str = ""
    representative_title: str = ""
    headquarters_address: str = ""
    revenue_current_jpy: int = 0
    revenue_previous_jpy: int = 0
    revenue_observed_at: str = ""
    website_url: str = ""
    affiliate_brands: list[str] = field(default_factory=list)
    fc_recruitment_url: str = ""
    sources: list[str] = field(default_factory=list)  # ["jfa", "gbiz", "official", "orm"]
    visited_urls: list[str] = field(default_factory=list)
    confidence: float = 0.0
    errors: list[str] = field(default_factory=list)
    fetched_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # CSV friendly: list → ";" 連結
        d["affiliate_brands"] = ";".join(self.affiliate_brands)
        d["sources"] = ";".join(self.sources)
        d["visited_urls"] = ";".join(self.visited_urls)
        d["errors"] = ";".join(self.errors)
        return d


# ─── Fetch helpers (各 source を 1 関数 + graceful) ───────────


async def _fetch_jfa(
    brand_name: str, *, ormpath: str | None = None,
) -> dict[str, Any]:
    """ORM から JFA 経由で登録された brand × franchisor の 1 次情報を取る。

    取得項目: franchisor_name, website_url (JFA member URL),
    fc_recruitment_url (過去に保存済があれば)。
    """
    from pizza_delivery.orm import (
        BrandOperatorLink, FranchiseBrand, OperatorCompany,
        default_engine, make_session,
    )

    out: dict[str, Any] = {"source": "jfa"}
    engine = default_engine() if ormpath is None else None
    sess = make_session(engine)
    try:
        # brand 名で partial match (ゆらぎ: 「モスバーガー」 vs 「株式会社モスバーガー」)
        brand = (
            sess.query(FranchiseBrand)
            .filter(FranchiseBrand.name == brand_name)
            .one_or_none()
        )
        if brand is None:
            brand = (
                sess.query(FranchiseBrand)
                .filter(FranchiseBrand.name.like(f"%{brand_name}%"))
                .first()
            )
        if brand is None:
            return out
        out["brand_id"] = brand.id
        out["franchisor_name"] = brand.master_franchisor_name or ""
        out["fc_recruitment_url"] = brand.fc_recruitment_url or ""
        # franchisor の operator record を link 経由で辿る (複数候補あれば最良を選ぶ)
        links = (
            sess.query(BrandOperatorLink)
            .filter(
                BrandOperatorLink.brand_id == brand.id,
                BrandOperatorLink.operator_type == "franchisor",
            )
            .all()
        )
        # link の operator_id から op 群を拾う + master_franchisor_name でも広げる
        operator_ids = {link.operator_id for link in links}
        ops: list[OperatorCompany] = list(
            sess.query(OperatorCompany).filter(OperatorCompany.id.in_(operator_ids)).all()
        ) if operator_ids else []
        # 同名の別 record (duplicate) も集める — 各フィールドで best を選ぶため
        if out["franchisor_name"]:
            same_name_ops = (
                sess.query(OperatorCompany)
                .filter(OperatorCompany.name == out["franchisor_name"])
                .all()
            )
            for op in same_name_ops:
                if op.id not in operator_ids:
                    ops.append(op)

        if ops:
            # 各フィールドで "最初に非空の値を持つ operator" を採用
            def _pick(attr: str) -> str:
                for op in ops:
                    v = getattr(op, attr, "")
                    if v:
                        return v
                return ""

            def _pick_int(attr: str) -> int:
                for op in ops:
                    v = int(getattr(op, attr, 0) or 0)
                    if v:
                        return v
                return 0

            best_op = ops[0]
            out["operator_id"] = best_op.id
            out["corporate_number"] = _pick("corporate_number")
            out["head_office"] = _pick("head_office")
            out["representative_name"] = _pick("representative_name")
            out["representative_title"] = _pick("representative_title")
            out["website_url"] = _pick("website_url")
            out["revenue_current_jpy"] = _pick_int("revenue_current_jpy")
            out["revenue_previous_jpy"] = _pick_int("revenue_previous_jpy")
            out["revenue_observed_at"] = _pick("revenue_observed_at")
            if not out["franchisor_name"]:
                out["franchisor_name"] = best_op.name
            if links:
                out["source_url"] = links[0].source_url or ""
    finally:
        sess.close()
    return out


async def _fetch_places_fallback(
    brand_name: str,
) -> dict[str, Any]:
    """brand が ORM 未登録時、Places Text Search で 1 店舗引いて websiteUri を取る。

    websiteUri は通常 `www.brand.co.jp` 形式なので、これを TLD+1 抽出すれば
    franchisor の公式 HP 候補になる。
    API key 未設定なら skip。
    """
    from pizza_delivery.places_client import PlacesClient, PlacesAPIError

    out: dict[str, Any] = {"source": "places"}
    api_key = os.getenv("GOOGLE_MAPS_API_KEY", "")
    if not api_key:
        out["source"] = "places_skipped"
        return out
    try:
        client = PlacesClient()
        # 複数店舗で最頻出 root domain を本社候補とみなす (1 店舗だけだと偏る)
        res = await client.search_text(f"{brand_name} 店舗", max_result_count=10)
        if not res.places:
            return out
        from collections import Counter
        from urllib.parse import urlparse

        def _root(u: str) -> str:
            p = urlparse(u)
            if not (p.scheme and p.netloc):
                return ""
            # subdomain を落とす (www.example.co.jp / shibuya.tsite.jp → example.co.jp / tsite.jp)
            host = p.netloc
            parts = host.split(".")
            if len(parts) >= 3 and parts[-2] in ("co", "or", "ne", "ac", "go"):
                # example.co.jp 型
                host_root = ".".join(parts[-3:])
            elif len(parts) >= 2:
                host_root = ".".join(parts[-2:])
            else:
                host_root = host
            return f"{p.scheme}://www.{host_root}/"

        roots = Counter()
        name_hint = ""
        for place in res.places:
            if place.website_uri:
                r = _root(place.website_uri)
                if r:
                    roots[r] += 1
            if not name_hint and place.name:
                name_hint = place.name
        if not roots:
            return out
        website, _n = roots.most_common(1)[0]
        out["website_url"] = website
        out["name_hint"] = name_hint
    except PlacesAPIError as e:
        out["error"] = f"places: {e}"
    except Exception as e:
        out["error"] = str(e)
    return out


def _guess_franchisor_from_brand(brand_name: str) -> dict[str, Any]:
    """brand 名しか判明していない場合、houjin_csv を部分一致で探索して
    最も本部らしい (コーポレーション / ホールディングス / グループ 等を含む)
    法人を 1 つ選ぶ。"""
    if not brand_name:
        return {"source": "houjin_csv_guess_skipped"}
    try:
        from pizza_delivery.houjin_csv import HoujinCSVIndex

        idx = HoujinCSVIndex()
        if idx.count() == 0:
            return {"source": "houjin_csv_guess_skipped"}
        # "ハードオフ" で substring 検索
        recs = idx.search_by_name(brand_name, limit=20, active_only=False)
    except Exception as e:
        return {"source": "houjin_csv_guess", "error": str(e)}
    if not recs:
        return {"source": "houjin_csv_guess"}

    # FC 運営の対象外 (financial/educational 等) を reject する法人格
    reject_forms = (
        "財団法人", "社団法人", "組合", "連合会", "協議会",
        "独立行政法人", "国立大学", "公立大学",
    )
    # 本部らしいキーワード優先度
    corp_hints = (
        "ホールディングス", "コーポレーション", "グループ",
        "ジャパン", "インターナショナル",
    )

    def _score(r) -> tuple[int, int]:
        name = r.name
        # reject 法人格
        if any(rf in name for rf in reject_forms):
            return (-1000, 0)
        s = 0
        if brand_name in name:
            s += 10
        has_hint = any(h in name for h in corp_hints)
        if has_hint:
            s += 5
        if name.startswith("株式会社") or name.startswith("有限会社"):
            s += 1
        # 完全一致 (株式会社 + brand_name) ならフルスコア
        if name == f"株式会社{brand_name}":
            s += 20
        # 厳格化: 個別加盟店 (XXX店 / XXXデザイン / XXX支店 等) を penalize
        # brand_name 直後に「店/XX支店/フランチャイズ」以外の固有名詞が来るなら下げる
        if brand_name in name:
            idx = name.find(brand_name) + len(brand_name)
            suffix = name[idx:]
            bad_suffix = ("デザイン", "不動産", "土地", "エンター",
                          "サービス", "物流", "建設")
            if suffix and any(b in suffix for b in bad_suffix):
                s -= 15
            # 地名 suffix (「硯町」「神戸店」「渋谷店」 等) を 除外
            # suffix に 「市/町/区/店」 を含み、コーポレーション/ホールディングス等が無いなら penalty
            if (any(c in suffix for c in "市町区店支局") and not has_hint
                    and not name.endswith("コーポレーション")):
                s -= 15
        return (s, -len(name))

    best = max(recs, key=_score)
    _s = _score(best)
    # 採用基準: 完全一致 (brand + 本部 keyword) OR exact "株式会社+brand"
    if _s[0] < 15:
        return {"source": "houjin_csv_guess"}
    return {
        "source": "houjin_csv_guess",
        "corporate_number": best.corporate_number,
        "franchisor_name_houjin": best.name,
        "headquarters_address": best.address,
        "prefecture": best.prefecture,
    }


def _fetch_houjin_csv(
    franchisor_name: str,
) -> dict[str, Any]:
    """国税庁 法人番号 CSV (local SQLite, 577 万件) から法人番号 + 住所を取得。

    GBIZ_API_TOKEN 無しでも動く最強の Ground Truth source。
    `search_by_name` は exact → prefix → substring の 3 段階 fallback 内蔵。
    """
    out: dict[str, Any] = {"source": "houjin_csv_skipped"}
    if not franchisor_name:
        return out
    try:
        from pizza_delivery.houjin_csv import HoujinCSVIndex

        idx = HoujinCSVIndex()
        if idx.count() == 0:
            return out
        # active_only=True で検索 → miss なら active_only=False で再試行
        # (商号変更等で process code が active 外になるケースがあるため)
        recs = idx.search_by_name(franchisor_name, limit=3, active_only=True)
        if not recs:
            recs = idx.search_by_name(franchisor_name, limit=3, active_only=False)
    except Exception as e:
        out["error"] = str(e)
        return out
    if not recs:
        return {"source": "houjin_csv"}
    # exact match を最優先 (search_by_name は exact→prefix→substring 順で返す)
    best = None
    for r in recs:
        if r.name == franchisor_name:
            best = r
            break
    if best is None:
        best = recs[0]
    return {
        "source": "houjin_csv",
        "corporate_number": best.corporate_number,
        "franchisor_name_houjin": best.name,
        "headquarters_address": best.address,
        "prefecture": best.prefecture,
    }


async def _fetch_multi_brand_discovery(
    website_url: str,
    self_brand_name: str,
) -> list[str]:
    """operator 公式サイトの nav から他ブランド link を決定論抽出。

    operator_spider.discover_multi_brand を使い、ORM に登録済の全 brand 名を
    辞書として使って anchor text を match する (既定の _KNOWN_FC_BRANDS だけだと
    14 brand 候補をカバーしきれないため、ORM の 245 brand を動的注入)。
    """
    if not website_url:
        return []
    # ORM の全 brand 名を辞書として取得
    try:
        from pizza_delivery.orm import FranchiseBrand, make_session

        sess = make_session()
        try:
            all_brands = {b.name for b in sess.query(FranchiseBrand).all() if b.name}
        finally:
            sess.close()
    except Exception as e:
        logger.debug("load ORM brands failed: %s", e)
        return []

    # Scrapling で HP を fetch して anchor text を走査
    try:
        from pizza_delivery.scrapling_fetcher import ScraplingFetcher
        import re
        from urllib.parse import urljoin

        sf = ScraplingFetcher()
        html = await asyncio.to_thread(sf.fetch_static, website_url)
        if not html:
            return []
    except Exception as e:
        logger.debug("fetch for multi-brand failed: %s", e)
        return []

    # anchor text に ORM brand 名が現れる link を集める
    link_re = re.compile(
        r'<a\s+[^>]*href="([^"]+)"[^>]*>([^<]+)</a>',
        re.IGNORECASE | re.DOTALL,
    )

    # 自己参照除外: self_brand_name を casefold 比較で落とす
    self_key = (self_brand_name or "").casefold()

    def _is_self_ref(b: str) -> bool:
        if not b:
            return True
        bk = b.casefold()
        # 完全一致 or 一方が他方を含む (例: "Itto個別指導学院" vs "ITTO個別指導学院")
        return bk == self_key or bk in self_key or self_key in bk

    found: set[str] = set()
    for m in link_re.finditer(html):
        anchor = re.sub(r"\s+", " ", m.group(2).strip())
        if not anchor or len(anchor) > 40:
            continue
        for brand in all_brands:
            if _is_self_ref(brand):
                continue
            if brand in anchor:
                found.add(brand)
                break
    return sorted(found)


async def _fetch_gbiz(
    franchisor_name: str, *, corporate_number: str = "",
) -> dict[str, Any]:
    """gBizINFO で代表者 / 住所 / 法人番号を取得。Token 未設定なら skip。"""
    from pizza_delivery.gbiz_client import GBizClient

    client = GBizClient()
    if not client.ready():
        return {"source": "gbiz_skipped"}
    try:
        rec = None
        if corporate_number:
            rec = await client.get_by_corporate_number(corporate_number)
        if rec is None and franchisor_name:
            res = await client.search_by_name(franchisor_name, limit=5)
            if res.found:
                # 最も名前が近い 1 件を採用 (上位 1 のみ単純選択)
                rec = res.records[0]
        if rec is None:
            return {"source": "gbiz"}
        return {
            "source": "gbiz",
            "corporate_number": rec.corporate_number,
            "franchisor_name_gbiz": rec.name,
            "representative_name": rec.representative_name,
            "representative_title": rec.representative_title,
            "headquarters_address": rec.address,
            "capital_stock": rec.capital_stock,
        }
    except Exception as e:
        return {"source": "gbiz", "error": str(e)}


async def _fetch_official(website_url: str) -> dict[str, Any]:
    """公式 HP + 会社概要 + IR から決定論抽出 (Scrapling 経由)。"""
    if not website_url:
        return {"source": "official_skipped"}
    from pizza_delivery.sources.official_site import fetch_official_site

    try:
        d = await fetch_official_site(website_url)
    except Exception as e:
        return {"source": "official", "error": str(e)}
    return {
        "source": "official",
        "company_name": d.company_name,
        "fc_store_count": d.fc_store_count,
        "representative_name": d.representative_name,
        "representative_title": d.representative_title,
        "headquarters_address": d.headquarters_address,
        "revenue_current_jpy": d.revenue.current_jpy,
        "revenue_previous_jpy": d.revenue.previous_jpy,
        "revenue_observed_at": d.revenue.observed_at,
        "fc_recruitment_url": d.fc_recruitment.url,
        "visited_urls": d.visited_urls,
        "website_url": website_url,
    }


def _fetch_affiliate_brands(
    brand_name: str,
    franchisor_name: str,
    pipeline_db: str | None = None,
) -> list[str]:
    """pipeline SQLite の operator_stores から cross-brand 集計で
    『この brand の加盟店 operator が他に展開しているブランド』 list を構築。

    自ブランドは除外。"""
    if pipeline_db is None or not Path(pipeline_db).exists():
        return []
    try:
        conn = sqlite3.connect(pipeline_db)
        try:
            # このブランドに関与する operator 一覧を取得
            ops = conn.execute(
                "SELECT DISTINCT operator_name FROM operator_stores "
                "WHERE brand = ? AND operator_name != '' "
                "AND COALESCE(operator_type,'') NOT IN ('franchisor','direct')",
                (brand_name,),
            ).fetchall()
            op_names = [o[0] for o in ops if o[0]]
            if not op_names:
                return []
            # それらの operator が他に持つブランド
            placeholders = ",".join(["?"] * len(op_names))
            rows = conn.execute(
                f"SELECT DISTINCT brand FROM operator_stores "
                f"WHERE operator_name IN ({placeholders}) AND brand != ?",
                (*op_names, brand_name),
            ).fetchall()
            return sorted({r[0] for r in rows if r[0]})
        finally:
            conn.close()
    except Exception as e:
        logger.debug("affiliate_brands query failed: %s", e)
        return []


# ─── Merge ロジック (優先順位テーブル実装) ───────────────────


def _merge_profile(
    brand_name: str,
    jfa: dict,
    gbiz: dict,
    official: dict,
    affiliate: list[str],
    houjin_csv: dict | None = None,
) -> BrandProfile:
    """4 source の結果を優先順位テーブルで融合して 1 つの BrandProfile に。

    優先順位 (上→下、上で埋まれば以降 skip):
      franchisor_name      : JFA (ORM)
      corporate_number     : gBiz → ORM(JFA)
      fc_store_count       : official
      representative_name  : gBiz → official → ORM
      headquarters_address : gBiz → ORM → official
      revenue_*            : official (出典無しなら空、ハルシネ禁止)
      website_url          : JFA → official
      fc_recruitment_url   : ORM (JFA で集めた) → official
      affiliate_brands     : pipeline cross-brand
    """
    from datetime import datetime

    houjin_csv = houjin_csv or {}
    p = BrandProfile(brand_name=brand_name, fetched_at=datetime.utcnow().isoformat())
    sources: list[str] = []

    # franchisor name (JFA → gBiz → official site 会社名抽出 → houjin_csv)
    name = (
        jfa.get("franchisor_name")
        or gbiz.get("franchisor_name_gbiz")
        or official.get("company_name")
        or houjin_csv.get("franchisor_name_houjin")
        or ""
    )
    p.franchisor_name = name

    # corporate_number (gBiz → houjin_csv → JFA)
    p.corporate_number = (
        gbiz.get("corporate_number")
        or houjin_csv.get("corporate_number")
        or jfa.get("corporate_number")
        or ""
    )

    # fc_store_count
    p.fc_store_count = int(official.get("fc_store_count") or 0)
    if p.fc_store_count and "official" not in sources:
        sources.append("official")

    # representative
    p.representative_name = (
        gbiz.get("representative_name")
        or official.get("representative_name")
        or jfa.get("representative_name")
        or ""
    )
    p.representative_title = (
        gbiz.get("representative_title")
        or official.get("representative_title")
        or jfa.get("representative_title")
        or ""
    )

    # headquarters_address (gBiz → houjin_csv → JFA → official)
    p.headquarters_address = (
        gbiz.get("headquarters_address")
        or houjin_csv.get("headquarters_address")
        or jfa.get("head_office")
        or official.get("headquarters_address")
        or ""
    )

    # revenue (official のみ信頼、出典無しなら空 — ハルシネ禁止)
    p.revenue_current_jpy = int(
        official.get("revenue_current_jpy") or jfa.get("revenue_current_jpy") or 0
    )
    p.revenue_previous_jpy = int(
        official.get("revenue_previous_jpy") or jfa.get("revenue_previous_jpy") or 0
    )
    p.revenue_observed_at = (
        official.get("revenue_observed_at") or jfa.get("revenue_observed_at") or ""
    )

    # website_url
    p.website_url = jfa.get("website_url") or official.get("website_url") or ""

    # fc_recruitment_url
    p.fc_recruitment_url = (
        jfa.get("fc_recruitment_url") or official.get("fc_recruitment_url") or ""
    )

    p.affiliate_brands = affiliate

    # source 集約 (gbiz_skipped は除外)
    if jfa.get("source") == "jfa" and (
        jfa.get("franchisor_name") or jfa.get("website_url")
    ):
        sources.append("jfa")
    if gbiz.get("source") == "gbiz" and (
        gbiz.get("representative_name") or gbiz.get("corporate_number")
    ):
        sources.append("gbiz")
    if official.get("source") == "official" and (
        official.get("fc_store_count")
        or official.get("revenue_current_jpy")
        or official.get("representative_name")
        or official.get("fc_recruitment_url")
    ):
        sources.append("official")
    if affiliate:
        sources.append("orm_crossbrand")
    if houjin_csv.get("source") in ("houjin_csv", "houjin_csv_guess") and (
        houjin_csv.get("corporate_number") or houjin_csv.get("headquarters_address")
    ):
        sources.append(houjin_csv["source"])
    p.sources = sources
    p.visited_urls = official.get("visited_urls") or []

    # confidence: 埋まった項目数 / 10
    filled = sum([
        bool(p.franchisor_name),
        bool(p.brand_name),
        bool(p.fc_store_count),
        bool(p.representative_name),
        bool(p.headquarters_address),
        bool(p.revenue_current_jpy),
        bool(p.revenue_previous_jpy),
        bool(p.website_url),
        bool(p.affiliate_brands),
        bool(p.fc_recruitment_url),
    ])
    p.confidence = round(filled / 10, 2)

    # エラー集約
    for d in (jfa, gbiz, official):
        if d.get("error"):
            p.errors.append(f"{d.get('source')}: {d['error']}")
    return p


# ─── Orchestrator ─────────────────────────────────────────


@dataclass
class BrandProfiler:
    """14 ブランドの並列プロファイリング coordinator。"""

    brand_concurrency: int = 4
    intra_concurrency: int = 3
    pipeline_db: str | None = None  # var/pizza.sqlite

    async def profile_one(self, brand_name: str) -> BrandProfile:
        """1 ブランド の 6 source 並列 fetch + merge。

        Source 群:
          1. JFA (ORM)         — 本部企業名 + JFA 会員 URL
          2. Places Text Search — 非 JFA brand の HP 逆引き
          3. gBizINFO API       — 代表者 + 本社住所 (Token 設定時)
          4. 公式 HP scrape     — Scrapling で 決定論抽出
          5. Houjin CSV (577万) — 法人番号 + 住所 (local SQLite、Token 不要)
          6. operator_spider    — 他ブランド link (affiliate_brands)
          7. pipeline operator_stores cross-brand (fallback)
        """
        intra_sem = asyncio.Semaphore(self.intra_concurrency)

        async def _run_jfa() -> dict:
            async with intra_sem:
                return await _fetch_jfa(brand_name)

        jfa = await _run_jfa()
        franchisor = jfa.get("franchisor_name") or ""
        website = jfa.get("website_url") or ""
        corp_number = jfa.get("corporate_number") or ""

        # JFA に website_url が無ければ Places Text Search で補完
        places_res: dict = {}
        if not website:
            async with intra_sem:
                places_res = await _fetch_places_fallback(brand_name)
            website = places_res.get("website_url") or ""

        async def _run_gbiz() -> dict:
            async with intra_sem:
                return await _fetch_gbiz(franchisor, corporate_number=corp_number)

        async def _run_official() -> dict:
            async with intra_sem:
                return await _fetch_official(website)

        async def _run_houjin_csv() -> dict:
            # CSV 検索は sync API なので to_thread 化 (index は 577 万件 SQLite)
            return await asyncio.to_thread(_fetch_houjin_csv, franchisor)

        async def _run_multi_brand() -> list[str]:
            async with intra_sem:
                return await _fetch_multi_brand_discovery(website, brand_name)

        def _run_affiliate_pipeline() -> list[str]:
            return _fetch_affiliate_brands(brand_name, franchisor, self.pipeline_db)

        # 並列に実行 (website ある場合のみ official と multi_brand を叩く)
        tasks = [_run_gbiz(), _run_official(), _run_houjin_csv(), _run_multi_brand()]
        gbiz_res, official_res, houjin_res, multi_brand_res = await asyncio.gather(
            *tasks, return_exceptions=False,
        )

        # affiliate_brands は multi_brand_discovery (公式 HP) → pipeline の 2 段
        affiliate_res = multi_brand_res or _run_affiliate_pipeline()

        # franchisor_name が official で発見された場合、houjin_csv を再走 (住所取得)
        if not franchisor and official_res.get("company_name"):
            franchisor_official = official_res["company_name"]
            retry_houjin = await asyncio.to_thread(
                _fetch_houjin_csv, franchisor_official,
            )
            if retry_houjin.get("corporate_number"):
                houjin_res = retry_houjin

        # 最後の手段: brand 名から法人名を houjin_csv で guess
        # (例: ハードオフ → "株式会社ハードオフコーポレーション" 探索)
        if not franchisor and not houjin_res.get("corporate_number"):
            guessed = await asyncio.to_thread(
                _guess_franchisor_from_brand, brand_name,
            )
            if guessed.get("corporate_number"):
                houjin_res = guessed

        if places_res.get("website_url"):
            jfa.setdefault("website_url", places_res["website_url"])

        profile = _merge_profile(
            brand_name, jfa, gbiz_res, official_res, affiliate_res,
            houjin_csv=houjin_res,
        )
        if places_res.get("website_url") and "places" not in profile.sources:
            profile.sources.append("places")
        return profile

    async def profile_many(
        self, brands: list[str],
    ) -> list[BrandProfile]:
        """brand_concurrency 並列で 14 ブランドを処理。"""
        brand_sem = asyncio.Semaphore(self.brand_concurrency)

        async def _one(b: str) -> BrandProfile:
            async with brand_sem:
                try:
                    return await self.profile_one(b)
                except Exception as e:
                    logger.exception("brand %s failed", b)
                    p = BrandProfile(brand_name=b)
                    p.errors.append(f"fatal: {e}")
                    return p

        return await asyncio.gather(*(_one(b) for b in brands))


# ─── CSV / JSON 出力 ──────────────────────────────────────


CSV_COLUMNS = [
    "brand_name", "franchisor_name", "corporate_number",
    "fc_store_count", "representative_name", "representative_title",
    "headquarters_address",
    "revenue_current_jpy", "revenue_previous_jpy", "revenue_observed_at",
    "website_url", "affiliate_brands", "fc_recruitment_url",
    "sources", "confidence", "fetched_at",
]


def export_csv(profiles: list[BrandProfile], out_path: str | Path) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv_mod.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for p in profiles:
            w.writerow(p.to_dict())


def export_json(profiles: list[BrandProfile], out_path: str | Path) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    data = [p.to_dict() for p in profiles]
    # visited_urls と errors も debug 用に含める
    for prof, raw in zip(profiles, data):
        raw["visited_urls"] = prof.visited_urls
        raw["errors"] = prof.errors
        raw["affiliate_brands"] = prof.affiliate_brands
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── CLI ──────────────────────────────────────────────


def _main() -> None:
    import argparse
    import sys

    ap = argparse.ArgumentParser(
        description="Phase 25: 14 ブランド並列プロファイリング"
    )
    ap.add_argument("--brands", required=True, help="カンマ区切り ブランド一覧")
    ap.add_argument(
        "--brand-concurrency", type=int, default=4,
        help="ブランド間並列度 (default 4)",
    )
    ap.add_argument(
        "--intra-concurrency", type=int, default=3,
        help="1 ブランド内の source 並列度 (default 3)",
    )
    ap.add_argument(
        "--pipeline-db", default="var/pizza.sqlite",
        help="cross-brand 計算用 pipeline SQLite",
    )
    ap.add_argument("--out", default="var/brand-profiles.csv")
    ap.add_argument("--out-json", default="")
    args = ap.parse_args()

    brands = [b.strip() for b in args.brands.split(",") if b.strip()]
    if not brands:
        print("❌ no brands", file=sys.stderr)
        sys.exit(2)

    pipeline_db = args.pipeline_db if Path(args.pipeline_db).exists() else None

    profiler = BrandProfiler(
        brand_concurrency=args.brand_concurrency,
        intra_concurrency=args.intra_concurrency,
        pipeline_db=pipeline_db,
    )
    profiles = asyncio.run(profiler.profile_many(brands))
    export_csv(profiles, args.out)
    if args.out_json:
        export_json(profiles, args.out_json)

    print(f"✅ brand-profile done — {len(profiles)} brands")
    for p in profiles:
        print(
            f"  {p.brand_name:<20} filled={p.confidence*10:.0f}/10 "
            f"sources={','.join(p.sources) or '-'} "
            f"{'⚠' if p.errors else ''}"
        )
    print(f"📄 CSV: {args.out}")
    if args.out_json:
        print(f"📄 JSON: {args.out_json}")


if __name__ == "__main__":
    _main()
