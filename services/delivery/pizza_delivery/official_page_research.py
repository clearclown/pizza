"""公式店舗ページ本文から operator を抽出する pipeline。

求人サイトより証拠寿命が長い公式店舗ページを優先して、Scrapling で HTML を取得し、
本文中の「運営会社」「事業主」「会社名」などの明示ラベルだけを採用する。

DB に入れる条件:
  - operator 名が公式ページ本文に存在する
  - 店舗識別子 (店名/住所/電話) も同じページに存在する
  - 既知本部名ではない
  - 国税庁 CSV で normalized exact match する

LLM 不使用。抽出は正規表現 + 国税庁照合のみ。
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from pathlib import Path

from pizza_delivery.recruitment_research import build_store_keys


TARGET_BRANDS = [
    "カーブス",
    "モスバーガー",
    "業務スーパー",
    "Itto個別指導学院",
    "エニタイムフィットネス",
    "コメダ珈琲",
    "シャトレーゼ",
    "ハードオフ",
    "オフハウス",
    "Kids Duo",
    "アップガレージ",
    "カルビ丼とスン豆腐専門店韓丼",
    "Brand off",
    "TSUTAYA",
]


@dataclass
class OfficialPageMatch:
    brand: str
    place_id: str
    store_name: str
    official_url: str
    operator_name: str = ""
    corporate_number: str = ""
    source_pattern: str = ""
    matched_store_key: str = ""
    accepted: bool = False
    reject_reason: str = ""


@dataclass
class OfficialPageStats:
    target_stores: int = 0
    fetched: int = 0
    extracted: int = 0
    houjin_verified: int = 0
    accepted: int = 0
    applied_rows: int = 0
    rejected: list[str] = field(default_factory=list)


def _text_from_html(html: str) -> str:
    if not html:
        return ""
    try:
        from bs4 import BeautifulSoup

        return BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    except Exception:
        return re.sub(r"<[^>]+>", " ", html)


def _load_unknown_stores(db_path: str | Path, brands: list[str], max_stores: int) -> list[tuple]:
    """Load stores that still lack a corporate-number verified operator."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT s.place_id, s.brand, s.name, COALESCE(s.address,''),
                   COALESCE(s.phone,''), COALESCE(s.official_url,'')
            FROM stores s
            WHERE s.brand IN (%s)
              AND COALESCE(s.official_url,'') != ''
              AND s.place_id NOT IN (
                SELECT os.place_id FROM operator_stores os
                WHERE os.operator_name != ''
                  AND COALESCE(os.operator_type,'') NOT IN ('franchisor','direct')
                  AND COALESCE(os.corporate_number,'') != ''
              )
            ORDER BY s.brand, s.place_id
            """ % ",".join("?" * len(brands)),
            brands,
        ).fetchall()
    finally:
        conn.close()
    if max_stores > 0:
        rows = rows[:max_stores]
    return rows


def _load_franchisor_blocklist() -> set[str]:
    from pizza_delivery.normalize import canonical_key
    from pizza_delivery.registry_expander import _load_known_franchisor_names

    return {canonical_key(n) for n in _load_known_franchisor_names()}


def _verify_houjin_exact(name: str):
    from pizza_delivery.houjin_csv import HoujinCSVIndex
    from pizza_delivery.normalize import canonical_key

    idx = HoujinCSVIndex()
    target = canonical_key(name)
    recs = idx.search_by_name(name, limit=10, active_only=True)
    if not recs:
        recs = idx.search_by_name(name, limit=10, active_only=False)
    for r in recs:
        if r.name == name or canonical_key(r.name) == target:
            return r
    return None


async def _fetch_html(url: str, *, timeout: float) -> str:
    from pizza_delivery.scrapling_fetcher import ScraplingFetcher

    fetcher = ScraplingFetcher(timeout_static_sec=timeout, timeout_dynamic_sec=timeout)
    html = await asyncio.to_thread(fetcher.fetch_static, url)
    return html or ""


async def research_one_store(
    row: tuple,
    *,
    timeout: float = 8.0,
    franchisor_blocklist: set[str] | None = None,
) -> OfficialPageMatch:
    from pizza_delivery.normalize import canonical_key
    from pizza_delivery.scrapling_fetcher import extract_operator_from_html

    place_id, brand, store_name, address, phone, url = row
    out = OfficialPageMatch(
        brand=brand,
        place_id=place_id,
        store_name=store_name,
        official_url=url,
    )
    try:
        html = await _fetch_html(url, timeout=timeout)
    except Exception as e:
        out.reject_reason = f"fetch_error:{e}"
        return out
    if not html:
        out.reject_reason = "empty_html"
        return out

    extracted = extract_operator_from_html(html, source_url=url, brand_hint=brand)
    if not extracted.name:
        out.reject_reason = "no_operator_label"
        return out
    out.operator_name = extracted.name
    out.source_pattern = extracted.pattern
    if extracted.pattern == "bare-株式会社" or extracted.confidence < 0.7:
        out.reject_reason = "operator_label_not_explicit"
        return out

    block = franchisor_blocklist if franchisor_blocklist is not None else _load_franchisor_blocklist()
    if canonical_key(extracted.name) in block:
        out.reject_reason = "blocked_franchisor"
        return out

    text = _text_from_html(html)
    keys = build_store_keys(store_name, address, phone, brand)
    matched_key = next((k for k in keys if k and k in text), "")
    if not matched_key:
        out.reject_reason = "store_key_missing_in_page"
        return out
    out.matched_store_key = matched_key

    rec = _verify_houjin_exact(extracted.name)
    if rec is None:
        out.reject_reason = "houjin_no_exact_match"
        return out
    out.operator_name = rec.name
    out.corporate_number = rec.corporate_number
    out.accepted = True
    return out


def _apply(db_path: str | Path, matches: list[OfficialPageMatch]) -> int:
    conn = sqlite3.connect(db_path)
    applied = 0
    try:
        for m in matches:
            if not m.accepted:
                continue
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO operator_stores
                  (operator_name, place_id, brand, operator_type, confidence,
                   discovered_via, verification_score, corporate_number,
                   verification_source)
                VALUES (?, ?, ?, 'franchisee', 0.84,
                        'official_page_houjin_verified', 1.0, ?,
                        'houjin_csv')
                """,
                (m.operator_name, m.place_id, m.brand, m.corporate_number),
            )
            applied += cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return applied


async def official_page_research(
    db_path: str | Path,
    *,
    brands: list[str],
    max_stores: int = 0,
    dry_run: bool = False,
    concurrency: int = 12,
    timeout: float = 8.0,
    request_delay: float = 0.0,
) -> tuple[OfficialPageStats, list[OfficialPageMatch]]:
    rows = _load_unknown_stores(db_path, brands, max_stores)
    stats = OfficialPageStats(target_stores=len(rows))
    block = _load_franchisor_blocklist()
    sem = asyncio.Semaphore(max(1, concurrency))

    async def _task(row: tuple) -> OfficialPageMatch:
        async with sem:
            result = await research_one_store(
                row,
                timeout=timeout,
                franchisor_blocklist=block,
            )
            if request_delay > 0:
                await asyncio.sleep(request_delay)
            return result

    matches = await asyncio.gather(*(_task(r) for r in rows))
    for m in matches:
        if m.reject_reason != "fetch_error" and m.reject_reason != "empty_html":
            stats.fetched += 1
        if m.operator_name:
            stats.extracted += 1
        if m.corporate_number:
            stats.houjin_verified += 1
        if m.accepted:
            stats.accepted += 1
        else:
            stats.rejected.append(f"{m.place_id}:{m.reject_reason}")

    if not dry_run:
        stats.applied_rows = _apply(db_path, matches)
    return stats, matches


def _parse_brands(raw: str) -> list[str]:
    if not raw:
        return TARGET_BRANDS
    return [b.strip() for b in raw.split(",") if b.strip()]


def _main() -> None:
    ap = argparse.ArgumentParser(
        description="公式店舗ページ本文から運営会社を抽出し、国税庁CSV exact verify で採用"
    )
    ap.add_argument("--brands", default="", help="カンマ区切りブランド。空なら14ブランド")
    ap.add_argument("--db", default="var/pizza.sqlite")
    ap.add_argument("--max-stores", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--concurrency", type=int, default=12)
    ap.add_argument("--timeout", type=float, default=8.0)
    ap.add_argument(
        "--request-delay", type=float, default=0.0,
        help="fetch ごとの sleep 秒。429 回避用",
    )
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    stats, matches = asyncio.run(official_page_research(
        args.db,
        brands=_parse_brands(args.brands),
        max_stores=args.max_stores,
        dry_run=args.dry_run,
        concurrency=args.concurrency,
        timeout=args.timeout,
        request_delay=args.request_delay,
    ))
    print(f"✅ official-page-research {'dry-run' if args.dry_run else 'apply'}")
    print(f"   target_stores   = {stats.target_stores}")
    print(f"   fetched         = {stats.fetched}")
    print(f"   extracted       = {stats.extracted}")
    print(f"   houjin_verified = {stats.houjin_verified}")
    print(f"   accepted        = {stats.accepted}")
    if not args.dry_run:
        print(f"   applied_rows    = {stats.applied_rows}")
    for r in stats.rejected[:5]:
        print(f"   reject: {r}")

    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", encoding="utf-8", newline="") as f:
            fields = list(asdict(matches[0]).keys()) if matches else [
                "brand", "place_id", "store_name", "official_url", "operator_name",
                "corporate_number", "source_pattern", "matched_store_key",
                "accepted", "reject_reason",
            ]
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for m in matches:
                w.writerow(asdict(m))
        print(f"📄 matches: {args.out}")


if __name__ == "__main__":
    _main()
