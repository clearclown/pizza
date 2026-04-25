"""公式求人サイトをクロールして店舗 operator を特定する pipeline。

対象は Recop/jobfind 系の公式採用ページ。店舗詳細ページに
「事業内容 ...（募集者：株式会社XXX）」と店舗名・住所が同時に出るため、
検索 snippet や LLM 知識に頼らず、本文 gate + 国税庁 exact verify で採用できる。
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from pathlib import Path
from urllib.parse import urljoin

from pizza_delivery.recruitment_research import build_store_keys


JOBFIND_SOURCES = {
    "モスバーガー": "https://mos-recruit.net/jobfind-pc/area/All",
    "カーブス": "https://curves-job.net/jobfind-pc/area/All",
    "コメダ珈琲": "https://komeda-recruit.net/jobfind-pc/area/All",
    "シャトレーゼ": "https://chateraise-job.net/brand-jobfind/area/All?jobtype=00017%2C00018%2C00019%2C00147%2C00148%2C00149",
    "業務スーパー": "https://ocean-gyomusuper-job.net/jobfind-pc/area/All",
}

_CORP_RE = (
    r"(?:(?:株式会社|有限会社|合同会社|㈱|㈲|\(株\)|（株）)"
    r"[一-龥ぁ-んァ-ヶA-Za-z0-9・\-ー 　]{2,40}"
    r"|"
    r"[一-龥ぁ-んァ-ヶA-Za-z0-9・\-ー 　]{2,40}"
    r"(?:株式会社|有限会社|合同会社|㈱|㈲|\(株\)|（株）))"
)
_RE_OPERATOR = re.compile(
    r"(?:募集者|雇用主|運営会社|会社名|企業名|法人名|募集企業|採用企業)[：:\s]*("
    + _CORP_RE
    + r")"
)
_RE_ADDRESS = re.compile(
    r"((?:北海道|東京都|大阪府|京都府|.{2,3}県)"
    r"[一-龥ぁ-んァ-ヶ0-9０-９\-ーのノヶヶ、・\s]{5,80}"
    r"[0-9０-９][^\n]{0,30})"
)


@dataclass
class RecruitPage:
    brand: str
    url: str
    store_name: str = ""
    store_address: str = ""
    operator_name: str = ""
    corporate_number: str = ""
    matched_place_id: str = ""
    matched_store_name: str = ""
    accepted: bool = False
    reject_reason: str = ""


@dataclass
class CrawlStats:
    list_pages_fetched: int = 0
    detail_urls_found: int = 0
    detail_pages_fetched: int = 0
    operator_extracted: int = 0
    store_matched: int = 0
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


def _clean_operator_name(raw: str) -> str:
    from pizza_delivery.normalize import normalize_operator_name

    return normalize_operator_name((raw or "").strip(" 「」『』【】()（）"))


def _core_store_name(name: str, brand: str) -> str:
    s = re.sub(r"\s+", "", name or "")
    s = re.sub(r"^【[^】]+】", "", s)
    for b in sorted(
        {
            brand,
            "MOSBURGER",
            "MOS BURGER",
            "モスバーガー",
            "カーブス",
            "Curves",
            "Ｃｕｒｖｅｓ",
            "コメダ珈琲店",
            "コメダ珈琲",
            "珈琲所コメダ珈琲店",
            "シャトレーゼ",
            "業務スーパー",
            "ITTO個別指導学院",
            "Itto個別指導学院",
            "エニタイムフィットネス",
            "Anytime Fitness",
            "ハードオフ",
            "HARD OFF",
            "オフハウス",
            "OFF HOUSE",
            "Kids Duo",
            "キッズデュオ",
            "アップガレージ",
            "UP GARAGE",
            "カルビ丼とスン豆腐専門店韓丼",
            "韓丼",
            "Brand off",
            "BRAND OFF",
            "TSUTAYA",
        },
        key=len,
        reverse=True,
    ):
        if b:
            s = re.sub(rf"^{re.escape(b)}", "", s, flags=re.IGNORECASE)
    s = re.sub(r"(?:店|店舗)$", "", s)
    return s.strip(" 　-ー")


def _extract_job_links(base_url: str, html: str) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    pattern = (
        r'href=["\']([^"\']*/(?:jobfind-pc|brand-jobfind)/job/[^"\']+)["\']'
    )
    for m in re.finditer(pattern, html):
        u = urljoin(base_url, m.group(1))
        if u not in seen:
            seen.add(u)
            links.append(u)
    return links


def _parse_detail_page(brand: str, url: str, html: str) -> RecruitPage:
    out = RecruitPage(brand=brand, url=url)
    if not html:
        out.reject_reason = "empty_html"
        return out
    text = _text_from_html(html)
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        h2 = soup.find("h2")
        if h2:
            out.store_name = h2.get_text(" ", strip=True)
    except Exception:
        pass
    if not out.store_name:
        m = re.search(
            r"("
            r"モスバーガー[^\s]+店|"
            r"(?:カーブス|Curves)[^\s]+店?|"
            r"コメダ珈琲店\s*[^\s]+店|"
            r"シャトレーゼ\s*[^\s]+店|"
            r"業務スーパー[^\s]+店"
            r")",
            text,
        )
        if m:
            out.store_name = m.group(1)

    op_m = _RE_OPERATOR.search(text)
    if not op_m:
        out.reject_reason = "operator_label_missing"
        return out
    out.operator_name = _clean_operator_name(op_m.group(1))
    if not out.operator_name:
        out.reject_reason = "operator_empty"
        return out

    addresses = [m.group(1).strip() for m in _RE_ADDRESS.finditer(text)]
    # 店舗名近傍の住所を優先。無ければ最後に出る住所を採用する。
    if addresses:
        out.store_address = addresses[-1]
    return out


async def _fetch_html(url: str, timeout: float) -> str:
    from pizza_delivery.scrapling_fetcher import ScraplingFetcher

    fetcher = ScraplingFetcher(timeout_static_sec=timeout, timeout_dynamic_sec=timeout)
    return await asyncio.to_thread(fetcher.fetch_static, url) or ""


def _load_unknown_store_index(db_path: str | Path, brand: str) -> list[dict]:
    """Load stores that still lack a corporate-number verified operator.

    Earlier enrichment passes may have inserted unverified/generic operator rows.
    Official recruitment pages are stronger evidence, so do not let those weaker
    rows block a verified insert.
    """
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT s.place_id, s.name, COALESCE(s.address,''), COALESCE(s.phone,'')
            FROM stores s
            WHERE s.brand = ?
              AND s.place_id NOT IN (
                SELECT os.place_id FROM operator_stores os
                WHERE os.operator_name != ''
                  AND COALESCE(os.operator_type,'') NOT IN ('franchisor','direct')
                  AND COALESCE(os.corporate_number,'') != ''
              )
            """,
            (brand,),
        ).fetchall()
    finally:
        conn.close()
    out: list[dict] = []
    for pid, name, address, phone in rows:
        out.append({
            "place_id": pid,
            "name": name,
            "address": address,
            "phone": phone,
            "core": _core_store_name(name, brand),
            "keys": build_store_keys(name, address, phone, brand),
        })
    return out


def _address_match(page_address: str, store_address: str) -> bool:
    if not page_address or not store_address:
        return False
    p = re.sub(r"\s+", "", page_address)
    s = re.sub(r"\s+", "", store_address)
    return p in s or s in p


def _match_store(page: RecruitPage, stores: list[dict]) -> tuple[str, str, str]:
    if not page.store_name:
        return "", "", "store_name_missing"
    page_core = _core_store_name(page.store_name, page.brand)
    if not page_core:
        return "", "", "store_core_missing"

    candidates: list[dict] = []
    for s in stores:
        core = s["core"]
        if not core:
            continue
        if page_core == core or page_core in core or core in page_core:
            candidates.append(s)
    if not candidates:
        return "", "", "store_not_found"
    if page.store_address:
        addr_hits = [s for s in candidates if _address_match(page.store_address, s["address"])]
        if len(addr_hits) == 1:
            s = addr_hits[0]
            return s["place_id"], s["name"], ""
        if len(addr_hits) > 1:
            return "", "", "store_address_ambiguous"
    if len(candidates) == 1:
        s = candidates[0]
        return s["place_id"], s["name"], ""
    return "", "", "store_name_ambiguous"


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


async def _crawl_brand_details(
    brand: str,
    start_url: str,
    *,
    max_pages: int,
    timeout: float,
) -> tuple[CrawlStats, list[str]]:
    stats = CrawlStats()
    seen: set[str] = set()
    urls: list[str] = []
    for page_no in range(1, max_pages + 1):
        url = start_url if page_no == 1 else f"{start_url}?page={page_no}"
        html = await _fetch_html(url, timeout)
        if not html:
            break
        stats.list_pages_fetched += 1
        page_links = _extract_job_links(url, html)
        new_links = [u for u in page_links if u not in seen]
        if not new_links and page_no > 1:
            break
        for u in new_links:
            seen.add(u)
            urls.append(u)
    stats.detail_urls_found = len(urls)
    return stats, urls


async def crawl_official_recruitment(
    db_path: str | Path,
    *,
    brand: str,
    start_url: str,
    max_pages: int = 999,
    max_details: int = 0,
    concurrency: int = 16,
    timeout: float = 8.0,
    request_delay: float = 0.0,
    max_empty_streak: int = 30,
    dry_run: bool = False,
) -> tuple[CrawlStats, list[RecruitPage]]:
    stats, detail_urls = await _crawl_brand_details(
        brand, start_url, max_pages=max_pages, timeout=timeout,
    )
    if max_details > 0:
        detail_urls = detail_urls[:max_details]
    stores = _load_unknown_store_index(db_path, brand)
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(url: str) -> RecruitPage:
        async with sem:
            html = await _fetch_html(url, timeout)
            if request_delay > 0:
                await asyncio.sleep(request_delay)
        p = _parse_detail_page(brand, url, html)
        if html:
            stats.detail_pages_fetched += 1
        if p.operator_name:
            stats.operator_extracted += 1
        pid, store_name, reason = _match_store(p, stores)
        if not pid:
            p.reject_reason = p.reject_reason or reason
            return p
        p.matched_place_id = pid
        p.matched_store_name = store_name
        stats.store_matched += 1
        rec = _verify_houjin_exact(p.operator_name)
        if rec is None:
            p.reject_reason = "houjin_no_exact_match"
            return p
        p.operator_name = rec.name
        p.corporate_number = rec.corporate_number
        stats.houjin_verified += 1
        p.accepted = True
        return p

    pages: list[RecruitPage] = []
    empty_streak = 0
    batch_size = max(1, concurrency)
    for i in range(0, len(detail_urls), batch_size):
        batch = await asyncio.gather(*(one(u) for u in detail_urls[i:i + batch_size]))
        pages.extend(batch)
        for p in batch:
            if p.reject_reason == "empty_html":
                empty_streak += 1
            else:
                empty_streak = 0
        if max_empty_streak > 0 and empty_streak >= max_empty_streak:
            stats.rejected.append(
                f"stopped_after_empty_streak:{empty_streak}:next_index={i + batch_size}"
            )
            break
    for p in pages:
        if p.accepted:
            stats.accepted += 1
        else:
            stats.rejected.append(f"{p.url}:{p.reject_reason}")

    if not dry_run:
        conn = sqlite3.connect(db_path)
        try:
            for p in pages:
                if not p.accepted:
                    continue
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO operator_stores
                      (operator_name, place_id, brand, operator_type, confidence,
                       discovered_via, verification_score, corporate_number,
                       verification_source)
                    VALUES (?, ?, ?, 'franchisee', 0.88,
                            'official_recruit_jobfind_houjin_verified', 1.0, ?,
                            'houjin_csv')
                    """,
                    (p.operator_name, p.matched_place_id, brand, p.corporate_number),
                )
                stats.applied_rows += cur.rowcount
            conn.commit()
        finally:
            conn.close()
    return stats, pages


def _parse_sources(raw: str) -> list[tuple[str, str]]:
    if not raw:
        return list(JOBFIND_SOURCES.items())
    out: list[tuple[str, str]] = []
    for part in raw.split(","):
        if not part.strip():
            continue
        if "=" in part:
            brand, url = part.split("=", 1)
            out.append((brand.strip(), url.strip()))
        else:
            b = part.strip()
            if b in JOBFIND_SOURCES:
                out.append((b, JOBFIND_SOURCES[b]))
    return out


def _main() -> None:
    ap = argparse.ArgumentParser(description="公式 jobfind 求人ページから operator を特定")
    ap.add_argument("--db", default="var/pizza.sqlite")
    ap.add_argument("--sources", default="", help="'brand=url,brand2=url2'。空なら既知 sources")
    ap.add_argument("--max-pages", type=int, default=999)
    ap.add_argument("--max-details", type=int, default=0)
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--timeout", type=float, default=8.0)
    ap.add_argument(
        "--request-delay", type=float, default=0.0,
        help="detail fetch ごとの sleep 秒。429 回避用",
    )
    ap.add_argument(
        "--max-empty-streak", type=int, default=30,
        help="empty/429 相当が連続したら detail crawl を停止する。0 で無効",
    )
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    all_pages: list[RecruitPage] = []
    for brand, url in _parse_sources(args.sources):
        stats, pages = asyncio.run(crawl_official_recruitment(
            args.db,
            brand=brand,
            start_url=url,
            max_pages=args.max_pages,
            max_details=args.max_details,
            concurrency=args.concurrency,
            timeout=args.timeout,
            request_delay=args.request_delay,
            max_empty_streak=args.max_empty_streak,
            dry_run=args.dry_run,
        ))
        all_pages.extend(pages)
        print(f"✅ official-recruitment-crawl {'dry-run' if args.dry_run else 'apply'} brand={brand}")
        print(f"   list_pages_fetched   = {stats.list_pages_fetched}")
        print(f"   detail_urls_found    = {stats.detail_urls_found}")
        print(f"   detail_pages_fetched = {stats.detail_pages_fetched}")
        print(f"   operator_extracted   = {stats.operator_extracted}")
        print(f"   store_matched        = {stats.store_matched}")
        print(f"   houjin_verified      = {stats.houjin_verified}")
        print(f"   accepted             = {stats.accepted}")
        if not args.dry_run:
            print(f"   applied_rows         = {stats.applied_rows}")
        for r in stats.rejected[:3]:
            print(f"   reject: {r}")
    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        fields = list(asdict(all_pages[0]).keys()) if all_pages else list(asdict(RecruitPage("", "")).keys())
        with out.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for p in all_pages:
                w.writerow(asdict(p))
        print(f"📄 matches: {args.out}")


if __name__ == "__main__":
    _main()
