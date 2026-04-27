"""operator 公式サイトから FC ブランド運営 link を追加収集する。

入力は ORM 上で既に確認済みの operator 公式 URL。検索スニペットや LLM 知識は
使わず、operator 公式サイトの HTML anchor / 画像 alt だけを evidence にする。
店舗数根拠はこの経路では増やさないため、追加 link は estimated_store_count=0。
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Protocol
from urllib.parse import urldefrag, urljoin, urlparse

from pizza_delivery.operator_spider import (
    canonical_fc_brand_name,
    extract_brand_candidates_from_html,
)
from pizza_delivery.scrapling_fetcher import ScraplingFetcher


SOURCE = "operator_official_brand_link"

_BUSINESS_LINK_HINTS = (
    "事業",
    "事業内容",
    "事業紹介",
    "ブランド",
    "運営",
    "店舗",
    "サービス",
    "フランチャイズ",
    "business",
    "brand",
    "service",
    "shop",
    "store",
    "franchise",
)

_SKIP_HREF_PREFIXES = ("mailto:", "tel:", "javascript:")
_SKIP_PATH_SUFFIXES = (
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".svg",
    ".zip",
)
_REVIEW_LINK_HINTS = (
    "news",
    "topics",
    "ir/",
    "recruit",
    "career",
    "job",
    "jobdescription",
    "staff",
    "voice",
    "interview",
    "採用",
    "求人",
    "募集",
    "社員",
    "オープン",
    "休業",
    "閉店",
    "臨時",
    "?p=",
    "お知らせ",
    "ニュース",
)


class FetcherWithMode(Protocol):
    def fetch_with_mode(self, url: str, mode: str = "auto") -> str | None: ...


@dataclass(frozen=True)
class OperatorSeed:
    operator_id: int
    operator_name: str
    corporate_number: str
    website_url: str
    operator_total_stores_est: int


@dataclass
class DiscoveryRow:
    operator_id: int
    operator_name: str
    corporate_number: str
    website_url: str
    operator_total_stores_est: int
    page_url: str
    brand_name: str
    anchor_text: str
    href: str
    status: str
    reason: str = ""
    applied: str = ""


@dataclass(frozen=True)
class DiscoveryStats:
    operators_considered: int
    pages_fetched: int
    rows: int
    accepted: int
    applied: int
    fetch_failed: int


def _clean_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, flags=re.IGNORECASE):
        u = "https://" + u
    return u


def _host(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _valid_http_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and "." in parsed.netloc


def _is_same_site(base_url: str, target_url: str) -> bool:
    return bool(_host(base_url) and _host(base_url) == _host(target_url))


def _defrag(url: str) -> str:
    return urldefrag(url).url


def _iter_anchor_links(html: str) -> list[tuple[str, str]]:
    if not html:
        return []
    try:
        from bs4 import BeautifulSoup  # type: ignore

        soup = BeautifulSoup(html, "html.parser")
        out: list[tuple[str, str]] = []
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
            out.append((href, anchor))
        return out
    except Exception:
        link_re = re.compile(
            r'<a\s+[^>]*href="([^"]+)"[^>]*>([^<]*)</a>',
            re.IGNORECASE | re.DOTALL,
        )
        return [(m.group(1), re.sub(r"<[^>]+>", " ", m.group(2))) for m in link_re.finditer(html)]


def find_business_links(base_url: str, html: str, *, max_links: int = 4) -> list[str]:
    """operator 公式内の事業/ブランド紹介ページらしい same-site link を返す。"""
    out: list[str] = []
    seen: set[str] = set()
    for href, anchor in _iter_anchor_links(html):
        href = (href or "").strip()
        if not href or href.startswith("#") or href.lower().startswith(_SKIP_HREF_PREFIXES):
            continue
        abs_url = _defrag(urljoin(base_url, href))
        if not _is_same_site(base_url, abs_url):
            continue
        if urlparse(abs_url).path.lower().endswith(_SKIP_PATH_SUFFIXES):
            continue
        if abs_url.rstrip("/") == base_url.rstrip("/"):
            continue
        haystack = f"{anchor} {href}".casefold()
        if not any(h.casefold() in haystack for h in _BUSINESS_LINK_HINTS):
            continue
        if abs_url in seen:
            continue
        seen.add(abs_url)
        out.append(abs_url)
        if len(out) >= max_links:
            break
    return out


def load_candidate_operators(
    orm_db: str | Path,
    *,
    min_total: int = 20,
    limit: int = 0,
) -> list[OperatorSeed]:
    conn = sqlite3.connect(orm_db)
    conn.row_factory = sqlite3.Row
    try:
        sql = """
        WITH per_brand AS (
          SELECT operator_id, brand_id, MAX(COALESCE(estimated_store_count, 0)) AS cnt
          FROM brand_operator_link
          GROUP BY operator_id, brand_id
        ),
        totals AS (
          SELECT operator_id, SUM(cnt) AS total
          FROM per_brand
          GROUP BY operator_id
        )
        SELECT
          o.id AS operator_id,
          o.name AS operator_name,
          COALESCE(o.corporate_number, '') AS corporate_number,
          COALESCE(o.website_url, '') AS website_url,
          COALESCE(t.total, 0) AS operator_total_stores_est
        FROM operator_company o
        JOIN totals t ON t.operator_id = o.id
        WHERE COALESCE(o.website_url, '') != ''
          AND COALESCE(t.total, 0) >= ?
          AND EXISTS (
            SELECT 1
            FROM brand_operator_link l
            WHERE l.operator_id = o.id
              AND COALESCE(l.operator_type, '') NOT IN ('franchisor', 'direct')
          )
        ORDER BY COALESCE(t.total, 0) DESC, o.name
        """
        params: list[object] = [min_total]
        if limit > 0:
            sql += " LIMIT ?"
            params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        return [
            OperatorSeed(
                operator_id=int(r["operator_id"]),
                operator_name=str(r["operator_name"] or ""),
                corporate_number=str(r["corporate_number"] or ""),
                website_url=_clean_url(str(r["website_url"] or "")),
                operator_total_stores_est=int(r["operator_total_stores_est"] or 0),
            )
            for r in rows
            if _valid_http_url(_clean_url(str(r["website_url"] or "")))
        ]
    finally:
        conn.close()


def load_existing_brand_names(orm_db: str | Path, operator_id: int) -> set[str]:
    conn = sqlite3.connect(orm_db)
    try:
        rows = conn.execute(
            """
            SELECT b.name
            FROM brand_operator_link l
            JOIN franchise_brand b ON b.id = l.brand_id
            WHERE l.operator_id = ?
            """,
            (operator_id,),
        ).fetchall()
        return {str(r[0]) for r in rows if r[0]}
    finally:
        conn.close()


async def _fetch_html(
    fetcher: FetcherWithMode,
    url: str,
    mode: str,
) -> str:
    return await asyncio.to_thread(fetcher.fetch_with_mode, url, mode) or ""


async def discover_for_operator(
    seed: OperatorSeed,
    *,
    orm_db: str | Path,
    fetcher: FetcherWithMode,
    fetcher_mode: str = "static",
    max_follow_links: int = 4,
    brand_filter: set[str] | None = None,
    allow_external_links: bool = False,
) -> tuple[list[DiscoveryRow], int, bool]:
    """1 operator の公式サイトを巡回し、追加できる brand link 候補を返す。"""
    root_url = _clean_url(seed.website_url)
    try:
        root_html = await _fetch_html(fetcher, root_url, fetcher_mode)
    except Exception as e:
        return ([
            DiscoveryRow(
                operator_id=seed.operator_id,
                operator_name=seed.operator_name,
                corporate_number=seed.corporate_number,
                website_url=root_url,
                operator_total_stores_est=seed.operator_total_stores_est,
                page_url=root_url,
                brand_name="",
                anchor_text="",
                href="",
                status="fetch_failed",
                reason=str(e),
            )
        ], 0, True)
    if not root_html:
        return ([
            DiscoveryRow(
                operator_id=seed.operator_id,
                operator_name=seed.operator_name,
                corporate_number=seed.corporate_number,
                website_url=root_url,
                operator_total_stores_est=seed.operator_total_stores_est,
                page_url=root_url,
                brand_name="",
                anchor_text="",
                href="",
                status="fetch_failed",
                reason="empty_response",
            )
        ], 0, True)

    pages: list[tuple[str, str]] = [(root_url, root_html)]
    for link in find_business_links(root_url, root_html, max_links=max_follow_links):
        try:
            html = await _fetch_html(fetcher, link, fetcher_mode)
        except Exception:
            continue
        if html:
            pages.append((link, html))

    existing = load_existing_brand_names(orm_db, seed.operator_id)
    out: list[DiscoveryRow] = []
    seen: set[str] = set()
    for page_url, html in pages:
        for cand in extract_brand_candidates_from_html(html, base_url=page_url):
            brand = canonical_fc_brand_name(cand.brand_name)
            if brand_filter is not None and brand not in brand_filter:
                continue
            href = _defrag(cand.href)
            key = f"{brand}|{href}|{_defrag(page_url)}"
            if key in seen:
                continue
            seen.add(key)
            status = "accepted"
            reason = "same_site_operator_anchor"
            if brand in existing:
                status = "existing_link"
                reason = "operator_already_has_brand_link"
            elif href and not _is_same_site(root_url, href):
                if allow_external_links:
                    status = "accepted"
                    reason = "external_anchor_allowed"
                else:
                    status = "external_link_review"
                    reason = "external_href_not_auto_applied"
            haystack = f"{cand.anchor_text} {href} {page_url}".casefold()
            if status == "accepted" and any(h.casefold() in haystack for h in _REVIEW_LINK_HINTS):
                status = "review_link_not_auto_applied"
                reason = "news_recruit_or_ir_context"
            out.append(
                DiscoveryRow(
                    operator_id=seed.operator_id,
                    operator_name=seed.operator_name,
                    corporate_number=seed.corporate_number,
                    website_url=root_url,
                    operator_total_stores_est=seed.operator_total_stores_est,
                    page_url=page_url,
                    brand_name=brand,
                    anchor_text=cand.anchor_text,
                    href=href,
                    status=status,
                    reason=reason,
                )
            )
    return out, len(pages), False


def _get_or_create_brand(conn: sqlite3.Connection, brand_name: str) -> int:
    row = conn.execute(
        "SELECT id FROM franchise_brand WHERE name = ?",
        (brand_name,),
    ).fetchone()
    if row:
        return int(row[0])
    cur = conn.execute(
        """
        INSERT INTO franchise_brand
          (name, industry, master_franchisor_name, master_franchisor_corp,
           jfa_member, source, fc_recruitment_url)
        VALUES (?, '', '', '', 0, ?, '')
        """,
        (brand_name, SOURCE),
    )
    return int(cur.lastrowid)


def apply_discoveries(orm_db: str | Path, rows: list[DiscoveryRow]) -> int:
    accepted_by_key: dict[tuple[int, str], DiscoveryRow] = {}
    for row in rows:
        if row.status == "accepted" and row.brand_name:
            accepted_by_key[(row.operator_id, row.brand_name)] = row
    accepted = list(accepted_by_key.values())
    if not accepted:
        return 0
    applied = 0
    observed_at = date.today().isoformat()
    conn = sqlite3.connect(orm_db)
    try:
        for row in accepted:
            brand_id = _get_or_create_brand(conn, row.brand_name)
            existing = conn.execute(
                """
                SELECT id
                FROM brand_operator_link
                WHERE brand_id = ? AND operator_id = ? AND source = ?
                """,
                (brand_id, row.operator_id, SOURCE),
            ).fetchone()
            note = f"anchor={row.anchor_text}; href={row.href}; page={row.page_url}"
            if existing:
                conn.execute(
                    """
                    UPDATE brand_operator_link
                    SET observed_at = ?, operator_type = 'unknown',
                        source_url = ?, note = ?
                    WHERE id = ?
                    """,
                    (observed_at, row.page_url, note, int(existing[0])),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO brand_operator_link
                      (brand_id, operator_id, estimated_store_count, observed_at,
                       operator_type, source, source_url, note)
                    VALUES (?, ?, 0, ?, 'unknown', ?, ?, ?)
                    """,
                    (brand_id, row.operator_id, observed_at, SOURCE, row.page_url, note),
                )
            row.applied = "1"
            applied += 1
        conn.commit()
        return applied
    finally:
        conn.close()


def write_discovery_csv(path: str | Path, rows: list[DiscoveryRow]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(DiscoveryRow.__annotations__.keys())
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        w.writeheader()
        for row in rows:
            w.writerow(asdict(row))


async def discover_operator_brand_links(
    *,
    orm_db: str | Path = "var/pizza-registry.sqlite",
    out: str | Path = "var/phase28/operator-brand-discovery.csv",
    min_total: int = 20,
    limit: int = 0,
    concurrency: int = 8,
    timeout: float = 8.0,
    fetcher_mode: str = "static",
    max_follow_links: int = 4,
    brands: set[str] | None = None,
    allow_external_links: bool = False,
    dry_run: bool = False,
    fetcher: FetcherWithMode | None = None,
) -> DiscoveryStats:
    seeds = load_candidate_operators(orm_db, min_total=min_total, limit=limit)
    effective_fetcher = fetcher or ScraplingFetcher(
        timeout_static_sec=timeout,
        timeout_dynamic_sec=max(timeout, 8.0),
    )
    sem = asyncio.Semaphore(max(1, concurrency))
    all_rows: list[DiscoveryRow] = []
    pages_fetched = 0
    fetch_failed = 0

    async def run_one(seed: OperatorSeed) -> tuple[list[DiscoveryRow], int, bool]:
        async with sem:
            return await discover_for_operator(
                seed,
                orm_db=orm_db,
                fetcher=effective_fetcher,
                fetcher_mode=fetcher_mode,
                max_follow_links=max_follow_links,
                brand_filter=brands,
                allow_external_links=allow_external_links,
            )

    for rows, pages, failed in await asyncio.gather(*(run_one(seed) for seed in seeds)):
        all_rows.extend(rows)
        pages_fetched += pages
        if failed:
            fetch_failed += 1

    applied = 0
    if not dry_run:
        applied = apply_discoveries(orm_db, all_rows)
    write_discovery_csv(out, all_rows)
    accepted = sum(1 for r in all_rows if r.status == "accepted")
    return DiscoveryStats(
        operators_considered=len(seeds),
        pages_fetched=pages_fetched,
        rows=len(all_rows),
        accepted=accepted,
        applied=applied,
        fetch_failed=fetch_failed,
    )


def _brand_filter(raw: str) -> set[str] | None:
    if not raw.strip():
        return None
    return {
        canonical_fc_brand_name(part.strip())
        for part in raw.split(",")
        if part.strip()
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description="operator 公式サイトの事業/ブランドリンクから FC brand link を追加収集"
    )
    ap.add_argument("--orm-db", default="var/pizza-registry.sqlite")
    ap.add_argument("--out", default="var/phase28/nationwide-coverage/operator-brand-discovery.csv")
    ap.add_argument("--min-total", type=int, default=20)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--timeout", type=float, default=8.0)
    ap.add_argument("--fetcher", default="static", choices=["static", "dynamic", "camofox", "auto"])
    ap.add_argument("--max-follow-links", type=int, default=4)
    ap.add_argument("--brands", default="", help="カンマ区切り brand filter。空なら既知FC全体")
    ap.add_argument(
        "--allow-external-links",
        action="store_true",
        help="operator 公式上の外部 href brand anchor も自動反映する",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    stats = asyncio.run(
        discover_operator_brand_links(
            orm_db=args.orm_db,
            out=args.out,
            min_total=args.min_total,
            limit=args.limit,
            concurrency=args.concurrency,
            timeout=args.timeout,
            fetcher_mode=args.fetcher,
            max_follow_links=args.max_follow_links,
            brands=_brand_filter(args.brands),
            allow_external_links=args.allow_external_links,
            dry_run=args.dry_run,
        )
    )
    print(
        "operator-brand-discovery: "
        f"operators={stats.operators_considered} pages={stats.pages_fetched} "
        f"rows={stats.rows} accepted={stats.accepted} applied={stats.applied} "
        f"fetch_failed={stats.fetch_failed} out={args.out}"
    )


if __name__ == "__main__":
    main()
