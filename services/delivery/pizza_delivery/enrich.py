"""Places Details + browser-use 逆引きで operator を一括抽出。

Mos 等「公式ページに FC 加盟店名が載らない」ブランドでも、Google Maps の
**店舗電話番号は加盟店個別のもの**であることを利用し、電話番号 → 会社名
(iタウンページ等) で運営会社を突き止める一括 pipeline。

処理フロー (1 店舗あたり):
  1. stores テーブルから place_id を取得 (operator 未確定の店舗のみ)
  2. PlacesClient.get_place_details(place_id) で phone 取得
  3. BrowserScraper.lookup_operator_by_phone(phone) で会社名逆引き
  4. operator_stores に upsert (discovered_via='enrich_phone_lookup')

- Places Details は課金、rate limit あり → 既存 phone なら skip
- browser-use は rate_limit_sec で同ホスト連打を防止
- 全体 max_stores で ガード (運用中の暴走防止)

CLI:
    pizza enrich --brand モスバーガー --max-stores 50
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class EnrichStats:
    """1 回の enrich 実行の集計。"""

    total_candidates: int = 0
    details_fetched: int = 0
    phones_obtained: int = 0
    operators_found: int = 0
    errors: list[str] = field(default_factory=list)


# ─── DB helpers ────────────────────────────────────────────


def _candidate_stores(
    db_path: str | Path,
    *,
    brand: str = "",
    max_stores: int = 0,
) -> list[tuple[str, str, str, str, str]]:
    """operator 未確定の店舗 (place_id, name, address, phone, official_url) を返す。

    - stores テーブルから brand 指定で拾う
    - operator_stores に per_store / enrich 経由の record が既にある place_id は除外
    - phone 欄が空の店舗を優先 (Places Details を追加で叩く対象)
    """
    conn = sqlite3.connect(db_path)
    try:
        q = (
            "SELECT s.place_id, s.name, s.address, "
            "       COALESCE(s.phone,'') AS phone, "
            "       COALESCE(s.official_url,'') AS url "
            "FROM stores s "
            "LEFT JOIN operator_stores os "
            "  ON os.place_id = s.place_id "
            "  AND os.discovered_via IN ('per_store','chain_verified','enrich_phone_lookup') "
            "WHERE os.place_id IS NULL "
        )
        args: list = []
        if brand:
            q += " AND s.brand = ? "
            args.append(brand)
        q += " ORDER BY (s.phone IS NULL OR s.phone = '') DESC, s.place_id "
        if max_stores > 0:
            q += " LIMIT ? "
            args.append(max_stores)
        return conn.execute(q, args).fetchall()
    finally:
        conn.close()


def _upsert_phone(db_path: str | Path, place_id: str, phone: str) -> None:
    """stores.phone を Places Details 取得結果で補完する。"""
    if not phone:
        return
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE stores SET phone = ? WHERE place_id = ? AND (phone IS NULL OR phone = '')",
            (phone, place_id),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_operator_store(
    db_path: str | Path,
    *,
    place_id: str,
    brand: str,
    operator_name: str,
    corporate_number: str = "",
    confidence: float = 0.5,
) -> None:
    """operator_stores に 1 行追加 (PK 重複は skip)。"""
    if not operator_name:
        return
    conn = sqlite3.connect(db_path)
    try:
        cols = {
            r[1] for r in conn.execute("PRAGMA table_info(operator_stores)").fetchall()
        }
        if "verification_score" in cols:
            conn.execute(
                """
                INSERT OR IGNORE INTO operator_stores
                  (operator_name, place_id, brand, operator_type, confidence,
                   discovered_via, verification_score, corporate_number,
                   verification_source)
                VALUES (?, ?, ?, 'franchisee', ?, 'enrich_phone_lookup',
                        0.7, ?, 'browser_phone_lookup')
                """,
                (operator_name, place_id, brand, confidence, corporate_number),
            )
        else:
            conn.execute(
                """
                INSERT OR IGNORE INTO operator_stores
                  (operator_name, place_id, brand, operator_type, confidence,
                   discovered_via)
                VALUES (?, ?, ?, 'franchisee', ?, 'enrich_phone_lookup')
                """,
                (operator_name, place_id, brand, confidence),
            )
        conn.commit()
    finally:
        conn.close()


# ─── Orchestrator ─────────────────────────────────────────


@dataclass
class Enricher:
    """一括 enrich の coordinator。テストで依存を inject 可能。"""

    places_client: Any = None           # PlacesClient
    browser_scraper: Any = None         # BrowserScraper
    details_concurrency: int = 4        # Places Details の並列上限
    lookup_concurrency: int = 2         # browser-use の並列上限 (low!)

    def _resolve_deps(self) -> None:
        if self.places_client is None:
            from pizza_delivery.places_client import PlacesClient

            self.places_client = PlacesClient()
        if self.browser_scraper is None:
            from pizza_delivery.browser_scraper import BrowserScraper

            self.browser_scraper = BrowserScraper()

    async def enrich(
        self,
        *,
        db_path: str | Path,
        brand: str = "",
        max_stores: int = 50,
    ) -> EnrichStats:
        self._resolve_deps()
        stats = EnrichStats()
        stores = _candidate_stores(db_path, brand=brand, max_stores=max_stores)
        stats.total_candidates = len(stores)
        if not stores:
            return stats

        # Step 1: Places Details で phone 補完 (並列、ただし 4 並列くらい)
        det_sem = asyncio.Semaphore(self.details_concurrency)

        async def _fetch_phone(pid: str, existing_phone: str) -> str:
            if existing_phone:
                return existing_phone
            async with det_sem:
                try:
                    details = await self.places_client.get_place_details(pid)
                except Exception as e:
                    stats.errors.append(f"details {pid}: {e}")
                    return ""
                if details is None:
                    return ""
                stats.details_fetched += 1
                if details.phone:
                    _upsert_phone(db_path, pid, details.phone)
                    return details.phone
                return ""

        phones = await asyncio.gather(
            *(_fetch_phone(s[0], s[3]) for s in stores), return_exceptions=False,
        )
        stats.phones_obtained = sum(1 for p in phones if p)

        # Step 2: browser-use で 電話逆引き (より慎重に並列制限)
        look_sem = asyncio.Semaphore(self.lookup_concurrency)

        async def _lookup(pid: str, phone: str, store_name: str) -> None:
            if not phone:
                return
            async with look_sem:
                try:
                    info = await self.browser_scraper.lookup_operator_by_phone(
                        phone, brand_hint=brand,
                    )
                except Exception as e:
                    stats.errors.append(f"lookup {pid} phone={phone}: {e}")
                    return
                if info is None or not info.name:
                    return
                _insert_operator_store(
                    db_path,
                    place_id=pid,
                    brand=brand,
                    operator_name=info.name,
                    corporate_number=info.corporate_number,
                    confidence=info.confidence or 0.5,
                )
                stats.operators_found += 1

        await asyncio.gather(
            *(
                _lookup(s[0], phones[i], s[1])
                for i, s in enumerate(stores)
            ),
            return_exceptions=False,
        )
        return stats


# ─── CLI ───────────────────────────────────────────────────


def _main() -> None:
    import argparse
    import sys

    ap = argparse.ArgumentParser(
        description="Places Details + browser-use 逆引きで operator を一括特定"
    )
    ap.add_argument("--brand", default="", help="対象ブランド (空で全件)")
    ap.add_argument(
        "--db", default="var/pizza.sqlite", help="pipeline SQLite"
    )
    ap.add_argument("--max-stores", type=int, default=50)
    ap.add_argument(
        "--details-concurrency", type=int, default=4
    )
    ap.add_argument(
        "--lookup-concurrency", type=int, default=2,
        help="browser-use 並列数 (rate limit の観点で低めに)"
    )
    args = ap.parse_args()

    if not Path(args.db).exists():
        print(f"❌ db not found: {args.db}", file=sys.stderr)
        sys.exit(2)

    enricher = Enricher(
        details_concurrency=args.details_concurrency,
        lookup_concurrency=args.lookup_concurrency,
    )
    stats = asyncio.run(
        enricher.enrich(db_path=args.db, brand=args.brand, max_stores=args.max_stores)
    )
    print(f"✅ enrich done")
    print(f"   candidates       = {stats.total_candidates}")
    print(f"   details_fetched  = {stats.details_fetched}")
    print(f"   phones_obtained  = {stats.phones_obtained}")
    print(f"   operators_found  = {stats.operators_found}")
    for e in stats.errors[:10]:
        print(f"   ⚠️  {e}", file=sys.stderr)
    if len(stats.errors) > 10:
        print(f"   (and {len(stats.errors) - 10} more errors)", file=sys.stderr)


if __name__ == "__main__":
    _main()
