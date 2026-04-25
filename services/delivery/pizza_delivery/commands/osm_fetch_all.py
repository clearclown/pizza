"""pizza osm-fetch-all — Places API quota 切れ時の代替経路。

OSM Overpass API で 11 brand (or 任意 brand list) を全国 bbox 取得し、
brand name で filter して pipeline `stores` table に upsert する。

特徴:
  - 完全無料 (Overpass API、quota なし)
  - precision: brand:ja タグでフィルタ → 60-70% (誤検出は同業他店)
  - recall: 20-50% (OSM の日本カバレッジは Places より低い、tag 不明の店多)
  - SQL pipeline 既存 stores schema 互換 (place_id を `osm:<id>` 形式で挿入)

使い方:
  pizza osm-fetch-all --brands "Itto個別指導学院,エニタイム,..." \
      --db var/pizza.sqlite

Notes:
  - OSM の Overpass API には server-side rate limit あり (1 instance あたり 2/sec、
    burst 限度あり)。並列ではなく順次で。
  - tag 未対応 brand は skip (warning のみ)。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


# 日本本土 (北海道~沖縄を粗く包含する bbox)。離島の一部は外れる。
JAPAN_BBOX = (24.0, 122.0, 46.0, 154.0)


@dataclass
class FetchStats:
    brands_processed: int = 0
    brands_skipped: int = 0
    osm_total: int = 0
    upserted: int = 0
    filtered_out: int = 0
    errors: list[str] = field(default_factory=list)


def _matches_brand(osm_name: str, osm_tags: dict, brand: str) -> bool:
    """OSM の name / brand:ja / brand tag に対象 brand 名が含まれるか。

    完全一致だけでなく substring も許容 (「TSUTAYA 銀座店」等のため)。
    """
    if not brand:
        return False
    name = osm_name or ""
    candidates = [
        name,
        osm_tags.get("name", ""),
        osm_tags.get("name:ja", ""),
        osm_tags.get("brand", ""),
        osm_tags.get("brand:ja", ""),
        osm_tags.get("operator", ""),
        osm_tags.get("operator:ja", ""),
    ]
    return any(brand in c for c in candidates if c)


def _upsert_store(conn: sqlite3.Connection, *,
                  brand: str, name: str, address: str,
                  lat: float, lng: float, osm_id: int) -> bool:
    """pipeline `stores` テーブルに upsert (重複 place_id は skip)。"""
    place_id = f"osm:{osm_id}"
    try:
        cur = conn.execute(
            "INSERT OR IGNORE INTO stores "
            "(place_id, brand, name, address, lat, lng, official_url, phone, grid_cell_id) "
            "VALUES (?, ?, ?, ?, ?, ?, '', '', '')",
            (place_id, brand, name, address, lat, lng),
        )
        return cur.rowcount > 0
    except Exception as e:
        logger.warning("upsert failed %s: %s", place_id, e)
        return False


async def fetch_brand_via_osm(brand: str, db_path: str) -> tuple[int, int, int]:
    """1 brand を OSM 全国 fetch + brand filter + DB upsert。

    Returns (osm_total, upserted, filtered_out).
    """
    from pizza_delivery.osm_overpass import OverpassClient, brand_to_osm_tags

    tags = brand_to_osm_tags(brand)
    if not tags:
        logger.warning("brand %r has no OSM tag mapping; skip", brand)
        return 0, 0, 0

    client = OverpassClient(timeout=120.0)
    all_places: list = []
    seen_ids: set[int] = set()
    for tag in tags:
        try:
            places = await client.query_by_tag(tag=tag, bbox=JAPAN_BBOX)
        except Exception as e:
            logger.warning("Overpass query failed for %s/%s: %s", brand, tag, e)
            continue
        for p in places:
            if p.osm_id in seen_ids:
                continue
            seen_ids.add(p.osm_id)
            all_places.append(p)

    osm_total = len(all_places)
    upserted = 0
    filtered_out = 0
    conn = sqlite3.connect(db_path)
    try:
        for p in all_places:
            if not _matches_brand(p.name, p.tags, brand):
                filtered_out += 1
                continue
            if _upsert_store(
                conn, brand=brand, name=p.name, address=p.address,
                lat=p.lat, lng=p.lng, osm_id=p.osm_id,
            ):
                upserted += 1
        conn.commit()
    finally:
        conn.close()
    return osm_total, upserted, filtered_out


async def run_all(brands: list[str], db_path: str) -> FetchStats:
    stats = FetchStats()
    if not Path(db_path).exists():
        stats.errors.append(f"db not found: {db_path}")
        return stats
    for brand in brands:
        try:
            osm_total, upserted, filtered = await fetch_brand_via_osm(brand, db_path)
            stats.osm_total += osm_total
            stats.upserted += upserted
            stats.filtered_out += filtered
            if osm_total == 0:
                stats.brands_skipped += 1
                logger.info("[skip] %s — no OSM tag mapping", brand)
            else:
                stats.brands_processed += 1
                logger.info(
                    "[done] %s — osm=%d upserted=%d filtered=%d",
                    brand, osm_total, upserted, filtered,
                )
        except Exception as e:
            stats.errors.append(f"{brand}: {e}")
            logger.exception("fetch_brand_via_osm failed for %s", brand)
    return stats


def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    ap = argparse.ArgumentParser(
        description="OSM Overpass で全国 brand 店舗を fetch + pipeline DB upsert (Places API 不要)",
    )
    ap.add_argument("--brands", required=True,
                    help="カンマ区切り brand list")
    ap.add_argument("--db", default="var/pizza.sqlite")
    args = ap.parse_args()

    brands = [b.strip() for b in args.brands.split(",") if b.strip()]
    stats = asyncio.run(run_all(brands, args.db))

    print(f"✅ osm-fetch-all done")
    print(f"   brands processed = {stats.brands_processed}")
    print(f"   brands skipped   = {stats.brands_skipped} (no OSM tag)")
    print(f"   osm_total        = {stats.osm_total}")
    print(f"   upserted         = {stats.upserted}")
    print(f"   filtered_out     = {stats.filtered_out} (brand name 不一致)")
    for e in stats.errors[:5]:
        print(f"   ⚠  {e}")


if __name__ == "__main__":
    _main()
