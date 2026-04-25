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
    operator_tags_captured: int = 0
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


def _load_franchisor_blocklist(brand: str) -> set[str]:
    """対象 brand の本部名を registry から取得して operator tag 誤採用を防ぐ。"""
    try:
        from pizza_delivery.normalize import canonical_key
        from pizza_delivery.orm import FranchiseBrand, OperatorCompany, make_session
    except Exception:
        return set()

    names = {brand}
    try:
        sess = make_session()
        try:
            rows = sess.query(FranchiseBrand).filter(FranchiseBrand.name == brand).all()
            for b in rows:
                if b.master_franchisor_name:
                    names.add(b.master_franchisor_name)
            rows2 = sess.query(OperatorCompany).filter_by(kind="franchisor").all()
            for op in rows2:
                if op.name:
                    names.add(op.name)
        finally:
            sess.close()
    except Exception:
        pass
    return {canonical_key(n) or n for n in names if n}


def _operator_from_osm_tags(
    osm_tags: dict,
    *,
    brand: str,
    franchisor_blocklist: set[str] | None = None,
) -> str:
    """OSM operator tag から法人名を保守的に抽出する。

    OSM は外部 user-maintained source なので法人番号 verified ではない。
    そのため、会社形態を含む値だけを `osm_operator_tag_unverified` として採用する。
    """
    try:
        from pizza_delivery.normalize import canonical_key, operators_match
    except Exception:
        canonical_key = lambda x: x  # type: ignore  # noqa: E731
        operators_match = lambda a, b: a == b  # type: ignore  # noqa: E731

    candidates = [
        osm_tags.get("operator:ja", ""),
        osm_tags.get("operator", ""),
        osm_tags.get("owner:ja", ""),
        osm_tags.get("owner", ""),
    ]
    blocklist = {canonical_key(n) or n for n in (franchisor_blocklist or set())}
    brand_key = canonical_key(brand) or brand
    def _core(s: str) -> str:
        x = (canonical_key(s) or s).replace("株式会社", "").replace("有限会社", "")
        x = x.replace("合同会社", "").replace("・", "").replace(" ", "")
        return x

    blocked_cores = {_core(n) for n in (franchisor_blocklist or set()) if n}

    for raw in candidates:
        op = str(raw or "").strip()
        if not op:
            continue
        if not any(s in op for s in ("株式会社", "有限会社", "合同会社", "Inc.", "Co., Ltd")):
            continue
        key = canonical_key(op) or op
        if key == brand_key or brand_key in key or key in blocklist:
            continue
        if any(operators_match(op, blocked) for blocked in (franchisor_blocklist or set())):
            continue
        op_core = _core(op)
        if any(
            len(bc) >= 4 and len(op_core) >= 4 and (
                op_core.startswith(bc[:4]) or bc.startswith(op_core[:4])
            )
            for bc in blocked_cores
        ):
            continue
        return op
    return ""


def _osm_place_id(osm_id: int, osm_type: str = "node") -> str:
    """既存互換: node は従来の osm:<id>、way/relation は型付き ID。"""
    return f"osm:{osm_id}" if osm_type == "node" else f"osm:{osm_type}:{osm_id}"


def _upsert_store(conn: sqlite3.Connection, *,
                  brand: str, name: str, address: str,
                  lat: float, lng: float, osm_id: int,
                  osm_type: str = "node") -> bool:
    """pipeline `stores` テーブルに upsert (重複 place_id は skip)。"""
    place_id = _osm_place_id(osm_id, osm_type)
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


def _upsert_operator_from_osm(
    conn: sqlite3.Connection,
    *,
    brand: str,
    operator_name: str,
    osm_id: int,
    osm_type: str = "node",
) -> bool:
    """OSM operator tag を operator_stores に採用する。法人番号は後段 cleanse 対象。"""
    if not operator_name:
        return False
    try:
        cur = conn.execute(
            "INSERT OR IGNORE INTO operator_stores "
            "(operator_name, place_id, brand, operator_type, confidence, "
            " discovered_via, corporate_number, verification_source) "
            "VALUES (?, ?, ?, 'franchisee', 0.55, "
            "        'osm_operator_tag_unverified', '', 'osm_operator_tag')",
            (operator_name, _osm_place_id(osm_id, osm_type), brand),
        )
        return cur.rowcount > 0
    except Exception as e:
        logger.warning("operator upsert failed osm:%s:%s: %s", osm_type, osm_id, e)
        return False


async def fetch_brand_via_osm(brand: str, db_path: str) -> tuple[int, int, int, int]:
    """1 brand を OSM 全国 fetch + brand filter + DB upsert。

    Returns (osm_total, upserted, operator_tags_captured, filtered_out).
    """
    from pizza_delivery.osm_overpass import OverpassClient, brand_to_osm_tags

    tags = brand_to_osm_tags(brand)
    if not tags:
        logger.warning("brand %r has no OSM tag mapping; skip", brand)
        return 0, 0, 0, 0

    client = OverpassClient(timeout=120.0)
    all_places: list = []
    seen_ids: set[tuple[str, int]] = set()
    for tag in tags:
        try:
            places = await client.query_by_tag(tag=tag, bbox=JAPAN_BBOX)
        except Exception as e:
            logger.warning("Overpass query failed for %s/%s: %s", brand, tag, e)
            continue
        for p in places:
            key = (getattr(p, "osm_type", "node"), p.osm_id)
            if key in seen_ids:
                continue
            seen_ids.add(key)
            all_places.append(p)

    osm_total = len(all_places)
    upserted = 0
    operator_tags_captured = 0
    filtered_out = 0
    franchisor_blocklist = _load_franchisor_blocklist(brand)
    conn = sqlite3.connect(db_path)
    try:
        for p in all_places:
            if not _matches_brand(p.name, p.tags, brand):
                filtered_out += 1
                continue
            if _upsert_store(
                conn, brand=brand, name=p.name, address=p.address,
                lat=p.lat, lng=p.lng, osm_id=p.osm_id,
                osm_type=getattr(p, "osm_type", "node"),
            ):
                upserted += 1
            op = _operator_from_osm_tags(
                p.tags, brand=brand, franchisor_blocklist=franchisor_blocklist,
            )
            if _upsert_operator_from_osm(
                conn, brand=brand, operator_name=op, osm_id=p.osm_id,
                osm_type=getattr(p, "osm_type", "node"),
            ):
                operator_tags_captured += 1
        conn.commit()
    finally:
        conn.close()
    return osm_total, upserted, operator_tags_captured, filtered_out


async def run_all(brands: list[str], db_path: str) -> FetchStats:
    stats = FetchStats()
    if not Path(db_path).exists():
        stats.errors.append(f"db not found: {db_path}")
        return stats
    for brand in brands:
        try:
            osm_total, upserted, operator_tags, filtered = await fetch_brand_via_osm(brand, db_path)
            stats.osm_total += osm_total
            stats.upserted += upserted
            stats.operator_tags_captured += operator_tags
            stats.filtered_out += filtered
            if osm_total == 0:
                stats.brands_skipped += 1
                logger.info("[skip] %s — no OSM tag mapping", brand)
            else:
                stats.brands_processed += 1
                logger.info(
                    "[done] %s — osm=%d upserted=%d operators=%d filtered=%d",
                    brand, osm_total, upserted, operator_tags, filtered,
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
    print(f"   operator_tags    = {stats.operator_tags_captured} (operator_stores)")
    print(f"   filtered_out     = {stats.filtered_out} (brand name 不一致)")
    for e in stats.errors[:5]:
        print(f"   ⚠  {e}")


if __name__ == "__main__":
    _main()
