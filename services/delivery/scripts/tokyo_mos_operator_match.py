"""東京都モスバーガー店舗 × registry 登録 operator の place_id 突合スクリプト。

目的:
  全 registry 社 (本社不問) について「東京都のモス店舗を保有しているか」を
  Places search_by_operator (area_hint=東京都) で検索し、SQLite 側の
  東京都 192 店舗と place_id で突合する。

出力:
  var/mos-tokyo-fullrun/final/tokyo-mos-operators-matched.csv
"""

from __future__ import annotations

import asyncio
import csv as csv_mod
import os
import sqlite3
import sys
from pathlib import Path

from pizza_delivery.franchisee_registry import load_registry
from pizza_delivery.places_client import PlacesClient


DB_PATH = "/Users/ablaze/Projects/pizza/var/pizza.sqlite"
OUT_CSV = "/Users/ablaze/Projects/pizza/var/mos-tokyo-fullrun/final/tokyo-mos-operators-matched.csv"
BRAND = "モスバーガー"
AREA_HINT = "東京都"


def fetch_tokyo_mos_place_ids() -> set[str]:
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT place_id, name, address, lat, lng FROM stores "
            "WHERE brand=? AND address LIKE '%東京都%'",
            (BRAND,),
        ).fetchall()
    finally:
        conn.close()
    return {r[0] for r in rows}, {r[0]: (r[1], r[2], r[3], r[4]) for r in rows}


async def main() -> None:
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "")
    if not api_key:
        print("GOOGLE_MAPS_API_KEY が未設定", file=sys.stderr)
        sys.exit(2)

    tokyo_ids, tokyo_info = fetch_tokyo_mos_place_ids()
    print(f"🗾 東京都 Mos 店舗 = {len(tokyo_ids)} 件")

    reg = load_registry()
    mos_operators = []
    # brands.モスバーガー.known_franchisees 全件
    if BRAND in reg.brands:
        for fr in reg.brands[BRAND].known_franchisees:
            mos_operators.append((fr.name, fr.corporate_number, fr.head_office))
    # multi_brand_operators の中で brands に モスバーガー を持つ社
    for mbo in reg.multi_brand_operators:
        if BRAND in mbo.brands:
            if any(o[0] == mbo.name for o in mos_operators):
                continue
            mos_operators.append((mbo.name, mbo.corporate_number, mbo.head_office))

    print(f"🏢 registry 掲載の Mos 運営社 = {len(mos_operators)} 社")

    client = PlacesClient(api_key=api_key, language_code="ja", region_code="JP")
    results = []
    for i, (name, corp, hq) in enumerate(mos_operators, 1):
        # 2 通りのクエリを試す: (1) operator + モスバーガー + 東京都
        #                     (2) operator + 東京都
        found_tokyo_mos = []
        tried = set()
        for q in (name, f"{name} モスバーガー"):
            try:
                places = await client.search_by_operator(q, area_hint=AREA_HINT)
            except Exception as e:
                print(f"  ⚠️ {name} query={q!r}: {e}", file=sys.stderr)
                continue
            for p in places:
                pid = p.place_id
                if not pid or pid in tried:
                    continue
                tried.add(pid)
                if pid in tokyo_ids:
                    found_tokyo_mos.append((pid, p.name))
        print(f"  [{i:2d}/{len(mos_operators)}] {name} → tokyo_match={len(found_tokyo_mos)}")
        results.append({
            "operator_name": name,
            "corporate_number": corp,
            "head_office": hq,
            "tokyo_match_count": len(found_tokyo_mos),
            "matched_place_ids": ";".join(p[0] for p in found_tokyo_mos[:10]),
            "matched_examples": "|".join(f"{p[1]} ({p[0][:20]}...)" for p in found_tokyo_mos[:3]),
        })

    # Sort by tokyo_match_count DESC
    results.sort(key=lambda r: -r["tokyo_match_count"])

    Path(OUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv_mod.DictWriter(f, fieldnames=[
            "operator_name", "corporate_number", "head_office",
            "tokyo_match_count", "matched_place_ids", "matched_examples",
        ])
        writer.writeheader()
        writer.writerows(results)
    print(f"✅ wrote {OUT_CSV}")

    # Print a summary
    print()
    print("━━━━ 東京都 Mos 店舗を保有する事業会社 (place_id 突合) ━━━━")
    for r in results:
        if r["tokyo_match_count"] > 0:
            print(f"  {r['tokyo_match_count']:3d} 店  {r['operator_name']:40s}  {r['head_office']}")
    print()
    no_match = [r for r in results if r["tokyo_match_count"] == 0]
    print(f"(Places API 突合 0 件だった社 = {len(no_match)} / {len(results)})")


if __name__ == "__main__":
    asyncio.run(main())
