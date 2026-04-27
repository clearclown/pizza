"""14 ブランド店舗の operator coverage CSV を生成する補助 CLI。"""

from __future__ import annotations

import argparse
import csv
import math
import re
import sqlite3
from collections import defaultdict
from pathlib import Path


PREFECTURES = (
    "北海道",
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "茨城県",
    "栃木県",
    "群馬県",
    "埼玉県",
    "千葉県",
    "東京都",
    "神奈川県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
    "山梨県",
    "長野県",
    "岐阜県",
    "静岡県",
    "愛知県",
    "三重県",
    "滋賀県",
    "京都府",
    "大阪府",
    "兵庫県",
    "奈良県",
    "和歌山県",
    "鳥取県",
    "島根県",
    "岡山県",
    "広島県",
    "山口県",
    "徳島県",
    "香川県",
    "愛媛県",
    "高知県",
    "福岡県",
    "佐賀県",
    "長崎県",
    "熊本県",
    "大分県",
    "宮崎県",
    "鹿児島県",
    "沖縄県",
)

TARGET_BRANDS = (
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
)


def _prefecture(address: str) -> str:
    s = re.sub(r"\s+", "", address or "")
    for pref in PREFECTURES:
        if pref in s:
            return pref
    return ""


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlng / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(a))


def _as_float(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _build_prefecture_anchors(
    stores: list[sqlite3.Row],
) -> list[tuple[float, float, str]]:
    anchors: list[tuple[float, float, str]] = []
    for s in stores:
        pref = _prefecture(s["address"])
        lat = _as_float(s["lat"])
        lng = _as_float(s["lng"])
        if pref and lat is not None and lng is not None:
            anchors.append((lat, lng, pref))
    return anchors


def _prefecture_for_store(
    store: sqlite3.Row,
    anchors: list[tuple[float, float, str]],
    *,
    max_distance_km: float = 60.0,
) -> tuple[str, str]:
    pref = _prefecture(store["address"])
    if pref:
        return pref, "address"
    lat = _as_float(store["lat"])
    lng = _as_float(store["lng"])
    if lat is None or lng is None:
        return "", ""
    best_pref = ""
    best_distance = float("inf")
    for anchor_lat, anchor_lng, anchor_pref in anchors:
        distance = _haversine_km(lat, lng, anchor_lat, anchor_lng)
        if distance < best_distance:
            best_distance = distance
            best_pref = anchor_pref
    if best_pref and best_distance <= max_distance_km:
        return best_pref, "nearest_coordinate"
    return "", ""


def _split_brands(raw: str) -> list[str]:
    if not raw:
        return list(TARGET_BRANDS)
    return [b.strip() for b in raw.split(",") if b.strip()]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def export_coverage(
    db_path: str | Path,
    *,
    brands: list[str],
    out_dir: str | Path,
) -> dict[str, int]:
    out_dir = Path(out_dir)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        stores = conn.execute(
            """
            SELECT place_id, brand, name, COALESCE(address,'') AS address,
                   lat, lng,
                   COALESCE(phone,'') AS phone
            FROM stores
            WHERE brand IN ({})
            ORDER BY brand, place_id
            """.format(",".join("?" * len(brands))),
            brands,
        ).fetchall()
        ops = conn.execute(
            """
            SELECT os.place_id, os.brand, os.operator_name,
                   COALESCE(os.operator_type,'') AS operator_type,
                   COALESCE(os.corporate_number,'') AS corporate_number,
                   COALESCE(os.discovered_via,'') AS discovered_via,
                   COALESCE(os.verification_source,'') AS verification_source,
                   COALESCE(os.confidence,0) AS confidence,
                   COALESCE(os.verification_score,0) AS verification_score
            FROM operator_stores os
            WHERE os.brand IN ({})
              AND os.operator_name != ''
              AND COALESCE(os.operator_type,'') NOT IN ('franchisor','direct')
            ORDER BY os.brand, os.place_id, os.operator_name
            """.format(",".join("?" * len(brands))),
            brands,
        ).fetchall()
    finally:
        conn.close()

    store_by_id = {s["place_id"]: s for s in stores}
    anchors = _build_prefecture_anchors(list(stores))
    pref_by_place: dict[str, str] = {}
    pref_source_by_place: dict[str, str] = {}
    for s in stores:
        pref, source = _prefecture_for_store(s, anchors)
        pref_by_place[s["place_id"]] = pref
        pref_source_by_place[s["place_id"]] = source
    known_by_store: dict[str, list[sqlite3.Row]] = defaultdict(list)
    verified_by_store: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for op in ops:
        known_by_store[op["place_id"]].append(op)
        if op["corporate_number"]:
            verified_by_store[op["place_id"]].append(op)

    def store_op_rows(source: dict[str, list[sqlite3.Row]]) -> list[dict]:
        rows: list[dict] = []
        for pid, op_rows in sorted(source.items()):
            s = store_by_id.get(pid)
            if not s:
                continue
            for op in op_rows:
                rows.append(
                    {
                        "brand": s["brand"],
                        "place_id": pid,
                        "store_name": s["name"],
                        "address": s["address"],
                        "prefecture": pref_by_place.get(pid, ""),
                        "prefecture_source": pref_source_by_place.get(pid, ""),
                        "phone": s["phone"],
                        "operator_name": op["operator_name"],
                        "corporate_number": op["corporate_number"],
                        "operator_type": op["operator_type"],
                        "discovered_via": op["discovered_via"],
                        "verification_source": op["verification_source"],
                        "confidence": op["confidence"],
                        "verification_score": op["verification_score"],
                    }
                )
        return rows

    fields = [
        "brand",
        "place_id",
        "store_name",
        "address",
        "prefecture",
        "prefecture_source",
        "phone",
        "operator_name",
        "corporate_number",
        "operator_type",
        "discovered_via",
        "verification_source",
        "confidence",
        "verification_score",
    ]
    known_rows = store_op_rows(known_by_store)
    verified_rows = store_op_rows(verified_by_store)
    _write_csv(out_dir / "store-operators-known-14brand.csv", fields, known_rows)
    _write_csv(out_dir / "store-operators-verified-14brand.csv", fields, verified_rows)

    unknown_rows: list[dict] = []
    for s in stores:
        if s["place_id"] in verified_by_store:
            continue
        unknown_rows.append(
            {
                "brand": s["brand"],
                "place_id": s["place_id"],
                "store_name": s["name"],
                "address": s["address"],
                "prefecture": pref_by_place.get(s["place_id"], ""),
                "prefecture_source": pref_source_by_place.get(s["place_id"], ""),
                "phone": s["phone"],
            }
        )
    _write_csv(
        out_dir / "unknown-stores-14brand.csv",
        ["brand", "place_id", "store_name", "address", "prefecture", "prefecture_source", "phone"],
        unknown_rows,
    )

    coverage_rows: list[dict] = []
    for brand in brands:
        brand_stores = [s for s in stores if s["brand"] == brand]
        brand_store_ids = {s["place_id"] for s in brand_stores}
        known_ids = brand_store_ids & set(known_by_store)
        verified_ids = brand_store_ids & set(verified_by_store)
        known_operators = {
            op["operator_name"]
            for pid in known_ids
            for op in known_by_store.get(pid, [])
        }
        operators = {
            op["operator_name"]
            for pid in verified_ids
            for op in verified_by_store.get(pid, [])
        }
        total = len(brand_stores)
        known = len(known_ids)
        verified = len(verified_ids)
        addressed = sum(1 for s in brand_stores if pref_by_place.get(s["place_id"], ""))
        pref_set = {
            pref_by_place.get(s["place_id"], "")
            for s in brand_stores
            if pref_by_place.get(s["place_id"], "")
        }
        missing_prefs = [p for p in PREFECTURES if p not in pref_set]
        coverage_rows.append(
            {
                "brand": brand,
                "stores": total,
                "stores_with_prefecture": addressed,
                "stores_missing_prefecture": total - addressed,
                "prefectures_with_stores": len(pref_set),
                "prefectures_missing": len(missing_prefs),
                "missing_prefecture_list": "|".join(missing_prefs),
                "known_operator_stores": known,
                "known_operators": len(known_operators),
                "known_store_coverage": f"{known / total:.4f}" if total else "0.0000",
                "verified_operator_stores": verified,
                "verified_operators": len(operators),
                "verified_store_coverage": f"{verified / total:.4f}" if total else "0.0000",
            }
        )
    _write_csv(
        out_dir / "brand-operator-coverage.csv",
        [
            "brand",
            "stores",
            "stores_with_prefecture",
            "stores_missing_prefecture",
            "prefectures_with_stores",
            "prefectures_missing",
            "missing_prefecture_list",
            "known_operator_stores",
            "known_operators",
            "known_store_coverage",
            "verified_operator_stores",
            "verified_operators",
            "verified_store_coverage",
        ],
        coverage_rows,
    )

    bp_rows: list[dict] = []
    for brand in brands:
        brand_stores = [s for s in stores if s["brand"] == brand]
        for pref in PREFECTURES:
            pref_stores = [
                s for s in brand_stores
                if pref_by_place.get(s["place_id"], "") == pref
            ]
            pref_ids = {s["place_id"] for s in pref_stores}
            known_ids = pref_ids & set(known_by_store)
            verified_ids = pref_ids & set(verified_by_store)
            known_operators = {
                op["operator_name"]
                for pid in known_ids
                for op in known_by_store.get(pid, [])
            }
            operators = {
                op["operator_name"]
                for pid in verified_ids
                for op in verified_by_store.get(pid, [])
            }
            total = len(pref_stores)
            known = len(known_ids)
            verified = len(verified_ids)
            bp_rows.append(
                {
                    "brand": brand,
                    "prefecture": pref,
                    "stores": total,
                    "coverage_status": "observed" if total else "no_store_observed",
                    "known_operator_stores": known,
                    "known_operators": len(known_operators),
                    "known_store_coverage": f"{known / total:.4f}" if total else "0.0000",
                    "verified_operator_stores": verified,
                    "verified_operators": len(operators),
                    "verified_store_coverage": f"{verified / total:.4f}" if total else "0.0000",
                }
            )
    _write_csv(
        out_dir / "brand-prefecture-coverage.csv",
        [
            "brand",
            "prefecture",
            "stores",
            "coverage_status",
            "known_operator_stores",
            "known_operators",
            "known_store_coverage",
            "verified_operator_stores",
            "verified_operators",
            "verified_store_coverage",
        ],
        bp_rows,
    )

    missing_pref_rows = [
        row for row in bp_rows if row["coverage_status"] == "no_store_observed"
    ]
    _write_csv(
        out_dir / "brand-prefecture-missing-14brand.csv",
        [
            "brand",
            "prefecture",
            "stores",
            "coverage_status",
            "known_operator_stores",
            "known_operators",
            "known_store_coverage",
            "verified_operator_stores",
            "verified_operators",
            "verified_store_coverage",
        ],
        missing_pref_rows,
    )

    missing_prefecture_store_rows = [
        {
            "brand": s["brand"],
            "place_id": s["place_id"],
            "store_name": s["name"],
            "address": s["address"],
            "lat": s["lat"],
            "lng": s["lng"],
            "phone": s["phone"],
        }
        for s in stores
        if not pref_by_place.get(s["place_id"], "")
    ]
    _write_csv(
        out_dir / "stores-missing-prefecture-14brand.csv",
        ["brand", "place_id", "store_name", "address", "lat", "lng", "phone"],
        missing_prefecture_store_rows,
    )

    return {
        "stores": len(stores),
        "known_rows": len(known_rows),
        "verified_rows": len(verified_rows),
        "unknown_stores": len(unknown_rows),
        "brand_prefecture_rows": len(bp_rows),
        "brand_prefecture_missing_rows": len(missing_pref_rows),
        "stores_missing_prefecture": len(missing_prefecture_store_rows),
    }


def _main() -> None:
    ap = argparse.ArgumentParser(description="operator coverage CSV export")
    ap.add_argument("--db", default="var/pizza.sqlite")
    ap.add_argument("--brands", default="", help="空なら14ブランド")
    ap.add_argument("--out-dir", default="var/phase27/deliverable")
    args = ap.parse_args()
    stats = export_coverage(
        args.db,
        brands=_split_brands(args.brands),
        out_dir=args.out_dir,
    )
    print("✅ coverage exports")
    for k, v in stats.items():
        print(f"   {k} = {v}")


if __name__ == "__main__":
    _main()
