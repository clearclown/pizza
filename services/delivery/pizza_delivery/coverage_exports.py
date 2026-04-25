"""14 ブランド店舗の operator coverage CSV を生成する補助 CLI。"""

from __future__ import annotations

import argparse
import csv
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


def _prefecture(address: str) -> str:
    s = re.sub(r"\s+", "", address or "")
    for pref in PREFECTURES:
        if pref in s:
            return pref
    return ""


def _split_brands(raw: str) -> list[str]:
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
                        "prefecture": _prefecture(s["address"]),
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
                "prefecture": _prefecture(s["address"]),
                "phone": s["phone"],
            }
        )
    _write_csv(
        out_dir / "unknown-stores-14brand.csv",
        ["brand", "place_id", "store_name", "address", "prefecture", "phone"],
        unknown_rows,
    )

    coverage_rows: list[dict] = []
    for brand in brands:
        brand_stores = [s for s in stores if s["brand"] == brand]
        brand_store_ids = {s["place_id"] for s in brand_stores}
        verified_ids = brand_store_ids & set(verified_by_store)
        operators = {
            op["operator_name"]
            for pid in verified_ids
            for op in verified_by_store.get(pid, [])
        }
        total = len(brand_stores)
        verified = len(verified_ids)
        coverage_rows.append(
            {
                "brand": brand,
                "stores": total,
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
            pref_stores = [s for s in brand_stores if _prefecture(s["address"]) == pref]
            pref_ids = {s["place_id"] for s in pref_stores}
            verified_ids = pref_ids & set(verified_by_store)
            operators = {
                op["operator_name"]
                for pid in verified_ids
                for op in verified_by_store.get(pid, [])
            }
            total = len(pref_stores)
            verified = len(verified_ids)
            bp_rows.append(
                {
                    "brand": brand,
                    "prefecture": pref,
                    "stores": total,
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
            "verified_operator_stores",
            "verified_operators",
            "verified_store_coverage",
        ],
        bp_rows,
    )

    return {
        "stores": len(stores),
        "known_rows": len(known_rows),
        "verified_rows": len(verified_rows),
        "unknown_stores": len(unknown_rows),
        "brand_prefecture_rows": len(bp_rows),
    }


def _main() -> None:
    ap = argparse.ArgumentParser(description="operator coverage CSV export")
    ap.add_argument("--db", default="var/pizza.sqlite")
    ap.add_argument("--brands", required=True)
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
