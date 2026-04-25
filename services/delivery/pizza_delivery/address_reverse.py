"""住所逆引き — 店舗住所 → 国税庁 CSV 同住所の株式会社検索。

**ハルシネ 0 設計**:
  1. Mos 387 店の住所を prefix-match (都道府県+市+町名) で国税庁 577 万 CSV 検索
  2. 同住所 exact match する株式会社が **ただ 1 社** の店舗を候補化
     (複数なら LLM critic で選定、後続で拡張予定)
  3. Places 由来 phone も cross-check (同住所 + 電話一致で確度 up)
  4. 結果を `operator_stores` に `discovered_via='address_reverse_houjin'` tag 付きで追加
  5. 既存 operator 名と衝突したら skip (上書きしない、transparency)

対象 use-case: Mos 161 不明店、業務スーパー 112、シャトレーゼ 72 等、
operator 空の店舗。
"""

from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class AddressMatch:
    place_id: str
    store_address: str
    candidates: list[tuple[str, str]] = field(default_factory=list)  # (name, corp)
    chosen: str = ""
    chosen_corp: str = ""
    reason: str = ""


@dataclass
class ReverseStats:
    target_stores: int = 0
    addresses_queried: int = 0
    single_match: int = 0        # 1 社のみ同住所 → 高確度
    multi_match: int = 0         # 複数候補 → LLM 必要
    no_match: int = 0
    applied_rows: int = 0


def _parse_pref_city_town(address: str) -> tuple[str, str, str]:
    """住所から (pref, city, town_prefix) を抽出。

    Example:
      '東京都港区芝浦3-9-1' → ('東京都', '港区', '芝浦')
      '〒402-0054 山梨県都留市田原２丁目' → ('山梨県', '都留市', '田原')
    """
    if not address:
        return "", "", ""
    # 〒123-4567 prefix / 半角空白 を skip
    stripped = re.sub(r"^〒?\d{3}[-‐]?\d{4}\s*", "", address).strip()
    # 都道府県
    pref_m = re.match(
        r"(北海道|東京都|大阪府|京都府|.{2,3}県)",
        stripped,
    )
    if not pref_m:
        return "", "", ""
    address = stripped
    pref = pref_m.group(1)
    rest = address[len(pref):]
    # 市区町村
    city_m = re.match(r"([一-龥ぁ-んァ-ヶ]{1,8}(?:市|区|町|村))", rest)
    city = city_m.group(1) if city_m else ""
    rest = rest[len(city):] if city else rest
    # 町名 (数字までの漢字/かな、最大 6 chars)
    town_m = re.match(r"([一-龥ぁ-んァ-ヶ]{1,6})", rest)
    town = town_m.group(1) if town_m else ""
    return pref, city, town


def _search_houjin_by_location(
    idx, pref: str, city: str, town: str, limit: int = 20,
) -> list:
    """pref + city + town で国税庁 CSV 部分一致検索。

    町名は prefix-match (例: 「芝浦」で 芝浦2, 芝浦3, 芝浦4 全部 hit)。
    """
    if not pref or not city:
        return []
    conn = sqlite3.connect(idx.db_path)
    try:
        if town:
            rows = conn.execute(
                "SELECT corporate_number, name, prefecture, city, street "
                "FROM houjin_registry "
                "WHERE prefecture = ? AND city = ? AND street LIKE ? "
                "AND process IN ('01','11','12','13','21','22','31') "
                "LIMIT ?",
                (pref, city, town + "%", limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT corporate_number, name, prefecture, city, street "
                "FROM houjin_registry "
                "WHERE prefecture = ? AND city = ? "
                "AND process IN ('01','11','12','13','21','22','31') "
                "LIMIT ?",
                (pref, city, limit),
            ).fetchall()
        return rows
    finally:
        conn.close()


_BUSINESS_TYPE_MARKERS = (
    # FC 運営会社の typical 法人格
    "株式会社", "有限会社", "合同会社",
)

# 同住所にあっても FC operator ではない法人格
_REJECT_BUSINESS = (
    "医療法人", "学校法人", "社会福祉法人", "宗教法人", "財団法人",
    "社団法人", "組合", "連合会", "協議会", "独立行政法人",
    "国立大学", "公立大学",
)


def reverse_lookup_for_brand(
    db_path: str | Path,
    *,
    brand: str,
    max_stores: int = 0,
    dry_run: bool = False,
    require_single_match: bool = True,
) -> tuple[ReverseStats, list[AddressMatch]]:
    """brand の operator 不明店舗に対し、住所逆引きで operator 候補を提案。

    `require_single_match=True` で 候補 1 社のみの店舗だけ DB 更新 (high confidence)。
    多数候補は LLM critic 経由で後続処理する想定。
    """
    from pizza_delivery.houjin_csv import HoujinCSVIndex

    stats = ReverseStats()
    idx = HoujinCSVIndex()
    if idx.count() == 0:
        return stats, []

    # 対象 store: operator_name 空 or 本部のみ
    conn = sqlite3.connect(db_path)
    try:
        # 本部 only attribution の store も対象に含める
        q = """
        SELECT s.place_id, s.address
        FROM stores s
        WHERE s.brand = ?
        AND s.address != ''
        AND s.place_id NOT IN (
            SELECT os.place_id FROM operator_stores os
            WHERE os.operator_name != ''
              AND COALESCE(os.operator_type,'') NOT IN ('franchisor')
        )
        """
        rows = conn.execute(q, (brand,)).fetchall()
        if max_stores > 0:
            rows = rows[:max_stores]
        stats.target_stores = len(rows)
    finally:
        conn.close()

    matches: list[AddressMatch] = []
    for place_id, addr in rows:
        pref, city, town = _parse_pref_city_town(addr)
        if not pref or not city:
            continue
        stats.addresses_queried += 1
        recs = _search_houjin_by_location(idx, pref, city, town)
        # FC 運営候補 filter: 株式会社/有限会社/合同会社 のみ、reject 法人除外
        candidates: list[tuple[str, str]] = []
        for corp_num, name, p, c, street in recs:
            if any(rej in name for rej in _REJECT_BUSINESS):
                continue
            if not any(marker in name for marker in _BUSINESS_TYPE_MARKERS):
                continue
            candidates.append((name, corp_num))
        m = AddressMatch(place_id=place_id, store_address=addr, candidates=candidates)
        if len(candidates) == 1:
            stats.single_match += 1
            m.chosen, m.chosen_corp = candidates[0]
            m.reason = "single_match"
        elif len(candidates) > 1:
            stats.multi_match += 1
            m.reason = f"multi_match({len(candidates)})"
        else:
            stats.no_match += 1
            m.reason = "no_match"
        matches.append(m)

    # DB update (dry_run 時 skip)
    if not dry_run:
        conn = sqlite3.connect(db_path)
        try:
            for m in matches:
                if not m.chosen:
                    continue
                if require_single_match and m.reason != "single_match":
                    continue
                cur = conn.execute(
                    "INSERT OR IGNORE INTO operator_stores "
                    "(operator_name, place_id, brand, operator_type, confidence, "
                    " discovered_via, corporate_number) "
                    "VALUES (?, ?, ?, 'franchisee', 0.6, 'address_reverse_houjin', ?)",
                    (m.chosen, m.place_id, brand, m.chosen_corp),
                )
                stats.applied_rows += cur.rowcount
            conn.commit()
        finally:
            conn.close()

    return stats, matches


def _main() -> None:
    import argparse
    import csv
    import sys

    ap = argparse.ArgumentParser(
        description="住所逆引き: Mos 等の不明店舗 → 国税庁 CSV 同住所 株式会社検索"
    )
    ap.add_argument("--brand", required=True)
    ap.add_argument("--db", default="var/pizza.sqlite")
    ap.add_argument("--max-stores", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--allow-multi", action="store_true",
                    help="複数候補でも DB 更新 (non-single、要 LLM critic 併用)")
    ap.add_argument("--out", default="", help="matches CSV 出力")
    args = ap.parse_args()

    if not Path(args.db).exists():
        print(f"❌ db not found: {args.db}", file=sys.stderr)
        sys.exit(2)

    stats, matches = reverse_lookup_for_brand(
        args.db, brand=args.brand,
        max_stores=args.max_stores, dry_run=args.dry_run,
        require_single_match=not args.allow_multi,
    )
    print(f"✅ address reverse {'dry-run' if args.dry_run else 'apply'}  brand={args.brand}")
    print(f"   target_stores     = {stats.target_stores}")
    print(f"   addresses_queried = {stats.addresses_queried}")
    print(f"   single_match      = {stats.single_match}")
    print(f"   multi_match       = {stats.multi_match}")
    print(f"   no_match          = {stats.no_match}")
    if not args.dry_run:
        print(f"   applied_rows      = {stats.applied_rows}")
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["place_id", "store_address", "chosen", "corp", "reason", "candidate_count"])
            for m in matches:
                w.writerow([m.place_id, m.store_address, m.chosen, m.chosen_corp,
                            m.reason, len(m.candidates)])
        print(f"📄 matches: {args.out}")


if __name__ == "__main__":
    _main()
