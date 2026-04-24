"""pizza operator-spider — ORM 登録済 operator の公式 HP から store 一覧を抽出して
pipeline DB の Places 店舗と住所 match で operator 確定 (Phase 27 Step B)。

**ハルシネ 0 設計**:
  - operator_name は ORM 既存 record 由来 (LLM 生成なし)
  - place_id は Places bake で発見済の既存 (synthetic なし)
  - 住所 matching は決定論 prefix 比較 (address_reverse の _parse_pref_city_town reuse)
  - match しない operator 名は DB に書かない

**対象**:
  ORM `brand_operator_link` で `operator_type='franchisee'` または空の operator で
  `website_url` が非空なもの。EDINET-sync で入った新 13 社等も拾う。

CLI:
  pizza operator-spider --brand モスバーガー --db var/pizza.sqlite \
      --out var/phase27/spider-mos.csv --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class SpiderMatch:
    operator_name: str
    operator_corp: str
    place_id: str
    store_name: str
    store_address: str
    candidate_address: str
    source_url: str


@dataclass
class SpiderStats:
    operators_considered: int = 0
    candidates_discovered: int = 0
    matched_stores: int = 0
    applied_rows: int = 0
    errors: list[str] = field(default_factory=list)


def _parse_pref_city_town(address: str) -> tuple[str, str, str]:
    """住所を (pref, city, town_prefix) に分解 (address_reverse と同 logic)."""
    if not address:
        return "", "", ""
    stripped = re.sub(r"^〒?\d{3}[-‐]?\d{4}\s*", "", address).strip()
    pref_m = re.match(
        r"(北海道|東京都|大阪府|京都府|.{2,3}県)",
        stripped,
    )
    if not pref_m:
        return "", "", ""
    pref = pref_m.group(1)
    rest = stripped[len(pref):]
    city_m = re.match(r"([一-龥ぁ-んァ-ヶ]{1,8}(?:市|区|町|村))", rest)
    city = city_m.group(1) if city_m else ""
    rest = rest[len(city):] if city else rest
    town_m = re.match(r"([一-龥ぁ-んァ-ヶ]{1,6})", rest)
    town = town_m.group(1) if town_m else ""
    return pref, city, town


_KANJI_NUM = {
    "〇": "0", "零": "0",
    "一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
    "六": "6", "七": "7", "八": "8", "九": "9",
}
# ハイフン / ダッシュ類 (〜 はダッシュではないので含めない)
_HYPHEN_CLASS = "‐‑‒–—―−ーｰ"


def _normalize_japan_address(address: str) -> str:
    """日本の住所表記揺れを吸収した正規化。

    吸収対象:
      - 空白 / 全角空白
      - 全角英数字 → 半角
      - 漢数字 (一〜九、十) → アラビア数字
      - 〒 郵便番号 prefix
      - 「N丁目M番O号」/「N-M-O」/「N番地の M」 → "N-M-O"
      - 各種ダッシュ (‐ ‑ – — ― − ー 等) → 標準ハイフン
      - 末尾の建物名・フロア (「〜ビル3F」等) を切り落とし

    例: "〒100-0001 東京都千代田区千代田1丁目1番1号 日本ビル3F"
         → "東京都千代田区千代田1-1-1"
    """
    if not address:
        return ""
    s = re.sub(r"[\s　]+", "", address)
    # 全角数字 → 半角
    s = s.translate(str.maketrans("０１２３４５6789", "0123456789"))
    # 漢数字 (一〜九)
    for k, v in _KANJI_NUM.items():
        s = s.replace(k, v)
    # 「十」は 2 桁以上の先頭なら「1X」扱い、単独「十」は「10」
    s = re.sub(r"十([0-9])", r"1\1", s)
    s = s.replace("十", "10")
    # 〒 prefix 削除
    s = re.sub(r"^〒?\d{3}[" + _HYPHEN_CLASS + r"\-]?\d{4}", "", s)
    # 丁目 / 番地 / 番 / 号 → ハイフン統一
    s = re.sub(r"(\d+)丁目", r"\1-", s)
    s = re.sub(r"(\d+)番地", r"\1-", s)
    s = re.sub(r"(\d+)番", r"\1-", s)
    s = re.sub(r"(\d+)号", r"\1", s)
    # 「X-の Y」/「X の Y」の連結助詞「の」削除 (例: "2-の3" → "2-3")
    s = re.sub(r"(\d+)-の(\d+)", r"\1-\2", s)
    s = re.sub(r"(\d+)の(\d+)", r"\1-\2", s)
    # ダッシュ類を標準ハイフンに
    s = re.sub(r"[" + _HYPHEN_CLASS + r"]", "-", s)
    # 連続ハイフンを 1 つに
    s = re.sub(r"-+", "-", s)
    # 末尾ハイフン削除
    s = re.sub(r"-+$", "", s)
    # 番地 "X-Y-Z" の後の非数字 (ビル名等) を切り落とす
    s = re.sub(r"(\d+(?:-\d+){1,3})(?:[^\d\-].*)$", r"\1", s)
    return s


def _address_key(address: str) -> str:
    """住所の正規化 key (pref + city + town + 番地)。

    番地は最大 3 階層 (X-Y-Z) まで取り込む。建物名・フロア表記の差異は
    `_normalize_japan_address` 側で切り落とされるので、同ブロック違う階の
    店舗は同一 key になる。
    """
    normalized = _normalize_japan_address(address)
    if not normalized:
        return ""
    pref, city, town = _parse_pref_city_town(normalized)
    if not pref or not city:
        return normalized[:40]
    rest = normalized[len(pref) + len(city) + len(town):]
    num_m = re.match(r"(\d{1,3}(?:-\d{1,3}){0,2})", rest)
    num = num_m.group(1) if num_m else ""
    return f"{pref}{city}{town}{num}"


def _address_prefix_key(address: str, depth: int = 2) -> str:
    """exact match fail 時の fallback key (近隣 block 単位)。

    depth=2 なら "1-2-3" → "1-2"、depth=1 なら "1" まで。候補を広げるので
    caller 側で 1 operator の estimated_store_count 以下に制限推奨。
    """
    normalized = _normalize_japan_address(address)
    if not normalized:
        return ""
    pref, city, town = _parse_pref_city_town(normalized)
    if not pref or not city:
        return normalized[:30]
    rest = normalized[len(pref) + len(city) + len(town):]
    if depth == 1:
        num_m = re.match(r"(\d{1,3})", rest)
    elif depth == 2:
        num_m = re.match(r"(\d{1,3}-\d{1,3})", rest)
    else:
        pattern = r"(\d{1,3}(?:-\d{1,3}){0," + str(depth - 1) + r"})"
        num_m = re.match(pattern, rest)
    num = num_m.group(1) if num_m else ""
    return f"{pref}{city}{town}{num}"


def _load_target_operators(brand: str) -> list[tuple[str, str, str]]:
    """ORM から指定 brand の operator (name, website_url, corp_number) 一覧。

    website_url が空の operator は skip。
    """
    from pizza_delivery.orm import (
        BrandOperatorLink, FranchiseBrand, OperatorCompany, make_session,
    )

    sess = make_session()
    try:
        rows = (
            sess.query(OperatorCompany, BrandOperatorLink)
            .join(BrandOperatorLink, BrandOperatorLink.operator_id == OperatorCompany.id)
            .join(FranchiseBrand, BrandOperatorLink.brand_id == FranchiseBrand.id)
            .filter(FranchiseBrand.name == brand)
            .all()
        )
        out: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        for op, _link in rows:
            if not op.website_url or op.name in seen:
                continue
            seen.add(op.name)
            out.append((op.name, op.website_url, op.corporate_number or ""))
        return out
    finally:
        sess.close()


def _load_brand_stores(db_path: str, brand: str) -> list[tuple[str, str, str]]:
    """pipeline DB から brand の全 store (place_id, name, address) を取得。"""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT place_id, name, address FROM stores "
            "WHERE brand = ? AND address != ''",
            (brand,),
        ).fetchall()
        return [(r[0], r[1], r[2]) for r in rows]
    finally:
        conn.close()


async def spider_brand_operators(
    db_path: str,
    brand: str,
    *,
    dry_run: bool = False,
    concurrency: int = 2,
    max_follow_links: int = 5,
) -> tuple[SpiderStats, list[SpiderMatch]]:
    """brand の全 operator 公式 HP を回遊して store 一覧を抽出 → 住所 match。"""
    from pizza_delivery.operator_spider import OperatorSpider

    stats = SpiderStats()
    matches: list[SpiderMatch] = []

    operators = _load_target_operators(brand)
    stats.operators_considered = len(operators)
    if not operators:
        stats.errors.append(
            f"ORM に brand='{brand}' の website_url 有 operator が 0 件 "
            "(pizza edinet-sync で先に追加してください)"
        )
        return stats, matches

    stores = _load_brand_stores(db_path, brand)
    # 住所 key → store index (複数同 key store があり得る)
    store_by_key: dict[str, list[tuple[str, str, str]]] = {}
    for pid, sn, addr in stores:
        k = _address_key(addr)
        if not k:
            continue
        store_by_key.setdefault(k, []).append((pid, sn, addr))

    spider = OperatorSpider(max_follow_links=max_follow_links)
    sem = asyncio.Semaphore(concurrency)

    async def _one(op_name: str, website: str, corp: str) -> list[SpiderMatch]:
        async with sem:
            try:
                cands = await spider.discover(
                    operator_name=op_name, official_url=website,
                )
            except Exception as e:
                stats.errors.append(f"spider {op_name}: {e}")
                return []
        local: list[SpiderMatch] = []
        for cand in cands:
            key = _address_key(cand.address)
            if not key or key not in store_by_key:
                continue
            for pid, sn, addr in store_by_key[key]:
                local.append(SpiderMatch(
                    operator_name=op_name,
                    operator_corp=corp,
                    place_id=pid,
                    store_name=sn,
                    store_address=addr,
                    candidate_address=cand.address,
                    source_url=cand.source_url,
                ))
        return local

    all_results = await asyncio.gather(
        *(_one(n, u, c) for n, u, c in operators),
        return_exceptions=False,
    )
    for r in all_results:
        matches.extend(r)
        stats.candidates_discovered += len(r)

    # dedup by (place_id, operator_name)
    seen: set[tuple[str, str]] = set()
    unique_matches: list[SpiderMatch] = []
    for m in matches:
        key = (m.place_id, m.operator_name)
        if key in seen:
            continue
        seen.add(key)
        unique_matches.append(m)
    matches = unique_matches
    stats.matched_stores = len(matches)

    if not dry_run and matches:
        conn = sqlite3.connect(db_path)
        try:
            cols = {r[1] for r in conn.execute(
                "PRAGMA table_info(operator_stores)"
            ).fetchall()}
            has_corp = "corporate_number" in cols
            for m in matches:
                if has_corp:
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO operator_stores "
                        "(operator_name, place_id, brand, operator_type, confidence, "
                        " discovered_via, corporate_number) "
                        "VALUES (?, ?, ?, 'franchisee', 0.80, "
                        "       'operator_spider_address_match', ?)",
                        (m.operator_name, m.place_id, brand, m.operator_corp),
                    )
                else:
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO operator_stores "
                        "(operator_name, place_id, brand, operator_type, confidence, "
                        " discovered_via) "
                        "VALUES (?, ?, ?, 'franchisee', 0.80, "
                        "       'operator_spider_address_match')",
                        (m.operator_name, m.place_id, brand),
                    )
                stats.applied_rows += cur.rowcount
            conn.commit()
        finally:
            conn.close()
    return stats, matches


def _main() -> None:
    ap = argparse.ArgumentParser(
        description="operator_spider: ORM 登録済 operator 公式 HP → 店舗一覧 → 住所 match"
    )
    ap.add_argument("--brand", required=True)
    ap.add_argument("--db", default="var/pizza.sqlite")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--concurrency", type=int, default=2)
    ap.add_argument("--max-follow-links", type=int, default=5)
    ap.add_argument("--out", default="", help="matches CSV 出力")
    args = ap.parse_args()

    if not Path(args.db).exists():
        print(f"❌ db not found: {args.db}", file=sys.stderr)
        sys.exit(2)

    stats, matches = asyncio.run(spider_brand_operators(
        args.db, args.brand,
        dry_run=args.dry_run,
        concurrency=args.concurrency,
        max_follow_links=args.max_follow_links,
    ))
    print(f"✅ operator-spider {'dry-run' if args.dry_run else 'apply'}  brand={args.brand}")
    print(f"   operators_considered  = {stats.operators_considered}")
    print(f"   candidates_discovered = {stats.candidates_discovered}")
    print(f"   matched_stores        = {stats.matched_stores}")
    if not args.dry_run:
        print(f"   applied_rows          = {stats.applied_rows}")
    for e in stats.errors[:5]:
        print(f"   ⚠  {e}", file=sys.stderr)
    if args.out and matches:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["operator_name", "corp", "place_id", "store_name",
                        "store_address", "candidate_address", "source_url"])
            for m in matches:
                w.writerow([m.operator_name, m.operator_corp, m.place_id,
                            m.store_name, m.store_address,
                            m.candidate_address, m.source_url])
        print(f"📄 matches CSV: {args.out}")


if __name__ == "__main__":
    _main()
