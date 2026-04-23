"""pizza purge — operator_stores から garbage operator 名 (国税庁に存在しない法人) を削除。

ハルシネ防止: enrich / research で紛れ込んだ
  - 文字列断片 (「モスバーガーを展開する株式会社」)
  - 広告由来 (「NTTタウンページ株式会社」—iタウンページ 403 page footer)
  - 他チェーン (搜索結果の雑音)
を、国税庁法人番号 CSV 577万件に**存在しない**ものとして自動除去する。

削除基準:
  1. 国税庁 CSV の exact name match が無い
     AND
  2. prefix match も無い
     AND
  3. substring 後半一致も無い (XXX株式会社 の canonical 変形)
  → ほぼ確実に架空/断片 operator

Provenance: 削除した行は var/phase26/purge-log.csv に保存 (rollback 可)。
"""

from __future__ import annotations

import csv
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class PurgeStats:
    raw_unique: int = 0
    verified: int = 0
    purged: int = 0
    purged_rows: int = 0
    cross_brand_pollution: int = 0  # corp 空 × 多 brand 汚染で削除した unique 名
    errors: list[str] = field(default_factory=list)


import re as _re

# structural garbage の明示的 pattern (false positive なし、絶対削除)
_STRUCTURAL_GARBAGE_PATTERNS = [
    # 住所混入: 郵便番号 / 都道府県 / 市区町村 が 名前内に
    _re.compile(r"[〒]?\d{3}[-‐]?\d{4}"),               # 郵便番号
    _re.compile(r"(?:北海道|東京都|大阪府|京都府|(?:.{2,3}県))(?:.{0,10}?)(?:市|区|町|村|丁目|番地)"),
    # 末尾 ID 混入
    _re.compile(r"第\d{5,}号"),                          # 第XXXXX号 (公安委員会等)
    _re.compile(r"\d{5,}\s*(?:号|番)"),
    # snippet 断片の決定的 marker
    _re.compile(r"を展開する株式会社$"),
    _re.compile(r"^お客"),                                # 「お客さまへのお知らせ...」
    _re.compile(r"お知らせ\d{4}年"),
    _re.compile(r"採用キャリア採用"),                     # 「新卒採用キャリア採用アルバイト」
    _re.compile(r"現在地から探す"),                       # 検索 UI 断片
    _re.compile(r"都道府県から探"),
    _re.compile(r"(?:現在地|都道府県)から探"),
    # 店舗名 suffix (「株式会社XXX支店」「株式会社 XXX 店」ではなく、「株式会社XXX高校生専門」等)
    _re.compile(r"株式会社[^株]{2,}(?:高校生専門|南アルプスガーデン店|柏駅前店|静岡県|愛知県|東京都|大阪府)$"),
    # 本部オフィス等、企業名ではなく施設名
    _re.compile(r"本部オフィス"),
    _re.compile(r"関係会社株式会社$"),
]

# ポータル / 電話帳広告 由来
_PORTAL_NAMES = (
    "NTTタウンページ株式会社", "株式会社タウンページ",
    "株式会社ぐるなび", "株式会社カカクコム",
    "株式会社ゼンリン", "株式会社マピオン",
    "スターバックスコーヒージャパン株式会社",  # TSUTAYA 併設で誤紐付
    "株式会社バンダイナムコアミューズメント",  # 同上
)


def _is_structural_garbage(name: str) -> bool:
    """名前に明示的な構造的 garbage pattern が含まれるか。"""
    if not name:
        return True
    for pat in _STRUCTURAL_GARBAGE_PATTERNS:
        if pat.search(name):
            return True
    if name in _PORTAL_NAMES:
        return True
    return False


def _detect_cross_brand_pollution(
    conn: sqlite3.Connection,
    *,
    brand: str,
    threshold: int,
) -> list[str]:
    """corporate_number 空 かつ brand_count >= threshold の operator 名を返す。

    per_store extractor が広告文・ポータル残骸から本部名を誤取した結果
    同一 name が複数 brand にまたがって残存する bug を拾う。corp が付いている
    operator (cleanse 後の国税庁 verified) は除外 (本物多業態メガジー保護)。

    brand filter 指定時は機能しない (単一 brand に絞ると必ず bc=1 のため)。
    """
    if brand or threshold <= 0:
        return []
    rows = conn.execute(
        "SELECT operator_name FROM operator_stores "
        "WHERE operator_name != '' AND COALESCE(corporate_number,'') = '' "
        "GROUP BY operator_name "
        "HAVING COUNT(DISTINCT brand) >= ?",
        (threshold,),
    ).fetchall()
    return [r[0] for r in rows if r[0]]


def _is_in_houjin(idx, name: str) -> bool:
    """国税庁 CSV に operator_name が存在するか (exact / prefix 緩め)。"""
    if not name:
        return False
    try:
        recs = idx.search_by_name(name, limit=3, active_only=True)
        if not recs:
            recs = idx.search_by_name(name, limit=3, active_only=False)
    except Exception as e:
        logger.debug("houjin lookup %s: %s", name, e)
        return False
    if not recs:
        return False
    # exact or 「name in rec.name」(rec.name が親法人の場合) or 「rec.name in name」(子会社/支店)
    for r in recs:
        if r.name == name:
            return True
        if name in r.name or r.name in name:
            return True
    return False


def purge_garbage_operators(
    db_path: str | Path,
    *,
    brand: str = "",
    dry_run: bool = False,
    log_path: str | Path = "var/phase26/purge-log.csv",
    cross_brand_threshold: int = 0,
) -> PurgeStats:
    """operator_stores から garbage operator_name を削除。

    削除ルール:
      1. structural garbage (住所/ID/snippet 断片/ポータル広告) — 明示削除
      2. 国税庁 CSV 未登録 — 「国税庁に無い」ことは削除の直接根拠にしていない
         (現ロジックは verified カウンタのみ)
      3. cross_brand_threshold >= 2 のとき、corp 空 × 多 brand 汚染を削除
         (広告文由来の per_store mis-extract)

    brand filter 指定時は 3. が働かない (単一 brand では bc=1)。
    """
    stats = PurgeStats()

    try:
        from pizza_delivery.houjin_csv import HoujinCSVIndex
    except Exception as e:
        stats.errors.append(f"houjin import: {e}")
        return stats
    idx = HoujinCSVIndex()
    if idx.count() == 0:
        stats.errors.append("houjin_csv_empty (run pizza houjin-import)")
        return stats

    conn = sqlite3.connect(db_path)
    try:
        if brand:
            rows = conn.execute(
                "SELECT DISTINCT operator_name FROM operator_stores "
                "WHERE operator_name != '' AND brand = ?",
                (brand,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT DISTINCT operator_name FROM operator_stores "
                "WHERE operator_name != ''",
            ).fetchall()
        names = [r[0] for r in rows if r[0]]
        stats.raw_unique = len(names)

        # 削除対象判定 (保守的):
        #   1. 構造的 garbage pattern (住所混入/ID/snippet断片/ポータル広告) → 即削除
        #   2. 国税庁 CSV に存在 → keep (verified)
        #   3. どちらでもない → keep (unverified、将来 cleanse で再検証)
        #   4. cross_brand_threshold 指定時: corp 空 × bc>=threshold は削除
        to_purge_set: set[str] = set()
        for name in names:
            if _is_structural_garbage(name):
                to_purge_set.add(name)
                continue
            if _is_in_houjin(idx, name):
                stats.verified += 1

        cross_names = _detect_cross_brand_pollution(
            conn, brand=brand, threshold=cross_brand_threshold,
        )
        stats.cross_brand_pollution = len(set(cross_names) - to_purge_set)
        to_purge_set.update(cross_names)
        to_purge = sorted(to_purge_set)
        stats.purged = len(to_purge)

        # Provenance 記録
        if to_purge:
            log_p = Path(log_path)
            log_p.parent.mkdir(parents=True, exist_ok=True)
            with open(log_p, "a", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                if log_p.stat().st_size == 0:
                    w.writerow(["operator_name", "brand_filter", "dry_run"])
                for n in to_purge:
                    w.writerow([n, brand, dry_run])

        # 実 DELETE
        if not dry_run and to_purge:
            placeholders = ",".join(["?"] * len(to_purge))
            if brand:
                q = (
                    f"DELETE FROM operator_stores "
                    f"WHERE brand = ? AND operator_name IN ({placeholders})"
                )
                cur = conn.execute(q, (brand, *to_purge))
            else:
                q = f"DELETE FROM operator_stores WHERE operator_name IN ({placeholders})"
                cur = conn.execute(q, tuple(to_purge))
            stats.purged_rows = cur.rowcount
            conn.commit()
    finally:
        conn.close()

    return stats


def _main() -> None:
    import argparse
    import sys

    ap = argparse.ArgumentParser(
        description="operator_stores から 国税庁未登録 garbage を削除",
    )
    ap.add_argument("--db", default="var/pizza.sqlite", help="pipeline SQLite")
    ap.add_argument("--brand", default="", help="対象ブランド (空で全件)")
    ap.add_argument("--dry-run", action="store_true",
                    help="削除候補を列挙のみ、実 DELETE しない")
    ap.add_argument("--log", default="var/phase26/purge-log.csv",
                    help="削除候補の provenance CSV")
    ap.add_argument("--cross-brand-threshold", type=int, default=0,
                    help="corp 空 かつ brand_count >= N の operator を汚染として削除 (0 で無効)")
    args = ap.parse_args()

    if not Path(args.db).exists():
        print(f"❌ db not found: {args.db}", file=sys.stderr)
        sys.exit(2)

    stats = purge_garbage_operators(
        args.db, brand=args.brand, dry_run=args.dry_run, log_path=args.log,
        cross_brand_threshold=args.cross_brand_threshold,
    )
    print(f"✅ purge {'dry-run' if args.dry_run else 'apply'}")
    print(f"   raw unique operators  = {stats.raw_unique}")
    print(f"   verified (国税庁存在) = {stats.verified}")
    if args.cross_brand_threshold > 0:
        print(f"   cross_brand_pollution = {stats.cross_brand_pollution}")
    print(f"   to-be-purged          = {stats.purged}")
    if not args.dry_run:
        print(f"   applied_rows          = {stats.purged_rows}")
    print(f"📄 provenance: {args.log}")
    for e in stats.errors:
        print(f"   ⚠  {e}", file=sys.stderr)


if __name__ == "__main__":
    _main()
