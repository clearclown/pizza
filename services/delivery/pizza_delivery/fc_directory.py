"""都道府県別 FC 運営事業会社ディレクトリ (Phase 25+)。

**pizza が自律的に** 以下を実行:
  1. ORM (FranchiseBrand × OperatorCompany × BrandOperatorLink) 走査
  2. 国税庁法人番号 CSV (577 万件) で prefecture/住所補完
  3. corporate_number で dedup
  4. brand_count 降順 + total_stores 降順で CSV / JSON 出力

CLI:
    pizza fc-directory --prefecture 東京都 --out var/tokyo-fc-operators.csv

LLM 不使用、ハルシネーション 0。
"""

from __future__ import annotations

import csv as csv_mod
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DirectoryEntry:
    """FC 事業会社 1 行分。"""

    operator_name: str
    corporate_number: str = ""
    head_office: str = ""
    prefecture: str = ""                 # operator 本社の都道府県
    website_url: str = ""
    brand_count: int = 0
    total_stores_est: int = 0            # 全国 estimated_store_count (ORM 由来)
    stores_in_target_pref: int = 0       # Phase 26: stores_in_prefecture 時の対象県店舗数
    brands_breakdown: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "operator_name": self.operator_name,
            "corporate_number": self.corporate_number,
            "head_office": self.head_office,
            "hq_prefecture": self.prefecture,
            "website_url": self.website_url,
            "brand_count": self.brand_count,
            "total_stores_est": self.total_stores_est,
            "stores_in_target_pref": self.stores_in_target_pref,
            "brands_breakdown": ";".join(
                f"{k}:{v}" for k, v in sorted(
                    self.brands_breakdown.items(), key=lambda x: -x[1],
                )
            ),
        }


def _resolve_prefecture(
    ops_head_office: str,
    ops_prefecture: str,
    corporate_number: str,
    operator_name: str,
    idx,
    *,
    hydrate_by_name: bool = False,
) -> tuple[str, str, str]:
    """prefecture / address / corp_number を段階的に解決。

    返却: (prefecture, address, corporate_number) — 解決できない場合 ("", head_office, corp)
    """
    # ORM head_office 最優先
    if ops_head_office:
        for pref in (
            "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
            "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
            "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
            "岐阜県", "静岡県", "愛知県", "三重県",
            "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
            "鳥取県", "島根県", "岡山県", "広島県", "山口県",
            "徳島県", "香川県", "愛媛県", "高知県",
            "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県",
            "鹿児島県", "沖縄県",
        ):
            if ops_head_office.startswith(pref):
                return pref, ops_head_office, corporate_number

    if ops_prefecture:
        return ops_prefecture, ops_head_office, corporate_number

    # 法人番号で houjin_csv lookup
    if corporate_number:
        try:
            conn = sqlite3.connect(idx.db_path)
            r = conn.execute(
                "SELECT prefecture, city, street FROM houjin_registry WHERE corporate_number = ?",
                (corporate_number,),
            ).fetchone()
            conn.close()
            if r:
                pref, city, street = r
                return pref or "", f"{pref}{city}{street}", corporate_number
        except Exception as e:
            logger.debug("houjin lookup by corp failed: %s", e)

    if not hydrate_by_name:
        return "", ops_head_office, corporate_number

    # 法人名で houjin_csv lookup。大量実行では遅いため明示指定時のみ。
    try:
        recs = idx.search_by_name(operator_name, limit=1, active_only=True)
        if not recs:
            recs = idx.search_by_name(operator_name, limit=1, active_only=False)
    except Exception as e:
        logger.debug("houjin lookup by name failed: %s", e)
        recs = []
    if recs:
        r = recs[0]
        return r.prefecture or "", r.address, r.corporate_number

    return "", ops_head_office, corporate_number


def _operators_with_stores_in_prefecture(
    pipeline_db_path: str, prefecture: str,
) -> dict[str, int]:
    """pipeline DB の operator_stores JOIN stores で、
    指定都道府県内に 1+ 店舗を持つ operator 名 → 店舗数 dict を返す。

    Phase 26: operator 本社所在地に関係なく「当該県内に進出している」
    operator を検出する。地方本社 FC (ありがとう愛媛/大和フーヅ埼玉 等) が
    東京都進出リストに含まれる様に。
    """
    if not pipeline_db_path or not prefecture:
        return {}
    p = Path(pipeline_db_path)
    if not p.exists():
        return {}
    try:
        conn = sqlite3.connect(pipeline_db_path)
        try:
            rows = conn.execute(
                "SELECT os.operator_name, COUNT(DISTINCT os.place_id) "
                "FROM operator_stores os "
                "JOIN stores s ON s.place_id = os.place_id "
                "WHERE os.operator_name != '' "
                "  AND s.address LIKE ? "
                "GROUP BY os.operator_name",
                (f"%{prefecture}%",),
            ).fetchall()
            return {name: int(cnt) for name, cnt in rows if name}
        finally:
            conn.close()
    except Exception as e:
        logger.debug("operators_with_stores_in_prefecture failed: %s", e)
        return {}


def build_directory(
    *,
    prefecture: str = "",
    stores_in_prefecture: str = "",
    pipeline_db: str = "",
    brands_filter: set[str] | None = None,
    include_zero_stores: bool = True,
    hydrate_by_name: bool = False,
    include_franchisor: bool = False,
) -> list[DirectoryEntry]:
    """ORM + 国税庁 CSV を融合して事業会社一覧を構築。

    Args:
      prefecture: 本社所在地の都道府県 filter (空で全国)
      stores_in_prefecture: Phase 26 — pipeline DB に当該都道府県内店舗を
        持つ operator のみ残す (本社所在地 不問)。空で filter 無し。
      pipeline_db: pipeline SQLite path (stores_in_prefecture 利用時必須)
      include_zero_stores: estimated_store_count=0 の operator も含めるか
      hydrate_by_name: 法人番号なし operator を国税庁 name lookup で補完するか。
        substring fallback が重いため、通常の全国出力では False 推奨。
      include_franchisor: 本部/direct link も出力対象に含めるか。

    両 filter 指定時は AND (本社 pref AND 店舗進出 pref)。
    """
    from pizza_delivery.houjin_csv import HoujinCSVIndex
    from pizza_delivery.normalize import canonical_key
    from pizza_delivery.orm import (
        BrandOperatorLink, FranchiseBrand, OperatorCompany, make_session,
    )
    from pizza_delivery.registry_expander import _load_known_franchisor_names

    # Phase 26: stores_in_prefecture 指定時は pipeline DB を参照して
    # 進出 operator の set を先に取る
    stores_in_pref_map: dict[str, int] = {}
    if stores_in_prefecture:
        db = pipeline_db or "var/pizza.sqlite"
        stores_in_pref_map = _operators_with_stores_in_prefecture(
            db, stores_in_prefecture,
        )

    sess = make_session()
    idx = HoujinCSVIndex()
    franchisor_block = {canonical_key(n) for n in _load_known_franchisor_names()}
    try:
        # corporate_number で dedup (空なら name で dedup)
        grouped: dict[str, dict] = {}

        rows = (
            sess.query(OperatorCompany, FranchiseBrand, BrandOperatorLink)
            .join(BrandOperatorLink, BrandOperatorLink.operator_id == OperatorCompany.id)
            .join(FranchiseBrand, BrandOperatorLink.brand_id == FranchiseBrand.id)
            .all()
        )
        for op, brand, link in rows:
            if brands_filter and brand.name not in brands_filter:
                continue
            if not include_franchisor and (op.kind or "") == "franchisor":
                continue
            if (
                not include_franchisor
                and (link.operator_type or "") in {"franchisor", "direct"}
            ):
                continue
            if not include_franchisor and canonical_key(op.name) in franchisor_block:
                continue
            key = op.corporate_number if op.corporate_number else f"NAME::{op.name}"
            e = grouped.setdefault(key, {
                "name": op.name,
                "corp": op.corporate_number or "",
                "head_office": op.head_office or "",
                "prefecture": op.prefecture or "",
                "website": op.website_url or "",
                "brands": {},
            })
            # 情報量多い方で上書き
            if op.head_office and len(op.head_office) > len(e["head_office"]):
                e["head_office"] = op.head_office
            if op.website_url and not e["website"]:
                e["website"] = op.website_url
            if op.corporate_number and not e["corp"]:
                e["corp"] = op.corporate_number
            if op.prefecture and not e["prefecture"]:
                e["prefecture"] = op.prefecture
            cnt = int(link.estimated_store_count or 0)
            b_name = brand.name
            e["brands"][b_name] = max(e["brands"].get(b_name, 0), cnt)

        results: list[DirectoryEntry] = []
        for _, e in grouped.items():
            pref, address, corp = _resolve_prefecture(
                e["head_office"], e["prefecture"], e["corp"], e["name"], idx,
                hydrate_by_name=hydrate_by_name,
            )
            # 本社 pref filter
            if prefecture and pref != prefecture:
                continue
            # Phase 26: store 進出 pref filter
            stores_in_target = 0
            if stores_in_prefecture:
                stores_in_target = stores_in_pref_map.get(e["name"], 0)
                if stores_in_target == 0:
                    continue
            total = sum(e["brands"].values())
            if not include_zero_stores and total == 0 and stores_in_target == 0:
                continue
            results.append(DirectoryEntry(
                operator_name=e["name"],
                corporate_number=corp,
                head_office=address or e["head_office"],
                prefecture=pref,
                website_url=e["website"],
                brand_count=len(e["brands"]),
                total_stores_est=total,
                stores_in_target_pref=stores_in_target,
                brands_breakdown=dict(e["brands"]),
            ))

        # brand_count 降順 → total_stores 降順 → 名前
        results.sort(key=lambda x: (-x.brand_count, -x.total_stores_est, x.operator_name))
        return results
    finally:
        sess.close()


def export_csv(entries: list[DirectoryEntry], out_path: str | Path) -> None:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "operator_name", "corporate_number", "head_office", "hq_prefecture",
        "website_url", "brand_count", "total_stores_est",
        "stores_in_target_pref", "brands_breakdown",
    ]
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv_mod.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for e in entries:
            w.writerow(e.to_dict())


def export_json(entries: list[DirectoryEntry], out_path: str | Path) -> None:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    data = [e.to_dict() for e in entries]
    # brands_breakdown を dict 形式でも保持
    for raw, entry in zip(data, entries):
        raw["brands_breakdown_dict"] = entry.brands_breakdown
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _entry_key(name: str, corporate_number: str) -> str:
    return corporate_number or f"NAME::{name}"


def export_component_csv(
    entries: list[DirectoryEntry],
    out_path: str | Path,
    *,
    brands_filter: set[str] | None = None,
    include_franchisor: bool = False,
    qualified_total_threshold: int = 0,
) -> None:
    """Qualified operator の全 brand component を出力する。

    `--min-total 10` で operator を絞った後、その operator の brand 内訳は
    1 店舗の side brand でも残す。例: モス 1 + エニタイム 30 は total 31
    として採用し、component CSV にはモス 1 行も出す。
    """
    from pizza_delivery.normalize import canonical_key
    from pizza_delivery.orm import (
        BrandOperatorLink, FranchiseBrand, OperatorCompany, make_session,
    )
    from pizza_delivery.registry_expander import _load_known_franchisor_names

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "qualified_total_threshold",
        "brand",
        "operator_name",
        "corporate_number",
        "head_office",
        "hq_prefecture",
        "website_url",
        "brand_estimated_store_count",
        "operator_total_stores_est",
        "operator_brand_count",
        "sources",
        "source_urls",
        "notes",
    ]
    qualified = {
        _entry_key(e.operator_name, e.corporate_number): e
        for e in entries
    }
    if not qualified:
        with open(out, "w", encoding="utf-8", newline="") as f:
            csv_mod.DictWriter(f, fieldnames=cols).writeheader()
        return

    sess = make_session()
    franchisor_block = {canonical_key(n) for n in _load_known_franchisor_names()}
    components: dict[tuple[str, str], dict[str, Any]] = {}
    try:
        rows = (
            sess.query(OperatorCompany, FranchiseBrand, BrandOperatorLink)
            .join(BrandOperatorLink, BrandOperatorLink.operator_id == OperatorCompany.id)
            .join(FranchiseBrand, BrandOperatorLink.brand_id == FranchiseBrand.id)
            .all()
        )
        for op, brand, link in rows:
            if brands_filter and brand.name not in brands_filter:
                continue
            if not include_franchisor and (op.kind or "") == "franchisor":
                continue
            if (
                not include_franchisor
                and (link.operator_type or "") in {"franchisor", "direct"}
            ):
                continue
            if not include_franchisor and canonical_key(op.name) in franchisor_block:
                continue
            key = _entry_key(op.name, op.corporate_number or "")
            entry = qualified.get(key)
            if entry is None:
                continue
            ckey = (key, brand.name)
            item = components.setdefault(ckey, {
                "qualified_total_threshold": qualified_total_threshold,
                "brand": brand.name,
                "operator_name": entry.operator_name,
                "corporate_number": entry.corporate_number,
                "head_office": entry.head_office,
                "hq_prefecture": entry.prefecture,
                "website_url": entry.website_url,
                "brand_estimated_store_count": 0,
                "operator_total_stores_est": entry.total_stores_est,
                "operator_brand_count": entry.brand_count,
                "sources": set(),
                "source_urls": set(),
                "notes": set(),
            })
            item["brand_estimated_store_count"] = max(
                int(item["brand_estimated_store_count"]),
                int(link.estimated_store_count or 0),
            )
            if link.source:
                item["sources"].add(link.source)
            if link.source_url:
                item["source_urls"].add(link.source_url)
            if link.note:
                item["notes"].add(link.note)
    finally:
        sess.close()

    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv_mod.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for item in sorted(
            components.values(),
            key=lambda r: (
                -int(r["operator_total_stores_est"]),
                str(r["operator_name"]),
                -int(r["brand_estimated_store_count"]),
                str(r["brand"]),
            ),
        ):
            row = dict(item)
            row["sources"] = ";".join(sorted(row["sources"]))
            row["source_urls"] = ";".join(sorted(row["source_urls"]))
            row["notes"] = ";".join(sorted(row["notes"]))
            w.writerow(row)


def _main() -> None:
    import argparse
    import sys

    ap = argparse.ArgumentParser(
        description="都道府県別 FC 運営事業会社ディレクトリを ORM + 国税庁 CSV から構築"
    )
    ap.add_argument(
        "--prefecture", default="",
        help="本社所在地の都道府県 filter (例: 東京都)、空で全国",
    )
    ap.add_argument(
        "--stores-in-prefecture", default="",
        help="Phase 26: pipeline DB 上で当該都道府県に店舗を持つ operator "
             "のみ残す (本社所在地 不問)、例: 東京都",
    )
    ap.add_argument(
        "--pipeline-db", default="var/pizza.sqlite",
        help="stores_in_prefecture filter 用の pipeline SQLite",
    )
    ap.add_argument(
        "--brands", default="",
        help="カンマ区切りブランド filter。空なら全ブランド",
    )
    ap.add_argument("--out", default="var/fc-directory.csv")
    ap.add_argument("--out-json", default="")
    ap.add_argument(
        "--component-out", default="",
        help="qualified operator の brand 別 component CSV 出力。"
             "--min-total と組み合わせると、合計条件を満たした operator の"
             "全ブランド内訳を 1 店舗 side brand まで残す",
    )
    ap.add_argument(
        "--min-total", type=int, default=0,
        help="operator 合計 estimated_store_count の下限。0 で無効",
    )
    ap.add_argument(
        "--min-brands", type=int, default=0,
        help="operator brand_count の下限。0 で無効",
    )
    ap.add_argument(
        "--include-zero-stores", action="store_true",
        help="estimated_store_count=0 の operator も含める (default: 含める)",
    )
    ap.add_argument(
        "--exclude-zero-stores", action="store_true",
        help="estimated_store_count=0 を除外",
    )
    ap.add_argument(
        "--hydrate-by-name", action="store_true",
        help="法人番号なし operator を国税庁 name lookup で補完する "
             "(全国一括では遅い)",
    )
    ap.add_argument(
        "--include-franchisor", action="store_true",
        help="本部/direct link も出力に含める",
    )
    args = ap.parse_args()

    include_zero = not args.exclude_zero_stores
    brands_filter = {b.strip() for b in args.brands.split(",") if b.strip()} or None
    entries = build_directory(
        prefecture=args.prefecture,
        stores_in_prefecture=args.stores_in_prefecture,
        pipeline_db=args.pipeline_db,
        brands_filter=brands_filter,
        include_zero_stores=include_zero,
        hydrate_by_name=args.hydrate_by_name,
        include_franchisor=args.include_franchisor,
    )
    if args.min_total > 0:
        entries = [e for e in entries if e.total_stores_est >= args.min_total]
    if args.min_brands > 0:
        entries = [e for e in entries if e.brand_count >= args.min_brands]
    export_csv(entries, args.out)
    if args.out_json:
        export_json(entries, args.out_json)
    if args.component_out:
        export_component_csv(
            entries,
            args.component_out,
            brands_filter=brands_filter,
            include_franchisor=args.include_franchisor,
            qualified_total_threshold=args.min_total,
        )

    total_stores = sum(e.total_stores_est for e in entries)
    total_tp = sum(e.stores_in_target_pref for e in entries)
    print(f"✅ FC operator directory: {len(entries)} companies")
    if args.prefecture:
        print(f"   HQ prefecture filter: {args.prefecture}")
    if args.stores_in_prefecture:
        print(f"   stores-in-prefecture: {args.stores_in_prefecture}")
        print(f"   total stores in {args.stores_in_prefecture}: {total_tp}")
    if brands_filter:
        print(f"   brand filter: {len(brands_filter)} brands")
    if args.min_total > 0:
        print(f"   min total stores: {args.min_total}")
    if args.min_brands > 0:
        print(f"   min brands: {args.min_brands}")
    print(f"   total_stores_estimated = {total_stores}")
    print(f"📄 CSV: {args.out}")
    if args.out_json:
        print(f"📄 JSON: {args.out_json}")
    if args.component_out:
        print(f"📄 Components: {args.component_out}")
    print()
    print("Top 20 by brand_count (then total_stores):")
    for e in entries[:20]:
        brands = ", ".join(sorted(e.brands_breakdown.keys()))[:45]
        tp_suffix = f"/ {e.stores_in_target_pref}店(当該県)" if args.stores_in_prefecture else ""
        print(
            f"  {e.brand_count:2d}ブランド / {e.total_stores_est:5d}店{tp_suffix}  "
            f"{e.operator_name[:30]:30s}  {e.corporate_number or '-':13s}  "
            f"[{brands}]"
        )


if __name__ == "__main__":
    _main()
