"""FC 関連事業会社 総合リスト生成 — 複数ソースの ORM 統合。

このモジュールが PI-ZZA の「事業会社を主語にした網羅リスト」構築の中核。
以下の 3 ソースを 1 つの ORM DB (var/pizza-registry.sqlite) に統合する:

  1. **JFA 会員一覧** (pipeline: pizza jfa-sync)
     → franchisor (本部) 企業と代表ブランド

  2. **国税庁 法人番号 CSV** (pipeline: pizza houjin-import)
     → 法人番号 / 本社住所 の Ground Truth。operator の検証に使用

  3. **operator_stores テーブル** (pipeline: pizza bake → research)
     → Places scan / per_store 抽出で発見された実 franchisee 候補

統合処理:
  - 全 operator に法人番号を付与する (名寄せ → Houjin CSV lookup)
  - franchisor/franchisee 両方を OperatorCompany に集約
  - brand × operator の全リンクを BrandOperatorLink に記録
  - 結果を CSV で export (pizza integrate export)

本モジュールには **LLM 推論を一切介在させない** (決定論)。
LLM による operator 名正規化は別の専用モジュール (llm_cleanser.py) に分離。
"""

from __future__ import annotations

import csv as csv_mod
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from pizza_delivery.orm import (
    BrandOperatorLink,
    FranchiseBrand,
    OperatorCompany,
    link_brand_operator,
    make_session,
    upsert_brand,
    upsert_operator,
)

logger = logging.getLogger(__name__)


@dataclass
class IntegrationStats:
    """統合処理の集計。"""

    houjin_hydrated: int = 0       # Houjin CSV で法人番号付与に成功した operator 数
    pipeline_operators_added: int = 0  # operator_stores から ORM に追加した件数
    brand_links_added: int = 0     # BrandOperatorLink 新規追加
    errors: list[str] = field(default_factory=list)


# ─── 1. Houjin CSV で法人番号を注入 ─────────────────────────────


def hydrate_corporate_numbers(
    session: Session,
    houjin_db_path: str | Path | None = None,
    min_similarity: float = 0.9,
) -> int:
    """OperatorCompany の corporate_number が空な行に対して、
    Houjin CSV index で名寄せして法人番号を注入する。

    戻り値: 新たに corporate_number を付与できた operator 数。
    """
    from pizza_delivery.houjin_csv import HoujinCSVIndex
    from pizza_delivery.houjin_bangou import _name_similarity

    idx = HoujinCSVIndex(houjin_db_path)
    if idx.count() == 0:
        logger.warning("Houjin CSV index が空。pizza houjin-import を先に実行してください")
        return 0

    hydrated = 0
    operators = (
        session.query(OperatorCompany)
        .filter(OperatorCompany.corporate_number == "")
        .all()
    )
    for op in operators:
        # 「株式会社XXX ABC Inc.」のような複合表記から日本語 法人名部分だけ抽出する
        # → JFA sync 結果は「株式会社モスフードサービス MOS FOOD SERVICES INC.」の形式
        jp_name = _extract_japanese_prefix(op.name)
        if not jp_name:
            continue
        candidates = idx.search_by_name(jp_name, limit=10)
        if not candidates:
            continue
        # 類似度で best match 選抜
        best_sim = 0.0
        best = None
        for c in candidates:
            s = _name_similarity(jp_name, c.name)
            if s > best_sim:
                best_sim = s
                best = c
        if best is None or best_sim < min_similarity:
            continue

        # 同じ 法人番号を既に別 operator が持っていたら、そちらに merge する
        existing = (
            session.query(OperatorCompany)
            .filter(
                OperatorCompany.corporate_number == best.corporate_number,
                OperatorCompany.id != op.id,
            )
            .one_or_none()
        )
        if existing is not None:
            # op の全 link を existing に付け替え、op を削除
            for link in list(op.links):
                link.operator_id = existing.id
            session.flush()
            session.delete(op)
            # existing の住所を補完 (空なら)
            if not existing.head_office:
                existing.head_office = (
                    f"{best.prefecture}{best.city}{best.street}".strip()
                )
            if not existing.prefecture:
                existing.prefecture = best.prefecture
        else:
            op.corporate_number = best.corporate_number
            op.head_office = f"{best.prefecture}{best.city}{best.street}".strip()
            if not op.prefecture:
                op.prefecture = best.prefecture
        hydrated += 1
    session.commit()
    return hydrated


def _extract_japanese_prefix(s: str) -> str:
    """『株式会社モスフードサービス MOS FOOD SERVICES INC.』→
    『株式会社モスフードサービス』を返す。

    空白以降の英数字部を切り落とす。先頭から日本語/全角+社格までを採用。
    """
    s = (s or "").strip()
    if not s:
        return ""
    # 1. 連続スペースの前の部分を先に切り出し
    parts = s.split()
    # 先頭から『株式会社…』の連続を探す。ASCII のみの token は除外。
    taken: list[str] = []
    for p in parts:
        if _is_ascii_token(p):
            break
        taken.append(p)
    jp = "".join(taken) if taken else parts[0]
    return jp


def _is_ascii_token(t: str) -> bool:
    return all(ord(c) < 128 for c in t) and len(t) > 0


# ─── 2. operator_stores を ORM に取込 ───────────────────────────


def import_pipeline_operators(
    session: Session,
    pipeline_db_path: str | Path,
    *,
    min_stores: int = 1,
) -> int:
    """pipeline 側の SQLite (var/pizza.sqlite) から operator_stores を読み、
    ORM の BrandOperatorLink + OperatorCompany に upsert。

    cross-brand の本部混入 (例: モスバーガー brand で『ドムドムフードサービス』
    が抽出されるケース) は registry_expander のブロックリストで除外する。

    戻り値: 新規 link 数。
    """
    from pizza_delivery.normalize import canonical_key
    from pizza_delivery.registry_expander import _load_known_franchisor_names

    path = Path(pipeline_db_path)
    if not path.exists():
        logger.warning("pipeline DB が見つからない: %s", path)
        return 0

    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(
            """
            SELECT operator_name, brand, COUNT(DISTINCT place_id) AS n,
                   COALESCE(MAX(corporate_number),'') AS cn,
                   COALESCE(MIN(operator_type),'') AS ot,
                   COALESCE(MIN(discovered_via),'') AS dv
            FROM operator_stores
            WHERE operator_name != '' AND brand != ''
            GROUP BY operator_name, brand
            HAVING n >= ?
            """,
            (min_stores,),
        ).fetchall()
    finally:
        conn.close()

    # 本部・異 brand 本部 ブロックリスト (canonical_key 正規化済)
    block = {canonical_key(n) for n in _load_known_franchisor_names()}

    added = 0
    skipped = 0
    for op_name, brand_name, n, cn, ot, dv in rows:
        if canonical_key(op_name) in block:
            logger.info(
                "integrate: skip franchisor/別本部混入 %s × %s",
                op_name, brand_name,
            )
            skipped += 1
            continue
        brand = upsert_brand(session, brand_name, source="pipeline")
        op = upsert_operator(
            session,
            name=op_name,
            corporate_number=cn,
            kind=ot or "unknown",
            source="pipeline",
        )
        session.flush()  # ids を確定
        link_brand_operator(
            session,
            brand=brand,
            operator=op,
            estimated_store_count=int(n),
            operator_type=ot or "franchisee",
            source="pipeline",
            note=f"discovered_via={dv}",
        )
        added += 1
    if skipped:
        logger.info("integrate: skipped %d franchisor-like rows", skipped)
    session.commit()
    return added


# ─── 3. 総合統合 ─────────────────────────────────────────────


def integrate_all(
    *,
    pipeline_db_path: str | Path,
    houjin_db_path: str | Path | None = None,
    orm_session: Session | None = None,
    skip_houjin: bool = False,
) -> IntegrationStats:
    """3 ソースを ORM に統合する total run。

    Steps:
      1. pipeline (operator_stores) を ORM に取込
      2. Houjin CSV で全 operator の corporate_number 埋める
    """
    stats = IntegrationStats()
    sess = orm_session or make_session()
    try:
        try:
            stats.brand_links_added = import_pipeline_operators(sess, pipeline_db_path)
        except Exception as e:
            stats.errors.append(f"pipeline import: {e}")

        if not skip_houjin:
            try:
                stats.houjin_hydrated = hydrate_corporate_numbers(sess, houjin_db_path)
            except Exception as e:
                stats.errors.append(f"houjin hydrate: {e}")
    finally:
        if orm_session is None:
            sess.close()
    return stats


# ─── 4. Unified export ────────────────────────────────────────


def export_unified_csv(
    out_path: str | Path,
    *,
    orm_session: Session | None = None,
    source_filter: str | None = None,
) -> int:
    """全ての BrandOperatorLink を 1 CSV にまとめて出力 (FC 事業会社 総合リスト)。

    列:
      brand_name, industry, operator_name, corporate_number, head_office,
      prefecture, operator_type, estimated_store_count, source, source_url, note
    """
    sess = orm_session or make_session()
    rows: list[tuple] = []
    try:
        q = (
            sess.query(BrandOperatorLink)
            .options(
                joinedload(BrandOperatorLink.brand),
                joinedload(BrandOperatorLink.operator),
            )
        )
        if source_filter:
            q = q.filter(BrandOperatorLink.source == source_filter)
        for link in q.order_by(
            BrandOperatorLink.estimated_store_count.desc(),
            BrandOperatorLink.id,
        ).all():
            rows.append(
                (
                    link.brand.name if link.brand else "",
                    link.brand.industry if link.brand else "",
                    link.operator.name if link.operator else "",
                    link.operator.corporate_number if link.operator else "",
                    link.operator.head_office if link.operator else "",
                    link.operator.prefecture if link.operator else "",
                    link.operator_type,
                    link.estimated_store_count,
                    link.source,
                    link.source_url,
                    link.note,
                )
            )
    finally:
        if orm_session is None:
            sess.close()

    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv_mod.writer(f)
        w.writerow([
            "brand_name", "industry", "operator_name", "corporate_number",
            "head_office", "prefecture", "operator_type", "estimated_store_count",
            "source", "source_url", "note",
        ])
        w.writerows(rows)
    return len(rows)


# ─── CLI ────────────────────────────────────────────────────


def _main() -> None:
    import argparse
    import sys

    ap = argparse.ArgumentParser(description="FC 事業会社 総合リスト統合ツール")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="3 ソース統合を実行")
    p_run.add_argument("--pipeline-db", default="var/pizza.sqlite")
    p_run.add_argument("--houjin-db", default="", help="空で default var/houjin/registry.sqlite")
    p_run.add_argument(
        "--skip-houjin", action="store_true",
        help="pipeline operator import のみ実行し、国税庁 hydrate は後で回す",
    )

    p_export = sub.add_parser("export", help="総合 CSV 出力")
    p_export.add_argument("--out", required=True)
    p_export.add_argument("--source", default="", help="source フィルタ (空で全件)")

    args = ap.parse_args()

    if args.cmd == "run":
        houjin = args.houjin_db or None
        stats = integrate_all(
            pipeline_db_path=args.pipeline_db,
            houjin_db_path=houjin,
            skip_houjin=args.skip_houjin,
        )
        print("✅ integration complete")
        print(f"   pipeline_operators_added = {stats.brand_links_added}")
        print(f"   houjin_hydrated         = {stats.houjin_hydrated}")
        for e in stats.errors:
            print(f"   ⚠️  {e}", file=sys.stderr)
        return

    if args.cmd == "export":
        source = args.source or None
        n = export_unified_csv(args.out, source_filter=source)
        print(f"✅ wrote {n} rows → {args.out}")
        return


if __name__ == "__main__":
    _main()
