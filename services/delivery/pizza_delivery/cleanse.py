"""pizza cleanse — operator_stores の dirty 名を LLM canonicalize + 国税庁検証。

Phase 26 の garbage issue 対応:
  - `株式会社コメダホールディングスコメダ` (連結 bug)
  - `株式会社シャトレーゼ企業` (suffix ゴミ)
  - `-0023東京都新宿区...` (住所 mis-extract)
  - `株式会社STAYGOLD東京都公安委員会第303311408` (末尾 ID 混入)
  - `HARD OFF` vs `ハードオフ` (表記揺れ)

ハルシネーション防止設計:
  - LLM は **既存 dirty 文字列の canonical 変換のみ** (free-form 生成禁止)
  - 変換後の canonical 名は **必ず 国税庁 577 万 CSV で存在検証**
  - 国税庁 に無い → reject (update しない)
  - provenance: raw name と canonical 名を両方保持、discovered_via タグで由来明示

CLI:
  pizza cleanse --db var/pizza.sqlite --brand モスバーガー
  pizza cleanse --db var/pizza.sqlite --dry-run    # 提案のみ、apply せず
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class CleanseProposal:
    """1 operator 名のクレンジング提案 (未 apply)。"""

    raw_name: str
    canonical_name: str = ""
    corporate_number: str = ""
    verified: bool = False
    reason: str = ""          # 却下理由 or 受理メモ
    confidence: float = 0.0


@dataclass
class CleanseStats:
    """1 回の cleanse 実行の集計。"""

    raw_operators: int = 0
    proposed: int = 0
    verified_updates: int = 0
    rejected_not_in_houjin: int = 0
    rejected_not_legal_entity: int = 0
    llm_failures: int = 0
    applied_rows: int = 0       # UPDATE 成立行数
    errors: list[str] = field(default_factory=list)


async def _cleanse_one(
    raw_name: str,
    llm: Any,
    houjin_idx: Any,
) -> CleanseProposal:
    """1 operator 名 を LLM canonicalize + 国税庁 CSV 検証。"""
    from pizza_delivery.llm_cleanser import canonicalize_operator_name

    # 1. LLM canonicalize (失敗時 graceful)
    try:
        r = await canonicalize_operator_name(raw_name, llm)
    except Exception as e:
        return CleanseProposal(raw_name=raw_name, reason=f"llm_error: {e}")

    if not r.is_legal_entity:
        return CleanseProposal(
            raw_name=raw_name, reason="not_legal_entity",
            confidence=r.confidence,
        )
    if not r.canonical or r.canonical == raw_name:
        # 変換不要 or 空 → そのまま検証のみ
        canonical = r.canonical or raw_name
    else:
        canonical = r.canonical

    # 2. 国税庁 CSV で exact match 検証
    recs = []
    try:
        recs = houjin_idx.search_by_name(canonical, limit=3, active_only=True)
        if not recs:
            recs = houjin_idx.search_by_name(canonical, limit=3, active_only=False)
    except Exception as e:
        return CleanseProposal(
            raw_name=raw_name, canonical_name=canonical,
            reason=f"houjin_error: {e}", confidence=r.confidence,
        )

    # 3. exact match を優先、無ければ prefix も許容
    best = None
    for rec in recs:
        if rec.name == canonical:
            best = rec
            break
    if best is None and recs:
        # 最も短い候補 (= より親 level の法人) を採用
        best = min(recs, key=lambda x: len(x.name))

    if best is None:
        return CleanseProposal(
            raw_name=raw_name, canonical_name=canonical,
            reason="not_in_houjin", confidence=r.confidence,
        )

    return CleanseProposal(
        raw_name=raw_name,
        canonical_name=best.name,
        corporate_number=best.corporate_number,
        verified=True,
        reason="verified",
        confidence=min(1.0, r.confidence + 0.1),
    )


async def cleanse_operator_stores(
    db_path: str | Path,
    *,
    brand: str = "",
    llm: Any = None,
    dry_run: bool = False,
    concurrency: int = 3,
) -> tuple[CleanseStats, list[CleanseProposal]]:
    """operator_stores の dirty operator_name を一括 cleanse。

    dry_run=True なら proposal を作成するだけで DB を update しない。
    """
    stats = CleanseStats()
    proposals: list[CleanseProposal] = []

    # LLM provider 準備
    if llm is None:
        try:
            from pizza_delivery.providers.registry import get_provider

            provider = None
            for name in ("anthropic", "gemini"):
                try:
                    p = get_provider(name)
                    if p.ready():
                        provider = p
                        break
                except Exception:
                    continue
            if provider is None:
                stats.errors.append("no_llm_provider_ready (set ANTHROPIC_API_KEY or GEMINI_API_KEY)")
                return stats, proposals
            llm = provider.make_llm()
        except Exception as e:
            stats.errors.append(f"llm_init_failed: {e}")
            return stats, proposals

    # Houjin CSV index
    from pizza_delivery.houjin_csv import HoujinCSVIndex

    houjin_idx = HoujinCSVIndex()
    if houjin_idx.count() == 0:
        stats.errors.append("houjin_csv_empty (run pizza houjin-import first)")
        return stats, proposals

    # 1. dirty operator names 取得
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
        raw_names = [r[0] for r in rows if r[0]]
    finally:
        conn.close()

    stats.raw_operators = len(raw_names)
    if not raw_names:
        return stats, proposals

    # 2. 並列 canonicalize (concurrency で制御)
    sem = asyncio.Semaphore(concurrency)

    async def _task(name: str) -> CleanseProposal:
        async with sem:
            return await _cleanse_one(name, llm, houjin_idx)

    proposals = await asyncio.gather(*(_task(n) for n in raw_names))

    # 3. 統計集計
    for p in proposals:
        stats.proposed += 1
        if p.reason == "not_legal_entity":
            stats.rejected_not_legal_entity += 1
        elif p.reason == "not_in_houjin":
            stats.rejected_not_in_houjin += 1
        elif p.reason.startswith("llm_error") or p.reason.startswith("houjin_error"):
            stats.llm_failures += 1
            stats.errors.append(f"{p.raw_name}: {p.reason}")
        if p.verified and p.canonical_name != p.raw_name:
            stats.verified_updates += 1

    # 4. DB update (dry_run 時 skip)
    if not dry_run:
        conn = sqlite3.connect(db_path)
        try:
            # operator_stores のスキーマに corporate_number 列があるか確認
            cols = {r[1] for r in conn.execute("PRAGMA table_info(operator_stores)").fetchall()}
            has_corp = "corporate_number" in cols
            has_disc = "discovered_via" in cols

            for p in proposals:
                if not p.verified:
                    continue
                if has_corp and has_disc:
                    cur = conn.execute(
                        "UPDATE operator_stores SET operator_name=?, "
                        "corporate_number=COALESCE(NULLIF(corporate_number,''),?), "
                        "discovered_via=COALESCE(NULLIF(discovered_via,''),?) || ';llm_cleanse_houjin_verified' "
                        "WHERE operator_name=?",
                        (p.canonical_name, p.corporate_number,
                         "llm_cleanse_houjin_verified", p.raw_name),
                    )
                elif has_corp:
                    cur = conn.execute(
                        "UPDATE operator_stores SET operator_name=?, "
                        "corporate_number=COALESCE(NULLIF(corporate_number,''),?) "
                        "WHERE operator_name=?",
                        (p.canonical_name, p.corporate_number, p.raw_name),
                    )
                else:
                    cur = conn.execute(
                        "UPDATE operator_stores SET operator_name=? "
                        "WHERE operator_name=?",
                        (p.canonical_name, p.raw_name),
                    )
                stats.applied_rows += cur.rowcount
            conn.commit()
        finally:
            conn.close()

    return stats, proposals


def _main() -> None:
    import argparse
    import sys
    import json

    ap = argparse.ArgumentParser(
        description="operator_stores の dirty 名を LLM canonicalize + 国税庁検証で cleanse",
    )
    ap.add_argument("--db", default="var/pizza.sqlite", help="pipeline SQLite")
    ap.add_argument("--brand", default="", help="対象ブランド (空で全件)")
    ap.add_argument("--dry-run", action="store_true",
                    help="提案のみ、DB update しない")
    ap.add_argument("--concurrency", type=int, default=3,
                    help="LLM 並列呼出数 (rate limit 配慮)")
    ap.add_argument("--out", default="", help="提案 JSON 出力 (空で skip)")
    args = ap.parse_args()

    if not Path(args.db).exists():
        print(f"❌ db not found: {args.db}", file=sys.stderr)
        sys.exit(2)

    stats, proposals = asyncio.run(
        cleanse_operator_stores(
            args.db, brand=args.brand, dry_run=args.dry_run,
            concurrency=args.concurrency,
        )
    )
    print(f"✅ cleanse {'dry-run' if args.dry_run else 'apply'}")
    print(f"   raw operators     = {stats.raw_operators}")
    print(f"   verified updates  = {stats.verified_updates}")
    print(f"   rejected (not_legal_entity) = {stats.rejected_not_legal_entity}")
    print(f"   rejected (not_in_houjin)    = {stats.rejected_not_in_houjin}")
    print(f"   llm_failures      = {stats.llm_failures}")
    if not args.dry_run:
        print(f"   applied_rows      = {stats.applied_rows}")
    for e in stats.errors[:5]:
        print(f"   ⚠  {e}", file=sys.stderr)

    if args.out:
        data = [
            {
                "raw": p.raw_name, "canonical": p.canonical_name,
                "corp": p.corporate_number, "verified": p.verified,
                "reason": p.reason, "confidence": p.confidence,
            }
            for p in proposals
        ]
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"📄 proposals: {args.out}")


if __name__ == "__main__":
    _main()
