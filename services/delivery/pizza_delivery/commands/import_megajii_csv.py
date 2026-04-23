"""pizza import-megajii-csv — 人手集計 TSV を ORM に取り込む。

ユーザー提供の BC誌 / JFA 派生 人手集計リストを
LLM cleansing (Gemini/Anthropic) + 国税庁 CSV verify + ORM upsert の
3 段で ORM (operator_company / franchise_brand / brand_operator_link) に反映する。

CLAUDE.md の規則に従い:
  - CSV 自体は var/external/ に置き、Git 管理外
  - source タグ (例: manual_megajii_YYYY_MM_DD) で provenance 明示
  - 国税庁 verify が通らない operator も登録 (corp="") するが、source に
    `_unverified` を付記して後段で識別可能にする
  - LLM は operator 名の canonical 化のみ (free-form 生成禁止)

入力 TSV フォーマット (2 sections、# コメント / 空行 無視):

  # section: megajii
  企業名\t業態\t店舗数\t代表\t住所\t当期千円\t前期千円\t前々期千円\tHPURL\t加盟ブランド

  # section: franchisor
  企業名\tブランド\t業態\tFC店舗数\t代表\t住所\t当期千円\t前期\t前々期\tHPURL\t加盟ブランド\tFC募集

CLI:
  pizza import-megajii-csv --csv var/external/megajii-manual.tsv --dry-run
  pizza import-megajii-csv --csv var/external/megajii-manual.tsv --out var/phase27/mega-proposals.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ─── データクラス ───────────────────────────────────────────────


@dataclass
class ImportRow:
    section: str  # "megajii" or "franchisor"
    raw_name: str
    industry: str = ""
    store_count: int = 0
    representative: str = ""
    address: str = ""
    revenue_current_jpy: int = 0
    revenue_previous_jpy: int = 0
    website_url: str = ""
    raw_brands: str = ""
    brand_name: str = ""  # franchisor section のみ
    recruit_url: str = ""
    raw_line: int = 0


@dataclass
class ImportStats:
    rows_read: int = 0
    rows_skipped: int = 0
    corp_verified: int = 0
    corp_unverified: int = 0
    llm_failures: int = 0
    operators_upserted: int = 0
    links_created: int = 0
    brands_upserted: int = 0
    errors: list[str] = field(default_factory=list)


# ─── 純粋関数群 (単体テスト対象) ────────────────────────────────


# 人手 CSV の表記揺れを pipeline 既存 brand 名に寄せる alias
_BRAND_ALIAS: dict[str, str] = {
    "ITTO個別指導学院": "Itto個別指導学院",
    "ITTO": "Itto個別指導学院",
    "Chateraise": "シャトレーゼ",
    "Autobacs": "オートバックス",
    "BOOKOFF": "ブックオフ",
    "HARD OFF": "ハードオフ",
    "Curves": "カーブス",
    "BRAND OFF": "Brand off",
    "セブン-イレブン": "セブンイレブン",
    "Anytime Fitness": "エニタイムフィットネス",
}


def canonicalize_brand(name: str) -> str:
    n = name.strip()
    return _BRAND_ALIAS.get(n, n)


def split_brands(raw: str) -> list[str]:
    """加盟ブランド文字列を list 化。区切り: 中点 / 全角カンマ / 半角カンマ / 全角中黒。

    "X" や空文字 は除外。
    """
    if not raw:
        return []
    parts = re.split(r"[・、,/／]", raw)
    out: list[str] = []
    for p in parts:
        p = p.strip()
        if not p or p.upper() == "X":
            continue
        out.append(canonicalize_brand(p))
    # dedup (order preserving)
    seen: set[str] = set()
    uniq: list[str] = []
    for b in out:
        if b in seen:
            continue
        seen.add(b)
        uniq.append(b)
    return uniq


def parse_int_yen_thousand(s: str) -> int:
    """千円単位の金額文字列 → 円 (int)。空文字/数値以外は 0。"""
    if not s:
        return 0
    s = s.replace(",", "").replace("，", "").strip()
    if not s:
        return 0
    try:
        return int(float(s)) * 1000
    except (ValueError, TypeError):
        return 0


def parse_int_plain(s: str) -> int:
    if not s:
        return 0
    s = s.replace(",", "").replace("，", "").strip()
    if not s:
        return 0
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def prefecture_from_address(address: str) -> str:
    if not address:
        return ""
    m = re.match(r"(北海道|東京都|大阪府|京都府|.{2,3}県)", address)
    return m.group(1) if m else ""


def save_rows_to_sqlite(rows: list[ImportRow], db_path: Path) -> int:
    """TSV parse 結果を SQLite に保存。再 import で truncate 上書き。

    ユーザーが SQL で直接検索できるようにする (LIKE / JOIN / 集計)。
    migrations は不要、単一 table。
    """
    import sqlite3
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS megajii_rows ("
            "line INTEGER PRIMARY KEY, "
            "section TEXT, "
            "raw_name TEXT, "
            "industry TEXT, "
            "store_count INTEGER, "
            "representative TEXT, "
            "address TEXT, "
            "revenue_current_jpy INTEGER, "
            "revenue_previous_jpy INTEGER, "
            "website_url TEXT, "
            "raw_brands TEXT, "
            "brand_name TEXT, "
            "recruit_url TEXT"
            ")"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mg_raw_name ON megajii_rows(raw_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mg_section ON megajii_rows(section)")
        conn.execute("DELETE FROM megajii_rows")
        conn.executemany(
            "INSERT INTO megajii_rows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    r.raw_line, r.section, r.raw_name, r.industry,
                    r.store_count, r.representative, r.address,
                    r.revenue_current_jpy, r.revenue_previous_jpy,
                    r.website_url, r.raw_brands, r.brand_name,
                    r.recruit_url,
                )
                for r in rows
            ],
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def read_tsv(path: Path) -> list[ImportRow]:
    """TSV を読んで section 判別 + parse。空行 / # コメント / header 行 skip。"""
    rows: list[ImportRow] = []
    current_section = "megajii"
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line.strip():
                continue
            if line.startswith("#"):
                if "franchisor" in line:
                    current_section = "franchisor"
                elif "megajii" in line:
                    current_section = "megajii"
                continue
            fields = line.split("\t")
            raw_name = fields[0].strip() if fields else ""
            if not raw_name or raw_name == "企業名":
                continue
            if current_section == "megajii":
                if len(fields) < 10:
                    continue
                raw_brands = fields[9].strip()
                if not raw_brands or raw_brands.upper() == "X":
                    continue
                rows.append(ImportRow(
                    section="megajii",
                    raw_name=raw_name,
                    industry=fields[1].strip(),
                    store_count=parse_int_plain(fields[2]),
                    representative=fields[3].strip(),
                    address=fields[4].strip(),
                    revenue_current_jpy=parse_int_yen_thousand(fields[5]),
                    revenue_previous_jpy=parse_int_yen_thousand(fields[6]),
                    website_url=fields[8].strip(),
                    raw_brands=raw_brands,
                    raw_line=lineno,
                ))
            else:  # franchisor
                if len(fields) < 12:
                    continue
                brand_name = fields[1].strip()
                if not brand_name:
                    continue
                rows.append(ImportRow(
                    section="franchisor",
                    raw_name=raw_name,
                    brand_name=canonicalize_brand(brand_name),
                    industry=fields[2].strip(),
                    store_count=parse_int_plain(fields[3]),
                    representative=fields[4].strip(),
                    address=fields[5].strip(),
                    revenue_current_jpy=parse_int_yen_thousand(fields[6]),
                    revenue_previous_jpy=parse_int_yen_thousand(fields[7]),
                    website_url=fields[9].strip(),
                    raw_brands=fields[10].strip(),
                    recruit_url=fields[11].strip(),
                    raw_line=lineno,
                ))
    return rows


# ─── LLM + 国税庁 による operator name 解決 ──────────────────────


def _gather_houjin_candidates(
    canonical: str,
    houjin_idx: Any,
    *,
    prefecture: str = "",
    limit: int = 6,
) -> list[Any]:
    """国税庁 CSV から canonical を軸に複数 variant で候補を収集する。

    - LLM が法人格を付けない素の法人名 (「ウイズダム」「キノシタ」) でも
      株式会社/有限会社の前後付与で拾えるよう、検索クエリを多面的に出す。
    - prefecture が指定されていれば、候補の prefecture 一致をまず優先、
      一致するものが皆無なら最終 fallback で pref 不一致候補も返す。
    - 最終的にどの候補を採用するかは Claude critic が rerank で決める。
    """
    queries: list[str] = [canonical]
    if not canonical.startswith(("株式会社", "有限会社", "合同会社")) \
            and "株式会社" not in canonical:
        queries.append(f"株式会社{canonical}")
        queries.append(f"{canonical}株式会社")
    if "有限会社" not in canonical:
        queries.append(f"有限会社{canonical}")
    seen: set[str] = set()
    all_recs: list[Any] = []
    for q in queries:
        try:
            recs = houjin_idx.search_by_name(q, limit=10, active_only=True)
            if not recs:
                recs = houjin_idx.search_by_name(q, limit=10, active_only=False)
        except Exception as e:
            logger.debug("houjin lookup failed for %s: %s", q, e)
            continue
        for rec in recs:
            key = rec.corporate_number or rec.name
            if key in seen:
                continue
            seen.add(key)
            all_recs.append(rec)

    # prefecture 優先フィルタ: 一致があればそれのみ、なければ全件
    if prefecture:
        matched = [r for r in all_recs if r.prefecture == prefecture]
        if matched:
            return matched[:limit]
    return all_recs[:limit]


class _LLMWithFallback:
    """primary LLM が quota 切れや permanent error を返したら自動で fallback に切替。

    `ainvoke(messages, output_format=...)` のみ proxy する最小 wrapper。
    一度 primary が枯渇と判断されたら以降は fallback を使い続ける (flip-flop 防止)。
    primary と fallback が同一インスタンスなら単純な passthrough。
    """

    _EXHAUSTION_MARKERS = (
        "429",
        "RESOURCE_EXHAUSTED",
        "ResourceExhausted",
        "quota",
        "rate limit",
        "rate_limit",
        "over_quota",
    )

    def __init__(self, primary: Any, fallback: Any) -> None:
        self.primary = primary
        self.fallback = fallback
        self._primary_exhausted = primary is fallback

    def _is_exhaustion(self, exc: Exception) -> bool:
        s = f"{type(exc).__name__} {exc}"
        return any(m in s for m in self._EXHAUSTION_MARKERS)

    async def ainvoke(self, messages: Any, output_format: Any | None = None) -> Any:
        if not self._primary_exhausted:
            try:
                return await self.primary.ainvoke(messages, output_format=output_format)
            except Exception as e:
                if self._is_exhaustion(e):
                    logger.warning("primary LLM exhausted (%s); switching to fallback", e)
                    self._primary_exhausted = True
                else:
                    raise
        return await self.fallback.ainvoke(messages, output_format=output_format)


_RERANK_CONTEXT_SYSTEM = """あなたは法人名マッチングの専門家。
入力された operator (name + 所在地) に対し、候補 list の中から「最も同じ法人を指す」ものを選ぶ。

判定ルール:
- name の一致度より **住所 (都道府県 → 市区町村) の一致** を最重視する
- 候補の住所と入力住所の市区町村が明確に異なれば別法人
- 明らかな同一法人 (住所の市 / 町 / 丁目まで一致) があればそれを選ぶ
- 同名・同県だが市区町村の手がかりも無く特定できない場合は best_index=-1
- 候補が全て外れと判断できる場合も best_index=-1

JSON 必須: {best_index (0-indexed or -1), confidence(0-1), reason(50字以内)}。
"""


async def _rerank_with_address(
    raw_name: str,
    raw_address: str,
    candidates: list[Any],
    llm: Any,
):
    """住所情報を添えて Claude critic に rerank させる。

    既存 rerank_candidates は name list のみ比較するため、同名法人が複数あって
    住所でしか区別できないケースを解決できない。ここは住所を context に含めた
    独自 prompt で critic に判定させる。
    """
    from pizza_delivery.llm_cleanser import RerankPick, _invoke_structured

    if not candidates:
        return RerankPick(best_index=-1, confidence=0.0)
    listing = "\n".join(
        f"  [{i}] name={c.name}  住所={c.address}"
        for i, c in enumerate(candidates)
    )
    user = (
        f"入力:\n  name: {raw_name}\n  住所: {raw_address}\n\n"
        f"候補:\n{listing}\n\n"
        f"住所の一致度を最重視して、入力と同じ法人を指す候補の index を返してください (無ければ -1)。"
    )
    r = await _invoke_structured(llm, _RERANK_CONTEXT_SYSTEM, user, RerankPick)
    if r is None:
        return RerankPick(best_index=-1, confidence=0.0)
    if r.best_index >= len(candidates):
        r.best_index = -1
    return r


async def _resolve_operator(
    row: ImportRow,
    llm_primary: Any,
    llm_critic: Any,
    houjin_idx: Any,
) -> tuple[str, str, bool]:
    """Gemini (canonicalize) + 国税庁 (候補収集) + Claude critic (rerank) の 3 段。

    - Step 1: Gemini で raw → canonical (is_legal_entity=False なら reject)
    - Step 2: 国税庁 CSV で variant 検索 (決定論で候補収集)
    - Step 3: 候補 0 → verified=False、候補 1 → そのまま採用、
              候補 2+ → Claude critic に rerank させて best を採用 (-1 なら reject)
    """
    from pizza_delivery.llm_cleanser import canonicalize_operator_name, rerank_candidates

    raw = row.raw_name
    try:
        r = await canonicalize_operator_name(raw, llm_primary)
    except Exception as e:
        logger.warning("gemini canonicalize failed for %r: %s", raw, e)
        r = None

    # BC誌等の人手 CSV では法人格省略の略称 (例: 「ウイズダム」) が多い。
    # Gemini が「法人格が付いていない → is_legal_entity=False」と判定しても
    # 国税庁 variant 検索 (株式会社X / X株式会社 / 有限会社X) + Claude critic で救う。
    canonical = r.canonical if (r and r.canonical) else raw
    pref = prefecture_from_address(row.address)
    candidates = _gather_houjin_candidates(canonical, houjin_idx, prefecture=pref)
    if not candidates and canonical != raw:
        # raw 名でも再検索 (LLM が意味を変えた可能性への保険)
        candidates = _gather_houjin_candidates(raw, houjin_idx, prefecture=pref)

    if not candidates:
        return canonical, "", False
    if len(candidates) == 1:
        best = candidates[0]
        return best.name, best.corporate_number, True

    # 複数候補 → Claude critic (住所込み) で rerank
    try:
        pick = await _rerank_with_address(raw, row.address, candidates, llm_critic)
    except Exception as e:
        logger.warning("claude critic rerank failed for %r: %s", raw, e)
        return canonical, "", False

    if pick.best_index < 0:
        # Critic が一致なしと判断 → 安全側で reject
        return canonical, "", False
    best = candidates[pick.best_index]
    return best.name, best.corporate_number, True


# ─── メイン orchestrator ────────────────────────────────────────


async def import_megajii_tsv(
    csv_path: Path,
    *,
    dry_run: bool = False,
    source_tag: str = "manual_megajii_2026_04_24",
    out_proposals: Path | None = None,
    concurrency: int = 3,
) -> ImportStats:
    """TSV 取り込み本体。"""
    stats = ImportStats()

    rows = read_tsv(csv_path)
    stats.rows_read = len(rows)
    if not rows:
        return stats

    # LLM 2 層構成: primary = canonicalize、critic = rerank。
    # 優先順位: 環境変数 LLM_PROVIDER (gemini|anthropic) で primary 明示、
    # 未指定なら gemini > anthropic fallback。ただし quota 切れ時は Claude 単独でも動く。
    import os
    from pizza_delivery.providers.registry import get_provider

    def _maybe_make_llm(name: str) -> Any | None:
        try:
            p = get_provider(name)
            if p.ready():
                return p.make_llm()
        except Exception:
            return None
        return None

    primary_pref = os.getenv("LLM_PROVIDER", "gemini").lower()
    fallback_name = "anthropic" if primary_pref != "anthropic" else "gemini"
    primary_raw = _maybe_make_llm(primary_pref)
    fallback_raw = _maybe_make_llm(fallback_name)
    if primary_raw is None and fallback_raw is None:
        stats.errors.append("no LLM primary ready (set GEMINI_API_KEY or ANTHROPIC_API_KEY)")
        return stats
    # Primary が無ければ fallback を昇格 (single-LLM mode)
    if primary_raw is None:
        primary_raw = fallback_raw
    # runtime で 429/quota 切れを検知したら自動で fallback に切替
    if fallback_raw is not None and fallback_raw is not primary_raw:
        llm_primary = _LLMWithFallback(primary_raw, fallback_raw)
    else:
        llm_primary = primary_raw
    # Critic は Claude を優先、ダメなら fallback / primary の順
    llm_critic = _maybe_make_llm("anthropic") or fallback_raw or primary_raw

    # 国税庁 CSV
    try:
        from pizza_delivery.houjin_csv import HoujinCSVIndex
        houjin_idx = HoujinCSVIndex()
        if houjin_idx.count() == 0:
            stats.errors.append("houjin_csv empty (run pizza houjin-import)")
            return stats
    except Exception as e:
        stats.errors.append(f"houjin_csv load: {e}")
        return stats

    # resolve を並列 (I/O bound な LLM call)
    sem = asyncio.Semaphore(concurrency)

    async def _one(row: ImportRow):
        async with sem:
            try:
                canon, corp, verified = await _resolve_operator(
                    row, llm_primary, llm_critic, houjin_idx,
                )
                return row, canon, corp, verified, None
            except Exception as e:
                return row, row.raw_name, "", False, str(e)

    resolved = await asyncio.gather(*(_one(r) for r in rows))

    proposals: list[dict] = []
    from pizza_delivery.orm import (
        make_session, upsert_brand, upsert_operator, link_brand_operator,
    )
    sess = None if dry_run else make_session()

    try:
        for row, canon, corp, verified, err in resolved:
            if err:
                stats.llm_failures += 1
                stats.errors.append(f"line {row.raw_line}: {err}")
                continue

            if verified:
                stats.corp_verified += 1
            else:
                stats.corp_unverified += 1

            if row.section == "franchisor":
                brands = [row.brand_name] if row.brand_name else []
                operator_type = "franchisor"
            else:
                brands = split_brands(row.raw_brands)
                operator_type = "franchisee"
            if not brands:
                stats.rows_skipped += 1
                continue

            proposals.append({
                "line": row.raw_line,
                "section": row.section,
                "raw_name": row.raw_name,
                "canonical": canon,
                "corp": corp,
                "verified": verified,
                "brands": brands,
                "address": row.address,
                "website_url": row.website_url,
                "operator_type": operator_type,
            })

            if dry_run:
                continue

            pref = prefecture_from_address(row.address)
            op_source = source_tag if verified else f"{source_tag}_unverified"
            op = upsert_operator(
                sess,
                name=canon,
                corporate_number=corp,
                head_office=row.address,
                prefecture=pref,
                source=op_source,
                representative_name=row.representative,
                revenue_current_jpy=row.revenue_current_jpy,
                revenue_previous_jpy=row.revenue_previous_jpy,
                website_url=row.website_url,
            )
            stats.operators_upserted += 1
            sess.flush()

            for bname in brands:
                fb = upsert_brand(sess, name=bname, source=source_tag)
                sess.flush()
                link_brand_operator(
                    sess,
                    brand=fb, operator=op,
                    operator_type=operator_type,
                    source=source_tag,
                    estimated_store_count=row.store_count,
                )
                stats.links_created += 1
                stats.brands_upserted += 1

        if not dry_run:
            sess.commit()
    finally:
        if sess is not None:
            sess.close()

    if out_proposals:
        out_proposals.parent.mkdir(parents=True, exist_ok=True)
        with open(out_proposals, "w", encoding="utf-8") as f:
            json.dump(proposals, f, ensure_ascii=False, indent=2)

    return stats


def _main() -> None:
    ap = argparse.ArgumentParser(
        description="人手集計 TSV (メガジー/本部) を LLM+国税庁 verify して ORM に取込む",
    )
    ap.add_argument("--csv", required=True, help="入力 TSV")
    ap.add_argument("--source-tag", default="manual_megajii_2026_04_24",
                    help="ORM レコードの source タグ")
    ap.add_argument("--dry-run", action="store_true", help="提案のみで DB 更新しない")
    ap.add_argument("--out", default="", help="提案 JSON 出力パス (optional)")
    ap.add_argument("--concurrency", type=int, default=3, help="LLM 並列数")
    ap.add_argument("--save-db", default="",
                    help="TSV parse 結果を SQLite に保存 (例: var/external/megajii.sqlite)")
    args = ap.parse_args()

    p = Path(args.csv)
    if not p.exists():
        print(f"❌ csv not found: {p}", file=sys.stderr)
        sys.exit(2)

    if args.save_db:
        rows = read_tsv(p)
        saved = save_rows_to_sqlite(rows, Path(args.save_db))
        print(f"💾 saved {saved} rows → {args.save_db}")

    out_proposals = Path(args.out) if args.out else None
    stats = asyncio.run(import_megajii_tsv(
        p, dry_run=args.dry_run,
        source_tag=args.source_tag,
        out_proposals=out_proposals,
        concurrency=args.concurrency,
    ))

    print(f"✅ import-megajii-csv {'dry-run' if args.dry_run else 'apply'}")
    print(f"   rows_read         = {stats.rows_read}")
    print(f"   rows_skipped      = {stats.rows_skipped}")
    print(f"   corp verified     = {stats.corp_verified}")
    print(f"   corp unverified   = {stats.corp_unverified}")
    print(f"   llm_failures      = {stats.llm_failures}")
    if not args.dry_run:
        print(f"   operators_upserted = {stats.operators_upserted}")
        print(f"   links_created      = {stats.links_created}")
    for e in stats.errors[:5]:
        print(f"   ⚠  {e}", file=sys.stderr)
    if out_proposals:
        print(f"📄 proposals: {out_proposals}")


if __name__ == "__main__":
    _main()
