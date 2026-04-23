"""Phase 17.4: Registry 自動拡充 loop。

unknown_stores (registry 未突合の bottom-up 店舗) に per_store で見つかった
operator_name を集計し、頻度 >= min_stores の社を「registry 追加候補」として
YAML-ready 形式で書き出す。

運用:
  1. pizza scan / audit で unknown_stores.csv が生成される
  2. pizza registry-expand で候補を抽出 (operator_stores テーブルから集計)
  3. 人間がレビューして franchisee_registry.yaml に追記
     (or ファクトチェック agent に自動検証させる)
"""

from __future__ import annotations

import csv as csv_mod
import sqlite3
from dataclasses import dataclass
from pathlib import Path


# ─── Data types ────────────────────────────────────────────────────────


@dataclass
class CandidateFranchisee:
    """Registry 追加候補の 1 entry。"""

    name: str
    brand: str
    estimated_store_count: int
    source: str = "per_store extraction"
    corporate_number: str = ""       # 要手動確認 or gBizINFO lookup
    head_office: str = ""
    first_seen_via: str = ""         # operator_stores.discovered_via


# ─── 集計 ──────────────────────────────────────────────────────────────


def aggregate_unknown_operators(
    *,
    db_path: str,
    brand: str,
    min_stores: int = 2,
) -> list[CandidateFranchisee]:
    """operator_stores から registry 未登録の operator を集計。

    条件:
      - brand が指定ブランドと一致
      - discovered_via != 'registry' (既登録を除外)
      - operator_type が 'franchisor' / 'direct' でない (本部・直営は除外)
      - store_count >= min_stores

    Note: 既に registry 登録済の operator (discovered_via='registry' エントリが
    1 件でもあれば) はリストから除外する。
    """
    conn = sqlite3.connect(db_path)
    try:
        # Registry 既登録の operator name 一覧
        registered_names = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT operator_name FROM operator_stores "
                "WHERE discovered_via='registry' AND brand=?",
                (brand,),
            ).fetchall()
        }

        rows = conn.execute(
            "SELECT operator_name, COUNT(DISTINCT place_id) AS n, "
            "       MIN(discovered_via) AS via "
            "FROM operator_stores "
            "WHERE brand=? "
            "  AND operator_name != '' "
            "  AND COALESCE(operator_type,'') NOT IN ('franchisor','direct') "
            "  AND COALESCE(discovered_via,'') != 'registry' "
            "GROUP BY operator_name "
            "HAVING n >= ? "
            "ORDER BY n DESC",
            (brand, min_stores),
        ).fetchall()
    finally:
        conn.close()

    out: list[CandidateFranchisee] = []
    for name, n, via in rows:
        if name in registered_names:
            continue
        out.append(
            CandidateFranchisee(
                name=name, brand=brand,
                estimated_store_count=int(n),
                first_seen_via=str(via or ""),
            )
        )
    return out


# ─── YAML 出力 ────────────────────────────────────────────────────────


def export_candidates_to_yaml(
    candidates: list[CandidateFranchisee],
    *,
    out_path: str,
) -> None:
    """franchisee_registry.yaml に追記できる形式で候補を書き出す。

    人間が中身を確認して既存 YAML にコピペする運用。または別途
    agent で gBizINFO 検証 → corporate_number を埋める処理に繋げる。
    """
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Phase 17.4: Registry 拡充候補 (per_store extraction 由来)")
    lines.append(f"# 候補 {len(candidates)} 社")
    lines.append("")
    for c in candidates:
        lines.append(f"# brand: {c.brand}")
        lines.append(f"- name: {c.name}")
        lines.append(f'  corporate_number: ""  # TODO: gBizINFO で確認')
        lines.append(f"  estimated_store_count: {c.estimated_store_count}")
        lines.append(f'  source: "{c.source}"')
        lines.append(f'  first_seen_via: "{c.first_seen_via}"')
        lines.append(f"  verified_at: \"\"  # TODO: ファクトチェック後に埋める")
        lines.append("")
    p.write_text("\n".join(lines), encoding="utf-8")


# ─── CSV loader ────────────────────────────────────────────────────


def load_unknown_stores_csv(path: str) -> list[dict]:
    """audit の `*-unknown-stores.csv` を読み込む。"""
    out: list[dict] = []
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv_mod.DictReader(f)
        for row in reader:
            out.append(row)
    return out
