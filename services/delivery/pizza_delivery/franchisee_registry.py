"""franchisee_registry.yaml を読み込み、SQLite に seed する module。

Ground Truth (ファクトチェック済み franchisee) を operator_stores テーブルに
特別な place_id (`REG:<corporate_number>:<n>`) で seed することで、
mega_franchisees view に載せる。

- discovered_via = 'registry' で他の経路と区別
- operator_type = 'franchisee' (既確認済み)
- verification_source = 'manual_factcheck'
- corporate_number フィールドに法人番号を入れる

Python 側から呼び出し可能な公開関数:
  - load_registry(yaml_path) -> Registry
  - seed_registry_to_sqlite(db_path, registry)
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class KnownFranchisee:
    name: str
    corporate_number: str
    head_office: str = ""
    estimated_store_count: int = 0
    source_urls: list[str] = field(default_factory=list)
    verified_at: str = ""
    verified_via: list[str] = field(default_factory=list)
    aliases: list[str] = field(default_factory=list)
    note: str = ""


@dataclass
class BrandRegistry:
    brand: str
    master_franchisor: dict[str, Any] = field(default_factory=dict)
    known_franchisees: list[KnownFranchisee] = field(default_factory=list)


@dataclass
class MultiBrandOperator:
    """Phase 20: 複数ブランドの FC を運営している事業会社 1 行。

    例: 大和フーヅ = {モスバーガー:18, ミスタードーナツ:48}
    既存 `BrandRegistry.known_franchisees` は brand-first で 1 ブランド内の
    franchisee を列挙するが、こちらは operator-first で operator を
    主語に複数 brand を持たせる逆方向 index。
    """

    name: str
    corporate_number: str
    brands: dict[str, int] = field(default_factory=dict)
    head_office: str = ""
    total_stores: int = 0
    source_urls: list[str] = field(default_factory=list)
    verified_at: str = ""
    verified_via: list[str] = field(default_factory=list)
    bc_ranking_2024: int = 0
    observed_at: str = ""
    note: str = ""


@dataclass
class Registry:
    version: int
    updated_at: str
    brands: dict[str, BrandRegistry] = field(default_factory=dict)
    multi_brand_operators: list[MultiBrandOperator] = field(default_factory=list)


def _default_path() -> Path:
    # repo root 基準の internal/dough/knowledge/franchisee_registry.yaml
    here = Path(__file__).resolve()
    # services/delivery/pizza_delivery → repo root に遡る
    root = here.parents[3]
    return root / "internal" / "dough" / "knowledge" / "franchisee_registry.yaml"


def load_registry(path: Path | str | None = None) -> Registry:
    """YAML を読み込んで Registry dataclass を返す。"""
    p = Path(path) if path else _default_path()
    with p.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    reg = Registry(
        version=int(raw.get("version", 1)),
        updated_at=str(raw.get("updated_at", "")),
    )
    brands_raw = raw.get("brands") or {}
    for brand_name, brand_data in brands_raw.items():
        br = BrandRegistry(brand=brand_name)
        br.master_franchisor = brand_data.get("master_franchisor") or {}
        for fr in brand_data.get("known_franchisees") or []:
            br.known_franchisees.append(
                KnownFranchisee(
                    name=fr["name"],
                    corporate_number=str(fr.get("corporate_number", "")),
                    head_office=str(fr.get("head_office", "")),
                    estimated_store_count=int(fr.get("estimated_store_count", 0) or 0),
                    source_urls=list(fr.get("source_urls") or []),
                    verified_at=str(fr.get("verified_at", "")),
                    verified_via=list(fr.get("verified_via") or []),
                    aliases=list(fr.get("aliases") or []),
                    note=str(fr.get("note", "")),
                )
            )
        reg.brands[brand_name] = br

    # Phase 20: multi_brand_operators セクション
    for mbo in raw.get("multi_brand_operators") or []:
        brands_map: dict[str, int] = {}
        for b, n in (mbo.get("brands") or {}).items():
            try:
                brands_map[str(b)] = int(n)
            except (TypeError, ValueError):
                continue
        total = int(mbo.get("total_stores") or sum(brands_map.values()))
        reg.multi_brand_operators.append(
            MultiBrandOperator(
                name=str(mbo["name"]),
                corporate_number=str(mbo.get("corporate_number", "")),
                brands=brands_map,
                head_office=str(mbo.get("head_office", "")),
                total_stores=total,
                source_urls=list(mbo.get("source_urls") or []),
                verified_at=str(mbo.get("verified_at", "")),
                verified_via=list(mbo.get("verified_via") or []),
                bc_ranking_2024=int(mbo.get("bc_ranking_2024") or 0),
                observed_at=str(mbo.get("observed_at", "")),
                note=str(mbo.get("note", "")),
            )
        )
    return reg


def seed_registry_to_sqlite(
    db_path: str,
    registry: Registry,
    *,
    default_confidence: float = 0.95,
) -> int:
    """Registry を operator_stores に seed する。

    1 エントリにつき `estimated_store_count` 件の dummy place_id を登録する
    (`REG:<法人番号>:<i>` 形式)。これで mega_franchisees view で正しく
    集計される。

    戻り値: 新規 insert 件数。
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        # operator_stores に verification 列があるか確認 (後方互換)
        rows = cur.execute("PRAGMA table_info(operator_stores)").fetchall()
        has_ver_cols = any(r[1] == "verification_score" for r in rows)

        inserted = 0
        for brand_name, brand_reg in registry.brands.items():
            for fr in brand_reg.known_franchisees:
                count = max(fr.estimated_store_count, 1)
                for i in range(1, count + 1):
                    place_id = f"REG:{fr.corporate_number or 'no-no'}:{i}"
                    exists = cur.execute(
                        "SELECT 1 FROM operator_stores "
                        "WHERE operator_name=? AND place_id=? LIMIT 1",
                        (fr.name, place_id),
                    ).fetchone()
                    if exists:
                        continue
                    if has_ver_cols:
                        cur.execute(
                            """
                            INSERT INTO operator_stores
                              (operator_name, place_id, brand, operator_type,
                               confidence, discovered_via,
                               verification_score, corporate_number,
                               verification_source)
                            VALUES (?, ?, ?, 'franchisee', ?, 'registry',
                                    1.0, ?, 'manual_factcheck')
                            """,
                            (fr.name, place_id, brand_name, default_confidence,
                             fr.corporate_number),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO operator_stores
                              (operator_name, place_id, brand, operator_type,
                               confidence, discovered_via)
                            VALUES (?, ?, ?, 'franchisee', ?, 'registry')
                            """,
                            (fr.name, place_id, brand_name, default_confidence),
                        )
                    inserted += 1

        # Phase 20: multi_brand_operators の seed (operator × brand 毎に dummy place_id)
        for mbo in registry.multi_brand_operators:
            corp = mbo.corporate_number or "no-no"
            for brand_name, count in mbo.brands.items():
                if count <= 0:
                    continue
                # 重複回避: この (operator, brand) が既に known_franchisees 経由で
                # seed されているなら MBO 側は skip (double-count 防止)
                already_seeded = cur.execute(
                    "SELECT COUNT(*) FROM operator_stores "
                    "WHERE operator_name=? AND brand=? "
                    "  AND discovered_via IN ('registry','registry_mbo')",
                    (mbo.name, brand_name),
                ).fetchone()[0]
                if already_seeded >= count:
                    continue
                # brand ごとのスラッグで namespace を分ける (既存 REG: と衝突回避)
                brand_slug = brand_name.replace(" ", "").replace("/", "")
                for i in range(1, count + 1):
                    place_id = f"REG-MBO:{corp}:{brand_slug}:{i}"
                    exists = cur.execute(
                        "SELECT 1 FROM operator_stores "
                        "WHERE operator_name=? AND place_id=? LIMIT 1",
                        (mbo.name, place_id),
                    ).fetchone()
                    if exists:
                        continue
                    if has_ver_cols:
                        cur.execute(
                            """
                            INSERT INTO operator_stores
                              (operator_name, place_id, brand, operator_type,
                               confidence, discovered_via,
                               verification_score, corporate_number,
                               verification_source)
                            VALUES (?, ?, ?, 'franchisee', ?, 'registry_mbo',
                                    1.0, ?, 'manual_factcheck')
                            """,
                            (mbo.name, place_id, brand_name, default_confidence,
                             mbo.corporate_number),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO operator_stores
                              (operator_name, place_id, brand, operator_type,
                               confidence, discovered_via)
                            VALUES (?, ?, ?, 'franchisee', ?, 'registry_mbo')
                            """,
                            (mbo.name, place_id, brand_name, default_confidence),
                        )
                    inserted += 1

        conn.commit()
        return inserted
    finally:
        conn.close()
