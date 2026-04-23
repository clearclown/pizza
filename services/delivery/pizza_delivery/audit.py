"""BrandAuditor — Top-down (registry) × Bottom-up (SQLite stores) 突合 (Phase 8.2)。

1 ブランドについて:
  (A) Bottom-up: stores テーブルから該当 brand の店舗全件 load
  (B) Top-down: registry.brands[brand].known_franchisees 各社について
       places_client.search_by_operator(name, area_hint) → 候補 PlaceRaw list
  (C) 突合 (match.merge_all): place_id → address → proximity

結果:
  - FranchiseeCoverage × N 社 (registry 記載の registered vs 突合成功 bottom_up_matched)
  - unknown_stores: bottom-up にあるが registry 側 top-down 候補と突合できず
  - missing_operators: registry にあるが top-down (Places) で 0 件
"""

from __future__ import annotations

import csv
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from pizza_delivery.franchisee_registry import KnownFranchisee, Registry
from pizza_delivery.match import merge_all


# ─── Data types ────────────────────────────────────────────────────────


@dataclass
class FranchiseeCoverage:
    operator_name: str
    corporate_number: str
    head_office: str
    website: str
    registered_count: int
    found_count: int            # top-down で Places から返った件数
    bottom_up_matched_count: int  # 突合成功 place_id 数
    coverage_pct: float


@dataclass
class AuditReport:
    brand: str
    areas: list[str]
    bottom_up_total: int
    franchisees: list[FranchiseeCoverage] = field(default_factory=list)
    unknown_stores: list[dict] = field(default_factory=list)
    missing_operators: list[str] = field(default_factory=list)
    elapsed_sec: float = 0.0


# ─── DB I/O ────────────────────────────────────────────────────────────


def _load_bottom_up(db_path: str, brand: str) -> list[dict]:
    """stores テーブルから brand に該当する店舗全件を load。"""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT place_id, brand, name, address, lat, lng, official_url "
            "FROM stores WHERE brand = ?",
            (brand,),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "place_id": r[0],
            "brand": r[1],
            "name": r[2],
            "address": r[3] or "",
            "lat": r[4],
            "lng": r[5],
            "official_url": r[6] or "",
        }
        for r in rows
    ]


def _places_to_dicts(places: Iterable[Any]) -> list[dict]:
    """PlaceRaw list → dict list (match.merge_all 用)。"""
    out: list[dict] = []
    for p in places:
        out.append(
            {
                "place_id": p.place_id,
                "name": p.name,
                "address": p.address,
                "lat": p.lat,
                "lng": p.lng,
                "official_url": getattr(p, "website_uri", ""),
            }
        )
    return out


def _primary_url(fr: KnownFranchisee) -> str:
    return fr.source_urls[0] if fr.source_urls else ""


# ─── BrandAuditor ───────────────────────────────────────────────────────


@dataclass
class BrandAuditor:
    registry: Registry
    places_client: Any
    db_path: str
    addr_threshold: float = 0.7
    radius_m: float = 150.0
    max_result_count: int = 60

    async def run(
        self, *, brand: str, areas: list[str]
    ) -> AuditReport:
        t0 = time.time()
        report = AuditReport(brand=brand, areas=list(areas), bottom_up_total=0)

        # (A) Bottom-up: DB から全店舗
        bottom = _load_bottom_up(self.db_path, brand)
        report.bottom_up_total = len(bottom)

        br = self.registry.brands.get(brand)
        if br is None:
            report.elapsed_sec = time.time() - t0
            return report

        all_matched_bottom_ids: set[str] = set()

        # (B) Top-down: 各 franchisee について各 area で search_by_operator
        for fr in br.known_franchisees:
            top_places: list[Any] = []
            seen_pids: set[str] = set()
            for area in areas or [""]:
                places = await self.places_client.search_by_operator(
                    fr.name,
                    area_hint=area,
                    max_result_count=self.max_result_count,
                )
                for p in places:
                    if p.place_id and p.place_id not in seen_pids:
                        seen_pids.add(p.place_id)
                        top_places.append(p)

            top_dicts = _places_to_dicts(top_places)

            # (C) 突合: top × bottom
            merged = merge_all(
                top_dicts,
                bottom,
                addr_threshold=self.addr_threshold,
                radius_m=self.radius_m,
            )
            matched_ids = {m.bottom_id for m in merged.matches}
            all_matched_bottom_ids.update(matched_ids)

            registered = fr.estimated_store_count or 0
            bottom_matched = len(matched_ids)
            cov_pct = (bottom_matched / registered * 100.0) if registered > 0 else 0.0

            report.franchisees.append(
                FranchiseeCoverage(
                    operator_name=fr.name,
                    corporate_number=fr.corporate_number,
                    head_office=fr.head_office,
                    website=_primary_url(fr),
                    registered_count=registered,
                    found_count=len(top_places),
                    bottom_up_matched_count=bottom_matched,
                    coverage_pct=round(cov_pct, 2),
                )
            )
            if len(top_places) == 0:
                report.missing_operators.append(fr.name)

        # unknown_stores: bottom-up にあるが registry 突合に失敗
        report.unknown_stores = [
            {
                "place_id": b["place_id"],
                "name": b["name"],
                "address": b["address"],
                "lat": b["lat"],
                "lng": b["lng"],
                "official_url": b["official_url"],
            }
            for b in bottom
            if b["place_id"] not in all_matched_bottom_ids
        ]

        report.elapsed_sec = round(time.time() - t0, 2)
        return report


# ─── CSV 出力 helper ──────────────────────────────────────────────────


def write_report_csvs(report: AuditReport, out_csv: str) -> None:
    """main CSV + unknown_stores + missing_operators の 3 ファイル出力。"""
    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # メイン CSV
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "企業名",
                "本部所在地",
                "URL",
                "登録推定店舗数",
                "実発見数",
                "coverage%",
                "法人番号",
            ]
        )
        for c in report.franchisees:
            w.writerow(
                [
                    c.operator_name,
                    c.head_office,
                    c.website,
                    c.registered_count,
                    c.bottom_up_matched_count,
                    f"{c.coverage_pct:.2f}",
                    c.corporate_number,
                ]
            )

    # unknown stores
    stem = out_path.with_suffix("")
    unk_path = stem.parent / f"{stem.name}-unknown-stores.csv"
    with unk_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["place_id", "name", "address", "lat", "lng", "official_url"])
        for s in report.unknown_stores:
            w.writerow(
                [s["place_id"], s["name"], s["address"], s["lat"], s["lng"], s["official_url"]]
            )

    # missing operators
    miss_path = stem.parent / f"{stem.name}-missing-operators.csv"
    with miss_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["operator_name", "reason"])
        for op in report.missing_operators:
            w.writerow([op, "top-down search_by_operator returned 0 hits"])


async def run_audit(
    *,
    registry: Registry,
    places_client: Any,
    db_path: str,
    brand: str,
    areas: list[str],
    out_csv: str | None = None,
) -> AuditReport:
    """高レベル convenience: auditor を作って run + (optional) CSV 出力。"""
    auditor = BrandAuditor(
        registry=registry, places_client=places_client, db_path=db_path
    )
    report = await auditor.run(brand=brand, areas=areas)
    if out_csv:
        write_report_csvs(report, out_csv)
    return report
