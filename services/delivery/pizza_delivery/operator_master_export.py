"""事業会社を主キーにした FC 調査統合エクスポート。

LLM knowledge は使わず、既存の pipeline/ORM/CSV artifact だけを横断する。
主成果物は operator 1 行の wide CSV。URL や失敗理由は lossless に近い形で
別の evidence CSV にも展開する。
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pizza_delivery.normalize import canonical_key, normalize_operator_name


TARGET_BRANDS = [
    "カーブス",
    "モスバーガー",
    "業務スーパー",
    "Itto個別指導学院",
    "エニタイムフィットネス",
    "コメダ珈琲",
    "シャトレーゼ",
    "ハードオフ",
    "オフハウス",
    "Kids Duo",
    "アップガレージ",
    "カルビ丼とスン豆腐専門店韓丼",
    "Brand off",
    "TSUTAYA",
]

BRAND_TOKENS = {
    "カーブス": "curves",
    "モスバーガー": "mos",
    "業務スーパー": "gyomu_super",
    "Itto個別指導学院": "itto",
    "エニタイムフィットネス": "anytime",
    "コメダ珈琲": "komeda",
    "シャトレーゼ": "chateraise",
    "ハードオフ": "hardoff",
    "オフハウス": "offhouse",
    "Kids Duo": "kidsduo",
    "アップガレージ": "upgarage",
    "カルビ丼とスン豆腐専門店韓丼": "kandon",
    "Brand off": "brandoff",
    "TSUTAYA": "tsutaya",
}

QUALITY_SCORE = {
    "A_houjin_verified": 40,
    "B_manual_unverified": 30,
    "C_candidate_unverified": 20,
    "D_pipeline_only_unverified_review": 10,
}


@dataclass
class BrandBucket:
    store_count: int = 0
    priority_ranks: set[str] = field(default_factory=set)
    priority_tiers: set[str] = field(default_factory=set)
    quality_tiers: set[str] = field(default_factory=set)
    evidence_statuses: set[str] = field(default_factory=set)
    risk_flags: set[str] = field(default_factory=set)
    sources: set[str] = field(default_factory=set)
    source_urls: set[str] = field(default_factory=set)
    sample_stores: set[str] = field(default_factory=set)
    sample_urls: set[str] = field(default_factory=set)
    notes: set[str] = field(default_factory=set)


@dataclass
class OperatorBucket:
    key: str
    operator_names: Counter[str] = field(default_factory=Counter)
    corporate_numbers: set[str] = field(default_factory=set)
    head_offices: set[str] = field(default_factory=set)
    hq_prefectures: set[str] = field(default_factory=set)
    website_urls: set[str] = field(default_factory=set)
    representative_names: set[str] = field(default_factory=set)
    representative_titles: set[str] = field(default_factory=set)
    revenue_current_values: set[int] = field(default_factory=set)
    revenue_previous_values: set[int] = field(default_factory=set)
    revenue_observed_at: set[str] = field(default_factory=set)
    operator_kinds: set[str] = field(default_factory=set)
    sources: set[str] = field(default_factory=set)
    source_urls: set[str] = field(default_factory=set)
    notes: set[str] = field(default_factory=set)
    quality_tiers: set[str] = field(default_factory=set)
    risk_flags: set[str] = field(default_factory=set)
    evidence_statuses: set[str] = field(default_factory=set)
    priority_tiers: set[str] = field(default_factory=set)
    include_reasons: set[str] = field(default_factory=set)
    brand_info: dict[str, BrandBucket] = field(default_factory=dict)
    reported_total_values: set[int] = field(default_factory=set)
    reported_brand_count_values: set[int] = field(default_factory=set)
    pipeline_store_ids: set[str] = field(default_factory=set)
    pipeline_store_ids_by_brand: dict[str, set[str]] = field(default_factory=dict)
    pipeline_brands: set[str] = field(default_factory=set)
    pipeline_confidence_values: list[float] = field(default_factory=list)
    pipeline_discovered_via: set[str] = field(default_factory=set)
    pipeline_verification_sources: set[str] = field(default_factory=set)
    pipeline_evidence_urls: set[str] = field(default_factory=set)
    pipeline_sample_stores: set[str] = field(default_factory=set)
    recruitment_statuses: set[str] = field(default_factory=set)
    recruitment_store_ids: set[str] = field(default_factory=set)
    recruitment_store_ids_by_brand: dict[str, set[str]] = field(default_factory=dict)
    recruitment_brands: set[str] = field(default_factory=set)
    recruitment_urls: set[str] = field(default_factory=set)
    recruitment_failed_urls: set[str] = field(default_factory=set)
    recruitment_unverified_urls: set[str] = field(default_factory=set)
    recruitment_accepted_store_ids: set[str] = field(default_factory=set)
    recruitment_accepted_urls: set[str] = field(default_factory=set)
    recruitment_source_types: Counter[str] = field(default_factory=Counter)
    recruitment_reject_reasons: Counter[str] = field(default_factory=Counter)
    recruitment_sample_stores: set[str] = field(default_factory=set)

    def name(self) -> str:
        if not self.operator_names:
            return ""
        return self.operator_names.most_common(1)[0][0]

    def brand_bucket(self, brand: str) -> BrandBucket:
        return self.brand_info.setdefault(brand, BrandBucket())

    def brand_counts(self) -> dict[str, int]:
        counts = {b: v.store_count for b, v in self.brand_info.items() if v.store_count > 0}
        for brand, ids in self.pipeline_store_ids_by_brand.items():
            counts[brand] = max(counts.get(brand, 0), len(ids))
        for brand, ids in self.recruitment_store_ids_by_brand.items():
            counts[brand] = max(counts.get(brand, 0), len(ids))
        return counts

    def total_stores_est(self) -> int:
        return max(
            [0, *self.reported_total_values, sum(self.brand_counts().values()),
             len(self.pipeline_store_ids), len(self.recruitment_store_ids)]
        )

    def brand_count_est(self) -> int:
        return max(
            [0, *self.reported_brand_count_values, len(self.brand_counts()),
             len(self.pipeline_brands), len(self.recruitment_brands)]
        )


class BucketStore:
    def __init__(self) -> None:
        self.buckets: dict[str, OperatorBucket] = {}
        self.aliases: dict[str, str] = {}

    def _name_key(self, name: str) -> str:
        return f"NAME::{canonical_key(name)}"

    def _corp_key(self, corp: str) -> str:
        return f"CORP::{corp}"

    def get(self, name: str, corp: str = "") -> OperatorBucket | None:
        op = normalize_operator_name(name)
        if not op:
            return None
        corp = (corp or "").strip()
        name_key = self._name_key(op)
        key = self.aliases.get(name_key, name_key)
        if corp:
            corp_key = self._corp_key(corp)
            existing_key = self.aliases.get(corp_key)
            if existing_key and existing_key in self.buckets:
                key = existing_key
            elif key in self.buckets and key != corp_key:
                self.buckets[corp_key] = self.buckets.pop(key)
                self.buckets[corp_key].key = corp_key
                key = corp_key
            else:
                key = corp_key
            self.aliases[corp_key] = key
            self.aliases[name_key] = key
        if key not in self.buckets:
            self.buckets[key] = OperatorBucket(key=key)
        b = self.buckets[key]
        b.operator_names[op] += 1
        if corp:
            b.corporate_numbers.add(corp)
        self.aliases[name_key] = key
        return b


@dataclass
class EvidenceRow:
    operator_name: str
    corporate_number: str
    brand: str
    evidence_kind: str
    source_type: str
    status: str
    store_name: str
    place_id: str
    url: str
    reject_reason: str
    confidence: str
    snippet: str
    source_file: str

    def to_dict(self) -> dict[str, str]:
        return self.__dict__.copy()


def _read_csv(path: str | Path) -> list[dict[str, str]]:
    p = Path(path)
    if not p.exists():
        return []
    with p.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _split_multi(value: str) -> list[str]:
    parts: list[str] = []
    for sep in (" / ", ";", "|"):
        if sep in value:
            for item in value.split(sep):
                item = item.strip()
                if item:
                    parts.append(item)
            return parts
    value = value.strip()
    return [value] if value else []


def _add_int(target: set[int], value: Any) -> None:
    try:
        n = int(float(str(value or "0").replace(",", "")))
    except ValueError:
        return
    if n > 0:
        target.add(n)


def _first_corporate_number(value: str) -> str:
    for part in _split_multi(value or ""):
        cleaned = part.strip()
        if cleaned.isdigit() and len(cleaned) == 13:
            return cleaned
    value = (value or "").strip()
    return value if value.isdigit() and len(value) == 13 else ""


def _join(values: set[str] | list[str] | tuple[str, ...], *, limit: int = 0) -> str:
    cleaned = sorted({str(v).strip() for v in values if str(v).strip()})
    if limit > 0:
        cleaned = cleaned[:limit]
    return " / ".join(cleaned)


def _merge_set(dst: set[str], value: str) -> None:
    for item in _split_multi(value or ""):
        dst.add(item)


def _add_evidence(
    rows: list[EvidenceRow],
    bucket: OperatorBucket,
    *,
    brand: str = "",
    evidence_kind: str,
    source_type: str = "",
    status: str = "",
    store_name: str = "",
    place_id: str = "",
    url: str = "",
    reject_reason: str = "",
    confidence: str = "",
    snippet: str = "",
    source_file: str = "",
) -> None:
    if not url:
        return
    rows.append(EvidenceRow(
        operator_name=bucket.name(),
        corporate_number=_join(bucket.corporate_numbers),
        brand=brand,
        evidence_kind=evidence_kind,
        source_type=source_type,
        status=status,
        store_name=store_name,
        place_id=place_id,
        url=url,
        reject_reason=reject_reason,
        confidence=confidence,
        snippet=(snippet or "")[:500],
        source_file=source_file,
    ))


def load_priority_csv(store: BucketStore, evidence: list[EvidenceRow], path: str | Path) -> None:
    for row in _read_csv(path):
        b = store.get(row.get("operator_name", ""), row.get("corporate_number", ""))
        if b is None:
            continue
        brand = row.get("brand", "")
        bb = b.brand_bucket(brand)
        try:
            cnt = int(row.get("brand_estimated_store_count") or "0")
        except ValueError:
            cnt = 0
        bb.store_count = max(bb.store_count, cnt)
        if row.get("brand_priority_rank"):
            bb.priority_ranks.add(row["brand_priority_rank"])
        _merge_set(bb.priority_tiers, row.get("priority_tier", ""))
        _merge_set(bb.quality_tiers, row.get("quality_tier", ""))
        _merge_set(bb.evidence_statuses, row.get("evidence_status", ""))
        _merge_set(bb.risk_flags, row.get("risk_flags", ""))
        _merge_set(bb.sources, row.get("sources", ""))
        _merge_set(bb.source_urls, row.get("source_urls", ""))
        _merge_set(bb.sample_stores, row.get("sample_stores", ""))
        _merge_set(bb.sample_urls, row.get("sample_urls", ""))
        _merge_set(bb.notes, row.get("notes", ""))
        _merge_set(b.head_offices, row.get("head_office", ""))
        _merge_set(b.hq_prefectures, row.get("hq_prefecture", ""))
        _merge_set(b.website_urls, row.get("website_url", ""))
        _merge_set(b.sources, row.get("sources", ""))
        _merge_set(b.source_urls, row.get("source_urls", ""))
        _merge_set(b.notes, row.get("notes", ""))
        _merge_set(b.quality_tiers, row.get("quality_tier", ""))
        _merge_set(b.risk_flags, row.get("risk_flags", ""))
        _merge_set(b.evidence_statuses, row.get("evidence_status", ""))
        _merge_set(b.priority_tiers, row.get("priority_tier", ""))
        _merge_set(b.include_reasons, row.get("include_reason", ""))
        _add_int(b.reported_total_values, row.get("operator_total_stores_est", ""))
        _add_int(b.reported_brand_count_values, row.get("operator_brand_count", ""))
        for url in _split_multi(row.get("source_urls", "")):
            _add_evidence(
                evidence, b, brand=brand, evidence_kind="priority_source_url",
                source_type="priority", status=row.get("evidence_status", ""),
                url=url, source_file=str(path),
            )
        for url in _split_multi(row.get("sample_urls", "")):
            _add_evidence(
                evidence, b, brand=brand, evidence_kind="priority_sample_url",
                source_type="priority", status=row.get("evidence_status", ""),
                url=url, source_file=str(path),
            )


def load_directory_csv(store: BucketStore, path: str | Path) -> None:
    for row in _read_csv(path):
        b = store.get(row.get("operator_name", ""), row.get("corporate_number", ""))
        if b is None:
            continue
        _merge_set(b.head_offices, row.get("head_office", ""))
        _merge_set(b.hq_prefectures, row.get("hq_prefecture", ""))
        _merge_set(b.website_urls, row.get("website_url", ""))
        _add_int(b.reported_total_values, row.get("total_stores_est", ""))
        _add_int(b.reported_brand_count_values, row.get("brand_count", ""))
        for part in _split_multi((row.get("brands_breakdown") or "").replace(";", " / ")):
            if ":" not in part:
                continue
            brand, raw_count = part.rsplit(":", 1)
            if brand in TARGET_BRANDS:
                try:
                    cnt = int(raw_count)
                except ValueError:
                    continue
                bb = b.brand_bucket(brand)
                bb.store_count = max(bb.store_count, cnt)


def load_component_csv(store: BucketStore, evidence: list[EvidenceRow], path: str | Path) -> None:
    for row in _read_csv(path):
        b = store.get(row.get("operator_name", ""), row.get("corporate_number", ""))
        if b is None:
            continue
        brand = row.get("brand", "")
        bb = b.brand_bucket(brand)
        try:
            cnt = int(row.get("brand_estimated_store_count") or "0")
        except ValueError:
            cnt = 0
        bb.store_count = max(bb.store_count, cnt)
        _add_int(b.reported_total_values, row.get("operator_total_stores_est", ""))
        _add_int(b.reported_brand_count_values, row.get("operator_brand_count", ""))
        _merge_set(b.head_offices, row.get("head_office", ""))
        _merge_set(b.hq_prefectures, row.get("hq_prefecture", ""))
        _merge_set(b.website_urls, row.get("website_url", ""))
        _merge_set(b.sources, row.get("sources", ""))
        _merge_set(b.source_urls, row.get("source_urls", ""))
        _merge_set(b.notes, row.get("notes", ""))
        _merge_set(bb.sources, row.get("sources", ""))
        _merge_set(bb.source_urls, row.get("source_urls", ""))
        _merge_set(bb.notes, row.get("notes", ""))
        for url in _split_multi(row.get("source_urls", "")):
            _add_evidence(
                evidence, b, brand=brand, evidence_kind="orm_component_source_url",
                source_type="orm", url=url, source_file=str(path),
            )


def load_recruitment_ranking_csv(store: BucketStore, path: str | Path) -> None:
    for row in _read_csv(path):
        b = store.get(row.get("operator_name", ""), _first_corporate_number(row.get("corporate_numbers", "")))
        if b is None:
            continue
        _merge_set(b.recruitment_statuses, row.get("status", ""))
        _merge_set(b.corporate_numbers, row.get("corporate_numbers", ""))
        _add_int(b.reported_total_values, row.get("candidate_store_count", ""))
        _add_int(b.reported_brand_count_values, row.get("brand_count", ""))
        _merge_set(b.recruitment_sample_stores, row.get("sample_stores", ""))
        _merge_set(b.recruitment_urls, row.get("sample_urls", ""))
        _merge_set(b.recruitment_accepted_urls, row.get("accepted_evidence_urls", ""))
        for part in _split_multi(row.get("brands", "")):
            if part in TARGET_BRANDS:
                b.brand_bucket(part)
                b.recruitment_brands.add(part)


def load_recruitment_sidecar(
    store: BucketStore,
    evidence: list[EvidenceRow],
    path: str | Path,
    kind: str,
) -> None:
    rows = _read_csv(path)
    for row in rows:
        name = row.get("final_operator") or row.get("candidate_operator") or row.get("operator_name", "")
        corp = row.get("final_corp") or row.get("corporate_numbers") or row.get("corporate_number", "")
        b = store.get(name, corp)
        if b is None:
            continue
        brand = row.get("brand", "")
        if brand:
            b.brand_bucket(brand)
            b.recruitment_brands.add(brand)
        place_id = row.get("place_id", "")
        store_name = row.get("store_name", "")
        url = row.get("evidence_url", "")
        if place_id:
            b.recruitment_store_ids.add(place_id)
            if brand:
                b.recruitment_store_ids_by_brand.setdefault(brand, set()).add(place_id)
        if store_name:
            b.recruitment_sample_stores.add(store_name)
        if url:
            b.recruitment_urls.add(url)
        if kind == "accepted":
            if place_id:
                b.recruitment_accepted_store_ids.add(place_id)
            if url:
                b.recruitment_accepted_urls.add(url)
            _merge_set(b.evidence_statuses, "recruitment_accepted")
        elif kind == "failed":
            if url:
                b.recruitment_failed_urls.add(url)
        elif kind == "unverified":
            if url:
                b.recruitment_unverified_urls.add(url)
        if row.get("source_type"):
            b.recruitment_source_types[row["source_type"]] += 1
        reject = (
            row.get("proposal_reject_reason")
            or row.get("attempt_reject_reason")
            or row.get("reject_reason")
            or ""
        )
        if reject:
            b.recruitment_reject_reasons[reject] += 1
        _add_evidence(
            evidence,
            b,
            brand=brand,
            evidence_kind=f"recruitment_{kind}",
            source_type=row.get("source_type", ""),
            status="accepted" if kind == "accepted" else kind,
            store_name=store_name,
            place_id=place_id,
            url=url,
            reject_reason=reject,
            confidence=row.get("candidate_confidence", ""),
            snippet=row.get("snippet", ""),
            source_file=str(path),
        )


def load_orm_db(store: BucketStore, evidence: list[EvidenceRow], db_path: str | Path) -> None:
    p = Path(db_path)
    if not p.exists():
        return
    conn = sqlite3.connect(p)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
              op.name AS operator_name,
              op.corporate_number,
              op.head_office,
              op.prefecture,
              op.kind,
              op.source AS operator_source,
              op.note AS operator_note,
              op.representative_name,
              op.representative_title,
              op.revenue_current_jpy,
              op.revenue_previous_jpy,
              op.revenue_observed_at,
              op.website_url,
              b.name AS brand,
              l.estimated_store_count,
              l.observed_at,
              l.operator_type,
              l.source AS link_source,
              l.source_url,
              l.note AS link_note
            FROM operator_company op
            JOIN brand_operator_link l ON l.operator_id = op.id
            JOIN franchise_brand b ON b.id = l.brand_id
            WHERE b.name IN ({})
            """.format(",".join("?" for _ in TARGET_BRANDS)),
            TARGET_BRANDS,
        ).fetchall()
        for row in rows:
            b = store.get(row["operator_name"], row["corporate_number"] or "")
            if b is None:
                continue
            brand = row["brand"] or ""
            bb = b.brand_bucket(brand)
            cnt = int(row["estimated_store_count"] or 0)
            bb.store_count = max(bb.store_count, cnt)
            _merge_set(b.head_offices, row["head_office"] or "")
            _merge_set(b.hq_prefectures, row["prefecture"] or "")
            _merge_set(b.website_urls, row["website_url"] or "")
            _merge_set(b.operator_kinds, row["kind"] or "")
            if (row["kind"] or "") == "franchisor" or (row["operator_type"] or "") in {"franchisor", "direct"}:
                b.risk_flags.add("franchisor_or_direct_link")
                bb.risk_flags.add("franchisor_or_direct_link")
            _merge_set(b.sources, row["operator_source"] or "")
            _merge_set(b.sources, row["link_source"] or "")
            _merge_set(b.notes, row["operator_note"] or "")
            _merge_set(b.notes, row["link_note"] or "")
            _merge_set(b.source_urls, row["source_url"] or "")
            _merge_set(bb.sources, row["link_source"] or "")
            _merge_set(bb.source_urls, row["source_url"] or "")
            _merge_set(bb.notes, row["link_note"] or "")
            _merge_set(b.representative_names, row["representative_name"] or "")
            _merge_set(b.representative_titles, row["representative_title"] or "")
            _merge_set(b.revenue_observed_at, row["revenue_observed_at"] or "")
            _add_int(b.revenue_current_values, row["revenue_current_jpy"])
            _add_int(b.revenue_previous_values, row["revenue_previous_jpy"])
            if row["source_url"]:
                _add_evidence(
                    evidence, b, brand=brand, evidence_kind="orm_link_source_url",
                    source_type=row["link_source"] or "orm", url=row["source_url"],
                    source_file=str(db_path),
                )
    finally:
        conn.close()


def load_pipeline_db(store: BucketStore, evidence: list[EvidenceRow], db_path: str | Path) -> None:
    p = Path(db_path)
    if not p.exists():
        return
    conn = sqlite3.connect(p)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
              os.operator_name,
              os.corporate_number,
              os.brand,
              os.place_id,
              os.operator_type,
              os.confidence,
              os.discovered_via,
              os.verification_source,
              s.name AS store_name,
              se.evidence_url,
              se.reason,
              se.keyword,
              se.snippet
            FROM operator_stores os
            JOIN stores s ON s.place_id = os.place_id
            LEFT JOIN store_evidence se ON se.place_id = os.place_id
            WHERE os.operator_name != ''
              AND COALESCE(os.operator_type, '') != 'franchisor'
              AND COALESCE(os.brand, s.brand) IN ({})
            """.format(",".join("?" for _ in TARGET_BRANDS)),
            TARGET_BRANDS,
        ).fetchall()
        for row in rows:
            b = store.get(row["operator_name"], row["corporate_number"] or "")
            if b is None:
                continue
            brand = row["brand"] or ""
            if brand:
                bb = b.brand_bucket(brand)
                b.pipeline_brands.add(brand)
                bb.sources.add("pipeline")
            if row["place_id"]:
                b.pipeline_store_ids.add(row["place_id"])
                if brand:
                    b.pipeline_store_ids_by_brand.setdefault(brand, set()).add(row["place_id"])
            if row["store_name"]:
                b.pipeline_sample_stores.add(row["store_name"])
            try:
                b.pipeline_confidence_values.append(float(row["confidence"] or 0))
            except ValueError:
                pass
            _merge_set(b.pipeline_discovered_via, row["discovered_via"] or "")
            _merge_set(b.pipeline_verification_sources, row["verification_source"] or "")
            if row["evidence_url"]:
                b.pipeline_evidence_urls.add(row["evidence_url"])
                _add_evidence(
                    evidence,
                    b,
                    brand=brand,
                    evidence_kind="pipeline_store_evidence",
                    source_type=row["reason"] or "pipeline",
                    status=row["discovered_via"] or "",
                    store_name=row["store_name"] or "",
                    place_id=row["place_id"] or "",
                    url=row["evidence_url"],
                    snippet=row["snippet"] or "",
                    source_file=str(db_path),
                )
    finally:
        conn.close()


def _best_quality(b: OperatorBucket) -> str:
    if b.corporate_numbers:
        return "A_houjin_verified"
    if b.quality_tiers:
        return max(b.quality_tiers, key=lambda q: QUALITY_SCORE.get(q, 0))
    if b.recruitment_accepted_store_ids:
        return "A_houjin_verified"
    if b.recruitment_urls:
        return "C_candidate_unverified"
    if b.pipeline_store_ids:
        return "D_pipeline_only_unverified_review"
    return "unknown"


def _priority_segment(total: int) -> str:
    if total >= 100:
        return "P0_100plus"
    if total >= 50:
        return "P1_50plus"
    if total >= 20:
        return "P2_20plus"
    if total >= 10:
        return "P3_10plus"
    if total >= 2:
        return "P4_2plus"
    return "PX_single_or_unknown"


def _brand_breakdown(b: OperatorBucket) -> str:
    items = sorted(b.brand_counts().items(), key=lambda x: (-x[1], x[0]))
    return ";".join(f"{brand}:{count}" for brand, count in items)


def _source_type_summary(counter: Counter[str]) -> str:
    return " / ".join(f"{k}:{v}" for k, v in counter.most_common() if k)


def _risk_level(b: OperatorBucket) -> str:
    if "franchisor" in b.operator_kinds or "franchisor_or_direct_link" in b.risk_flags:
        return "franchisor_or_direct_review"
    if b.risk_flags:
        return "review_required"
    if _best_quality(b) == "D_pipeline_only_unverified_review":
        return "pipeline_review"
    if not b.corporate_numbers:
        return "unverified_review"
    return "verified"


def _all_brand_values(b: OperatorBucket, attr: str) -> set[str]:
    values: set[str] = set()
    for bb in b.brand_info.values():
        values.update(getattr(bb, attr))
    return values


def build_rows(store: BucketStore, *, min_total: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    included: list[OperatorBucket] = []
    excluded: list[OperatorBucket] = []
    for b in store.buckets.values():
        if b.total_stores_est() >= min_total:
            included.append(b)
        else:
            excluded.append(b)

    included.sort(
        key=lambda b: (
            -b.total_stores_est(),
            -b.brand_count_est(),
            -QUALITY_SCORE.get(_best_quality(b), 0),
            b.name(),
        )
    )
    rows: list[dict[str, Any]] = []
    for idx, b in enumerate(included, start=1):
        total = b.total_stores_est()
        all_evidence_urls = (
            set(b.source_urls)
            | set(b.pipeline_evidence_urls)
            | set(b.recruitment_urls)
            | set(b.recruitment_failed_urls)
            | set(b.recruitment_unverified_urls)
            | set(b.recruitment_accepted_urls)
            | _all_brand_values(b, "source_urls")
            | _all_brand_values(b, "sample_urls")
        )
        failed_or_unverified_urls = set(b.recruitment_failed_urls) | set(b.recruitment_unverified_urls)
        conf = (
            sum(b.pipeline_confidence_values) / len(b.pipeline_confidence_values)
            if b.pipeline_confidence_values else 0.0
        )
        row: dict[str, Any] = {
            "company_priority_rank": idx,
            "priority_segment": _priority_segment(total),
            "operator_name": b.name(),
            "normalized_operator_key": canonical_key(b.name()),
            "primary_corporate_number": sorted(b.corporate_numbers)[0] if b.corporate_numbers else "",
            "corporate_numbers": _join(b.corporate_numbers),
            "quality_best_tier": _best_quality(b),
            "risk_level": _risk_level(b),
            "risk_flags_all": _join(b.risk_flags),
            "operator_total_stores_est": total,
            "operator_brand_count_est": b.brand_count_est(),
            "brands_breakdown": _brand_breakdown(b),
            "head_offices": _join(b.head_offices, limit=5),
            "hq_prefectures": _join(b.hq_prefectures),
            "website_urls": _join(b.website_urls),
            "representative_names": _join(b.representative_names),
            "representative_titles": _join(b.representative_titles),
            "revenue_current_jpy_values": _join({str(v) for v in b.revenue_current_values}),
            "revenue_previous_jpy_values": _join({str(v) for v in b.revenue_previous_values}),
            "revenue_observed_at": _join(b.revenue_observed_at),
            "operator_kinds": _join(b.operator_kinds),
            "sources_all": _join(b.sources),
            "source_urls_all": _join(b.source_urls),
            "notes_all": _join(b.notes),
            "quality_tiers_all": _join(b.quality_tiers),
            "evidence_statuses_all": _join(b.evidence_statuses),
            "priority_tiers_all": _join(b.priority_tiers),
            "include_reasons_all": _join(b.include_reasons),
            "all_evidence_url_count": len(all_evidence_urls),
            "all_evidence_urls": _join(all_evidence_urls),
            "failed_or_unverified_url_count": len(failed_or_unverified_urls),
            "failed_or_unverified_urls": _join(failed_or_unverified_urls),
            "pipeline_known_store_count": len(b.pipeline_store_ids),
            "pipeline_brand_count": len(b.pipeline_brands),
            "pipeline_avg_confidence": f"{conf:.3f}" if conf else "",
            "pipeline_discovered_via": _join(b.pipeline_discovered_via),
            "pipeline_verification_sources": _join(b.pipeline_verification_sources),
            "pipeline_sample_stores": _join(b.pipeline_sample_stores),
            "pipeline_evidence_url_count": len(b.pipeline_evidence_urls),
            "pipeline_evidence_urls": _join(b.pipeline_evidence_urls),
            "recruitment_statuses": _join(b.recruitment_statuses),
            "recruitment_candidate_store_count": len(b.recruitment_store_ids),
            "recruitment_candidate_url_count": len(b.recruitment_urls),
            "recruitment_failed_url_count": len(b.recruitment_failed_urls),
            "recruitment_unverified_url_count": len(b.recruitment_unverified_urls),
            "recruitment_accepted_store_count": len(b.recruitment_accepted_store_ids),
            "recruitment_source_types": _source_type_summary(b.recruitment_source_types),
            "recruitment_reject_reasons": _source_type_summary(b.recruitment_reject_reasons),
            "recruitment_sample_stores": _join(b.recruitment_sample_stores),
            "recruitment_candidate_urls": _join(b.recruitment_urls),
            "recruitment_failed_urls": _join(b.recruitment_failed_urls),
            "recruitment_unverified_urls": _join(b.recruitment_unverified_urls),
            "recruitment_accepted_evidence_urls": _join(b.recruitment_accepted_urls),
        }
        for brand in TARGET_BRANDS:
            token = BRAND_TOKENS[brand]
            bb = b.brand_info.get(brand, BrandBucket())
            row[f"stores_{token}"] = b.brand_counts().get(brand, 0)
            row[f"rank_{token}"] = _join(bb.priority_ranks)
            row[f"tier_{token}"] = _join(bb.priority_tiers)
            row[f"quality_{token}"] = _join(bb.quality_tiers)
            row[f"evidence_{token}"] = _join(bb.evidence_statuses)
            row[f"risk_{token}"] = _join(bb.risk_flags)
            row[f"sources_{token}"] = _join(bb.sources)
            row[f"source_urls_{token}"] = _join(bb.source_urls)
            row[f"sample_stores_{token}"] = _join(bb.sample_stores)
            row[f"sample_urls_{token}"] = _join(bb.sample_urls)
            row[f"notes_{token}"] = _join(bb.notes)
        rows.append(row)

    excluded_rows = [
        {
            "operator_name": b.name(),
            "corporate_numbers": _join(b.corporate_numbers),
            "operator_total_stores_est": b.total_stores_est(),
            "operator_brand_count_est": b.brand_count_est(),
            "brands_breakdown": _brand_breakdown(b),
            "exclude_reason": f"operator_total_stores_est_below_{min_total}",
            "sources_all": _join(b.sources),
            "recruitment_candidate_url_count": len(b.recruitment_urls),
        }
        for b in sorted(excluded, key=lambda x: (-x.total_stores_est(), x.name()))
    ]
    return rows, excluded_rows


def _write_csv(path: str | Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
            lineterminator="\n",
        )
        w.writeheader()
        for row in rows:
            w.writerow(row)


def export_operator_master(
    *,
    out: str | Path,
    evidence_out: str | Path,
    excluded_out: str | Path,
    priority_csv: str | Path = "var/phase27/priority-megajii-operators-14brand.csv",
    directory_csv: str | Path = "var/phase27/deliverable/fc-operators-directory-14brand.csv",
    component_csv: str | Path = "var/phase27/deliverable/brand-operator-components-orm-14brand-10plus.csv",
    recruitment_ranking_csv: str | Path = "var/phase27/top-operators-chateraise-itto-kandon-review.csv",
    recruitment_sidecars: list[tuple[str | Path, str]] | None = None,
    orm_db: str | Path = "var/pizza-registry.sqlite",
    pipeline_db: str | Path = "var/pizza.sqlite",
    min_total: int = 2,
) -> tuple[list[dict[str, Any]], list[EvidenceRow], list[dict[str, Any]]]:
    store = BucketStore()
    evidence: list[EvidenceRow] = []
    load_orm_db(store, evidence, orm_db)
    load_pipeline_db(store, evidence, pipeline_db)
    load_directory_csv(store, directory_csv)
    load_component_csv(store, evidence, component_csv)
    load_priority_csv(store, evidence, priority_csv)
    load_recruitment_ranking_csv(store, recruitment_ranking_csv)
    if recruitment_sidecars is None:
        recruitment_sidecars = [
            ("var/phase27/recruitment-research-chateraise-combined-candidates.csv", "candidate"),
            ("var/phase27/recruitment-research-chateraise-combined-failed-urls.csv", "failed"),
            ("var/phase27/recruitment-research-chateraise-combined-unverified.csv", "unverified"),
            ("var/phase27/recruitment-research-chateraise-combined-accepted.csv", "accepted"),
            ("var/phase27/recruitment-research-itto-combined-candidates.csv", "candidate"),
            ("var/phase27/recruitment-research-itto-combined-failed-urls.csv", "failed"),
            ("var/phase27/recruitment-research-itto-combined-unverified.csv", "unverified"),
            ("var/phase27/recruitment-research-itto-combined-accepted.csv", "accepted"),
            ("var/phase27/recruitment-research-kandon-combined-candidates.csv", "candidate"),
            ("var/phase27/recruitment-research-kandon-combined-failed-urls.csv", "failed"),
            ("var/phase27/recruitment-research-kandon-combined-unverified.csv", "unverified"),
            ("var/phase27/recruitment-research-kandon-combined-accepted.csv", "accepted"),
        ]
    for path, kind in recruitment_sidecars:
        load_recruitment_sidecar(store, evidence, path, kind)
    rows, excluded_rows = build_rows(store, min_total=min_total)
    fieldnames = list(rows[0].keys()) if rows else []
    _write_csv(out, rows, fieldnames)
    _write_csv(evidence_out, [e.to_dict() for e in evidence], list(EvidenceRow.__annotations__.keys()))
    _write_csv(excluded_out, excluded_rows)
    return rows, evidence, excluded_rows


def _main() -> None:
    ap = argparse.ArgumentParser(description="事業会社基軸の 14 brand FC 調査統合 CSV を出力")
    ap.add_argument("--out", default="var/phase27/operator-centric-master-14brand.csv")
    ap.add_argument("--evidence-out", default="var/phase27/operator-centric-evidence-14brand.csv")
    ap.add_argument("--excluded-out", default="var/phase27/operator-centric-excluded-single-store-14brand.csv")
    ap.add_argument(
        "--review-out",
        default="",
        help="本部/direct 判定行だけを除いた review 用 wide CSV。未確認候補は残す。",
    )
    ap.add_argument("--min-total", type=int, default=2)
    ap.add_argument("--priority-csv", default="var/phase27/priority-megajii-operators-14brand.csv")
    ap.add_argument("--directory-csv", default="var/phase27/deliverable/fc-operators-directory-14brand.csv")
    ap.add_argument("--component-csv", default="var/phase27/deliverable/brand-operator-components-orm-14brand-10plus.csv")
    ap.add_argument("--recruitment-ranking-csv", default="var/phase27/top-operators-chateraise-itto-kandon-review.csv")
    ap.add_argument("--orm-db", default="var/pizza-registry.sqlite")
    ap.add_argument("--pipeline-db", default="var/pizza.sqlite")
    args = ap.parse_args()
    rows, evidence, excluded = export_operator_master(
        out=args.out,
        evidence_out=args.evidence_out,
        excluded_out=args.excluded_out,
        priority_csv=args.priority_csv,
        directory_csv=args.directory_csv,
        component_csv=args.component_csv,
        recruitment_ranking_csv=args.recruitment_ranking_csv,
        orm_db=args.orm_db,
        pipeline_db=args.pipeline_db,
        min_total=args.min_total,
    )
    print(f"operator-centric master: {args.out} rows={len(rows)} min_total={args.min_total}")
    print(f"operator-centric evidence: {args.evidence_out} rows={len(evidence)}")
    print(f"operator-centric excluded: {args.excluded_out} rows={len(excluded)}")
    if args.review_out:
        review_rows = [
            row for row in rows
            if row.get("risk_level") != "franchisor_or_direct_review"
        ]
        _write_csv(args.review_out, review_rows, list(rows[0].keys()) if rows else [])
        print(f"operator-centric review: {args.review_out} rows={len(review_rows)}")


if __name__ == "__main__":
    _main()
