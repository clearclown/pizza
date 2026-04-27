"""Export brand-level FC operator fill-rate diagnostics.

This report is a planning/audit artifact.  It does not create operator ground
truth; it summarizes how much of each brand's published store footprint is
covered by franchisee/operator evidence already present in ``fc-links.csv``.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from pathlib import Path

from pizza_delivery.extended_fc_brand_export import (
    BASE_LINK_FIELDS,
    EXCLUDED_EXISTING_NONSEED_BRAND_NAMES,
    _canonical_extended_brand,
)
from pizza_delivery.megafranchisee_clean_export import _dedupe_link_rows
from pizza_delivery.normalize import normalize_operator_name
from pizza_delivery.official_franchisee_sources import DEFAULT_SOURCES
from pizza_delivery.official_recruitment_crawl import JOBFIND_SOURCES


DEFAULT_FC_LINKS_PATH = Path("test/fixtures/megafranchisee/fc-links.csv")
DEFAULT_OUT = Path("test/fixtures/megafranchisee/brand-fill-rate.csv")
DEFAULT_OFFICIAL_SOURCE_OUT = Path("test/fixtures/megafranchisee/official-source-audit.csv")

BRAND_FILL_RATE_FIELDS = [
    "brand_name",
    "expected_total_stores",
    "franchisee_store_sum",
    "candidate_store_sum",
    "franchisee_link_count",
    "verified_franchisee_link_count",
    "unknown_link_count",
    "franchisee_fill_rate_pct",
    "candidate_fill_rate_pct",
    "coverage_status",
    "priority",
    "has_official_franchisee_parser",
    "has_official_recruitment_crawl",
    "has_official_store_sync",
    "source_count_basis",
    "sources",
    "notes",
]

OFFICIAL_SOURCE_AUDIT_FIELDS = [
    "brand_name",
    "source_type",
    "source_url",
    "usable_for_operator_ground_truth",
    "pipeline_command",
    "notes",
]

OFFICIAL_FRANCHISEE_PARSER_BRANDS = {
    "Brand off",
    "カルビ丼とスン豆腐専門店韓丼",
}

OFFICIAL_RECRUITMENT_CRAWL_BRANDS = {
    "モスバーガー",
    "カーブス",
    "コメダ珈琲",
    "シャトレーゼ",
    "業務スーパー",
}

OFFICIAL_STORE_SYNC_BRANDS = {
    "エニタイムフィットネス",
}

OFFICIAL_STORE_SYNC_URLS = {
    "エニタイムフィットネス": "https://www.anytimefitness.co.jp/",
}


@dataclass
class BrandFill:
    brand_name: str
    expected_total_stores: int = 0
    franchisee_counts: dict[str, int] = field(default_factory=dict)
    unknown_counts: dict[str, int] = field(default_factory=dict)
    verified_franchisees: set[str] = field(default_factory=set)
    sources: set[str] = field(default_factory=set)
    notes: set[str] = field(default_factory=set)

    def add_row(self, row: dict[str, str]) -> None:
        source = row.get("source") or ""
        if source:
            self.sources.add(source)
        count = _int_value(row.get("estimated_store_count"))
        operator_type = row.get("operator_type") or ""
        if operator_type in {"franchisor", "direct"}:
            self.expected_total_stores = max(self.expected_total_stores, count)
            return

        op_name = normalize_operator_name(row.get("operator_name") or "")
        op_key = row.get("corporate_number") or "".join(op_name.split())
        if not op_key:
            return
        if operator_type == "franchisee":
            self.franchisee_counts[op_key] = max(self.franchisee_counts.get(op_key, 0), count)
            if row.get("corporate_number"):
                self.verified_franchisees.add(op_key)
        elif operator_type == "unknown":
            self.unknown_counts[op_key] = max(self.unknown_counts.get(op_key, 0), count)

    @property
    def franchisee_store_sum(self) -> int:
        return sum(self.franchisee_counts.values())

    @property
    def candidate_store_sum(self) -> int:
        merged = dict(self.unknown_counts)
        for key, value in self.franchisee_counts.items():
            merged[key] = max(merged.get(key, 0), value)
        return sum(merged.values())

    def fill_pct(self, value: int) -> str:
        if self.expected_total_stores <= 0:
            return ""
        return f"{value * 100 / self.expected_total_stores:.1f}"

    def status(self) -> str:
        if self.expected_total_stores <= 0:
            return "no_franchisor_total"
        if not self.franchisee_counts:
            return "empty"
        pct = self.franchisee_store_sum * 100 / self.expected_total_stores
        if pct > 120:
            return "overfilled_review"
        if pct < 25:
            return "very_low"
        if pct < 50:
            return "low"
        if pct < 80:
            return "medium"
        return "covered"

    def priority(self) -> str:
        status = self.status()
        if status == "overfilled_review":
            return "REVIEW_OVERFILL"
        if status == "no_franchisor_total":
            return "P4_NEED_TOTAL"
        if status == "empty" and self.expected_total_stores >= 100:
            return "P0_FIND_OFFICIAL_LIST"
        if self.expected_total_stores >= 300 and status in {"very_low", "low"}:
            return "P0_EXPAND"
        if self.expected_total_stores >= 100 and status in {"very_low", "low"}:
            return "P1_EXPAND"
        if self.expected_total_stores >= 50 and status in {"very_low", "low"}:
            return "P1_EXPAND"
        if status == "medium":
            return "P2_EXPAND"
        return "P3_MONITOR"

    def row(self) -> dict[str, str]:
        notes = set(self.notes)
        if self.status() == "overfilled_review":
            notes.add("count_sum_exceeds_franchisor_total_review_manual_store_count_basis")
        if self.status() == "empty":
            notes.add("franchisor_total_exists_but_no_franchisee_operator_rows")
        return {
            "brand_name": self.brand_name,
            "expected_total_stores": str(self.expected_total_stores),
            "franchisee_store_sum": str(self.franchisee_store_sum),
            "candidate_store_sum": str(self.candidate_store_sum),
            "franchisee_link_count": str(len(self.franchisee_counts)),
            "verified_franchisee_link_count": str(len(self.verified_franchisees)),
            "unknown_link_count": str(len(self.unknown_counts)),
            "franchisee_fill_rate_pct": self.fill_pct(self.franchisee_store_sum),
            "candidate_fill_rate_pct": self.fill_pct(self.candidate_store_sum),
            "coverage_status": self.status(),
            "priority": self.priority(),
            "has_official_franchisee_parser": _yes_no(
                self.brand_name in OFFICIAL_FRANCHISEE_PARSER_BRANDS
            ),
            "has_official_recruitment_crawl": _yes_no(
                self.brand_name in OFFICIAL_RECRUITMENT_CRAWL_BRANDS
            ),
            "has_official_store_sync": _yes_no(self.brand_name in OFFICIAL_STORE_SYNC_BRANDS),
            "source_count_basis": "franchisee_store_sum_vs_max_franchisor_or_direct_count",
            "sources": ",".join(sorted(self.sources)),
            "notes": "; ".join(sorted(notes)),
        }


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"


def _int_value(value: str | None) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def load_fill_rows(fc_links_path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with fc_links_path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            brand = _canonical_extended_brand(row.get("brand_name", ""))
            if not brand or brand in EXCLUDED_EXISTING_NONSEED_BRAND_NAMES:
                continue
            out = {field: row.get(field, "") for field in BASE_LINK_FIELDS}
            out["brand_name"] = brand
            out["operator_name"] = normalize_operator_name(out.get("operator_name") or "")
            rows.append(out)
    return _dedupe_link_rows(rows)


def build_brand_fill_rates(fc_links_path: Path) -> list[dict[str, str]]:
    fills: dict[str, BrandFill] = {}
    for row in load_fill_rows(fc_links_path):
        brand = row.get("brand_name") or ""
        fill = fills.setdefault(brand, BrandFill(brand_name=brand))
        fill.add_row(row)
    rows = [fill.row() for fill in fills.values()]
    rows.sort(key=_fill_sort_key)
    return rows


def _fill_sort_key(row: dict[str, str]) -> tuple[int, int, float, str]:
    priority_rank = {
        "P0_FIND_OFFICIAL_LIST": 0,
        "P0_EXPAND": 1,
        "P1_EXPAND": 2,
        "P2_EXPAND": 3,
        "REVIEW_OVERFILL": 4,
        "P3_MONITOR": 5,
        "P4_NEED_TOTAL": 6,
    }.get(row.get("priority") or "", 9)
    expected = _int_value(row.get("expected_total_stores"))
    pct_raw = row.get("franchisee_fill_rate_pct") or "9999"
    try:
        pct = float(pct_raw)
    except ValueError:
        pct = 9999.0
    return (priority_rank, -expected, pct, row.get("brand_name") or "")


def write_brand_fill_rate_csv(fc_links_path: Path, out: Path) -> dict[str, int]:
    rows = build_brand_fill_rates(fc_links_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=BRAND_FILL_RATE_FIELDS,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    return {
        "brand_fill_rate_rows": len(rows),
        "p0_rows": sum(1 for r in rows if (r.get("priority") or "").startswith("P0")),
        "low_or_empty_rows": sum(
            1 for r in rows if r.get("coverage_status") in {"empty", "very_low", "low"}
        ),
        "official_parser_rows": sum(
            1 for r in rows if r.get("has_official_franchisee_parser") == "yes"
        ),
        "official_recruitment_rows": sum(
            1 for r in rows if r.get("has_official_recruitment_crawl") == "yes"
        ),
    }


def official_source_audit_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for spec in DEFAULT_SOURCES:
        rows.append(
            {
                "brand_name": _canonical_extended_brand(spec.brand),
                "source_type": "official_franchisee_parser",
                "source_url": spec.url,
                "usable_for_operator_ground_truth": "yes",
                "pipeline_command": "pizza official-franchisee-sources",
                "notes": f"parser={spec.parser}; official page body parsed then houjin verified",
            }
        )
    for brand, url in JOBFIND_SOURCES.items():
        rows.append(
            {
                "brand_name": _canonical_extended_brand(brand),
                "source_type": "official_recruitment_crawl",
                "source_url": url,
                "usable_for_operator_ground_truth": "yes_after_store_match_and_houjin_verify",
                "pipeline_command": "pizza official-recruitment-crawl",
                "notes": "official jobfind/recop detail pages; operator label extracted only from page body",
            }
        )
    for brand, url in OFFICIAL_STORE_SYNC_URLS.items():
        rows.append(
            {
                "brand_name": _canonical_extended_brand(brand),
                "source_type": "official_store_sync",
                "source_url": url,
                "usable_for_operator_ground_truth": "no_store_list_only",
                "pipeline_command": "pizza anytime-official-sync",
                "notes": "official store list improves store coverage; operator name is not published there",
            }
        )
    rows.sort(key=lambda r: (r["brand_name"], r["source_type"], r["source_url"]))
    return rows


def write_official_source_audit_csv(out: Path) -> int:
    rows = official_source_audit_rows()
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=OFFICIAL_SOURCE_AUDIT_FIELDS,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="export brand fill-rate diagnostics")
    parser.add_argument("--fc-links", type=Path, default=DEFAULT_FC_LINKS_PATH)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--official-source-out", type=Path, default=DEFAULT_OFFICIAL_SOURCE_OUT)
    args = parser.parse_args()
    stats = write_brand_fill_rate_csv(args.fc_links, args.out)
    stats["official_source_audit_rows"] = write_official_source_audit_csv(
        args.official_source_out
    )
    for key, value in stats.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
