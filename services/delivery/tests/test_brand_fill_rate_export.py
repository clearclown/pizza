from __future__ import annotations

import csv
from pathlib import Path

from pizza_delivery.brand_fill_rate_export import (
    build_brand_fill_rates,
    official_source_audit_rows,
    write_brand_fill_rate_csv,
    write_official_source_audit_csv,
)
from pizza_delivery.extended_fc_brand_export import BASE_LINK_FIELDS


def _write_links(path: Path) -> None:
    rows = [
        {
            "brand_name": "珈琲所コメダ珈琲店",
            "industry": "コーヒーショップ",
            "operator_name": "株式会社コメダ",
            "corporate_number": "4180001063075",
            "head_office": "愛知県",
            "prefecture": "愛知県",
            "operator_type": "franchisor",
            "estimated_store_count": "1000",
            "source": "manual_megajii_2026_04_24",
            "source_url": "",
            "note": "",
        },
        {
            "brand_name": "コメダ珈琲店",
            "industry": "コーヒーショップ",
            "operator_name": "株式会社大口FC",
            "corporate_number": "1234567890123",
            "head_office": "東京都",
            "prefecture": "東京都",
            "operator_type": "franchisee",
            "estimated_store_count": "300",
            "source": "manual_megajii_2026_04_24",
            "source_url": "",
            "note": "",
        },
        {
            "brand_name": "コメダ珈琲",
            "industry": "コーヒーショップ",
            "operator_name": "株式会社候補",
            "corporate_number": "",
            "head_office": "",
            "prefecture": "",
            "operator_type": "unknown",
            "estimated_store_count": "50",
            "source": "official_franchisee_page",
            "source_url": "https://example.invalid/evidence",
            "note": "",
        },
        {
            "brand_name": "スクールIE",
            "industry": "学習塾",
            "operator_name": "株式会社やる気スイッチグループ",
            "corporate_number": "5010001154032",
            "head_office": "東京都",
            "prefecture": "東京都",
            "operator_type": "franchisor",
            "estimated_store_count": "1182",
            "source": "jfa_disclosure",
            "source_url": "",
            "note": "",
        },
        {
            "brand_name": "オフハウス",
            "industry": "リユース",
            "operator_name": "株式会社A",
            "corporate_number": "2222222222222",
            "head_office": "",
            "prefecture": "",
            "operator_type": "franchisee",
            "estimated_store_count": "150",
            "source": "manual_megajii_2026_04_24",
            "source_url": "",
            "note": "",
        },
        {
            "brand_name": "オフハウス",
            "industry": "リユース",
            "operator_name": "株式会社ハードオフコーポレーション",
            "corporate_number": "6110001019325",
            "head_office": "新潟県",
            "prefecture": "新潟県",
            "operator_type": "franchisor",
            "estimated_store_count": "50",
            "source": "jfa_disclosure",
            "source_url": "",
            "note": "",
        },
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=BASE_LINK_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def test_build_brand_fill_rates_aggregates_aliases_and_priorities(tmp_path: Path) -> None:
    fc_links = tmp_path / "fc-links.csv"
    _write_links(fc_links)

    rows = build_brand_fill_rates(fc_links)
    by_brand = {r["brand_name"]: r for r in rows}

    assert by_brand["コメダ珈琲"]["expected_total_stores"] == "1000"
    assert by_brand["コメダ珈琲"]["franchisee_store_sum"] == "300"
    assert by_brand["コメダ珈琲"]["candidate_store_sum"] == "350"
    assert by_brand["コメダ珈琲"]["franchisee_fill_rate_pct"] == "30.0"
    assert by_brand["コメダ珈琲"]["coverage_status"] == "low"
    assert by_brand["コメダ珈琲"]["priority"] == "P0_EXPAND"
    assert by_brand["コメダ珈琲"]["has_official_recruitment_crawl"] == "yes"

    assert by_brand["スクールIE"]["coverage_status"] == "empty"
    assert by_brand["スクールIE"]["priority"] == "P0_FIND_OFFICIAL_LIST"
    assert "no franchisee" not in by_brand["スクールIE"]["notes"]

    assert by_brand["オフハウス"]["coverage_status"] == "overfilled_review"
    assert by_brand["オフハウス"]["priority"] == "REVIEW_OVERFILL"


def test_write_brand_fill_rate_csv_returns_counts(tmp_path: Path) -> None:
    fc_links = tmp_path / "fc-links.csv"
    out = tmp_path / "brand-fill-rate.csv"
    _write_links(fc_links)

    stats = write_brand_fill_rate_csv(fc_links, out)

    assert stats == {
        "brand_fill_rate_rows": 3,
        "p0_rows": 2,
        "low_or_empty_rows": 2,
        "official_parser_rows": 0,
        "official_recruitment_rows": 1,
    }
    assert out.exists()


def test_official_source_audit_includes_parser_and_recruitment_routes(tmp_path: Path) -> None:
    rows = official_source_audit_rows()

    assert any(
        r["brand_name"] == "Brand off"
        and r["source_type"] == "official_franchisee_parser"
        and r["usable_for_operator_ground_truth"] == "yes"
        for r in rows
    )
    assert any(
        r["brand_name"] == "カーブス"
        and r["source_type"] == "official_recruitment_crawl"
        and r["usable_for_operator_ground_truth"] == "yes_after_store_match_and_houjin_verify"
        for r in rows
    )
    assert any(
        r["brand_name"] == "エニタイムフィットネス"
        and r["source_type"] == "official_store_sync"
        and r["usable_for_operator_ground_truth"] == "no_store_list_only"
        for r in rows
    )

    out = tmp_path / "official-source-audit.csv"
    assert write_official_source_audit_csv(out) == len(rows)
    assert out.exists()
