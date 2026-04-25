from __future__ import annotations

import csv

from pizza_delivery.operator_master_export import (
    BucketStore,
    build_rows,
    load_component_csv,
    load_recruitment_sidecar,
)


def _write(path, rows):
    fields = sorted({k for row in rows for k in row})
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def test_operator_master_keeps_one_store_side_brand_when_company_total_qualifies(tmp_path) -> None:
    components = tmp_path / "components.csv"
    _write(components, [
        {
            "brand": "モスバーガー",
            "operator_name": "株式会社サンプルFC",
            "corporate_number": "1234567890123",
            "brand_estimated_store_count": "1",
            "operator_total_stores_est": "31",
            "operator_brand_count": "2",
            "sources": "manual_megajii",
        },
        {
            "brand": "エニタイムフィットネス",
            "operator_name": "株式会社サンプルFC",
            "corporate_number": "1234567890123",
            "brand_estimated_store_count": "30",
            "operator_total_stores_est": "31",
            "operator_brand_count": "2",
            "sources": "manual_megajii",
        },
    ])
    store = BucketStore()
    evidence = []
    load_component_csv(store, evidence, components)

    rows, excluded = build_rows(store, min_total=2)

    assert len(rows) == 1
    assert not excluded
    assert rows[0]["operator_total_stores_est"] == 31
    assert rows[0]["stores_mos"] == 1
    assert rows[0]["stores_anytime"] == 30


def test_operator_master_can_export_unverified_single_store_candidates_when_min_total_one(tmp_path) -> None:
    candidates = tmp_path / "candidates.csv"
    _write(candidates, [
        {
            "brand": "カルビ丼とスン豆腐専門店韓丼",
            "place_id": "p1",
            "store_name": "韓丼サンプル店",
            "candidate_operator": "未確認フーズ株式会社",
            "candidate_confidence": "0.7",
            "source_type": "job_site",
            "evidence_url": "https://jobs.example/sample",
            "proposal_reject_reason": "operator_missing",
        },
    ])
    store = BucketStore()
    evidence = []
    load_recruitment_sidecar(store, evidence, candidates, "candidate")

    rows_min2, excluded_min2 = build_rows(store, min_total=2)
    rows_min1, _ = build_rows(store, min_total=1)

    assert rows_min2 == []
    assert excluded_min2[0]["operator_name"] == "未確認フーズ株式会社"
    assert len(rows_min1) == 1
    assert rows_min1[0]["quality_best_tier"] == "C_candidate_unverified"
    assert rows_min1[0]["recruitment_candidate_url_count"] == 1
    assert rows_min1[0]["recruitment_reject_reasons"] == "operator_missing:1"
    assert evidence[0].url == "https://jobs.example/sample"
