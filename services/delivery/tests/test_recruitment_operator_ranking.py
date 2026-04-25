from __future__ import annotations

import csv

from pizza_delivery.recruitment_operator_ranking import aggregate_operator_candidates


def _write(path, rows):
    fields = sorted({k for r in rows for k in r})
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def test_aggregate_operator_candidates_counts_unique_stores_and_verified(tmp_path) -> None:
    candidates = tmp_path / "candidates.csv"
    accepted = tmp_path / "accepted.csv"
    _write(candidates, [
        {
            "brand": "シャトレーゼ",
            "place_id": "p1",
            "store_name": "シャトレーゼ七光台店",
            "candidate_operator": "東京日食株式会社",
            "candidate_confidence": "1.0",
            "source_type": "job_site",
            "evidence_url": "https://jobs.example/a",
            "proposal_reject_reason": "",
        },
        {
            "brand": "シャトレーゼ",
            "place_id": "p1",
            "store_name": "シャトレーゼ七光台店",
            "candidate_operator": "東京日食株式会社",
            "candidate_confidence": "0.9",
            "source_type": "job_site",
            "evidence_url": "https://jobs.example/b",
            "proposal_reject_reason": "",
        },
        {
            "brand": "Itto個別指導学院",
            "place_id": "p2",
            "store_name": "ITTO野田川間校",
            "candidate_operator": "株式会社リックプレイス",
            "candidate_confidence": "1.0",
            "source_type": "job_site",
            "evidence_url": "https://jobs.example/c",
            "proposal_reject_reason": "empty_html",
        },
    ])
    _write(accepted, [
        {
            "brand": "シャトレーゼ",
            "place_id": "p1",
            "store_name": "シャトレーゼ七光台店",
            "final_operator": "東京日食株式会社",
            "final_corp": "6040001019311",
            "evidence_url": "https://jobs.example/a",
        }
    ])

    rows = aggregate_operator_candidates([candidates], [accepted])
    by_name = {r["operator_name"]: r for r in rows}

    assert by_name["東京日食株式会社"]["candidate_store_count"] == 1
    assert by_name["東京日食株式会社"]["candidate_url_count"] == 2
    assert by_name["東京日食株式会社"]["accepted_store_count"] == 1
    assert by_name["東京日食株式会社"]["status"] == "verified"
    assert by_name["株式会社リックプレイス"]["status"] == "candidate_unverified"
