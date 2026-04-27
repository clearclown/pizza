"""求人横断 candidate から上位 operator 候補をランキング化する。

DB に採用する ground truth ではなく、取捨選択用の BI artifact。
候補・失敗 URL は捨てず、operator 別に店舗数/URL数/verified 状態を集約する。
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pizza_delivery.normalize import canonical_key, normalize_operator_name
from pizza_delivery.registry_expander import _load_known_franchisor_names


@dataclass
class OperatorBucket:
    key: str
    operator_name: str
    brands: set[str] = field(default_factory=set)
    store_ids: set[str] = field(default_factory=set)
    store_names: set[str] = field(default_factory=set)
    candidate_urls: set[str] = field(default_factory=set)
    source_types: Counter[str] = field(default_factory=Counter)
    reject_reasons: Counter[str] = field(default_factory=Counter)
    max_confidence: float = 0.0
    accepted_store_ids: set[str] = field(default_factory=set)
    accepted_store_names: set[str] = field(default_factory=set)
    corporate_numbers: set[str] = field(default_factory=set)
    accepted_evidence_urls: set[str] = field(default_factory=set)
    blocked_franchisor_rows: int = 0


def _read_csv(path: str | Path) -> list[dict[str, str]]:
    p = Path(path)
    if not p.exists():
        return []
    with p.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _bucket_for(buckets: dict[str, OperatorBucket], name: str) -> OperatorBucket | None:
    op = normalize_operator_name(name)
    if not op:
        return None
    key = canonical_key(op)
    if key not in buckets:
        buckets[key] = OperatorBucket(key=key, operator_name=op)
    return buckets[key]


def aggregate_operator_candidates(
    candidate_paths: list[str | Path],
    accepted_paths: list[str | Path],
) -> list[dict[str, Any]]:
    buckets: dict[str, OperatorBucket] = {}

    for path in candidate_paths:
        for row in _read_csv(path):
            name = row.get("candidate_operator") or ""
            b = _bucket_for(buckets, name)
            if b is None:
                continue
            brand = row.get("brand") or ""
            place_id = row.get("place_id") or ""
            store_name = row.get("store_name") or ""
            url = row.get("evidence_url") or ""
            b.brands.add(brand)
            if place_id:
                b.store_ids.add(place_id)
            if store_name:
                b.store_names.add(store_name)
            if url:
                b.candidate_urls.add(url)
            if row.get("source_type"):
                b.source_types[row["source_type"]] += 1
            if row.get("proposal_reject_reason"):
                b.reject_reasons[row["proposal_reject_reason"]] += 1
                if str(row["proposal_reject_reason"]).startswith("blocked_franchisor"):
                    b.blocked_franchisor_rows += 1
            try:
                b.max_confidence = max(b.max_confidence, float(row.get("candidate_confidence") or 0))
            except ValueError:
                pass

    for path in accepted_paths:
        for row in _read_csv(path):
            name = row.get("final_operator") or ""
            b = _bucket_for(buckets, name)
            if b is None:
                continue
            brand = row.get("brand") or ""
            place_id = row.get("place_id") or ""
            store_name = row.get("store_name") or ""
            b.brands.add(brand)
            if place_id:
                b.store_ids.add(place_id)
                b.accepted_store_ids.add(place_id)
            if store_name:
                b.store_names.add(store_name)
                b.accepted_store_names.add(store_name)
            if row.get("final_corp"):
                b.corporate_numbers.add(row["final_corp"])
            if row.get("evidence_url"):
                b.accepted_evidence_urls.add(row["evidence_url"])

    franchisor_keys = {canonical_key(n) for n in _load_known_franchisor_names() if n}
    rows: list[dict[str, Any]] = []
    for b in buckets.values():
        likely_franchisor = b.key in franchisor_keys or b.blocked_franchisor_rows > 0
        parent_or_hq = "ホールディングス" in b.operator_name
        status = "verified" if b.accepted_store_ids else "candidate_unverified"
        if likely_franchisor:
            status = "franchisor_or_hq_candidate"
        elif parent_or_hq:
            status = "parent_or_hq_candidate"
        rows.append({
            "operator_name": b.operator_name,
            "status": status,
            "brand_count": len({x for x in b.brands if x}),
            "brands": " / ".join(sorted(x for x in b.brands if x)),
            "candidate_store_count": len(b.store_ids),
            "candidate_url_count": len(b.candidate_urls),
            "accepted_store_count": len(b.accepted_store_ids),
            "corporate_numbers": " / ".join(sorted(b.corporate_numbers)),
            "max_confidence": f"{b.max_confidence:.2f}",
            "blocked_franchisor_rows": b.blocked_franchisor_rows,
            "source_types": " / ".join(f"{k}:{v}" for k, v in b.source_types.most_common()),
            "reject_reasons": " / ".join(f"{k}:{v}" for k, v in b.reject_reasons.most_common(5)),
            "sample_stores": " / ".join(sorted(b.store_names)[:6]),
            "sample_urls": " / ".join(sorted(b.candidate_urls)[:6]),
            "accepted_stores": " / ".join(sorted(b.accepted_store_names)),
            "accepted_evidence_urls": " / ".join(sorted(b.accepted_evidence_urls)),
        })

    rows.sort(
        key=lambda r: (
            int(r["accepted_store_count"]),
            int(r["candidate_store_count"]),
            int(r["candidate_url_count"]),
            float(r["max_confidence"] or 0),
            r["operator_name"],
        ),
        reverse=True,
    )
    return rows


def _write_csv(path: str | Path, rows: list[dict[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "operator_name", "status", "brand_count", "brands",
        "candidate_store_count", "candidate_url_count", "accepted_store_count",
        "corporate_numbers", "max_confidence", "blocked_franchisor_rows",
        "source_types", "reject_reasons", "sample_stores", "sample_urls",
        "accepted_stores", "accepted_evidence_urls",
    ]
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _split_csv_arg(value: str) -> list[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def _main() -> None:
    ap = argparse.ArgumentParser(description="求人候補から上位 operator 候補ランキングを出力")
    ap.add_argument("--candidates", required=True, help="candidate CSV path list, comma separated")
    ap.add_argument("--accepted", default="", help="accepted CSV path list, comma separated")
    ap.add_argument("--out", required=True)
    ap.add_argument("--out-review", default="", help="本部/親会社候補を除いた review CSV")
    args = ap.parse_args()

    rows = aggregate_operator_candidates(
        _split_csv_arg(args.candidates),
        _split_csv_arg(args.accepted),
    )
    _write_csv(args.out, rows)
    print(f"📄 operator ranking: {args.out} rows={len(rows)}")
    if args.out_review:
        review_rows = [
            r for r in rows
            if r["status"] not in {"franchisor_or_hq_candidate", "parent_or_hq_candidate"}
        ]
        _write_csv(args.out_review, review_rows)
        print(f"📄 operator ranking review: {args.out_review} rows={len(review_rows)}")


if __name__ == "__main__":
    _main()
