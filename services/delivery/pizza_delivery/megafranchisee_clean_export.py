"""Clean megafranchisee CSV exports from the registry ORM.

The legacy fixture exports count every brand link equally.  For review-quality
megafranchisee lists we need two separate ideas:

* all brand evidence, including official-site links with 0 stores;
* counted brand evidence, meaning non-franchisor/direct links with store counts.

This exporter keeps `fc-operators-all.csv` broad, but makes the megajii ranking
strict: total stores >= 20 and at least two counted brands.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path


TARGET_BRANDS = (
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
)

BRAND_ALIASES = {
    "モスバーガーチェーン": "モスバーガー",
    "MOS BURGER": "モスバーガー",
    "珈琲所コメダ珈琲店": "コメダ珈琲",
    "コメダ珈琲店": "コメダ珈琲",
    "セブンイレブン": "セブン-イレブン",
    "KFC": "ケンタッキー",
    "ケンタッキーフライドチキン": "ケンタッキー",
    "Anytime Fitness": "エニタイムフィットネス",
    "ANYTIME FITNESS": "エニタイムフィットネス",
    "ITTO個別指導学院": "Itto個別指導学院",
    "ITTO": "Itto個別指導学院",
    "HARD OFF": "ハードオフ",
    "OFF HOUSE": "オフハウス",
    "BOOKOFF": "ブックオフ",
    "韓丼": "カルビ丼とスン豆腐専門店韓丼",
    "BRAND OFF": "Brand off",
    "ブランドオフ": "Brand off",
    "KIDS DUO": "Kids Duo",
    "キッズデュオ": "Kids Duo",
    "UP GARAGE": "アップガレージ",
    "Curves": "カーブス",
    "CURVES": "カーブス",
}


def canonical_brand(name: str) -> str:
    raw = (name or "").strip()
    return BRAND_ALIASES.get(raw, raw)


@dataclass
class OperatorAggregate:
    operator_id: int
    name: str
    corp: str = ""
    hq_prefecture: str = ""
    head_office: str = ""
    representative: str = ""
    url: str = ""
    sources: set[str] = field(default_factory=set)
    brands_all: dict[str, int] = field(default_factory=dict)
    brands_counted: dict[str, int] = field(default_factory=dict)

    def add_link(self, *, brand: str, count: int, source: str, operator_type: str) -> None:
        b = canonical_brand(brand)
        self.sources.add(source)
        self.brands_all[b] = max(self.brands_all.get(b, 0), count)
        if operator_type not in {"franchisor", "direct"}:
            self.brands_counted[b] = max(self.brands_counted.get(b, 0), count)

    def row(self, *, counted: bool = False) -> dict[str, str | int]:
        if counted:
            brands = {b: c for b, c in self.brands_counted.items() if c > 0}
        else:
            brands = self.brands_all
        names = sorted(brands)
        target = [b for b in names if b in TARGET_BRANDS]
        counted_positive = {b: c for b, c in self.brands_counted.items() if c > 0}
        return {
            "operator_name": self.name,
            "corp": self.corp,
            "hq_prefecture": self.hq_prefecture,
            "head_office": self.head_office,
            "representative": self.representative,
            "url": self.url,
            "source": ",".join(sorted(s for s in self.sources if s)),
            "brand_count": len(names),
            "brands": ",".join(names),
            "target_brand_count": len(target),
            "target_brands": ",".join(target),
            "total_stores": sum(c for c in brands.values() if c > 0),
            "counted_brand_count": len(counted_positive),
            "evidence_brand_count": sum(1 for c in self.brands_all.values() if c <= 0),
        }

    def strict_phase_row(self) -> dict[str, str | int]:
        """Strict counted stores with broad target-brand evidence retained."""
        counted = self.row(counted=True)
        broad = self.row(counted=False)
        counted["target_brand_count"] = broad["target_brand_count"]
        counted["target_brands"] = broad["target_brands"]
        return counted


def _write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def load_operators(orm_db: str | Path) -> dict[int, OperatorAggregate]:
    conn = sqlite3.connect(orm_db)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
              o.id AS operator_id,
              o.name AS operator_name,
              COALESCE(o.corporate_number, '') AS corp,
              COALESCE(o.prefecture, '') AS hq_prefecture,
              COALESCE(o.head_office, '') AS head_office,
              COALESCE(o.representative_name, '') AS representative,
              COALESCE(o.website_url, '') AS url,
              b.name AS brand,
              COALESCE(l.estimated_store_count, 0) AS cnt,
              COALESCE(l.source, '') AS source,
              COALESCE(l.operator_type, '') AS operator_type
            FROM operator_company o
            JOIN brand_operator_link l ON l.operator_id = o.id
            JOIN franchise_brand b ON b.id = l.brand_id
            """
        ).fetchall()
    finally:
        conn.close()

    out: dict[int, OperatorAggregate] = {}
    for row in rows:
        op_id = int(row["operator_id"])
        op = out.setdefault(
            op_id,
            OperatorAggregate(
                operator_id=op_id,
                name=row["operator_name"],
                corp=row["corp"],
                hq_prefecture=row["hq_prefecture"],
                head_office=row["head_office"],
                representative=row["representative"],
                url=row["url"],
            ),
        )
        op.add_link(
            brand=row["brand"],
            count=int(row["cnt"] or 0),
            source=row["source"],
            operator_type=row["operator_type"],
        )
    return out


def export_clean_megajii(
    *,
    orm_db: str | Path = "var/pizza-registry.sqlite",
    fixture_root: str | Path = "test/fixtures/megafranchisee",
    phase_dir: str | Path = "var/phase28/nationwide-coverage",
) -> dict[str, int]:
    ops = load_operators(orm_db)
    fixture_root = Path(fixture_root)
    phase_dir = Path(phase_dir)
    by_view = fixture_root / "by-view"
    by_brand = by_view / "by-brand"

    all_rows = [op.row(counted=False) for op in ops.values()]
    all_rows.sort(key=lambda r: (-int(r["total_stores"]), -int(r["brand_count"]), str(r["operator_name"])))

    fc_fields = [
        "operator_name",
        "corp",
        "hq_prefecture",
        "head_office",
        "representative",
        "url",
        "source",
        "brand_count",
        "brands",
        "total_stores",
    ]
    _write_csv(fixture_root / "fc-operators-all.csv", all_rows, fc_fields)

    strict = []
    for op in ops.values():
        row = op.strict_phase_row()
        if int(row["total_stores"]) >= 20 and int(row["counted_brand_count"]) >= 2:
            strict.append(row)
    strict.sort(key=lambda r: (-int(r["total_stores"]), -int(r["counted_brand_count"]), str(r["operator_name"])))
    rank_rows = [
        {
            "operator": r["operator_name"],
            "corp": r["corp"],
            "hq": r["hq_prefecture"],
            "head_office": r["head_office"],
            "representative": r["representative"],
            "url": r["url"],
            "source": r["source"],
            "brand_count": r["counted_brand_count"],
            "brands": r["brands"],
            "total_stores_declared": r["total_stores"],
        }
        for r in strict
    ]
    _write_csv(
        by_view / "megajii-ranking.csv",
        rank_rows,
        [
            "operator",
            "corp",
            "hq",
            "head_office",
            "representative",
            "url",
            "source",
            "brand_count",
            "brands",
            "total_stores_declared",
        ],
    )

    phase_fields = [
        "operator_name",
        "corp",
        "hq_prefecture",
        "head_office",
        "representative",
        "url",
        "source",
        "brand_count",
        "brands",
        "target_brand_count",
        "target_brands",
        "total_stores",
    ]
    phase_sets = {
        "operators-allbrand-registry-min20.csv": [
            r for r in all_rows if int(r["total_stores"]) >= 20
        ],
        "megajii-allbrand-registry-min20-2brand.csv": strict,
        "operators-14brand-registry-min20.csv": [
            r for r in all_rows if int(r["total_stores"]) >= 20 and int(r["target_brand_count"]) >= 1
        ],
        "megajii-14brand-registry-min20-2brand.csv": [
            r for r in strict if int(r["target_brand_count"]) >= 1
        ],
    }
    for name, rows in phase_sets.items():
        _write_csv(phase_dir / name, rows, phase_fields)

    review_rows: list[dict] = []
    for op in ops.values():
        broad = op.row(counted=False)
        counted = op.row(counted=True)
        if (
            int(broad["total_stores"]) >= 20
            and int(broad["brand_count"]) >= 2
            and int(counted["counted_brand_count"]) < 2
        ):
            evidence_only = sorted(
                b
                for b, c in op.brands_all.items()
                if b not in op.brands_counted or op.brands_counted.get(b, 0) <= 0
            )
            review_rows.append(
                {
                    **{k: broad[k] for k in phase_fields},
                    "counted_brand_count": counted["counted_brand_count"],
                    "evidence_only_brands": ",".join(evidence_only),
                }
            )
    review_rows.sort(key=lambda r: (-int(r["total_stores"]), str(r["operator_name"])))
    review_fields = phase_fields + ["counted_brand_count", "evidence_only_brands"]
    _write_csv(phase_dir / "megajii-evidence-only-brand2-review.csv", review_rows, review_fields)
    _write_csv(phase_dir / "megajii-excluded-brand2-review.csv", review_rows, review_fields)

    links_path = fixture_root / "fc-links.csv"
    links = list(csv.DictReader(links_path.open(encoding="utf-8"))) if links_path.exists() else []
    if links:
        for brand in TARGET_BRANDS:
            rows = [r for r in links if canonical_brand(r.get("brand_name", "")) == brand]
            rows.sort(
                key=lambda r: (
                    -int(r.get("estimated_store_count") or 0),
                    r.get("operator_name", ""),
                    r.get("source", ""),
                )
            )
            _write_csv(by_brand / f"{brand}.csv", rows, list(links[0].keys()))

    return {
        "fc_operators_all": len(all_rows),
        "megajii_ranking": len(strict),
        "operators_allbrand_min20": len(phase_sets["operators-allbrand-registry-min20.csv"]),
        "megajii_allbrand_min20_2brand": len(phase_sets["megajii-allbrand-registry-min20-2brand.csv"]),
        "operators_14brand_min20": len(phase_sets["operators-14brand-registry-min20.csv"]),
        "megajii_14brand_min20_2brand": len(phase_sets["megajii-14brand-registry-min20-2brand.csv"]),
        "evidence_only_review": len(review_rows),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="clean megafranchisee CSV exports")
    ap.add_argument("--orm-db", default="var/pizza-registry.sqlite")
    ap.add_argument("--fixture-root", default="test/fixtures/megafranchisee")
    ap.add_argument("--phase-dir", default="var/phase28/nationwide-coverage")
    args = ap.parse_args()
    stats = export_clean_megajii(
        orm_db=args.orm_db,
        fixture_root=args.fixture_root,
        phase_dir=args.phase_dir,
    )
    for key, value in stats.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
