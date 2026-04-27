from __future__ import annotations

import csv
import sqlite3

from pizza_delivery.megafranchisee_clean_export import (
    OperatorAggregate,
    canonical_brand,
    consolidate_operators,
    export_clean_megajii,
)


def test_canonical_brand_normalizes_common_aliases() -> None:
    assert canonical_brand("ケンタッキーフライドチキン") == "ケンタッキー"
    assert canonical_brand("セブンイレブン") == "セブン-イレブン"
    assert canonical_brand("珈琲所コメダ珈琲店") == "コメダ珈琲"
    assert canonical_brand("韓丼") == "カルビ丼とスン豆腐専門店韓丼"


def test_target_only_row_excludes_outside_brands_and_totals() -> None:
    op = OperatorAggregate(operator_id=1, name="G-7ホールディングス")
    op.add_link(brand="オートバックス", count=700, source="manual", operator_type="franchisee")
    op.add_link(brand="カーブス", count=120, source="manual", operator_type="franchisee")
    op.add_link(brand="業務スーパー", count=180, source="jfa", operator_type="franchisee")

    row = op.strict_phase_row(target_only=True)

    assert row["brand_count"] == 2
    assert row["brands"] == "カーブス,業務スーパー"
    assert row["target_brand_count"] == 2
    assert row["total_stores"] == 300
    assert row["source"] == "jfa,manual"


def test_consolidate_operators_merges_same_name_with_missing_corp() -> None:
    verified = OperatorAggregate(operator_id=1, name="株式会社ハードオフコーポレーション", corp="6110001012853")
    verified.add_link(brand="ハードオフ", count=120, source="manual", operator_type="franchisee")
    unverified = OperatorAggregate(operator_id=2, name="株式会社ハードオフコーポレーション")
    unverified.add_link(brand="オフハウス", count=90, source="pipeline", operator_type="franchisee")

    merged = consolidate_operators({1: verified, 2: unverified})

    assert len(merged) == 1
    row = merged[0].strict_phase_row(target_only=True)
    assert row["corp"] == "6110001012853"
    assert row["brands"] == "オフハウス,ハードオフ"
    assert row["total_stores"] == 210


def test_export_writes_14brand_only_ranking(tmp_path) -> None:
    db = tmp_path / "registry.sqlite"
    fixture_root = tmp_path / "fixtures"
    phase_dir = tmp_path / "phase"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE operator_company (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            corporate_number TEXT,
            prefecture TEXT,
            head_office TEXT,
            representative_name TEXT,
            website_url TEXT
        );
        CREATE TABLE franchise_brand (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE brand_operator_link (
            id INTEGER PRIMARY KEY,
            brand_id INTEGER NOT NULL,
            operator_id INTEGER NOT NULL,
            estimated_store_count INTEGER,
            source TEXT,
            operator_type TEXT
        );
        """
    )
    conn.executemany(
        "INSERT INTO operator_company VALUES (?, ?, '', '', '', '', '')",
        [(1, "対象外だけ"), (2, "対象混在"), (3, "対象1ブランド")],
    )
    conn.executemany(
        "INSERT INTO franchise_brand VALUES (?, ?)",
        [(1, "ローソン"), (2, "カーブス"), (3, "業務スーパー"), (4, "TSUTAYA")],
    )
    conn.executemany(
        "INSERT INTO brand_operator_link VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, 1, 1, 100, "manual", "franchisee"),
            (2, 1, 2, 700, "manual", "franchisee"),
            (3, 2, 2, 120, "manual", "franchisee"),
            (4, 3, 2, 180, "jfa", "franchisee"),
            (5, 4, 3, 80, "pipeline", "franchisee"),
        ],
    )
    conn.commit()
    conn.close()

    stats = export_clean_megajii(orm_db=db, fixture_root=fixture_root, phase_dir=phase_dir)

    assert stats["fc_operators_all"] == 3
    assert stats["fc_operators_14brand_only"] == 2
    assert stats["megajii_ranking"] == 1

    ranking = list(csv.DictReader((fixture_root / "by-view/megajii-ranking.csv").open()))
    assert ranking == [
        {
            "operator": "対象混在",
            "corp": "",
            "hq": "",
            "head_office": "",
            "representative": "",
            "url": "",
            "source": "jfa,manual",
            "brand_count": "2",
            "brands": "カーブス,業務スーパー",
            "total_stores_declared": "300",
        }
    ]

    operators_14 = list(csv.DictReader((fixture_root / "fc-operators-14brand-only.csv").open()))
    assert [row["operator_name"] for row in operators_14] == ["対象混在", "対象1ブランド"]
    assert operators_14[0]["target_brands"] == "カーブス,業務スーパー"
    assert operators_14[0]["total_stores"] == "300"
