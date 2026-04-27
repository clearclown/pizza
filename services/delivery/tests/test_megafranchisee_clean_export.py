from __future__ import annotations

import csv
import sqlite3

from pizza_delivery.megafranchisee_clean_export import (
    OperatorAggregate,
    canonical_brand,
    consolidate_operators,
    export_clean_megajii,
    _dedupe_link_rows,
    _normalize_target_link_row,
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


def test_normalize_target_link_row_sets_curves_as_fitness() -> None:
    row = _normalize_target_link_row(
        {"brand_name": "カーブス", "industry": "学習塾・カルチャースクール"}
    )

    assert row["brand_name"] == "カーブス"
    assert row["industry"] == "フィットネス"


def test_normalize_target_link_row_applies_mos_fact_check_overrides() -> None:
    mos_store = _normalize_target_link_row(
        {
            "brand_name": "モスバーガー",
            "industry": "ハンバーガー",
            "operator_name": "株式会社モスストアカンパニー",
            "operator_type": "franchisee",
            "estimated_store_count": "266",
            "source_url": "",
            "note": "discovered_via=official_recruit_jobfind_houjin_verified",
        }
    )
    jrff = _normalize_target_link_row(
        {
            "brand_name": "モスバーガー",
            "industry": "ハンバーガー",
            "operator_name": "株式会社JR九州ファーストフーズ",
            "corporate_number": "",
            "operator_type": "unknown",
            "estimated_store_count": "0",
            "source_url": "https://www.jrff.co.jp/",
            "note": "anchor=モスバーガー",
        }
    )

    assert mos_store["operator_type"] == "direct"
    assert mos_store["estimated_store_count"] == "200"
    assert mos_store["source_url"] == "https://www.mos.co.jp/company/outline/profile/"
    assert "fact_check=mos_group_company" in mos_store["note"]
    assert jrff["operator_type"] == "franchisee"
    assert jrff["corporate_number"] == "6290001013578"
    assert jrff["estimated_store_count"] == "7"
    assert jrff["source_url"] == "https://www.jrff.co.jp/section-mos/"


def test_dedupe_link_rows_drops_unvetted_chain_unknown_and_keeps_stronger_duplicate() -> None:
    rows = [
        {
            "brand_name": "モスバーガー",
            "operator_name": "同一会社",
            "corporate_number": "",
            "head_office": "",
            "operator_type": "franchisee",
            "estimated_store_count": "5",
            "source": "pipeline",
            "source_url": "",
            "note": "discovered_via=registry",
        },
        {
            "brand_name": "モスバーガー",
            "operator_name": "同一会社",
            "corporate_number": "1234567890123",
            "head_office": "東京都",
            "operator_type": "franchisee",
            "estimated_store_count": "6",
            "source": "pipeline",
            "source_url": "",
            "note": "discovered_via=official_recruit_jobfind_houjin_verified",
        },
        {
            "brand_name": "モスバーガー",
            "operator_name": "別ブランド会社",
            "corporate_number": "9999999999999",
            "head_office": "東京都",
            "operator_type": "unknown",
            "estimated_store_count": "1",
            "source": "pipeline",
            "source_url": "",
            "note": "discovered_via=chain_verified",
        },
        {
            "brand_name": "業務スーパー",
            "operator_name": "株式会社ワッツオンラインショップ現在地から探す都道府県から探す北海道岩手県秋",
            "corporate_number": "",
            "head_office": "",
            "operator_type": "unknown",
            "estimated_store_count": "5",
            "source": "pipeline",
            "source_url": "",
            "note": "discovered_via=chain_discovery",
        },
    ]

    out = _dedupe_link_rows(rows)

    assert len(out) == 1
    assert out[0]["operator_name"] == "同一会社"
    assert out[0]["estimated_store_count"] == "6"


def test_dedupe_link_rows_collapses_source_and_corp_duplicates_by_display_name() -> None:
    rows = [
        {
            "brand_name": "モスバーガー",
            "operator_name": "株式会社モスフードサービス",
            "corporate_number": "5010701019713",
            "head_office": "東京都",
            "operator_type": "franchisor",
            "estimated_store_count": "1318",
            "source": "jfa_disclosure",
            "source_url": "https://www.jfa-fc.or.jp/fc-g-misc/pdf/152-1.pdf",
            "note": "",
        },
        {
            "brand_name": "モスバーガー",
            "operator_name": "株式会社モスフードサービス",
            "corporate_number": "5010701019713",
            "head_office": "東京都",
            "operator_type": "franchisor",
            "estimated_store_count": "1266",
            "source": "manual_megajii_2026_04_24",
            "source_url": "",
            "note": "",
        },
        {
            "brand_name": "モスバーガー",
            "operator_name": "株式会社三栄本社",
            "corporate_number": "1111111111111",
            "head_office": "",
            "operator_type": "franchisee",
            "estimated_store_count": "26",
            "source": "manual_megajii_2026_04_24",
            "source_url": "",
            "note": "",
        },
        {
            "brand_name": "モスバーガー",
            "operator_name": "株式会社三栄本社",
            "corporate_number": "2222222222222",
            "head_office": "",
            "operator_type": "franchisee",
            "estimated_store_count": "4",
            "source": "pipeline",
            "source_url": "",
            "note": "",
        },
    ]

    out = _dedupe_link_rows(rows)

    mos_rows = [r for r in out if r["operator_name"] == "株式会社モスフードサービス"]
    assert len(mos_rows) == 1
    assert mos_rows[0]["source"] == "jfa_disclosure"
    assert mos_rows[0]["estimated_store_count"] == "1318"
    sanei_rows = [r for r in out if r["operator_name"] == "株式会社三栄本社"]
    assert len(sanei_rows) == 1
    assert sanei_rows[0]["corporate_number"] == "1111111111111"
    assert sanei_rows[0]["estimated_store_count"] == "26"


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
