from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from pizza_delivery.houjin_csv import HoujinCSVIndex
from pizza_delivery.megafranchisee_review_hydrate import (
    apply_matches,
    find_houjin_matches,
    write_matches,
)


def _write_review(path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "operator_name",
                "primary_corporate_number",
                "operator_total_stores_est",
                "operator_brand_count_est",
                "brands_breakdown",
            ],
        )
        w.writeheader()
        w.writerow(
            {
                "operator_name": "ありがとうサービス",
                "primary_corporate_number": "",
                "operator_total_stores_est": "228",
                "operator_brand_count_est": "2",
                "brands_breakdown": "オフハウス:114;ハードオフ:114",
            }
        )
        w.writerow(
            {
                "operator_name": "エムシーアイ",
                "primary_corporate_number": "",
                "operator_total_stores_est": "164",
                "operator_brand_count_est": "2",
                "brands_breakdown": "TSUTAYA:82;シャトレーゼ:82",
            }
        )


def _setup_orm(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE operator_company (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            corporate_number TEXT NOT NULL DEFAULT '',
            head_office TEXT NOT NULL DEFAULT '',
            prefecture TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE brand_operator_link (
            id INTEGER PRIMARY KEY,
            brand_id INTEGER NOT NULL,
            operator_id INTEGER NOT NULL,
            estimated_store_count INTEGER NOT NULL,
            source TEXT NOT NULL
        );
        """
    )
    conn.executemany(
        """
        INSERT INTO operator_company
          (id, name, corporate_number, head_office, prefecture, source)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                1,
                "ありがとうサービス",
                "",
                "愛媛県今治市八町西3丁目6-30",
                "愛媛県",
                "manual",
            ),
            (
                2,
                "エムシーアイ",
                "",
                "大阪府大阪市中央区安土町3-5-6",
                "大阪府",
                "manual",
            ),
        ],
    )
    conn.execute(
        """
        INSERT INTO brand_operator_link
          (id, brand_id, operator_id, estimated_store_count, source)
        VALUES (1, 10, 1, 114, 'manual')
        """
    )
    conn.commit()
    conn.close()


def test_find_houjin_matches_uses_prefecture_to_disambiguate(tmp_path: Path) -> None:
    review = tmp_path / "review.csv"
    houjin_db = tmp_path / "houjin.sqlite"
    orm_db = tmp_path / "orm.sqlite"
    _write_review(review)
    _setup_orm(orm_db)
    csv_path = tmp_path / "houjin.csv"
    csv_path.write_text(
        "1,3140000000000,01,0,2024,2024,株式会社ありがとうサービス,,101,兵庫県,加古川市,野口町二屋１１５番１号,,,,,,,,,,,,\n"
        "2,2500001012603,01,0,2024,2024,株式会社ありがとうサービス,,101,愛媛県,今治市,八町西３丁目６番３０号,,,,,,,,,,,,\n"
        "3,3011201014818,01,0,2024,2024,株式会社エムシーアイ,,101,東京都,中野区,新井１丁目３５番１５号,,,,,,,,,,,,\n",
        encoding="utf-8",
    )
    HoujinCSVIndex(houjin_db).ingest_csv(csv_path)

    matches = find_houjin_matches(
        review_csv=review,
        houjin_db=houjin_db,
        orm_db=orm_db,
    )

    by_name = {m.operator_name: m for m in matches}
    assert by_name["ありがとうサービス"].status == "accepted_disambiguated"
    assert by_name["ありがとうサービス"].corporate_number == "2500001012603"
    assert by_name["エムシーアイ"].status == "prefecture_mismatch"


def test_apply_matches_updates_orm_and_writes_csv(tmp_path: Path) -> None:
    review = tmp_path / "review.csv"
    houjin_db = tmp_path / "houjin.sqlite"
    orm_db = tmp_path / "orm.sqlite"
    out = tmp_path / "matches.csv"
    _write_review(review)
    _setup_orm(orm_db)
    csv_path = tmp_path / "houjin.csv"
    csv_path.write_text(
        "1,2500001012603,01,0,2024,2024,株式会社ありがとうサービス,,101,愛媛県,今治市,八町西３丁目６番３０号,,,,,,,,,,,,\n",
        encoding="utf-8",
    )
    HoujinCSVIndex(houjin_db).ingest_csv(csv_path)
    matches = find_houjin_matches(
        review_csv=review,
        houjin_db=houjin_db,
        orm_db=orm_db,
    )
    write_matches(out, matches)
    stats = apply_matches(matches, orm_db=orm_db)

    assert out.exists()
    assert stats["applied"] == 1
    conn = sqlite3.connect(orm_db)
    try:
        assert conn.execute(
            """
            SELECT name, corporate_number, prefecture
            FROM operator_company
            WHERE id = 1
            """
        ).fetchone() == ("株式会社ありがとうサービス", "2500001012603", "愛媛県")
    finally:
        conn.close()
