from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from pizza_delivery.extended_fc_brand_export import (
    EXTENDED_LINK_FIELDS,
    load_seed_brands,
    export_extended_brands,
)


def _write_seed(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "franchisor_name\tbrand_name",
                "株式会社モスフードサービス\tモスバーガー",
                "株式会社壱番屋\tカレーハウスCoCo壱番屋",
                "株式会社ワークマン\tワークマン／ワークマンプラス",
                "株式会社IKEZOE TRUST\tRE／MAX",
                "株式会社セリア\tSeria（セリア）",
                "株式会社テスト\t未登録ブランド",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _write_links(path: Path) -> None:
    rows = [
        {
            "brand_name": "カレーハウスCoCo壱番屋",
            "industry": "カレー専門店",
            "operator_name": "株式会社フルラッキーコーポレーション",
            "corporate_number": "5290001016532",
            "head_office": "福岡県",
            "prefecture": "福岡県",
            "operator_type": "franchisee",
            "estimated_store_count": "30",
            "source": "manual_megajii_2026_04_24",
            "source_url": "",
            "note": "",
        },
        {
            "brand_name": "カレーハウスCoCo壱番屋",
            "industry": "カレー専門店",
            "operator_name": "株式会社壱番屋",
            "corporate_number": "2010501026037",
            "head_office": "愛知県",
            "prefecture": "愛知県",
            "operator_type": "franchisor",
            "estimated_store_count": "0",
            "source": "jfa",
            "source_url": "https://www.jfa-fc.or.jp/particle/38.html",
            "note": "",
        },
        {
            "brand_name": "カレーハウスCoCo壱番屋",
            "industry": "カレー専門店",
            "operator_name": "株式会社本文混入",
            "corporate_number": "1234567890123",
            "head_office": "",
            "prefecture": "",
            "operator_type": "unknown",
            "estimated_store_count": "3",
            "source": "pipeline",
            "source_url": "",
            "note": "discovered_via=chain_discovery",
        },
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EXTENDED_LINK_FIELDS[:11], lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_orm(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE operator_company (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            corporate_number TEXT NOT NULL,
            head_office TEXT NOT NULL,
            prefecture TEXT NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO operator_company
            (id, name, corporate_number, head_office, prefecture)
        VALUES
            (1, '株式会社ワークマン', '1070001013829', '群馬県伊勢崎市柴町1732番地', '群馬県'),
            (2, '株式会社テスト', '', '', '');
        """
    )
    conn.commit()
    conn.close()


def test_load_seed_brands_skips_finished_target_and_splits_multi_brand(tmp_path: Path) -> None:
    seed = tmp_path / "seed.tsv"
    _write_seed(seed)

    rows = load_seed_brands(seed)

    assert "モスバーガー" not in [r.brand_name for r in rows]
    assert [r.brand_name for r in rows] == [
        "カレーハウスCoCo壱番屋",
        "ワークマン",
        "ワークマンプラス",
        "RE/MAX",
        "Seria",
        "未登録ブランド",
    ]


def test_export_extended_brands_uses_existing_links_and_seed_only_rows(tmp_path: Path) -> None:
    seed = tmp_path / "seed.tsv"
    links = tmp_path / "fc-links.csv"
    orm = tmp_path / "orm.sqlite"
    out = tmp_path / "extended.csv"
    summary = tmp_path / "summary.csv"
    by_brand = tmp_path / "by-brand"
    fc_out = tmp_path / "fc.csv"
    fc_by_brand = tmp_path / "fc-by-brand"
    _write_seed(seed)
    _write_links(links)
    _write_orm(orm)

    stats = export_extended_brands(
        seed_path=seed,
        fc_links_path=links,
        orm_db=orm,
        houjin_db=None,
        out=out,
        summary_out=summary,
        by_brand_dir=by_brand,
        fc_out=fc_out,
        fc_by_brand_dir=fc_by_brand,
    )

    assert stats == {
        "seed_brands": 6,
        "extended_brand_links": 7,
        "extended_fc_operator_links": 1,
        "extended_by_brand_files": 6,
        "extended_fc_by_brand_files": 1,
        "operator_link_brands": 1,
        "franchisor_seed_only_brands": 5,
    }
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert "株式会社本文混入" not in [r["operator_name"] for r in rows]
    assert any(
        r["brand_name"] == "カレーハウスCoCo壱番屋"
        and r["operator_name"] == "株式会社フルラッキーコーポレーション"
        for r in rows
    )
    assert any(
        r["brand_name"] == "ワークマン"
        and r["operator_name"] == "株式会社ワークマン"
        and r["corporate_number"] == "1070001013829"
        and r["match_status"] == "franchisor_seed"
        for r in rows
    )
    assert (by_brand / "カレーハウスCoCo壱番屋.csv").exists()
    assert (by_brand / "未登録ブランド.csv").exists()
    fc_rows = list(csv.DictReader(fc_out.open(encoding="utf-8")))
    assert [r["operator_name"] for r in fc_rows] == ["株式会社フルラッキーコーポレーション"]
    assert (fc_by_brand / "カレーハウスCoCo壱番屋.csv").exists()
    assert not (fc_by_brand / "未登録ブランド.csv").exists()
