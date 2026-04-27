from __future__ import annotations

import sqlite3
from pathlib import Path

from pizza_delivery.anytime_official import (
    BRAND,
    apply_official_stores,
    list_false_positive_stores,
    list_non_official_stores,
    parse_prefecture_page,
)


SAMPLE_HTML = """
<ul>
<li>
<a href="/akb/">
<div class="info">
<p class="name">秋葉原店</p>
<p class="address">〒101-0021 東京都千代田区外神田2-5-12 タカラビル1F-B1</p>
<p class="access">JR線「秋葉原」駅より徒歩7分</p>
</div>
</a>
</li>
<li>
<a href="/shibuya/">
<div class="info">
<p class="name">渋谷店</p>
<p class="address">〒150-0031<br>東京都渋谷区桜丘町1-4</p>
</div>
</a>
</li>
</ul>
"""


def _setup_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE stores (
            place_id TEXT PRIMARY KEY,
            brand TEXT,
            name TEXT,
            address TEXT,
            lat REAL,
            lng REAL,
            official_url TEXT,
            phone TEXT,
            grid_cell_id TEXT
        );
        CREATE TABLE operator_stores (
            operator_name TEXT NOT NULL,
            place_id TEXT NOT NULL,
            brand TEXT,
            operator_type TEXT,
            confidence REAL DEFAULT 0.0,
            discovered_via TEXT DEFAULT 'per_store',
            corporate_number TEXT,
            verification_source TEXT,
            PRIMARY KEY (operator_name, place_id)
        );
        """
    )
    return conn


def test_parse_prefecture_page_extracts_store_cards() -> None:
    rows = parse_prefecture_page(SAMPLE_HTML, prefecture="東京都")

    assert len(rows) == 2
    assert rows[0].name == "秋葉原店"
    assert rows[0].store_name() == f"{BRAND} 秋葉原店"
    assert rows[0].url == "https://www.anytimefitness.co.jp/akb/"
    assert rows[0].place_id() == "anytime-official:akb"
    assert rows[1].address == "〒150-0031 東京都渋谷区桜丘町1-4"


def test_apply_official_stores_updates_existing_and_purges_false_positive(tmp_path: Path) -> None:
    db = tmp_path / "pizza.sqlite"
    conn = _setup_db(db)
    conn.executemany(
        """
        INSERT INTO stores
          (place_id, brand, name, address, lat, lng, official_url, phone, grid_cell_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, '')
        """,
        [
            (
                "existing-akb",
                BRAND,
                "エニタイムフィットネス 秋葉原店",
                "old",
                35.0,
                139.0,
                "https://www.anytimefitness.co.jp/akb/",
                "",
            ),
            (
                "false-positive",
                BRAND,
                "BLUE FITNESS 24",
                "東京都",
                35.0,
                139.0,
                "https://blue-fitness24.com/",
                "",
            ),
        ],
    )
    conn.execute(
        """
        INSERT INTO operator_stores
          (operator_name, place_id, brand, operator_type, confidence)
        VALUES ('株式会社ノイズ', 'false-positive', ?, 'franchisee', 0.1)
        """,
        (BRAND,),
    )
    conn.commit()
    conn.close()

    stores = parse_prefecture_page(SAMPLE_HTML, prefecture="東京都")
    stats = apply_official_stores(db, stores, purge_false_positives=True)

    assert stats["official_stores"] == 2
    assert stats["updated"] == 1
    assert stats["inserted"] == 1
    assert stats["false_positive_deleted"] == 1

    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT address FROM stores WHERE place_id='existing-akb'"
        ).fetchone()
        assert row == ("〒101-0021 東京都千代田区外神田2-5-12 タカラビル1F-B1",)
        assert conn.execute(
            "SELECT COUNT(*) FROM stores WHERE place_id='anytime-official:shibuya'"
        ).fetchone() == (1,)
        assert conn.execute(
            "SELECT COUNT(*) FROM operator_stores WHERE place_id='false-positive'"
        ).fetchone() == (0,)
        assert list_false_positive_stores(conn) == []
    finally:
        conn.close()


def test_apply_marks_fast_fitness_japan_as_franchisor(tmp_path: Path) -> None:
    db = tmp_path / "pizza.sqlite"
    conn = _setup_db(db)
    conn.execute(
        """
        INSERT INTO stores
          (place_id, brand, name, address, lat, lng, official_url, phone, grid_cell_id)
        VALUES ('existing-akb', ?, 'エニタイムフィットネス 秋葉原店', 'old', 35, 139,
                'https://www.anytimefitness.co.jp/akb/', '', '')
        """,
        (BRAND,),
    )
    conn.execute(
        """
        INSERT INTO operator_stores
          (operator_name, place_id, brand, operator_type, confidence)
        VALUES ('株式会社Fast Fitness Japan', 'existing-akb', ?, 'franchisee', 0.8)
        """,
        (BRAND,),
    )
    conn.commit()
    conn.close()

    stats = apply_official_stores(
        db,
        parse_prefecture_page(SAMPLE_HTML, prefecture="東京都"),
    )

    assert stats["master_operator_rows_marked"] == 1
    conn = sqlite3.connect(db)
    try:
        assert conn.execute(
            """
            SELECT operator_type
            FROM operator_stores
            WHERE operator_name = '株式会社Fast Fitness Japan'
            """
        ).fetchone() == ("franchisor",)
    finally:
        conn.close()


def test_purge_non_official_keeps_official_list_and_remaps_operator_links(tmp_path: Path) -> None:
    db = tmp_path / "pizza.sqlite"
    conn = _setup_db(db)
    conn.executemany(
        """
        INSERT INTO stores
          (place_id, brand, name, address, lat, lng, official_url, phone, grid_cell_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, '')
        """,
        [
            (
                "legacy-shibuya",
                BRAND,
                "エニタイムフィットネス 渋谷店",
                "old",
                35.0,
                139.0,
                "",
                "",
            ),
            (
                "duplicate-akb",
                BRAND,
                "エニタイムフィットネス 秋葉原店",
                "old",
                35.0,
                139.0,
                "https://www.anytimefitness.co.jp/akb/",
                "",
            ),
            (
                "duplicate-akb-2",
                BRAND,
                "エニタイムフィットネス 秋葉原店",
                "old 2",
                35.0,
                139.0,
                "https://www.anytimefitness.co.jp/akb/",
                "",
            ),
        ],
    )
    conn.execute(
        """
        INSERT INTO operator_stores
          (operator_name, place_id, brand, operator_type, confidence)
        VALUES ('株式会社渋谷FC', 'legacy-shibuya', ?, 'franchisee', 0.8)
        """,
        (BRAND,),
    )
    conn.commit()
    conn.close()

    stores = parse_prefecture_page(SAMPLE_HTML, prefecture="東京都")
    stats = apply_official_stores(db, stores, purge_non_official=True)

    assert stats["official_stores"] == 2
    assert stats["non_official_deleted"] == 1
    assert stats["duplicate_official_deleted"] == 1
    assert stats["operator_links_remapped"] == 1

    conn = sqlite3.connect(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM stores").fetchone() == (2,)
        assert conn.execute(
            "SELECT COUNT(*) FROM stores WHERE official_url IN (?, ?)",
            (
                "https://www.anytimefitness.co.jp/akb/",
                "https://www.anytimefitness.co.jp/shibuya/",
            ),
        ).fetchone() == (2,)
        shibuya_pid = conn.execute(
            """
            SELECT place_id
            FROM stores
            WHERE official_url = 'https://www.anytimefitness.co.jp/shibuya/'
            """
        ).fetchone()[0]
        assert conn.execute(
            """
            SELECT operator_name
            FROM operator_stores
            WHERE place_id = ?
            """,
            (shibuya_pid,),
        ).fetchone() == ("株式会社渋谷FC",)
        assert list_non_official_stores(conn, stores) == []
    finally:
        conn.close()
