from __future__ import annotations

import json
import sqlite3

from pizza_delivery.normalize import canonical_key
from pizza_delivery.official_franchisee_sources import (
    EvidenceRow,
    SourceSpec,
    parse_source,
    verify_rows,
)


def _insert_houjin(db_path, *, corp: str, name: str, pref: str, city: str, street: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO houjin_registry
              (corporate_number, process, update_date, name, normalized_name,
               prefecture, city, street)
            VALUES (?, '01', '2026-01-01', ?, ?, ?, ?, ?)
            """,
            (corp, name, canonical_key(name), pref, city, street),
        )
        conn.commit()
    finally:
        conn.close()


def test_parse_brand_off_fc_owner_voice() -> None:
    html = """
    <html><body>
      <p>法人加盟 東北</p>
      <p>加盟者名：株式会社TGM</p>
      <p>代表取締役　田上悠喜様</p>
      <p>ブランド買取専門店 BRAND OFF LIFE仙台六丁の目店、ブランド買取専門店 BRAND OFF LIFE仙台中倉店</p>
    </body></html>
    """
    rows = parse_source(
        html,
        SourceSpec("Brand off", "https://www.brandoff.co.jp/fc/", "brand_off_fc"),
    )

    assert len(rows) == 1
    assert rows[0].operator_name == "株式会社TGM"
    assert rows[0].estimated_store_count == 2
    assert rows[0].context_prefecture == "宮城県"


def test_parse_prtimes_next_data_operator() -> None:
    payload = {
        "props": {
            "pageProps": {
                "pressRelease": {
                    "title": "京都発祥の「韓丼」がついに秋田県初進出",
                    "subtitle": "",
                    "head": "韓丼秋田中央店をオープンいたします。",
                    "text": (
                        "■ 店舗概要<br>"
                        "◇店名 韓丼秋田中央店<br>"
                        "◇所在地 秋田県秋田市高陽幸町8-19<br>"
                        "◇運営会社 ユウクリエイティブ株式会社<br>"
                    ),
                }
            }
        }
    }
    html = f"<script id='__NEXT_DATA__' type='application/json'>{json.dumps(payload)}</script>"
    rows = parse_source(
        html,
        SourceSpec(
            "カルビ丼とスン豆腐専門店韓丼",
            "https://prtimes.jp/main/html/rd/p/000000006.000050301.html",
            "kandon_prtimes",
        ),
    )

    assert len(rows) == 1
    assert rows[0].operator_name == "ユウクリエイティブ株式会社"
    assert rows[0].store_names == ["韓丼秋田中央店"]
    assert rows[0].context_prefecture == "秋田県"
    assert rows[0].context_city == "秋田市"


def test_verify_rows_uses_context_to_disambiguate(tmp_path) -> None:
    db = tmp_path / "houjin.sqlite"
    from pizza_delivery.houjin_csv import HoujinCSVIndex

    HoujinCSVIndex(db)
    _insert_houjin(
        db,
        corp="3370000000001",
        name="株式会社ＴＧＭ",
        pref="宮城県",
        city="仙台市若林区",
        street="中倉３丁目１７番５７号",
    )
    _insert_houjin(
        db,
        corp="1010000000001",
        name="株式会社ＴＧＭ",
        pref="東京都",
        city="千代田区",
        street="大手町１丁目９番７号",
    )
    rows = [
        EvidenceRow(
            brand="Brand off",
            operator_name="株式会社TGM",
            source_url="https://www.brandoff.co.jp/fc/",
            parser="brand_off_fc",
            context_prefecture="宮城県",
            context_city="仙台市",
        )
    ]

    verified = verify_rows(rows, houjin_db=db)

    assert verified[0].corporate_number == "3370000000001"
    assert verified[0].verification_status == "houjin_context_match"
    assert verified[0].prefecture == "宮城県"
