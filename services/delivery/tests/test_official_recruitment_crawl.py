"""公式求人ページ crawler の deterministic gate テスト。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from pizza_delivery.official_recruitment_crawl import (
    RecruitPage,
    _core_store_name,
    _extract_job_links,
    _load_unknown_store_index,
    _match_store,
    _parse_detail_page,
)


def test_parse_detail_page_extracts_operator_store_and_address() -> None:
    html = """
    <html><body>
      <h2>モスバーガー神戸大開店</h2>
      <section>事業内容 「モスバーガー」の運営（募集者：株式会社モスストアカンパニー）</section>
      <table>
        <tr><th>勤務地</th><td>兵庫県神戸市兵庫区水木通7-1-10</td></tr>
      </table>
    </body></html>
    """

    page = _parse_detail_page("モスバーガー", "https://example.test/job", html)

    assert page.store_name == "モスバーガー神戸大開店"
    assert page.operator_name == "株式会社モスストアカンパニー"
    assert "兵庫県神戸市兵庫区" in page.store_address
    assert page.reject_reason == ""


def test_parse_detail_page_accepts_curves_operator_with_ascii_space() -> None:
    html = """
    <html><body>
      <h2>【津市】Curvesイオンタウン津城山</h2>
      <h2>事業内容</h2>
      <p>フィットネス施設「カーブス」の運営(募集者：株式会社Di-one Japan)</p>
      <label>三重県津市久居小野辺町1130-7</label>
    </body></html>
    """

    page = _parse_detail_page("カーブス", "https://example.test/job", html)

    assert page.store_name == "【津市】Curvesイオンタウン津城山"
    assert page.operator_name == "株式会社Di-one Japan"
    assert "三重県津市" in page.store_address


def test_core_store_name_normalizes_curves_city_prefix() -> None:
    assert _core_store_name("【津市】Curvesイオンタウン津城山", "カーブス") == (
        "イオンタウン津城山"
    )


def test_match_store_uses_address_to_disambiguate_same_name_core() -> None:
    page = RecruitPage(
        brand="モスバーガー",
        url="https://example.test/job",
        store_name="モスバーガー神戸大開店",
        store_address="兵庫県神戸市兵庫区水木通7-1-10",
    )
    stores = [
        {
            "place_id": "wrong",
            "name": "モスバーガー神戸大開店",
            "address": "東京都千代田区丸の内1-1",
            "phone": "",
            "core": "神戸大開",
            "keys": [],
        },
        {
            "place_id": "right",
            "name": "モスバーガー神戸大開店",
            "address": "兵庫県神戸市兵庫区水木通7-1-10",
            "phone": "",
            "core": "神戸大開",
            "keys": [],
        },
    ]

    assert _match_store(page, stores) == ("right", "モスバーガー神戸大開店", "")


def test_load_unknown_store_index_allows_replacing_unverified_rows(tmp_path: Path) -> None:
    db = tmp_path / "pipeline.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE stores (
            place_id TEXT PRIMARY KEY,
            name TEXT,
            address TEXT,
            phone TEXT,
            brand TEXT
        );
        CREATE TABLE operator_stores (
            operator_name TEXT,
            place_id TEXT,
            brand TEXT,
            operator_type TEXT,
            corporate_number TEXT
        );
        INSERT INTO stores VALUES
            ('unverified', 'モスバーガー未検証店', '東京都新宿区西新宿1-1', '', 'モスバーガー'),
            ('verified', 'モスバーガー検証済店', '東京都渋谷区宇田川町1-1', '', 'モスバーガー');
        INSERT INTO operator_stores VALUES
            ('株式会社候補', 'unverified', 'モスバーガー', 'franchisee', ''),
            ('株式会社検証済', 'verified', 'モスバーガー', 'franchisee', '1234567890123');
        """
    )
    conn.close()

    rows = _load_unknown_store_index(db, "モスバーガー")

    assert {r["place_id"] for r in rows} == {"unverified"}


def test_extract_job_links_normalizes_relative_urls_and_dedupes() -> None:
    html = """
    <a href="/jobfind-pc/job/All/1">one</a>
    <a href="https://mos-recruit.net/jobfind-pc/job/All/1">dup</a>
    <a href="/jobfind-pc/job/All/2?x=1">two</a>
    <a href="/brand-jobfind/job/All/3">three</a>
    """

    assert _extract_job_links("https://mos-recruit.net/jobfind-pc/area/All", html) == [
        "https://mos-recruit.net/jobfind-pc/job/All/1",
        "https://mos-recruit.net/jobfind-pc/job/All/2?x=1",
        "https://mos-recruit.net/brand-jobfind/job/All/3",
    ]


def test_parse_detail_page_accepts_employer_label() -> None:
    html = """
    <html><body>
      <h2>コメダ珈琲店 藤沢亀井野店</h2>
      <p>ホール・キッチンスタッフ求人</p>
      <p>雇用主：日翔フーズ株式会社</p>
      <p>神奈川県藤沢市亀井野2506-1</p>
    </body></html>
    """

    page = _parse_detail_page("コメダ珈琲", "https://example.test/job", html)

    assert page.store_name == "コメダ珈琲店 藤沢亀井野店"
    assert page.operator_name == "日翔フーズ株式会社"
    assert "神奈川県藤沢市" in page.store_address
