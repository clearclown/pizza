"""integrate: 3 ソース統合の動作検証。

ネットワーク・大容量 CSV は使わず、in-memory DB に小規模データを seed して
integrate_all / export_unified_csv の振る舞いを固定化する。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from pizza_delivery.integrate import (
    _extract_japanese_prefix,
    export_unified_csv,
    hydrate_corporate_numbers,
    import_pipeline_operators,
)
from pizza_delivery.orm import (
    BrandOperatorLink,
    FranchiseBrand,
    OperatorCompany,
    create_all,
    make_session,
    upsert_brand,
    upsert_operator,
)


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    create_all(engine)
    sess = make_session(engine)
    yield sess
    sess.close()


# ─── _extract_japanese_prefix ────────────────────────────────


def test_extract_japanese_prefix_strips_english_suffix() -> None:
    """『株式会社モスフードサービス MOS FOOD SERVICES INC.』→ 日本語部のみ。"""
    assert (
        _extract_japanese_prefix("株式会社モスフードサービス MOS FOOD SERVICES INC.")
        == "株式会社モスフードサービス"
    )


def test_extract_japanese_prefix_no_english() -> None:
    assert _extract_japanese_prefix("株式会社アレフ") == "株式会社アレフ"


def test_extract_japanese_prefix_mixed_alpha_in_name() -> None:
    """日本語の途中に alpha が出てきても、最初の ASCII-only token 以降を落とす。"""
    # 「JBNインターナショナル（株）」みたいな混在 token は 1 token として保持
    assert _extract_japanese_prefix("JBNインターナショナル株式会社 BNI Inc.") == "JBNインターナショナル株式会社"


def test_extract_japanese_prefix_empty() -> None:
    assert _extract_japanese_prefix("") == ""


# ─── import_pipeline_operators ──────────────────────────────


def _seed_pipeline_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE operator_stores (
          operator_name TEXT, place_id TEXT, brand TEXT,
          operator_type TEXT, confidence REAL,
          discovered_via TEXT, corporate_number TEXT,
          PRIMARY KEY (operator_name, place_id)
        );
        """
    )
    conn.executemany(
        "INSERT INTO operator_stores VALUES (?,?,?,?,?,?,?)",
        [
            ("株式会社アズ", "p1", "エニタイムフィットネス", "franchisee", 1.0, "registry", ""),
            ("株式会社アズ", "p2", "エニタイムフィットネス", "franchisee", 1.0, "registry", ""),
            ("株式会社アズ", "p3", "エニタイムフィットネス", "franchisee", 1.0, "registry", ""),
            ("株式会社Fusion'z", "p4", "ローソン", "franchisee", 1.0, "registry", ""),
        ],
    )
    conn.commit()
    conn.close()


def test_import_pipeline_operators(tmp_path: Path, session) -> None:
    pipeline_db = tmp_path / "pipe.sqlite"
    _seed_pipeline_db(pipeline_db)
    n = import_pipeline_operators(session, pipeline_db, min_stores=1)
    assert n == 2  # (株式会社アズ × エニタイム), (株式会社Fusion'z × ローソン)
    brands = {b.name for b in session.query(FranchiseBrand).all()}
    ops = {o.name for o in session.query(OperatorCompany).all()}
    assert "エニタイムフィットネス" in brands
    assert "ローソン" in brands
    assert "株式会社アズ" in ops
    # estimated_store_count の正確性
    az_link = (
        session.query(BrandOperatorLink)
        .join(OperatorCompany)
        .filter(OperatorCompany.name == "株式会社アズ")
        .one()
    )
    assert az_link.estimated_store_count == 3


def test_import_pipeline_respects_min_stores(tmp_path: Path, session) -> None:
    pipeline_db = tmp_path / "pipe.sqlite"
    _seed_pipeline_db(pipeline_db)
    n = import_pipeline_operators(session, pipeline_db, min_stores=3)
    # アズ 3 店 ≥ 3 (採用), Fusion'z 1 店 < 3 (除外)
    assert n == 1
    ops = {o.name for o in session.query(OperatorCompany).all()}
    assert "株式会社アズ" in ops
    assert "株式会社Fusion'z" not in ops


def test_import_pipeline_missing_db_returns_zero(tmp_path: Path, session) -> None:
    n = import_pipeline_operators(session, tmp_path / "nope.sqlite")
    assert n == 0


# ─── hydrate_corporate_numbers (要 Houjin index) ────────────


def test_hydrate_corporate_numbers_fills_empty(tmp_path: Path, session) -> None:
    # 1) Houjin index を 1 件 seed
    from pizza_delivery.houjin_csv import HoujinCSVIndex

    houjin_db = tmp_path / "houjin.sqlite"
    csv = tmp_path / "h.csv"
    csv.write_text(
        "1,3010701019707,01,0,2023-09,2023-09,"
        "株式会社モスストアカンパニー,カブシキガイシャモスストアカンパニー,"
        "101,東京都,品川区,大崎2-1-1,,13,13109,,,,,,,,,,,,\n",
        encoding="utf-8",
    )
    HoujinCSVIndex(houjin_db).ingest_csv(csv)

    # 2) ORM に法人番号 無しの operator を入れる
    upsert_operator(session, name="株式会社モスストアカンパニー MOS STORE COMPANY")
    session.commit()

    # 3) hydrate
    n = hydrate_corporate_numbers(session, houjin_db_path=houjin_db)
    assert n == 1
    op = session.query(OperatorCompany).one()
    assert op.corporate_number == "3010701019707"
    assert "品川区" in op.head_office


def test_hydrate_skips_already_hydrated(tmp_path: Path, session) -> None:
    from pizza_delivery.houjin_csv import HoujinCSVIndex

    houjin_db = tmp_path / "h.sqlite"
    csv = tmp_path / "h.csv"
    csv.write_text(
        "1,1111111111111,01,0,2023-01,2023-01,株式会社既登録,カ,101,東京都,渋谷区,A,,13,13113,,,,,,,,,,,,\n",
        encoding="utf-8",
    )
    HoujinCSVIndex(houjin_db).ingest_csv(csv)

    # corporate_number が埋まっている operator は hydrate 対象外
    upsert_operator(session, name="株式会社既登録", corporate_number="9999999999999")
    session.commit()
    n = hydrate_corporate_numbers(session, houjin_db_path=houjin_db)
    assert n == 0


# ─── export_unified_csv ──────────────────────────────────────


def test_export_unified_csv(tmp_path: Path, session) -> None:
    # seed
    from pizza_delivery.orm import link_brand_operator

    brand = upsert_brand(session, "モスバーガー", industry="外食")
    op = upsert_operator(
        session, name="株式会社モスフードサービス", corporate_number="4010701022117"
    )
    session.flush()
    link_brand_operator(
        session, brand=brand, operator=op, estimated_store_count=1310,
        operator_type="franchisor", source="jfa",
    )
    session.commit()

    out = tmp_path / "u.csv"
    n = export_unified_csv(out, orm_session=session)
    assert n == 1
    text = out.read_text(encoding="utf-8")
    assert "モスバーガー" in text
    assert "株式会社モスフードサービス" in text
    assert "4010701022117" in text
    assert "1310" in text
    assert "jfa" in text
