"""ORM Phase 25 列追加 migration のテスト。

既存 schema の DB に ALTER TABLE で列が追加されることを確認。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import sqlalchemy as sa
import pytest

from pizza_delivery.orm import (
    Base,
    FranchiseBrand,
    OperatorCompany,
    _ensure_phase25_columns,
    create_all,
    make_session,
    upsert_brand,
    upsert_operator,
)


def test_schema_has_phase25_columns(tmp_path: Path) -> None:
    """新規 DB 作成時、Phase 25 列が全部ある。"""
    engine = sa.create_engine(f"sqlite:///{tmp_path}/reg.sqlite", future=True)
    create_all(engine)
    insp = sa.inspect(engine)
    oc_cols = {c["name"] for c in insp.get_columns("operator_company")}
    fb_cols = {c["name"] for c in insp.get_columns("franchise_brand")}
    for col in ("representative_name", "representative_title",
                "revenue_current_jpy", "revenue_previous_jpy",
                "revenue_observed_at", "website_url"):
        assert col in oc_cols, f"missing {col}"
    assert "fc_recruitment_url" in fb_cols


def test_migration_on_legacy_db(tmp_path: Path) -> None:
    """古い schema (Phase 25 列なし) の DB に対して migration が走る。"""
    db_path = tmp_path / "legacy.sqlite"
    # 古い schema: minimum 列だけ
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE franchise_brand (
            id INTEGER PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            industry VARCHAR(80) DEFAULT '',
            master_franchisor_name VARCHAR(200) DEFAULT '',
            master_franchisor_corp VARCHAR(13) DEFAULT '',
            jfa_member BOOLEAN DEFAULT 0,
            source VARCHAR(40) DEFAULT 'manual',
            created_at DATETIME,
            updated_at DATETIME
        );
        CREATE TABLE operator_company (
            id INTEGER PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            corporate_number VARCHAR(13) DEFAULT '',
            head_office VARCHAR(200) DEFAULT '',
            prefecture VARCHAR(20) DEFAULT '',
            kind VARCHAR(40) DEFAULT '',
            source VARCHAR(40) DEFAULT 'manual',
            note TEXT DEFAULT '',
            created_at DATETIME,
            updated_at DATETIME
        );
    """)
    conn.commit()
    conn.close()

    engine = sa.create_engine(f"sqlite:///{db_path}", future=True)
    _ensure_phase25_columns(engine)

    insp = sa.inspect(engine)
    oc_cols = {c["name"] for c in insp.get_columns("operator_company")}
    fb_cols = {c["name"] for c in insp.get_columns("franchise_brand")}
    assert "representative_name" in oc_cols
    assert "website_url" in oc_cols
    assert "revenue_current_jpy" in oc_cols
    assert "fc_recruitment_url" in fb_cols


def test_upsert_operator_phase25_fields(tmp_path: Path) -> None:
    """upsert_operator に Phase 25 fields を渡せる。"""
    engine = sa.create_engine(f"sqlite:///{tmp_path}/r.sqlite", future=True)
    sess = make_session(engine)
    try:
        op = upsert_operator(
            sess, name="株式会社テスト",
            representative_name="山田太郎",
            representative_title="代表取締役社長",
            revenue_current_jpy=5_000_000_000,
            revenue_previous_jpy=4_500_000_000,
            revenue_observed_at="2024年3月期",
            website_url="https://example.co.jp/",
        )
        sess.commit()
        assert op.representative_name == "山田太郎"
        assert op.revenue_current_jpy == 5_000_000_000
        assert op.website_url == "https://example.co.jp/"
    finally:
        sess.close()


def test_upsert_brand_fc_recruitment_url(tmp_path: Path) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path}/r.sqlite", future=True)
    sess = make_session(engine)
    try:
        b = upsert_brand(sess, "テストブランド",
                         fc_recruitment_url="https://example.co.jp/fc/")
        sess.commit()
        assert b.fc_recruitment_url == "https://example.co.jp/fc/"
    finally:
        sess.close()
