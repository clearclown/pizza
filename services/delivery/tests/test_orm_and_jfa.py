"""ORM (SQLAlchemy) + JFA fetcher のユニットテスト。

ネットワークは httpx.MockTransport で完全に遮断。DB は in-memory sqlite。
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from sqlalchemy import create_engine

from pizza_delivery.jfa_fetcher import JFAFetcher, _parse_member_index
from pizza_delivery.orm import (
    BrandOperatorLink,
    FranchiseBrand,
    OperatorCompany,
    create_all,
    link_brand_operator,
    make_session,
    upsert_brand,
    upsert_operator,
)


# ─── ORM 基礎 ────────────────────────────────────────────


@pytest.fixture
def memory_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    create_all(engine)
    sess = make_session(engine)
    yield sess
    sess.close()


def test_upsert_brand_is_idempotent(memory_session) -> None:
    b1 = upsert_brand(memory_session, "モスバーガー", industry="外食")
    memory_session.commit()
    b2 = upsert_brand(memory_session, "モスバーガー", industry="飲食業")
    memory_session.commit()
    assert b1.id == b2.id
    # 新しい industry が上書きされている
    assert b2.industry == "飲食業"


def test_upsert_operator_prefers_corporate_number(memory_session) -> None:
    o1 = upsert_operator(
        memory_session,
        name="株式会社モスストアカンパニー",
        corporate_number="3010701019707",
    )
    memory_session.commit()

    # 同じ法人番号で別名で upsert → 同じ entity を更新
    o2 = upsert_operator(
        memory_session,
        name="モスストアカンパニー (表記ゆれ)",
        corporate_number="3010701019707",
    )
    memory_session.commit()
    assert o1.id == o2.id
    assert o2.name == "モスストアカンパニー (表記ゆれ)"


def test_link_brand_operator_unique_per_source(memory_session) -> None:
    brand = upsert_brand(memory_session, "モスバーガー")
    op = upsert_operator(memory_session, name="株式会社モスフードサービス")
    memory_session.commit()

    link_brand_operator(memory_session, brand=brand, operator=op, source="jfa")
    link_brand_operator(memory_session, brand=brand, operator=op, source="bc2024")
    link_brand_operator(memory_session, brand=brand, operator=op, source="jfa")
    memory_session.commit()

    links = (
        memory_session.query(BrandOperatorLink)
        .filter_by(brand_id=brand.id, operator_id=op.id)
        .all()
    )
    # (jfa, bc2024) の 2 本だけ
    sources = sorted([link.source for link in links])
    assert sources == ["bc2024", "jfa"]


# ─── JFA parser ──────────────────────────────────────────


# 実 JFA 会員一覧のレイアウトに合わせたサンプル:
#   col[0] = 会社名 (a で URL 付), col[1] = ブランド名 + 業種説明
_SAMPLE_JFA_TABLE = """
<html><body>
<table>
  <tr><th>社名</th><th>ブランド</th></tr>
  <tr>
    <td><a href="https://www.mos.jp/">株式会社モスフードサービス</a></td>
    <td>モスバーガー<br>外食 (ハンバーガー)</td>
  </tr>
  <tr>
    <td><a href="https://www.mcdonalds.co.jp/">株式会社日本マクドナルド</a></td>
    <td>マクドナルド<br>外食</td>
  </tr>
  <tr>
    <td><a href="https://example.com/">株式会社スタディプラス</a></td>
    <td>スタディプラス<br>教育サービス</td>
  </tr>
</table>
</body></html>
"""


def test_parse_member_index_extracts_corporate_names() -> None:
    members = _parse_member_index(_SAMPLE_JFA_TABLE, source_url="jfa://test")
    names = [m.operator_name for m in members]
    assert "株式会社モスフードサービス" in names
    assert "株式会社日本マクドナルド" in names
    assert "株式会社スタディプラス" in names
    # ブランド抽出
    mos = next(m for m in members if "モス" in m.operator_name)
    assert mos.brand_name == "モスバーガー"
    # 業種抽出 (会社名 cell は除外されるので空の可能性あり)
    # URL 抽出
    assert mos.url.startswith("https://www.mos.jp/")


def test_parse_member_index_normalizes_corp_abbrev() -> None:
    """（株）/ (株) / ㈱ は 株式会社 に正規化される。"""
    html = """<table>
      <tr>
        <td><a href="https://x">（株）アステック</a></td>
        <td>プロタイムズ<br>住宅塗装</td>
      </tr>
      <tr>
        <td><a href="https://y">上島珈琲貿易（株）</a></td>
        <td>MUC<br>外食</td>
      </tr>
    </table>"""
    members = _parse_member_index(html)
    names = {m.operator_name for m in members}
    assert "株式会社アステック" in names
    assert "上島珈琲貿易株式会社" in names


def test_parse_member_index_skips_industry_as_brand() -> None:
    """col[1] に業種説明のみ (brand 無し) の行では brand_name が空になる。
    セブン銀行行で ATM の業種説明が brand に混入するバグ防止。
    """
    html = """<table>
      <tr>
        <td>株式会社セブン銀行</td>
        <td>ATMの設置推進及びATMサービスの展開</td>
      </tr>
      <tr>
        <td>株式会社モスフードサービス</td>
        <td>モスバーガー<br>外食</td>
      </tr>
    </table>"""
    members = _parse_member_index(html)
    by_name = {m.operator_name: m for m in members}
    assert by_name["株式会社セブン銀行"].brand_name == ""
    assert "ATM" in by_name["株式会社セブン銀行"].industry
    assert by_name["株式会社モスフードサービス"].brand_name == "モスバーガー"


def test_parse_member_index_strips_english_suffix() -> None:
    """『株式会社モスフードサービス MOS FOOD SERVICES INC.』 → 日本語部のみ。"""
    html = """<table>
      <tr>
        <td>株式会社モスフードサービス MOS FOOD SERVICES INC.</td>
        <td>モスバーガー<br>外食</td>
      </tr>
    </table>"""
    members = _parse_member_index(html)
    assert members[0].operator_name == "株式会社モスフードサービス"


def test_parse_member_index_skips_non_corporate() -> None:
    html = """<table>
      <tr><td>こんにちは</td><td>普通のテキスト</td></tr>
      <tr><td>株式会社OK</td><td>ブランドOK</td></tr>
    </table>"""
    members = _parse_member_index(html)
    assert len(members) == 1
    assert members[0].operator_name == "株式会社OK"


# ─── JFAFetcher sync_to_orm ───────────────────────────────


def test_jfa_fetcher_sync_to_orm(memory_session) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=_SAMPLE_JFA_TABLE)

    fetcher = JFAFetcher(transport=httpx.MockTransport(handler))
    n = asyncio.run(fetcher.sync_to_orm(memory_session))
    assert n == 3
    brands = memory_session.query(FranchiseBrand).all()
    ops = memory_session.query(OperatorCompany).all()
    assert len(brands) == 3
    assert len(ops) == 3
    # jfa 取込 → 全ブランド jfa_member=True
    assert all(b.jfa_member for b in brands)
    # link 確認
    links = memory_session.query(BrandOperatorLink).all()
    assert len(links) == 3
    assert all(link.source == "jfa" for link in links)


def test_jfa_fetcher_network_failure_returns_empty() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="error")

    fetcher = JFAFetcher(transport=httpx.MockTransport(handler))
    members = asyncio.run(fetcher.fetch_members())
    # 失敗時は空を返す (graceful)
    assert members == []
