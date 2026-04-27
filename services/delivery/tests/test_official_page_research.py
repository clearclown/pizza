"""official_page_research の gate テスト。"""

from __future__ import annotations

import pytest

from pizza_delivery.official_page_research import research_one_store


@pytest.mark.asyncio
async def test_research_one_store_accepts_verified_operator(monkeypatch):
    html = """
    <html><body>
      <h1>テスト店</h1>
      <p>住所 東京都千代田区千代田1-1</p>
      <p>運営会社: 株式会社サンプル</p>
    </body></html>
    """

    class Rec:
        name = "株式会社サンプル"
        corporate_number = "1234567890123"

    async def fake_fetch(url: str, *, timeout: float):
        return html

    monkeypatch.setattr("pizza_delivery.official_page_research._fetch_html", fake_fetch)
    monkeypatch.setattr("pizza_delivery.official_page_research._verify_houjin_exact", lambda name: Rec())

    got = await research_one_store(
        (
            "pid1",
            "モスバーガー",
            "テスト店",
            "東京都千代田区千代田1-1",
            "03-1234-5678",
            "https://example.test/store",
        ),
        franchisor_blocklist=set(),
    )
    assert got.accepted is True
    assert got.operator_name == "株式会社サンプル"
    assert got.corporate_number == "1234567890123"


@pytest.mark.asyncio
async def test_research_one_store_rejects_when_store_key_missing(monkeypatch):
    html = "<html><body><p>運営会社: 株式会社サンプル</p></body></html>"

    class Rec:
        name = "株式会社サンプル"
        corporate_number = "1234567890123"

    async def fake_fetch(url: str, *, timeout: float):
        return html

    monkeypatch.setattr("pizza_delivery.official_page_research._fetch_html", fake_fetch)
    monkeypatch.setattr("pizza_delivery.official_page_research._verify_houjin_exact", lambda name: Rec())

    got = await research_one_store(
        (
            "pid1",
            "モスバーガー",
            "テスト店",
            "東京都千代田区千代田1-1",
            "",
            "https://example.test/store",
        ),
        franchisor_blocklist=set(),
    )
    assert got.accepted is False
    assert got.reject_reason == "store_key_missing_in_page"


@pytest.mark.asyncio
async def test_research_one_store_rejects_bare_corporate_name(monkeypatch):
    html = """
    <html><body>
      <h1>テスト店</h1>
      <p>住所 東京都千代田区千代田1-1</p>
      <p>株式会社サンプル</p>
    </body></html>
    """

    async def fake_fetch(url: str, *, timeout: float):
        return html

    monkeypatch.setattr("pizza_delivery.official_page_research._fetch_html", fake_fetch)

    got = await research_one_store(
        (
            "pid1",
            "アップガレージ",
            "テスト店",
            "東京都千代田区千代田1-1",
            "",
            "https://example.test/store",
        ),
        franchisor_blocklist=set(),
    )
    assert got.accepted is False
    assert got.reject_reason == "operator_label_not_explicit"
