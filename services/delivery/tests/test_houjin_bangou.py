"""国税庁法人番号公表サイト API クライアントのユニットテスト。

httpx.MockTransport でオフライン・決定論的に検証する。
実 API 叩きは test_live_houjin_bangou.py (gated) に分離。
"""

from __future__ import annotations

import os

import httpx
import pytest

from pizza_delivery.houjin_bangou import (
    HoujinBangouClient,
    HoujinRecord,
    HoujinSearchResult,
    verify_operator,
    _parse_xml,
)


SAMPLE_XML_ONE_HIT = """<?xml version="1.0" encoding="UTF-8"?>
<corporations>
  <lastUpdateDate>2026-04-20</lastUpdateDate>
  <count>1</count>
  <divideNumber>1</divideNumber>
  <divideSize>1</divideSize>
  <corporation>
    <sequenceNumber>1</sequenceNumber>
    <corporateNumber>1234567890123</corporateNumber>
    <process>01</process>
    <correct>0</correct>
    <updateDate>2020-01-15</updateDate>
    <changeDate>2020-01-10</changeDate>
    <name>株式会社Fast Fitness Japan</name>
    <nameImageId></nameImageId>
    <kind>301</kind>
    <prefectureName>東京都</prefectureName>
    <cityName>新宿区</cityName>
    <streetNumber>西新宿1-1-1</streetNumber>
    <addressImageId></addressImageId>
    <prefectureCode>13</prefectureCode>
    <cityCode>104</cityCode>
    <postCode>1600023</postCode>
    <addressOutside></addressOutside>
    <addressOutsideImageId></addressOutsideImageId>
    <closeDate></closeDate>
    <closeCause></closeCause>
    <successorCorporateNumber></successorCorporateNumber>
    <changeCause></changeCause>
    <assignmentDate>2015-10-05</assignmentDate>
    <latest>1</latest>
    <enName></enName>
    <enPrefectureName></enPrefectureName>
    <enCityName></enCityName>
    <enAddressOutside></enAddressOutside>
    <furigana>カブシキガイシャファストフィットネスジャパン</furigana>
    <hihyoji>0</hihyoji>
  </corporation>
</corporations>
"""

SAMPLE_XML_NO_HIT = """<?xml version="1.0" encoding="UTF-8"?>
<corporations>
  <lastUpdateDate>2026-04-20</lastUpdateDate>
  <count>0</count>
  <divideNumber>1</divideNumber>
  <divideSize>1</divideSize>
</corporations>
"""


# ─── XML parser ─────────────────────────────────────────────────────────


def test_parse_xml_one_hit() -> None:
    recs = _parse_xml(SAMPLE_XML_ONE_HIT)
    assert len(recs) == 1
    r = recs[0]
    assert r.corporate_number == "1234567890123"
    assert r.name == "株式会社Fast Fitness Japan"
    assert r.address == "東京都新宿区西新宿1-1-1"
    assert r.process == "01"
    assert r.active is True


def test_parse_xml_no_hit() -> None:
    assert _parse_xml(SAMPLE_XML_NO_HIT) == []


def test_record_active_respects_process_code() -> None:
    # 吸収合併による消滅 (process=71) は active=False
    r = HoujinRecord(
        corporate_number="1", name="合併で消滅した会社", address="", process="71", update="",
    )
    assert r.active is False

    # 商号変更 (process=11) は active
    r2 = HoujinRecord(
        corporate_number="2", name="名前を変えた会社", address="", process="11", update="",
    )
    assert r2.active is True


# ─── Client with MockTransport ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_search_by_name_hits_api() -> None:
    called = {}

    def handler(request: httpx.Request) -> httpx.Response:
        called["url"] = str(request.url)
        return httpx.Response(200, text=SAMPLE_XML_ONE_HIT)

    client = HoujinBangouClient(app_id="TEST_ID", transport=httpx.MockTransport(handler))
    result = await client.search_by_name("株式会社Fast Fitness Japan")
    assert isinstance(result, HoujinSearchResult)
    assert result.found is True
    assert len(result.records) == 1
    assert result.records[0].corporate_number == "1234567890123"
    # id / name / type=12 / history=0 が query に入る
    url = called["url"]
    assert "id=TEST_ID" in url
    assert "type=12" in url
    assert "history=0" in url


@pytest.mark.asyncio
async def test_search_by_name_empty_returns_empty_result() -> None:
    client = HoujinBangouClient(app_id="TEST_ID", transport=httpx.MockTransport(lambda r: httpx.Response(200, text=SAMPLE_XML_NO_HIT)))
    result = await client.search_by_name("   ")
    # 空クエリは API を叩かずに空 return
    assert result.found is False


@pytest.mark.asyncio
async def test_search_requires_app_id(monkeypatch) -> None:
    monkeypatch.delenv("HOUJIN_BANGOU_APP_ID", raising=False)
    client = HoujinBangouClient()
    with pytest.raises(ValueError, match="HOUJIN_BANGOU_APP_ID"):
        await client.search_by_name("何か")


# ─── verify_operator ───────────────────────────────────────────────────


def test_verify_operator_exact_match_returns_high_score() -> None:
    result = HoujinSearchResult(
        query="株式会社Fast Fitness Japan",
        records=[
            HoujinRecord(
                corporate_number="1234567890123",
                name="株式会社Fast Fitness Japan",
                address="東京都新宿区西新宿1-1-1",
                process="01",
                update="2020-01-15",
            )
        ],
    )
    v = verify_operator("株式会社Fast Fitness Japan", result)
    assert v["exists"] is True
    assert v["name_similarity"] == pytest.approx(1.0)
    assert v["best_match_number"] == "1234567890123"
    assert v["active"] is True


def test_verify_operator_notation_variant_still_matches() -> None:
    # 入力が "㈱Fast Fitness Japan" でも、normalize で同じ正規形に
    result = HoujinSearchResult(
        query="Fast Fitness Japan",
        records=[
            HoujinRecord(
                corporate_number="1234567890123",
                name="株式会社Fast Fitness Japan",
                address="東京都",
                process="01",
                update="",
            )
        ],
    )
    v = verify_operator("㈱Fast Fitness Japan", result)
    assert v["exists"] is True
    assert v["name_similarity"] >= 0.9


def test_verify_operator_no_hit_returns_false() -> None:
    v = verify_operator("実在しない株式会社", HoujinSearchResult(query="x", records=[]))
    assert v["exists"] is False
    assert v["name_similarity"] == 0.0
    assert v["active"] is False


def test_verify_operator_ignores_inactive_records() -> None:
    # 消滅済み法人しかヒットしなかった場合は exists=False
    result = HoujinSearchResult(
        query="x",
        records=[
            HoujinRecord(
                corporate_number="9",
                name="株式会社解散済み",
                address="",
                process="71",  # 吸収合併による消滅
                update="",
            )
        ],
    )
    v = verify_operator("株式会社解散済み", result)
    assert v["exists"] is False
    assert v["active"] is False
