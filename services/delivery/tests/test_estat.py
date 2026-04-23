"""Phase 17.2: e-Stat API client + recall audit のテスト (mock)。"""

from __future__ import annotations

import httpx
import pytest

from pizza_delivery.estat import (
    EstatClient,
    EstablishmentCount,
    INDUSTRY_CODE_MAP,
    RecallAudit,
    compute_recall_audit,
)


# ─── INDUSTRY_CODE_MAP (日本標準産業分類) ─────────────────────────


def test_industry_code_map_has_major_brands() -> None:
    """主要ブランドに対応する industry 分類コードが定義されている。"""
    assert "gym" in INDUSTRY_CODE_MAP  # 8048 フィットネスクラブ
    assert "convenience_store" in INDUSTRY_CODE_MAP  # 5891 コンビニエンスストア
    assert "fast_food" in INDUSTRY_CODE_MAP
    assert "cafe" in INDUSTRY_CODE_MAP
    assert "drugstore" in INDUSTRY_CODE_MAP


# ─── EstatClient (MockTransport) ─────────────────────────────────────


_SAMPLE_ESTAT_RESPONSE = {
    "GET_STATS_DATA": {
        "STATISTICAL_DATA": {
            "DATA_INF": {
                "VALUE": [
                    {"@area": "13101", "@cat01": "8048", "$": "45"},   # 千代田区 フィットネス 45 件
                    {"@area": "13102", "@cat01": "8048", "$": "38"},   # 中央区
                    {"@area": "13103", "@cat01": "8048", "$": "52"},   # 港区
                    {"@area": "13104", "@cat01": "8048", "$": "29"},   # 新宿区
                    {"@area": "13105", "@cat01": "8048", "$": "22"},   # 文京区
                ]
            }
        }
    }
}


@pytest.mark.asyncio
async def test_estat_client_fetches_establishment_counts() -> None:
    called = {}

    def handler(request: httpx.Request) -> httpx.Response:
        called["url"] = str(request.url)
        import json as json_mod
        return httpx.Response(200, text=json_mod.dumps(_SAMPLE_ESTAT_RESPONSE))

    client = EstatClient(app_id="TEST_APP_ID", transport=httpx.MockTransport(handler))
    counts = await client.fetch_establishment_counts(
        industry_code="8048",  # フィットネスクラブ
        prefecture_code="13",  # 東京都
    )
    assert len(counts) == 5
    assert all(isinstance(c, EstablishmentCount) for c in counts)
    areas = {c.area_code for c in counts}
    assert "13101" in areas  # 千代田区
    counts_by_area = {c.area_code: c for c in counts}
    assert counts_by_area["13101"].count == 45
    # API URL に app_id と industry/prefecture が入る
    assert "TEST_APP_ID" in called["url"]


@pytest.mark.asyncio
async def test_estat_client_requires_app_id() -> None:
    import os
    os.environ.pop("ESTAT_APP_ID", None)
    client = EstatClient()
    with pytest.raises(ValueError, match="ESTAT_APP_ID"):
        await client.fetch_establishment_counts(
            industry_code="8048", prefecture_code="13"
        )


@pytest.mark.asyncio
async def test_estat_client_handles_empty_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        import json as json_mod
        return httpx.Response(200, text=json_mod.dumps({"GET_STATS_DATA": {}}))

    client = EstatClient(app_id="T", transport=httpx.MockTransport(handler))
    counts = await client.fetch_establishment_counts(
        industry_code="8048", prefecture_code="13"
    )
    assert counts == []


# ─── Recall audit ──────────────────────────────────────────────────


def test_compute_recall_audit_calculates_per_area_ratio() -> None:
    places_counts = {
        "13101": 30,  # Places で取得
        "13102": 40,  # Places > e-Stat の場合もあり (Google データ新しい)
        "13103": 20,
    }
    reference_counts = {
        "13101": 45,  # e-Stat
        "13102": 38,
        "13103": 52,
        "13104": 29,  # Places では取っていない area
    }
    audit = compute_recall_audit(places_counts, reference_counts)
    assert isinstance(audit, RecallAudit)
    # 3 area 共通 + 1 area は places なし = 4 entries
    assert len(audit.per_area) == 4
    # recall = places / reference (reference ある area のみ)
    recall_by_area = {a.area_code: a.recall_ratio for a in audit.per_area}
    assert recall_by_area["13101"] == pytest.approx(30 / 45, abs=0.001)
    assert recall_by_area["13102"] == pytest.approx(40 / 38, abs=0.001)
    assert recall_by_area["13103"] == pytest.approx(20 / 52, abs=0.001)
    # Places で取れなかった area は recall=0
    assert recall_by_area["13104"] == 0.0
    # overall
    assert audit.overall_places_total == 90
    assert audit.overall_reference_total == 164
    assert audit.overall_recall == pytest.approx(90 / 164, abs=0.001)


def test_compute_recall_audit_handles_empty_references() -> None:
    audit = compute_recall_audit({"13101": 10}, {})
    assert audit.overall_reference_total == 0
    assert audit.overall_recall is None
