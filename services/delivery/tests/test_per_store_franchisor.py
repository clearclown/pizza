"""Phase 7 Step 2 — 本部 (franchisor) vs 加盟店 (franchisee) の区別。

エニタイムのように店舗ページが「株式会社 Fast Fitness Japan は
マスターフランチャイジーです。...フランチャイジーが運営します」と書いている場合、
抽出された株式会社名は**本部**であり、その店舗を実際に運営する**個別の
加盟店会社は別途存在する**が公開情報には無い。

この状況を `operator_type="franchisor"` で正しく分類することで、
mega_franchisees view (franchisee 集計) で本部が誤って mega 扱いされる
のを防ぐ。
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from pizza_delivery.evidence import Evidence
from pizza_delivery.per_store import PerStoreExtractor


@dataclass
class MockCollector:
    evidences: list[Evidence]

    async def collect(self, *, brand, official_url, extra_urls=None):
        return list(self.evidences)


def _ev(snippet: str) -> Evidence:
    return Evidence(
        source_url="https://x/",
        snippet=snippet,
        reason="operator_keyword",
        keyword="運営会社",
    )


@pytest.mark.asyncio
async def test_detects_master_franchisor_marks_operator_type() -> None:
    """"マスターフランチャイジー" 表記がある → operator_type=franchisor"""
    evs = [
        _ev("株式会社Fast Fitness Japanは、日本のマスターフランチャイジーです。"),
        _ev("当店はフランチャイジーが運営します。"),
    ]
    ex = PerStoreExtractor(collector=MockCollector(evs))
    res = await ex.extract(
        place_id="p1", brand="エニタイム", name="新宿店",
        official_url="https://www.anytimefitness.co.jp/shinjuku/",
    )
    assert "Fast Fitness Japan" in res.operator_name
    # 本部 = franchisor として標識
    assert res.operator_type == "franchisor"


@pytest.mark.asyncio
async def test_franchisor_has_moderate_confidence() -> None:
    """本部までしか分からない場合、confidence は direct/franchisee より低い。"""
    evs = [
        _ev("株式会社Test Corpは、マスターフランチャイジーとして事業展開中"),
    ]
    ex = PerStoreExtractor(collector=MockCollector(evs))
    res = await ex.extract(
        place_id="p1", brand="T", name="N", official_url="https://x/",
    )
    assert res.operator_type == "franchisor"
    # direct (0.85) や franchisee (0.9) より低く、unknown (0.5) より高いレンジ
    assert 0.55 <= res.confidence <= 0.75


@pytest.mark.asyncio
async def test_franchisor_does_not_overwrite_explicit_franchisee() -> None:
    """evidence に両方あるなら、具体的な franchisee 記載が優先される。"""
    evs = [
        _ev("株式会社Fast Fitness Japanは、マスターフランチャイジーです。"),
        _ev("運営会社: 株式会社新宿エニタイム運営 が当店を運営"),
    ]
    ex = PerStoreExtractor(collector=MockCollector(evs))
    res = await ex.extract(
        place_id="p1", brand="エニタイム", name="新宿店",
        official_url="https://x/",
    )
    # 具体的な運営会社が取れるなら franchisee としてそちらを採用
    assert "新宿エニタイム運営" in res.operator_name
    assert res.operator_type == "franchisee"
