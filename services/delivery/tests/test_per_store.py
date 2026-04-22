"""Unit tests for PerStoreExtractor — Phase 5 per-store deterministic extraction."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from pizza_delivery.evidence import Evidence, EvidenceCollector
from pizza_delivery.per_store import (
    PerStoreExtractor,
    StoreExtractionResult,
    _domain_root,
    _find_company_names_in_all,
    _find_explicit_franchisee_operator,
    _has_direct_evidence,
)


# ─── Mock collector ────────────────────────────────────────────────────


@dataclass
class MockCollector:
    evidences: list[Evidence]

    async def collect(self, *, brand, official_url, extra_urls=None):
        return self.evidences


# ─── Helper tests ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "url, expected",
    [
        ("https://www.anytimefitness.co.jp/shinjuku6/", "https://www.anytimefitness.co.jp/"),
        ("https://store.starbucks.co.jp/", "https://store.starbucks.co.jp/"),
        ("http://foo.example/a/b/c", "http://foo.example/"),
        ("", ""),
        ("not-a-url", ""),
    ],
)
def test_domain_root(url, expected):
    assert _domain_root(url) == expected


def test_find_explicit_franchisee_operator_finds_company() -> None:
    evs = [
        Evidence(
            source_url="https://x/",
            snippet="当店は、株式会社AFJ Project が運営する加盟店です。",
            reason="operator_keyword",
            keyword="運営会社",
        ),
    ]
    result = _find_explicit_franchisee_operator(evs)
    assert result is not None
    operator, snippet = result
    assert "AFJ Project" in operator
    assert "当店" in snippet


def test_find_explicit_franchisee_operator_with_explicit_header() -> None:
    evs = [
        Evidence(
            source_url="https://x/",
            snippet="運営会社: 株式会社テストフード が当店を運営しています。",
            reason="operator_keyword",
            keyword="運営会社",
        ),
    ]
    result = _find_explicit_franchisee_operator(evs)
    assert result is not None
    operator, _ = result
    assert "テストフード" in operator


def test_find_explicit_franchisee_operator_none_when_absent() -> None:
    evs = [
        Evidence(
            source_url="https://x/",
            snippet="株式会社ランダム",
            reason="operator_keyword",
            keyword="株式会社",
        ),
    ]
    assert _find_explicit_franchisee_operator(evs) is None


def test_has_direct_evidence() -> None:
    evs = [
        Evidence(
            source_url="https://x/",
            snippet="当店は全店直営で運営されています",
            reason="direct_keyword",
            keyword="全店直営",
        ),
    ]
    assert _has_direct_evidence(evs)

    evs2 = [
        Evidence(
            source_url="https://x/",
            snippet="本部直営の当社",
            reason="operator_keyword",
            keyword="株式会社",
        ),
    ]
    assert _has_direct_evidence(evs2)

    evs3 = [
        Evidence(
            source_url="https://x/",
            snippet="加盟店のみ",
            reason="operator_keyword",
            keyword="加盟店",
        ),
    ]
    assert not _has_direct_evidence(evs3)


def test_find_company_names_in_all_frequency_order() -> None:
    evs = [
        Evidence(source_url="u1", snippet="株式会社A が", reason="r", keyword="k"),
        Evidence(source_url="u2", snippet="株式会社A の子会社", reason="r", keyword="k"),
        Evidence(source_url="u3", snippet="株式会社B が", reason="r", keyword="k"),
    ]
    names = _find_company_names_in_all(evs)
    assert names[0] == "株式会社A"  # 2 回登場
    assert "株式会社B" in names


# ─── Extractor tests ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_extract_returns_unknown_when_no_url() -> None:
    extractor = PerStoreExtractor(collector=MockCollector(evidences=[]))
    result = await extractor.extract(
        place_id="p1",
        brand="B",
        name="N",
        official_url="",
    )
    assert result.operator_name == ""
    assert result.operator_type == "unknown"
    assert "official_url が空" in result.reasoning


@pytest.mark.asyncio
async def test_extract_identifies_explicit_franchisee() -> None:
    evs = [
        Evidence(
            source_url="https://store.example/",
            snippet="当店は、株式会社AFJ Project が運営する加盟店です。",
            reason="operator_keyword",
            keyword="加盟店",
        ),
    ]
    extractor = PerStoreExtractor(
        collector=MockCollector(evidences=evs),
        follow_domain_root=False,
    )
    result = await extractor.extract(
        place_id="p1",
        brand="エニタイム",
        name="新宿6丁目店",
        official_url="https://store.example/shinjuku6/",
    )
    assert result.operator_type == "franchisee"
    assert "AFJ Project" in result.operator_name
    assert result.confidence >= 0.8
    assert len(result.evidences) >= 1


@pytest.mark.asyncio
async def test_extract_identifies_direct_with_operator_name() -> None:
    evs = [
        Evidence(
            source_url="https://store.example/",
            snippet="スターバックス コーヒー ジャパン株式会社の全店直営店舗の 1 つです。",
            reason="direct_keyword",
            keyword="全店直営",
        ),
    ]
    extractor = PerStoreExtractor(
        collector=MockCollector(evidences=evs),
        follow_domain_root=False,
    )
    result = await extractor.extract(
        place_id="p1",
        brand="スターバックス",
        name="新宿店",
        official_url="https://store.example/shinjuku/",
    )
    assert result.operator_type == "direct"
    assert "スターバックス" in result.operator_name
    assert result.confidence >= 0.8


@pytest.mark.asyncio
async def test_extract_unknown_when_only_company_name_no_fc_hint() -> None:
    evs = [
        Evidence(
            source_url="https://store.example/",
            snippet="©2024 株式会社テストフード All Rights Reserved.",
            reason="metadata",
            keyword="description",
        ),
    ]
    extractor = PerStoreExtractor(
        collector=MockCollector(evidences=evs),
        follow_domain_root=False,
    )
    result = await extractor.extract(
        place_id="p1",
        brand="B",
        name="N",
        official_url="https://store.example/s/",
    )
    # direct/FC 不明だが、会社名は抽出できる
    assert result.operator_name == "株式会社テストフード"
    assert result.operator_type == "unknown"
    assert result.confidence < 0.7


@pytest.mark.asyncio
async def test_extract_fully_unknown_when_no_evidence_match() -> None:
    evs = [
        Evidence(
            source_url="https://store.example/",
            snippet="お知らせ: キャンペーン開催中です。",
            reason="metadata",
            keyword="description",
        ),
    ]
    extractor = PerStoreExtractor(
        collector=MockCollector(evidences=evs),
        follow_domain_root=False,
    )
    result = await extractor.extract(
        place_id="p1",
        brand="B",
        name="N",
        official_url="https://store.example/s/",
    )
    assert result.operator_name == ""
    assert result.operator_type == "unknown"
    assert result.confidence < 0.5
