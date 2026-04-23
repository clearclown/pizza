"""Unit tests for CrossVerifier — Phase 5 Step D."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from pizza_delivery.cross_verifier import (
    CrossVerifier,
    VerifyCandidate,
    verify_many,
)
from pizza_delivery.evidence import Evidence
from pizza_delivery.per_store import StoreExtractionResult


# ─── Mock extractor ────────────────────────────────────────────────────


@dataclass
class StubExtractor:
    """URL → 固定 result を返す mock。"""

    results_by_url: dict[str, StoreExtractionResult]

    async def extract(self, *, place_id, brand, name, official_url, extra_urls=None):
        if official_url in self.results_by_url:
            return self.results_by_url[official_url]
        # デフォルト: 何も見つからない
        return StoreExtractionResult(place_id=place_id, brand=brand, name=name)


def _mk_result(operator: str, op_type="franchisee", conf=0.9):
    return StoreExtractionResult(
        place_id="p1",
        brand="B",
        name="N",
        operator_name=operator,
        operator_type=op_type,
        confidence=conf,
        evidences=[
            Evidence(
                source_url="https://x/",
                snippet=f"運営: {operator}",
                reason="operator_keyword",
                keyword="運営",
            )
        ],
    )


# ─── Test cases ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_verify_confirms_primary_match() -> None:
    extractor = StubExtractor(
        results_by_url={
            "https://x/store": _mk_result("株式会社AFJ Project"),
        }
    )
    verifier = CrossVerifier(extractor=extractor)
    result = await verifier.verify(
        VerifyCandidate(
            place_id="p1",
            brand="エニタイム",
            name="新宿店",
            expected_operator="株式会社AFJ Project",
            primary_url="https://x/store",
        )
    )
    assert result.confirmed is True
    assert result.reason == "primary_match"
    assert result.confidence_boost == pytest.approx(0.1)
    assert len(result.evidences) >= 1


@pytest.mark.asyncio
async def test_verify_matches_after_normalization() -> None:
    """(株)AFJ と 株式会社AFJ は同じ法人として扱う。"""
    extractor = StubExtractor(
        results_by_url={
            "https://x/": _mk_result("(株)AFJ Project"),
        }
    )
    verifier = CrossVerifier(extractor=extractor)
    result = await verifier.verify(
        VerifyCandidate(
            place_id="p1",
            brand="B",
            name="N",
            expected_operator="株式会社AFJ Project",
            primary_url="https://x/",
        )
    )
    assert result.confirmed is True


@pytest.mark.asyncio
async def test_verify_falls_back_to_alt_url() -> None:
    extractor = StubExtractor(
        results_by_url={
            "https://primary/": _mk_result(""),  # primary で取れない
            "https://alt/": _mk_result("株式会社AFJ Project"),  # alt で発見
        }
    )
    verifier = CrossVerifier(extractor=extractor)
    result = await verifier.verify(
        VerifyCandidate(
            place_id="p1",
            brand="B",
            name="N",
            expected_operator="株式会社AFJ Project",
            primary_url="https://primary/",
            alt_urls=["https://alt/"],
        )
    )
    assert result.confirmed is True
    assert result.reason == "alt_match"
    assert result.confidence_boost == pytest.approx(0.05)


@pytest.mark.asyncio
async def test_verify_mismatch() -> None:
    extractor = StubExtractor(
        results_by_url={
            "https://x/": _mk_result("株式会社別会社"),
        }
    )
    verifier = CrossVerifier(extractor=extractor)
    result = await verifier.verify(
        VerifyCandidate(
            place_id="p1",
            brand="B",
            name="N",
            expected_operator="株式会社AFJ Project",
            primary_url="https://x/",
        )
    )
    assert result.confirmed is False
    assert "mismatch" in result.reason
    assert "株式会社別会社" in result.observed_operator


@pytest.mark.asyncio
async def test_verify_empty_expected_operator() -> None:
    extractor = StubExtractor(results_by_url={})
    verifier = CrossVerifier(extractor=extractor)
    result = await verifier.verify(
        VerifyCandidate(
            place_id="p1",
            brand="B",
            name="N",
            expected_operator="",
            primary_url="https://x/",
        )
    )
    assert result.confirmed is False
    assert "expected_operator is empty" in result.reason


@pytest.mark.asyncio
async def test_verify_no_evidence_anywhere() -> None:
    extractor = StubExtractor(
        results_by_url={
            "https://primary/": _mk_result(""),
            "https://alt1/": _mk_result(""),
            "https://alt2/": _mk_result(""),
        }
    )
    verifier = CrossVerifier(extractor=extractor)
    result = await verifier.verify(
        VerifyCandidate(
            place_id="p1",
            brand="B",
            name="N",
            expected_operator="株式会社AFJ",
            primary_url="https://primary/",
            alt_urls=["https://alt1/", "https://alt2/"],
        )
    )
    assert result.confirmed is False
    assert result.reason == "no_evidence"


@pytest.mark.asyncio
async def test_verify_many_runs_all() -> None:
    extractor = StubExtractor(
        results_by_url={
            "https://x1/": _mk_result("株式会社A"),
            "https://x2/": _mk_result("株式会社A"),
            "https://x3/": _mk_result("株式会社B"),  # mismatch
        }
    )
    verifier = CrossVerifier(extractor=extractor)
    cands = [
        VerifyCandidate(place_id="p1", brand="B", name="N1",
                        expected_operator="株式会社A", primary_url="https://x1/"),
        VerifyCandidate(place_id="p2", brand="B", name="N2",
                        expected_operator="株式会社A", primary_url="https://x2/"),
        VerifyCandidate(place_id="p3", brand="B", name="N3",
                        expected_operator="株式会社A", primary_url="https://x3/"),
    ]
    results = await verify_many(verifier, cands)
    assert len(results) == 3
    assert results[0].confirmed is True
    assert results[1].confirmed is True
    assert results[2].confirmed is False
