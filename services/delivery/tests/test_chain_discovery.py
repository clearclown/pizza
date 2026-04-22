"""Unit tests for ChainDiscovery — Phase 5 Step C."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from pizza_delivery.chain_discovery import (
    ChainDiscovery,
    ChainDiscoveryReport,
    OperatorSummary,
    StoreInput,
    _aggregate,
)
from pizza_delivery.evidence import Evidence
from pizza_delivery.per_store import StoreExtractionResult


# ─── _aggregate unit tests ─────────────────────────────────────────────


def _mk_result(place_id, op, op_type="franchisee", conf=0.9) -> StoreExtractionResult:
    return StoreExtractionResult(
        place_id=place_id,
        brand="B",
        name=f"店舗{place_id}",
        operator_name=op,
        operator_type=op_type,
        confidence=conf,
    )


def test_aggregate_empty() -> None:
    report = _aggregate([], 0)
    assert report.total_stores_checked == 0
    assert report.stores_with_operator == 0
    assert report.operators == []


def test_aggregate_groups_by_operator() -> None:
    results = [
        _mk_result("p1", "株式会社A"),
        _mk_result("p2", "株式会社A"),
        _mk_result("p3", "株式会社A"),
        _mk_result("p4", "株式会社B"),
        _mk_result("p5", ""),  # operator 不明
    ]
    report = _aggregate(results, total=5)
    assert report.total_stores_checked == 5
    assert report.stores_with_operator == 4
    assert report.stores_unknown == 1
    # coverage
    assert report.coverage_rate == pytest.approx(0.8)
    # 2 operators, A が 3 店舗、B が 1 店舗
    assert len(report.operators) == 2
    # 降順 (store_count)
    assert report.operators[0].operator_name == "株式会社A"
    assert report.operators[0].store_count == 3
    assert report.operators[1].operator_name == "株式会社B"
    assert report.operators[1].store_count == 1


def test_aggregate_is_mega_flag() -> None:
    # 20+ 店舗運営で is_mega=True
    results = [_mk_result(f"p{i}", "株式会社MEGA") for i in range(25)]
    report = _aggregate(results, total=25)
    assert report.operators[0].is_mega is True
    assert report.operators[0].store_count == 25

    # 19 店舗では False
    results2 = [_mk_result(f"p{i}", "株式会社SMALL") for i in range(19)]
    report2 = _aggregate(results2, total=19)
    assert report2.operators[0].is_mega is False


def test_aggregate_dominant_operator_type() -> None:
    results = [
        _mk_result("p1", "株式会社X", op_type="franchisee"),
        _mk_result("p2", "株式会社X", op_type="franchisee"),
        _mk_result("p3", "株式会社X", op_type="unknown"),
    ]
    report = _aggregate(results, total=3)
    assert report.operators[0].operator_type == "franchisee"


def test_aggregate_avg_confidence() -> None:
    results = [
        _mk_result("p1", "株式会社A", conf=0.9),
        _mk_result("p2", "株式会社A", conf=0.7),
    ]
    report = _aggregate(results, total=2)
    assert report.operators[0].avg_confidence == pytest.approx(0.8)


# ─── End-to-end with mock extractor ────────────────────────────────────


@dataclass
class StubExtractor:
    """固定マップで store → result を返す。"""

    results_by_place: dict[str, StoreExtractionResult]

    async def extract(self, *, place_id, brand, name, official_url, extra_urls=None):
        return self.results_by_place[place_id]


@pytest.mark.asyncio
async def test_discover_runs_extractor_for_each_store_and_groups() -> None:
    stub = StubExtractor(
        results_by_place={
            "p1": _mk_result("p1", "株式会社MEGA"),
            "p2": _mk_result("p2", "株式会社MEGA"),
            "p3": _mk_result("p3", ""),
        }
    )
    chain = ChainDiscovery(extractor=stub)
    stores = [
        StoreInput(place_id="p1", brand="B", name="N1", official_url="https://x/1"),
        StoreInput(place_id="p2", brand="B", name="N2", official_url="https://x/2"),
        StoreInput(place_id="p3", brand="B", name="N3", official_url="https://x/3"),
    ]
    report = await chain.discover(stores)
    assert report.total_stores_checked == 3
    assert report.stores_with_operator == 2
    assert len(report.operators) == 1
    assert report.operators[0].operator_name == "株式会社MEGA"


@pytest.mark.asyncio
async def test_discover_progress_callback_called_in_order() -> None:
    stub = StubExtractor(
        results_by_place={
            "p1": _mk_result("p1", "A"),
            "p2": _mk_result("p2", "A"),
        }
    )
    chain = ChainDiscovery(extractor=stub, max_concurrency=1)
    calls = []

    def track(i, total, result):
        calls.append((i, total, result.place_id))

    await chain.discover(
        [
            StoreInput(place_id="p1", brand="B", name="N1", official_url="https://x/1"),
            StoreInput(place_id="p2", brand="B", name="N2", official_url="https://x/2"),
        ],
        progress=track,
    )
    assert len(calls) == 2
    assert {c[2] for c in calls} == {"p1", "p2"}


@pytest.mark.asyncio
async def test_discover_empty_stores() -> None:
    stub = StubExtractor(results_by_place={})
    chain = ChainDiscovery(extractor=stub)
    report = await chain.discover([])
    assert report.total_stores_checked == 0
    assert report.operators == []
