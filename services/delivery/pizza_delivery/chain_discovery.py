"""ChainDiscovery — 芋づる式 operator 探索 (Phase 5 Step C)。

PerStoreExtractor を複数店舗に適用して、operator ごとに店舗をグループ化する。
同じ operator が複数店舗を運営している事実が見つかれば「メガジー候補」となる。

核心フロー:
  1. Input: 店舗 list (M1 Seed 出力)
  2. 各店舗で PerStoreExtractor → operator 抽出
  3. operator → [stores] の dict に蓄積
  4. operator ごとの store_count を返す
  5. Output: OperatorSummary list (operator, stores, confidence)

これは人間が「あるブランド全店舗を 1 つずつ調べ、
運営会社ごとに仕分ける」工程そのもの。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from pizza_delivery.evidence import Evidence
from pizza_delivery.per_store import PerStoreExtractor, StoreExtractionResult


# ─── Input / Output types ──────────────────────────────────────────────


@dataclass
class StoreInput:
    """Chain discovery への入力店舗。"""

    place_id: str
    brand: str
    name: str
    official_url: str
    extra_urls: list[str] = field(default_factory=list)


@dataclass
class OperatorSummary:
    """1 operator が運営する確定店舗の集約結果。"""

    operator_name: str
    operator_type: str           # direct | franchisee | unknown
    store_count: int
    stores: list[StoreExtractionResult]
    avg_confidence: float

    @property
    def is_mega(self) -> bool:
        """メガフランチャイジー判定 (20 店舗以上)。"""
        return self.store_count >= 20


@dataclass
class ChainDiscoveryReport:
    """ChainDiscovery の全体結果。"""

    total_stores_checked: int
    stores_with_operator: int        # operator 特定できた店舗数
    stores_unknown: int              # operator 不明店舗数
    operators: list[OperatorSummary] # store_count 降順

    @property
    def coverage_rate(self) -> float:
        if self.total_stores_checked == 0:
            return 0.0
        return self.stores_with_operator / self.total_stores_checked


# ─── Progress callback type ────────────────────────────────────────────


ProgressFn = Callable[[int, int, StoreExtractionResult], None]  # (idx, total, result)


# ─── ChainDiscovery ────────────────────────────────────────────────────


@dataclass
class ChainDiscovery:
    """ブランド店舗群を走査し、operator ごとにグループ化する。"""

    extractor: PerStoreExtractor = field(default_factory=PerStoreExtractor)
    max_concurrency: int = 4  # 同時並行 fetch 数

    async def discover(
        self,
        stores: list[StoreInput],
        *,
        progress: ProgressFn | None = None,
    ) -> ChainDiscoveryReport:
        """店舗 list から operator ごとの結果を生成する。"""
        total = len(stores)
        if total == 0:
            return ChainDiscoveryReport(
                total_stores_checked=0,
                stores_with_operator=0,
                stores_unknown=0,
                operators=[],
            )

        sem = asyncio.Semaphore(self.max_concurrency)
        results: list[StoreExtractionResult] = []

        async def _one(idx: int, st: StoreInput) -> StoreExtractionResult:
            async with sem:
                r = await self.extractor.extract(
                    place_id=st.place_id,
                    brand=st.brand,
                    name=st.name,
                    official_url=st.official_url,
                    extra_urls=st.extra_urls or None,
                )
                if progress:
                    progress(idx, total, r)
                return r

        coros = [_one(i, s) for i, s in enumerate(stores)]
        results = await asyncio.gather(*coros, return_exceptions=False)

        return _aggregate(results, total)


def _aggregate(
    results: list[StoreExtractionResult], total: int
) -> ChainDiscoveryReport:
    """results を operator_name でグループ化して Report を組む。"""
    groups: dict[str, list[StoreExtractionResult]] = {}
    unknown = 0
    for r in results:
        if r.has_operator:
            groups.setdefault(r.operator_name, []).append(r)
        else:
            unknown += 1

    operators: list[OperatorSummary] = []
    for op_name, rs in groups.items():
        # 最頻値で operator_type を決定 (複数 store が同 operator でも type が揺れる可能性)
        type_freq: dict[str, int] = {}
        for r in rs:
            type_freq[r.operator_type] = type_freq.get(r.operator_type, 0) + 1
        dominant_type = max(type_freq.keys(), key=lambda k: type_freq[k])
        avg_conf = sum(r.confidence for r in rs) / len(rs)
        operators.append(
            OperatorSummary(
                operator_name=op_name,
                operator_type=dominant_type,
                store_count=len(rs),
                stores=rs,
                avg_confidence=avg_conf,
            )
        )
    # store_count 降順、同数なら confidence 降順
    operators.sort(key=lambda o: (-o.store_count, -o.avg_confidence))

    return ChainDiscoveryReport(
        total_stores_checked=total,
        stores_with_operator=total - unknown,
        stores_unknown=unknown,
        operators=operators,
    )
