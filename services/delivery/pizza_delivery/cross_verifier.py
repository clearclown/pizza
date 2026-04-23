"""CrossVerifier — 発見した (operator, store) を二重確認する (Phase 5 Step D)。

ChainDiscovery で見つかった候補店舗を、別の根拠でもう一度確かめることで
false positive を減らす。

2 段階のクロス検証:
  1. **Store page re-extract**: 同じ店舗 URL を再度 fetch して
     PerStoreExtractor を走らせ、同じ operator が抽出されるか確認
     (サイトが更新された場合の防衛、fetch 時のランダム性排除)
  2. **Alt URL check**: official_url 以外の候補 URL
     (Google Maps の website フィールド、meta, OGP など) からも
     PerStoreExtractor を走らせ、operator が一致するか

operator 一致判定は normalize.operators_match を使用する。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pizza_delivery.normalize import normalize_operator_name, operators_match
from pizza_delivery.per_store import PerStoreExtractor, StoreExtractionResult


# ─── Input / Output ────────────────────────────────────────────────────


@dataclass
class VerifyCandidate:
    """verify 対象の候補。"""

    place_id: str
    brand: str
    name: str
    expected_operator: str  # 検証したい operator 名 (ChainDiscovery の発見結果)
    primary_url: str        # メイン公式 URL (store page)
    alt_urls: list[str] = field(default_factory=list)


@dataclass
class VerifyResult:
    place_id: str
    expected_operator: str
    confirmed: bool                 # True: operator 一致確認、False: 不一致 or 取得失敗
    observed_operator: str = ""     # 検証時に取得した operator 名
    confidence_boost: float = 0.0   # 元 confidence に足す量 (max +0.1)
    reason: str = ""                # "primary_match" | "alt_match" | "mismatch" | "no_evidence"
    evidences: list = field(default_factory=list)


# ─── Verifier ──────────────────────────────────────────────────────────


@dataclass
class CrossVerifier:
    extractor: PerStoreExtractor = field(default_factory=PerStoreExtractor)

    async def verify(self, candidate: VerifyCandidate) -> VerifyResult:
        result = VerifyResult(
            place_id=candidate.place_id,
            expected_operator=candidate.expected_operator,
            confirmed=False,
        )

        if not candidate.expected_operator:
            result.reason = "expected_operator is empty, cannot verify"
            return result

        # Phase 1: primary URL で再抽出
        primary = await self._extract_one(candidate, candidate.primary_url)
        if primary and primary.has_operator:
            if operators_match(primary.operator_name, candidate.expected_operator):
                result.confirmed = True
                result.observed_operator = normalize_operator_name(primary.operator_name)
                result.confidence_boost = 0.1
                result.reason = "primary_match"
                result.evidences = list(primary.evidences)
                return result
            # primary が別 operator を返した場合、alt で救済を試みる
            result.observed_operator = normalize_operator_name(primary.operator_name)

        # Phase 2: alt URL で抽出
        for alt in candidate.alt_urls:
            if not alt or alt == candidate.primary_url:
                continue
            alt_result = await self._extract_one(candidate, alt)
            if not alt_result or not alt_result.has_operator:
                continue
            if operators_match(alt_result.operator_name, candidate.expected_operator):
                result.confirmed = True
                result.observed_operator = normalize_operator_name(alt_result.operator_name)
                result.confidence_boost = 0.05  # alt match は primary より弱い
                result.reason = "alt_match"
                result.evidences = list(alt_result.evidences)
                return result

        # どちらでも match しなかった
        if result.observed_operator:
            result.reason = f"mismatch (observed={result.observed_operator})"
        else:
            result.reason = "no_evidence"
        return result

    async def _extract_one(
        self, candidate: VerifyCandidate, url: str
    ) -> StoreExtractionResult | None:
        try:
            return await self.extractor.extract(
                place_id=candidate.place_id,
                brand=candidate.brand,
                name=candidate.name,
                official_url=url,
            )
        except Exception:  # noqa: BLE001
            return None


async def verify_many(
    verifier: CrossVerifier,
    candidates: list[VerifyCandidate],
    *,
    max_concurrency: int = 3,
) -> list[VerifyResult]:
    """複数候補を並行 verify する。"""
    import asyncio

    sem = asyncio.Semaphore(max_concurrency)

    async def _one(c: VerifyCandidate) -> VerifyResult:
        async with sem:
            return await verifier.verify(c)

    return list(await asyncio.gather(*[_one(c) for c in candidates]))
