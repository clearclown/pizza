"""PerStoreExtractor — **特定店舗の** 運営会社を確定抽出する (Phase 5 Step A)。

EvidenceCollector がブランドサイトから証拠を広く集めるのに対し、
PerStoreExtractor は **1 店舗の個別 URL から、その店舗を運営する会社のみ** を
確定する。Phase 5 人間リサーチャー複製パイプラインの基本ビルディングブロック。

核心原則:
  1. **ブランド推論禁止** — "エニタイムだから FC" のような推測は返さない
  2. **店舗 URL 起点** — 個別店舗ページ + 同一ドメインの会社概要/加盟店ページまで
  3. **確定できなければ unknown** — 空で返して次段 (ChainDiscovery) が補完

使い方:
    extractor = PerStoreExtractor()
    result = await extractor.extract(
        place_id="ChIJ...",
        brand="エニタイムフィットネス",
        name="新宿6丁目店",
        official_url="https://www.anytimefitness.co.jp/shinjuku6/",
    )
    # => StoreExtractionResult(
    #      place_id="ChIJ...",
    #      operator_name="株式会社AFJ Project",  # 確定した運営会社 (or None)
    #      operator_type="franchisee",          # direct | franchisee | unknown
    #      evidences=[(url, snippet, reason), ...],
    #      confidence=0.85,
    #    )
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from pizza_delivery.evidence import (
    Evidence,
    EvidenceCollector,
    Fetcher,
    find_company_names_in_snippet,
    HttpxFetcher,
)


# ─── Result type ───────────────────────────────────────────────────────


@dataclass
class StoreExtractionResult:
    """1 店舗の運営会社抽出結果。"""

    place_id: str
    brand: str
    name: str
    operator_name: str = ""  # 確定した運営会社 ("" = 不明)
    operator_type: str = "unknown"  # direct | franchisee | unknown
    evidences: list[Evidence] = field(default_factory=list)
    confidence: float = 0.0
    reasoning: str = ""

    @property
    def has_operator(self) -> bool:
        return bool(self.operator_name)


# ─── Heuristic rules (deterministic, no LLM) ──────────────────────────


# ブランド本部 (franchisor) を示すヒント
FRANCHISOR_HINTS = [
    "本部",
    "株式会社 ",  # "株式会社 セブン-イレブン・ジャパン"
    "会社概要",
    "corporate",
    "company",
]

# "この店舗は加盟店/FC 店です" を示すヒント
FRANCHISEE_EXPLICIT_HINTS = [
    "加盟店です",
    "加盟店として運営",
    "フランチャイジー",
    "FC 加盟店",
    "運営: 株式会社",
    "運営会社: 株式会社",
    "運営法人: 株式会社",
    "運営者: 株式会社",
    "当店は、",  # 「当店は、株式会社○○が運営」
]

# "直営店" を示すヒント
DIRECT_EXPLICIT_HINTS = [
    "直営店",
    "直営店舗",
    "全店直営",
    "自社運営",
    "本部直営",
    "本店直営",
    "弊社直営",
]


# ─── Extractor ─────────────────────────────────────────────────────────


@dataclass
class PerStoreExtractor:
    """個別店舗 URL から運営会社を確定する。"""

    collector: EvidenceCollector = field(default_factory=EvidenceCollector)
    # 店舗 URL からドメインルートを辿って会社概要を探すか
    follow_domain_root: bool = True

    async def extract(
        self,
        *,
        place_id: str,
        brand: str,
        name: str,
        official_url: str,
        extra_urls: list[str] | None = None,
    ) -> StoreExtractionResult:
        result = StoreExtractionResult(
            place_id=place_id,
            brand=brand,
            name=name,
        )

        if not official_url:
            result.reasoning = "official_url が空、evidence 収集不能"
            return result

        # Step 1: 店舗 URL から evidence を集める
        evidences = await self.collector.collect(
            brand=brand,
            official_url=official_url,
            extra_urls=extra_urls or [],
        )

        # Step 2: ドメインルート も見る (会社概要が root にある想定)
        if self.follow_domain_root:
            root = _domain_root(official_url)
            if root and root != official_url:
                root_evs = await self.collector.collect(
                    brand=brand,
                    official_url=root,
                )
                evidences = evidences + root_evs

        # dedupe (snippet + url + reason)
        evidences = _dedupe(evidences)
        result.evidences = evidences

        if not evidences:
            result.reasoning = "evidence 収集できず (fetch 失敗 or sight had no relevant text)"
            return result

        # Step 3: deterministic rule extraction
        # (a) Explicit FC 記載 — "運営: 株式会社XXX"
        fc_op = _find_explicit_franchisee_operator(evidences)
        if fc_op:
            result.operator_name = fc_op[0]
            result.operator_type = "franchisee"
            result.confidence = 0.9
            result.reasoning = f"FC 加盟店として明示記載。evidence: {fc_op[1][:80]}..."
            return result

        # (b) Explicit direct 記載
        if _has_direct_evidence(evidences):
            # direct の場合、franchisor (本部) が operator となる
            candidates = _find_company_names_in_all(evidences)
            if candidates:
                result.operator_name = candidates[0]
                result.operator_type = "direct"
                result.confidence = 0.85
                result.reasoning = "直営店として明示記載、本部会社を operator とする"
                return result
            result.operator_type = "direct"
            result.confidence = 0.6
            result.reasoning = "直営記載はあるが運営会社名を特定できず"
            return result

        # (c) 店舗ページに「株式会社○○」単独記載がある場合
        # → franchisee か franchisor かは不明だが、ある会社が運営に関わっている
        candidates = _find_company_names_in_all(evidences)
        if candidates:
            # 複数候補なら最初を採用 (頻度順も考慮可)
            result.operator_name = candidates[0]
            result.operator_type = "unknown"  # direct/FC 不明
            result.confidence = 0.5
            result.reasoning = (
                f"evidence に会社名を検出したが、direct/FC の明示は不十分: "
                f"{candidates[:3]}"
            )
            return result

        # (d) 何も確定できない
        result.reasoning = (
            f"evidence {len(evidences)} 件収集したが、運営会社を特定できず"
        )
        return result


# ─── Helpers (pure functions, easy to test) ────────────────────────────


def _domain_root(url: str) -> str:
    """URL の scheme + host を返す。例: https://a.example.com/foo → https://a.example.com/"""
    m = re.match(r"(https?://[^/]+)", url)
    return f"{m.group(1)}/" if m else ""


def _dedupe(evidences: list[Evidence]) -> list[Evidence]:
    seen: set[tuple[str, str, str]] = set()
    out: list[Evidence] = []
    for e in evidences:
        key = (e.source_url, e.snippet[:120], e.reason)
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def _find_explicit_franchisee_operator(
    evidences: list[Evidence],
) -> tuple[str, str] | None:
    """FC 加盟店として運営会社が明示記載されている場合、(会社名, snippet) を返す。"""
    # 「運営: 株式会社XXX」「運営会社: 株式会社XXX」等の直後に会社名があるパターン
    for e in evidences:
        s = e.snippet
        for hint in FRANCHISEE_EXPLICIT_HINTS:
            idx = s.find(hint)
            if idx < 0:
                continue
            # hint の直後から会社名を抽出
            tail = s[idx : idx + 200]
            companies = find_company_names_in_snippet(tail)
            if companies:
                return (companies[0], tail)
    return None


def _has_direct_evidence(evidences: list[Evidence]) -> bool:
    for e in evidences:
        for hint in DIRECT_EXPLICIT_HINTS:
            if hint in e.snippet:
                return True
        if e.reason == "direct_keyword":
            return True
    return False


def _find_company_names_in_all(evidences: list[Evidence]) -> list[str]:
    """全 evidence の snippet から会社名を抽出、頻度順に返す。"""
    freq: dict[str, int] = {}
    for e in evidences:
        for name in find_company_names_in_snippet(e.snippet):
            freq[name] = freq.get(name, 0) + 1
    # 頻度降順、同頻度は出現順
    return sorted(freq.keys(), key=lambda n: -freq[n])
