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

# 抽出対象の会社名が 本部 (master franchisor) であることを示すヒント。
# ここがヒットした場合、その会社は個別の加盟店ではなく FC 本部であり、
# 店舗を運営する「真の franchisee」は別に存在する (公開情報にないことが多い)。
FRANCHISOR_MASTER_HINTS = [
    "マスターフランチャイジー",
    "master franchisee",
    "Master Franchisee",
    "フランチャイザー",
    "franchisor",
    "本部へのお問い合わせ",
    "本部へお問い合わせ",
    "として事業展開",
    "本部会社",
]


def _has_master_franchisor_hint(evidences: list["Evidence"]) -> bool:
    """evidence 内のどれかに franchisor (本部) ヒントが含まれているか。"""
    for e in evidences:
        for hint in FRANCHISOR_MASTER_HINTS:
            if hint in e.snippet:
                return True
    return False


# ─── Extractor ─────────────────────────────────────────────────────────


# 直営大手 (スタバ等) 対応: 店舗 detail ページに会社名が無くても
# ドメインルート配下の「会社概要/コーポレート」系 path を試す。
COMMON_ABOUT_SUFFIXES: tuple[str, ...] = (
    "company/",
    "company/summary/",
    "corporate/",
    "about/",
)


def _has_operator_evidence(evidences: list["Evidence"]) -> bool:
    """既存 evidence から operator 候補を deterministic に検出できるか。"""
    if _find_explicit_franchisee_operator(evidences) is not None:
        return True
    if _has_direct_evidence(evidences):
        return True
    if _find_company_names_in_all(evidences):
        return True
    return False


@dataclass
class PerStoreExtractor:
    """個別店舗 URL から運営会社を確定する。"""

    collector: EvidenceCollector = field(default_factory=EvidenceCollector)
    # 店舗 URL からドメインルートを辿って会社概要を探すか
    follow_domain_root: bool = True
    # Step 6.1: ドメインルートで見つからなかったとき、/company/ 等も試すか
    follow_common_about_paths: bool = True
    # /company/, /corporate/ 等の suffix を試す上限 (cost ガード)
    max_about_paths: int = 3

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
        # Phase 7 Step 1: _domain_root_candidates で複数候補を取得。
        # 例: map.mcdonalds.co.jp/x → [map.mcdonalds.co.jp/, www.mcdonalds.co.jp/]
        roots: list[str] = []
        if self.follow_domain_root:
            for cand in _domain_root_candidates(official_url):
                if cand == official_url:
                    continue
                roots.append(cand)
                root_evs = await self.collector.collect(
                    brand=brand,
                    official_url=cand,
                )
                evidences = evidences + root_evs

        # Step 2.5: ここまでで operator が取れていなければ /company/ 等を試す
        # 複数 root 候補それぞれに対して suffix 配列を試行 (直営大手対応)。
        if (
            self.follow_common_about_paths
            and roots
            and not _has_operator_evidence(_dedupe(evidences))
        ):
            for root in roots:
                if _has_operator_evidence(_dedupe(evidences)):
                    break
                for suffix in COMMON_ABOUT_SUFFIXES[: self.max_about_paths]:
                    about_url = root.rstrip("/") + "/" + suffix
                    about_evs = await self.collector.collect(
                        brand=brand,
                        official_url=about_url,
                    )
                    if about_evs:
                        evidences = evidences + about_evs
                        # operator 検出できたら早期終了
                        if _has_operator_evidence(_dedupe(evidences)):
                            break

        # dedupe (snippet + url + reason)
        evidences = _dedupe(evidences)
        result.evidences = evidences

        if not evidences:
            result.reasoning = "evidence 収集できず (fetch 失敗 or sight had no relevant text)"
            return result

        # Step 3: deterministic rule extraction
        # (a) Explicit FC 記載 — "運営: 株式会社XXX"  (個別 franchisee 名が取れる)
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

        # (c') 本部 (master franchisor) だけが取れるケース
        # 例: エニタイムの店舗ページに「株式会社Fast Fitness Japan はマスター
        # フランチャイジーです。当店はフランチャイジーが運営します」とある。
        # この場合、抽出される株式会社は**本部**であり、真の加盟店は別途存在
        # するが公開情報にない。mega_franchisees では除外すべき対象。
        if _has_master_franchisor_hint(evidences):
            candidates = _find_company_names_in_all(evidences)
            if candidates:
                result.operator_name = candidates[0]
                result.operator_type = "franchisor"
                result.confidence = 0.65
                result.reasoning = (
                    "本部 (master franchisor) を検出。個別加盟店会社は店舗ページ"
                    "に公開されていないため別経路での特定が必要"
                )
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
    """URL の scheme + host を返す。例: https://a.example.com/foo → https://a.example.com/

    後方互換のため 1 URL だけ返す。複数親候補は _domain_root_candidates()。
    """
    cands = _domain_root_candidates(url)
    return cands[0] if cands else ""


# 地図/店舗サブドメインとみなすホスト prefix。
# これらのサブドメインには会社概要ページが無いことが多いため、
# 親の www.* に昇格して /company/ を試す。
_SUBDOMAIN_TO_PROMOTE: tuple[str, ...] = (
    "map.",
    "maps.",
    "store.",
    "stores.",
    "shop.",
    "shops.",
    "locator.",
    "finder.",
    "as.",       # chizumaru などの外部地図サブドメイン
)


def _domain_root_candidates(url: str) -> list[str]:
    """URL から会社概要サイト候補を優先度順で返す。

    返却:
      [<同一ホストの root>, <親ドメイン (www.X) の root>, ...]

    例:
      https://map.mcdonalds.co.jp/map/13 →
        ["https://map.mcdonalds.co.jp/", "https://www.mcdonalds.co.jp/"]

      https://www.anytimefitness.co.jp/x/ →
        ["https://www.anytimefitness.co.jp/"]   (既に www、昇格なし)

      https://example.com/x/ →
        ["https://example.com/"]   (apex、昇格先なし)

    親昇格の判断基準:
      host が _SUBDOMAIN_TO_PROMOTE のいずれかで始まる場合のみ、
      先頭部分を `www.` に置換した候補を追加する。
    """
    m = re.match(r"(https?)://([^/]+)", url)
    if not m:
        return []
    scheme, host = m.group(1), m.group(2)
    out = [f"{scheme}://{host}/"]

    # 親ドメイン昇格: 特定 prefix を www.* に置換
    lower = host.lower()
    for prefix in _SUBDOMAIN_TO_PROMOTE:
        if lower.startswith(prefix):
            parent = "www." + host[len(prefix):]
            parent_url = f"{scheme}://{parent}/"
            if parent_url not in out:
                out.append(parent_url)
            break
    return out


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
