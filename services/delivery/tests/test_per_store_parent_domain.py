"""Phase 7 Step 1 — 親ドメイン昇格 fetch テスト。

Places が返す official_url が `map.X.co.jp` のような地図サブドメインや
`as.chizumaru.com/famima/` のような外部地図サービスだと、既存の _domain_root
fallback では本家サイトに到達できず operator 抽出できない。
`_domain_root_candidates()` で親ドメイン候補を複数返し、per_store の
fallback loop が それらを試行することで McDonald/スタバ/ファミマのような
直営/FC 大手の operator 抽出率を改善する。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from pizza_delivery.evidence import Evidence
from pizza_delivery.per_store import (
    PerStoreExtractor,
    _domain_root,
    _domain_root_candidates,
)


# ─── _domain_root_candidates unit tests ────────────────────────────────


@pytest.mark.parametrize(
    "url, expected",
    [
        # 既存のシンプルケース (後方互換: 先頭は _domain_root と一致)
        (
            "https://www.anytimefitness.co.jp/shinjuku6/",
            ["https://www.anytimefitness.co.jp/"],
        ),
        # map.X.co.jp → map.X.co.jp + www.X.co.jp (親ドメイン昇格)
        (
            "https://map.mcdonalds.co.jp/map/13764",
            ["https://map.mcdonalds.co.jp/", "https://www.mcdonalds.co.jp/"],
        ),
        # store.X.co.jp → store.X.co.jp + www.X.co.jp
        (
            "https://store.starbucks.co.jp/detail-204/",
            ["https://store.starbucks.co.jp/", "https://www.starbucks.co.jp/"],
        ),
        # apex ドメインは昇格先なし (そのホストのみ)
        (
            "https://example.com/foo",
            ["https://example.com/"],
        ),
        # 空 / invalid は空リスト
        ("", []),
        ("not-a-url", []),
    ],
)
def test_domain_root_candidates(url: str, expected: list[str]) -> None:
    assert _domain_root_candidates(url) == expected


def test_domain_root_is_first_candidate_for_backcompat() -> None:
    """既存コードが参照する _domain_root は候補リストの先頭と一致する。"""
    for url in [
        "https://www.anytimefitness.co.jp/x/",
        "https://map.mcdonalds.co.jp/",
        "https://store.starbucks.co.jp/detail-1/",
    ]:
        assert _domain_root(url) == _domain_root_candidates(url)[0]


# ─── integration: parent domain fallback in PerStoreExtractor ──────────


@dataclass
class URLAwareMockCollector:
    url_to_evidences: dict[str, list[Evidence]]
    called_urls: list[str] = field(default_factory=list)

    async def collect(self, *, brand, official_url, extra_urls=None):
        self.called_urls.append(official_url)
        return list(self.url_to_evidences.get(official_url, []))


def _ev(url: str, snippet: str) -> Evidence:
    return Evidence(source_url=url, snippet=snippet, reason="operator_keyword", keyword="運営会社")


@pytest.mark.asyncio
async def test_parent_domain_fallback_finds_operator() -> None:
    """map.mcdonalds.co.jp/map/13 → www.mcdonalds.co.jp/company/ で operator 発見。"""
    store_url = "https://map.mcdonalds.co.jp/map/13764"
    collector = URLAwareMockCollector(
        url_to_evidences={
            # 地図サブドメイン: 空
            store_url: [],
            "https://map.mcdonalds.co.jp/": [],
            "https://map.mcdonalds.co.jp/company/": [],
            # 親ドメインの /company/ にヒット
            "https://www.mcdonalds.co.jp/": [],
            "https://www.mcdonalds.co.jp/company/": [
                _ev(
                    "https://www.mcdonalds.co.jp/company/",
                    "運営会社: 日本マクドナルド 株式会社",
                )
            ],
        }
    )
    ex = PerStoreExtractor(collector=collector)
    res = await ex.extract(
        place_id="p1", brand="マクドナルド", name="新宿東口店", official_url=store_url,
    )
    assert "マクドナルド" in res.operator_name
    # 親ドメインの /company/ が呼ばれている
    assert "https://www.mcdonalds.co.jp/company/" in collector.called_urls


@pytest.mark.asyncio
async def test_parent_domain_not_called_when_host_is_www() -> None:
    """既に www.* なら追加の親ドメイン候補は発生しない (重複 fetch 防止)。"""
    store_url = "https://www.anytimefitness.co.jp/shinjuku6/"
    collector = URLAwareMockCollector(
        url_to_evidences={
            store_url: [],
            "https://www.anytimefitness.co.jp/": [],
        }
    )
    ex = PerStoreExtractor(collector=collector)
    await ex.extract(
        place_id="p1", brand="エニタイム", name="新宿6丁目店", official_url=store_url,
    )
    # www.anytimefitness.co.jp のみ。親候補として anytimefitness.co.jp 等を
    # 追加 fetch しないこと
    hosts = set()
    for u in collector.called_urls:
        if u.startswith("https://"):
            hosts.add(u.split("/")[2])
    assert hosts == {"www.anytimefitness.co.jp"}
