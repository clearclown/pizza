"""Step 6.1 — PerStoreExtractor の /company/ fallback fetch テスト。

直営大手 (スタバ等) は店舗 detail ページに会社名が載っていないため、
ドメインルートや /company/ /corporate/ /about/ に fetch しないと operator が
抽出できない。この fallback が正しく動くことを保証する。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from pizza_delivery.evidence import Evidence
from pizza_delivery.per_store import PerStoreExtractor


@dataclass
class URLAwareMockCollector:
    """URL ごとに異なる evidences を返す collector モック。"""

    url_to_evidences: dict[str, list[Evidence]]
    called_urls: list[str] = field(default_factory=list)

    async def collect(self, *, brand, official_url, extra_urls=None):
        self.called_urls.append(official_url)
        return list(self.url_to_evidences.get(official_url, []))


def _ev(url: str, snippet: str, reason: str = "operator_keyword") -> Evidence:
    return Evidence(source_url=url, snippet=snippet, reason=reason, keyword="運営会社")


# ─── Tests ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fallback_finds_operator_in_company_page() -> None:
    store_url = "https://store.starbucks.co.jp/detail-204/"
    root_url = "https://store.starbucks.co.jp/"
    company_url = "https://store.starbucks.co.jp/company/"

    collector = URLAwareMockCollector(
        url_to_evidences={
            # store detail / root には operator なし
            store_url: [],
            root_url: [],
            # /company/ に明示記載 → ヒット
            company_url: [
                _ev(company_url, "当店は、株式会社Starbucks Test が運営する加盟店です。")
            ],
        }
    )
    ex = PerStoreExtractor(collector=collector)
    res = await ex.extract(
        place_id="p1", brand="スターバックス",
        name="新宿東口店", official_url=store_url,
    )
    assert "Starbucks Test" in res.operator_name
    assert res.operator_type == "franchisee"
    # /company/ が呼ばれた
    assert company_url in collector.called_urls


@pytest.mark.asyncio
async def test_fallback_skipped_when_detail_already_yields_operator() -> None:
    store_url = "https://example.com/store/"
    root_url = "https://example.com/"

    collector = URLAwareMockCollector(
        url_to_evidences={
            # detail ページで operator 既に明示 (franchisee)
            store_url: [
                _ev(store_url, "運営会社: 株式会社detail運営 が当店を運営しています。")
            ],
            # root は空
            root_url: [],
            # /company/ が呼ばれたら fail させたいが実装上 root 後の fallback は
            # operator 既発見なので skip されるはず (テスト対象)
        }
    )
    ex = PerStoreExtractor(collector=collector)
    res = await ex.extract(
        place_id="p1", brand="B", name="N", official_url=store_url,
    )
    assert "detail運営" in res.operator_name
    # /company/ は呼ばれなかった (早期終了)
    assert "https://example.com/company/" not in collector.called_urls


@pytest.mark.asyncio
async def test_fallback_all_404_does_not_crash() -> None:
    store_url = "https://nowhere.example/x/"
    collector = URLAwareMockCollector(
        url_to_evidences={}  # どの URL も空 evidences
    )
    ex = PerStoreExtractor(collector=collector)
    res = await ex.extract(
        place_id="p1", brand="B", name="N", official_url=store_url,
    )
    # 例外が出ず、何かしら reasoning が付いていれば OK
    assert res.operator_name == ""
    assert res.reasoning != ""


@pytest.mark.asyncio
async def test_fallback_disabled_by_flag() -> None:
    store_url = "https://example.com/s/"
    company_url = "https://example.com/company/"

    collector = URLAwareMockCollector(
        url_to_evidences={
            store_url: [],
            "https://example.com/": [],
            company_url: [
                _ev(company_url, "当店は、株式会社XYZ が運営する加盟店です。")
            ],
        }
    )
    ex = PerStoreExtractor(
        collector=collector,
        follow_common_about_paths=False,
    )
    res = await ex.extract(
        place_id="p1", brand="B", name="N", official_url=store_url,
    )
    # フラグ off → /company/ は呼ばれず operator は発見できない
    assert company_url not in collector.called_urls
    assert res.operator_name == ""


@pytest.mark.asyncio
async def test_fallback_tries_multiple_suffixes_until_hit() -> None:
    """/company/ が空で /corporate/ でヒットする場合、/corporate/ まで掘る。"""
    store_url = "https://brand.example/s/"
    collector = URLAwareMockCollector(
        url_to_evidences={
            store_url: [],
            "https://brand.example/": [],
            "https://brand.example/company/": [],
            "https://brand.example/company/summary/": [],
            "https://brand.example/corporate/": [
                _ev(
                    "https://brand.example/corporate/",
                    "運営会社: 株式会社コーポレート",
                )
            ],
        }
    )
    ex = PerStoreExtractor(collector=collector)
    res = await ex.extract(
        place_id="p1", brand="B", name="N", official_url=store_url,
    )
    assert "コーポレート" in res.operator_name
    # /corporate/ にたどり着くまでに /company/ と /company/summary/ は試される
    assert "https://brand.example/company/" in collector.called_urls
    assert "https://brand.example/corporate/" in collector.called_urls


@pytest.mark.asyncio
async def test_fallback_respects_max_about_paths_limit() -> None:
    """max_about_paths=2 の場合、先頭 2 候補だけ試して残りは skip。"""
    store_url = "https://brand.example/s/"
    collector = URLAwareMockCollector(url_to_evidences={})  # 全部空
    ex = PerStoreExtractor(collector=collector, max_about_paths=2)
    await ex.extract(
        place_id="p1", brand="B", name="N", official_url=store_url,
    )
    about_calls = [u for u in collector.called_urls if "/company" in u or "/corporate" in u or "/about" in u]
    assert len(about_calls) == 2
