"""🔴 Red-phase test — 開発工程.md §3.1 Parser Test 相当。

Phase 3 で browser-use 統合が完了すれば is_franchise 判定が通る。
Phase 0 時点では NotImplementedError を返すことを baseline として契約する。
"""

from __future__ import annotations

import pytest

from pizza_delivery.agent import JudgeRequest, judge_franchise


@pytest.mark.asyncio
async def test_judge_franchise_phase0_baseline() -> None:
    req = JudgeRequest(
        place_id="p1",
        brand="テストブランド",
        name="テスト店舗",
        markdown="# 会社概要\n\n株式会社テスト運営",
    )
    with pytest.raises(NotImplementedError, match="Phase 3"):
        await judge_franchise(req)


@pytest.mark.asyncio
async def test_judge_franchise_identifies_mega_franchisee() -> None:
    """🔴 Phase 3 で有効化する。現在は NotImplementedError が期待値。"""
    req = JudgeRequest(
        place_id="p-mega-1",
        brand="エニタイムフィットネス",
        name="エニタイムフィットネス新宿店",
        markdown="## 会社概要\n運営: 株式会社メガ・スポーツ\n運営店舗数: 35 店舗",
    )
    with pytest.raises(NotImplementedError):
        reply = await judge_franchise(req)
        # Phase 3 で以下のアサーションを活性化する:
        #   assert reply.is_franchise is True
        #   assert reply.operator_name == "株式会社メガ・スポーツ"
        #   assert reply.store_count_estimate >= 20
        _ = reply
