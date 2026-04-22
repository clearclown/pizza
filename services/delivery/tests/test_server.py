"""🔴 Red-phase test for DeliveryServicer. Phase 3 で実装する。"""

from __future__ import annotations

import pytest

from pizza_delivery.server import DeliveryServicer


@pytest.mark.asyncio
async def test_servicer_raises_until_phase3() -> None:
    servicer = DeliveryServicer()
    with pytest.raises(NotImplementedError, match="Phase 3"):
        await servicer.JudgeFranchiseType(request=None, context=None)
