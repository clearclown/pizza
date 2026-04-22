"""gRPC server glue. Phase 0: スタブ。Phase 3 で pb.DeliveryServiceServicer を実装。"""

from __future__ import annotations


class DeliveryServicer:
    """pizza.v1.DeliveryService の Python 実装（Phase 3 で拡張）。"""

    async def JudgeFranchiseType(self, request, context):  # noqa: N802 - gRPC convention
        _ = request, context
        raise NotImplementedError("DeliveryServicer is Phase 3 target")
