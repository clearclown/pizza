"""Layer D (operator 実在検証) の統合 fallback チェーン。

複数の検証経路を graceful fallback で束ねる:

    1. 国税庁 Web-API (HOUJIN_BANGOU_APP_ID 設定時、houjin_bangou.py)
    2. 国税庁 CSV ローカル index (houjin_csv.py, DB 件数 > 0)
    3. gBizINFO API (GBIZ_API_TOKEN 設定時, gbiz_client.py)
    4. skip (graceful no-op)

各経路は `verify_operator` 互換の dict を返す:
    {exists, name_similarity, best_match_name, best_match_number, active, source}

使い方:
    pipe = VerifyPipeline()
    result = await pipe.verify("株式会社モスストアカンパニー")
    if result["exists"]:
        ...

いずれも Ground Truth データを「外部 or ローカル DB」から取るだけで、
LLM 推論・手書きデータ貼付・ハルシネーションは一切介在しない。
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


_EMPTY_RESULT: dict[str, Any] = {
    "exists": False,
    "name_similarity": 0.0,
    "best_match_name": "",
    "best_match_number": "",
    "active": False,
    "source": "skipped",
}


@dataclass
class VerifyPipeline:
    """複数検証経路の fallback 統括。

    Attributes:
      try_web_api:    HOUJIN_BANGOU_APP_ID があれば 国税庁 Web-API を叩く
      try_csv:        local CSV index (registry.sqlite) が作成済なら使う
      try_gbiz:       GBIZ_API_TOKEN があれば gBizINFO を叩く
      csv_db_path:    CSV index の場所 (default: var/houjin/registry.sqlite)
    """

    try_web_api: bool = True
    try_csv: bool = True
    try_gbiz: bool = True
    csv_db_path: str | None = None
    # Cached clients (lazy)
    _web_client: Any = field(default=None, init=False, repr=False)
    _csv_idx: Any = field(default=None, init=False, repr=False)
    _gbiz_client: Any = field(default=None, init=False, repr=False)

    # ── 経路別の有効性判定 ──────────────────────────

    def _web_ready(self) -> bool:
        return self.try_web_api and bool(os.getenv("HOUJIN_BANGOU_APP_ID", ""))

    def _csv_ready(self) -> bool:
        if not self.try_csv:
            return False
        try:
            from pizza_delivery.houjin_csv import HoujinCSVIndex
        except ImportError:
            return False
        if self._csv_idx is None:
            self._csv_idx = HoujinCSVIndex(self.csv_db_path) if self.csv_db_path else HoujinCSVIndex()
        try:
            return self._csv_idx.count() > 0
        except Exception as e:
            logger.debug("csv ready check failed: %s", e)
            return False

    def _gbiz_ready(self) -> bool:
        return self.try_gbiz and bool(os.getenv("GBIZ_API_TOKEN", ""))

    # ── 経路別の実行 (失敗は次 fallback に回る) ──────

    async def _try_web_api(self, name: str) -> dict[str, Any] | None:
        try:
            from pizza_delivery.houjin_bangou import HoujinBangouClient, verify_operator
        except ImportError:
            return None
        if self._web_client is None:
            self._web_client = HoujinBangouClient()
        try:
            search = await self._web_client.search_by_name(name)
        except Exception as e:
            logger.debug("web-api failed: %s", e)
            return None
        v = verify_operator(name, search)
        v["source"] = "houjin_nta_api"
        return v

    async def _try_csv(self, name: str) -> dict[str, Any] | None:
        try:
            from pizza_delivery.houjin_csv import verify_operator_via_csv
        except ImportError:
            return None
        try:
            return verify_operator_via_csv(name, idx=self._csv_idx)
        except Exception as e:
            logger.debug("csv lookup failed: %s", e)
            return None

    async def _try_gbiz(self, name: str) -> dict[str, Any] | None:
        try:
            from pizza_delivery.gbiz_client import GBizClient, verify_operator_via_gbiz
        except ImportError:
            return None
        if self._gbiz_client is None:
            self._gbiz_client = GBizClient()
        try:
            return await verify_operator_via_gbiz(name, client=self._gbiz_client)
        except Exception as e:
            logger.debug("gbiz lookup failed: %s", e)
            return None

    # ── 公開 API ──────────────────────────────────

    async def verify(self, name: str) -> dict[str, Any]:
        """operator 名を利用可能な経路で順番に検証。

        - 何らかの経路で exists=True なら即座に return (優先度順)
        - 全経路 miss/skipped なら最後の結果 (or EMPTY) を返す
        - source 列で検証経路を追跡できる
        """
        if not name or not name.strip():
            return dict(_EMPTY_RESULT)

        last_result: dict[str, Any] | None = None

        if self._web_ready():
            r = await self._try_web_api(name)
            if r and r.get("exists"):
                return r
            last_result = r or last_result

        if self._csv_ready():
            r = await self._try_csv(name)
            if r and r.get("exists"):
                return r
            last_result = r or last_result

        if self._gbiz_ready():
            r = await self._try_gbiz(name)
            if r and r.get("exists"):
                return r
            last_result = r or last_result

        return last_result or dict(_EMPTY_RESULT)

    def available_paths(self) -> list[str]:
        """現環境で有効な経路名を列挙 (診断用)。"""
        paths = []
        if self._web_ready():
            paths.append("houjin_nta_api")
        if self._csv_ready():
            paths.append("houjin_csv")
        if self._gbiz_ready():
            paths.append("gbiz")
        return paths
