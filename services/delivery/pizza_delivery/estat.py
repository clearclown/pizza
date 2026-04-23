"""Phase 17.2: 政府統計 e-Stat API client + recall audit。

e-Stat は日本政府統計の総合窓口で、**経済センサス 事業所名簿** から
業種 × 市区町村単位の事業所数を取得できる。Places API の recall (網羅率) を
検証するための外部 ground-truth として利用する。

App ID 取得:
  https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0#no2

代表的 stat:
  統計表 ID: 0003215383 (経済センサス活動調査 / 産業中分類・市区町村別事業所数)
  cat01 列は日本標準産業分類コード (gym=8048, convenience=5891 等)
  area 列は JIS X 0402 市区町村コード

recall audit 用途では、Places API で取得した件数と e-Stat の事業所数を
市区町村単位で突合し、recall = places / reference を算出する。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import httpx


DEFAULT_BASE_URL = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"


# 日本標準産業分類 (小分類) → 代表的 brand 業種のマッピング
INDUSTRY_CODE_MAP: dict[str, str] = {
    "gym": "8048",                  # フィットネスクラブ
    "convenience_store": "5891",    # コンビニエンスストア
    "fast_food": "7671",            # 持ち帰り飲食サービス業 (ファストフード)
    "cafe": "7672",                 # 喫茶店 (代表的)
    "restaurant": "7611",           # 食堂 (代表的)
    "drugstore": "6031",            # ドラッグストア
    "book_store": "6041",           # 書籍・文房具小売
    "reuse_store": "6092",          # 中古品小売業
}


# ─── Data types ────────────────────────────────────────────────────────


@dataclass
class EstablishmentCount:
    """e-Stat から取得した 1 area の事業所数。"""

    area_code: str   # JIS 市区町村コード (例: 13101 千代田区)
    industry_code: str  # 日本標準産業分類
    count: int


@dataclass
class AreaRecall:
    area_code: str
    places_count: int
    reference_count: int
    recall_ratio: float  # places / reference (0.0 if reference==0)


@dataclass
class RecallAudit:
    per_area: list[AreaRecall] = field(default_factory=list)
    overall_places_total: int = 0
    overall_reference_total: int = 0
    overall_recall: float | None = None


# ─── e-Stat client ───────────────────────────────────────────────────


@dataclass
class EstatClient:
    """e-Stat Web API (v3.0) の薄いラッパ。"""

    app_id: str = ""
    base_url: str = DEFAULT_BASE_URL
    timeout: float = 30.0
    transport: Any = None  # httpx.MockTransport 用

    def __post_init__(self) -> None:
        if not self.app_id:
            self.app_id = os.getenv("ESTAT_APP_ID", "")

    async def fetch_establishment_counts(
        self,
        *,
        industry_code: str,
        prefecture_code: str,
        stats_data_id: str = "0003215383",
    ) -> list[EstablishmentCount]:
        """経済センサス事業所数を市区町村 × 業種で取得。

        fetched data の cat01 列に産業分類、@area 列に市区町村コード。
        """
        if not self.app_id:
            raise ValueError("ESTAT_APP_ID is required (env or ctor)")
        params = {
            "appId": self.app_id,
            "statsDataId": stats_data_id,
            "cdCat01": industry_code,
            # 都道府県コード 2 桁 → JIS 市区町村コード 5 桁で絞り込み
            "cdAreaFrom": f"{prefecture_code}000",
            "cdAreaTo": f"{prefecture_code}999",
        }
        kwargs: dict[str, Any] = {"timeout": self.timeout}
        if self.transport is not None:
            kwargs["transport"] = self.transport
        async with httpx.AsyncClient(**kwargs) as client:
            resp = await client.get(self.base_url, params=params)
        resp.raise_for_status()
        data = resp.json() or {}
        out: list[EstablishmentCount] = []
        try:
            values = (
                data.get("GET_STATS_DATA", {})
                .get("STATISTICAL_DATA", {})
                .get("DATA_INF", {})
                .get("VALUE", [])
            ) or []
        except (AttributeError, TypeError):
            values = []
        for v in values:
            try:
                count = int(str(v.get("$", "0")))
            except (ValueError, TypeError):
                count = 0
            out.append(
                EstablishmentCount(
                    area_code=str(v.get("@area", "")),
                    industry_code=str(v.get("@cat01", industry_code)),
                    count=count,
                )
            )
        return out


# ─── Recall audit ──────────────────────────────────────────────────


def compute_recall_audit(
    places_counts: dict[str, int],
    reference_counts: dict[str, int],
) -> RecallAudit:
    """area_code ごとに (places/ reference) recall を算出する。

    places_counts: area_code -> Places で取得した件数
    reference_counts: area_code -> e-Stat or OSM の件数
    両方に存在する area は両方の値で ratio 算出。
    reference にしかない area は places=0 で recall=0 のエントリとして記録。
    places にしかない area は無視 (reference が 0 なら比較不能)。
    """
    audit = RecallAudit()
    all_areas = set(places_counts) | set(reference_counts)
    for area in sorted(all_areas):
        p = places_counts.get(area, 0)
        r = reference_counts.get(area, 0)
        if r == 0:
            continue  # reference が無ければ recall 計算不可
        ratio = round(p / r, 3)
        audit.per_area.append(
            AreaRecall(area_code=area, places_count=p, reference_count=r,
                       recall_ratio=ratio)
        )
        audit.overall_places_total += p
        audit.overall_reference_total += r
    if audit.overall_reference_total > 0:
        audit.overall_recall = round(
            audit.overall_places_total / audit.overall_reference_total, 3
        )
    return audit
