"""Phase 17.1: OpenStreetMap Overpass API client。

Places API の recall を補完するための外部 ground-truth データ源。
OSM tag で bbox 内の店舗を query し、Places 結果との gap を検出する。

使い道:
  - Places で取れなかった店舗を OSM で補完 (gap filling)
  - Places 網羅率 % (recall) を算出するための reference 値

注意: OSM の日本カバレッジは Places より低い (体感 20-30%)。
補助的用途としてのみ使用 (Places が primary)。
"""

from __future__ import annotations

import json as json_mod
import logging
from dataclasses import dataclass, field
from typing import Any

import httpx


DEFAULT_OVERPASS_URL = "https://overpass-api.de/api/interpreter"
_logger = logging.getLogger(__name__)


# ─── Data types ────────────────────────────────────────────────────────


@dataclass
class OSMPlace:
    osm_id: int
    name: str
    lat: float
    lng: float
    tags: dict[str, str] = field(default_factory=dict)

    @property
    def address(self) -> str:
        """OSM addr:* tag から住所文字列を組み立てる。addr:full があれば優先。"""
        if full := self.tags.get("addr:full"):
            return full
        parts: list[str] = []
        for k in ("addr:state", "addr:province"):
            if v := self.tags.get(k):
                parts.append(v)
                break
        if city := self.tags.get("addr:city"):
            parts.append(city)
        if street := self.tags.get("addr:street"):
            parts.append(street)
        if num := self.tags.get("addr:housenumber"):
            parts.append(num)
        return "".join(parts)


# ─── Brand → OSM tag マッピング ────────────────────────────────────


# 日本 FC ブランド → OSM tag 辞書。複数 tag が該当する場合もあり。
_BRAND_TO_TAGS: dict[str, list[str]] = {
    # フィットネス
    "エニタイムフィットネス": ["leisure=fitness_centre"],
    "chocoZAP": ["leisure=fitness_centre"],
    "JOYFIT24": ["leisure=fitness_centre"],
    "FIT PLACE24": ["leisure=fitness_centre"],
    "ゴールドジム": ["leisure=fitness_centre"],
    "ルネサンス": ["leisure=fitness_centre", "leisure=sports_centre"],
    "コナミスポーツ": ["leisure=fitness_centre", "leisure=sports_centre"],
    "ティップネス": ["leisure=fitness_centre"],
    # コンビニ
    "セブン-イレブン": ["shop=convenience"],
    "セブンイレブン": ["shop=convenience"],
    "ファミリーマート": ["shop=convenience"],
    "ローソン": ["shop=convenience"],
    "デイリーヤマザキ": ["shop=convenience"],
    "ミニストップ": ["shop=convenience"],
    "セイコーマート": ["shop=convenience"],
    # ファストフード
    "マクドナルド": ["amenity=fast_food"],
    "モスバーガー": ["amenity=fast_food"],
    "KFC": ["amenity=fast_food"],
    "バーガーキング": ["amenity=fast_food"],
    "ロッテリア": ["amenity=fast_food"],
    # カフェ
    "スターバックス": ["amenity=cafe"],
    "スターバックス コーヒー": ["amenity=cafe"],
    "ドトール": ["amenity=cafe"],
    "タリーズ": ["amenity=cafe"],
    "コメダ珈琲": ["amenity=cafe"],
    "プロント": ["amenity=cafe"],
    # 牛丼/弁当
    "すき家": ["amenity=restaurant"],
    "吉野家": ["amenity=restaurant"],
    "松屋": ["amenity=restaurant"],
    # ファミレス
    "ガスト": ["amenity=restaurant"],
    "サイゼリヤ": ["amenity=restaurant"],
    # 小売/中古
    "TSUTAYA": ["shop=books", "shop=video"],
    "ブックオフ": ["shop=second_hand", "shop=books"],
    "ゲオ": ["shop=second_hand", "shop=video"],
    "セカンドストリート": ["shop=second_hand"],
    # ドラッグストア
    "マツモトキヨシ": ["shop=chemist", "shop=pharmacy"],
    "ウエルシア": ["shop=chemist", "shop=pharmacy"],
    "スギ薬局": ["shop=chemist", "shop=pharmacy"],
    "ツルハ": ["shop=chemist", "shop=pharmacy"],
    # Phase 27: 14-brand 補強 (Places quota 切れ時の OSM 代替経路)
    "カーブス": ["leisure=fitness_centre"],
    "業務スーパー": ["shop=supermarket"],
    "Itto個別指導学院": ["amenity=school"],  # 学習塾は OSM tag 標準なし
    "シャトレーゼ": ["shop=confectionery", "shop=bakery"],
    "ハードオフ": ["shop=second_hand"],
    "オフハウス": ["shop=second_hand"],
    "Kids Duo": ["amenity=kindergarten", "amenity=childcare"],
    "アップガレージ": ["shop=car_parts"],
    "カルビ丼とスン豆腐専門店韓丼": ["amenity=restaurant"],
    "Brand off": ["shop=second_hand", "shop=jewelry"],
}


def brand_to_osm_tags(brand: str) -> list[str]:
    """brand 名から OSM tag list を返す。未登録は空 list。"""
    return list(_BRAND_TO_TAGS.get(brand, []))


# ─── OverpassClient ──────────────────────────────────────────────────


@dataclass
class OverpassClient:
    base_url: str = DEFAULT_OVERPASS_URL
    timeout: float = 60.0
    transport: Any = None  # httpx.MockTransport 用

    async def query_by_tag(
        self,
        *,
        tag: str,
        bbox: tuple[float, float, float, float],
    ) -> list[OSMPlace]:
        """OSM tag + bbox (min_lat, min_lng, max_lat, max_lng) で node を取得。"""
        query = self._build_query(tag, bbox)
        kwargs: dict[str, Any] = {"timeout": self.timeout}
        if self.transport is not None:
            kwargs["transport"] = self.transport
        try:
            async with httpx.AsyncClient(**kwargs) as client:
                resp = await client.post(
                    self.base_url,
                    data={"data": query},
                    headers={
                        # Overpass API は無 User-Agent や application/* 期待が無いと
                        # 406 Not Acceptable を返すことがある。fair-use policy 準拠。
                        "User-Agent": "PI-ZZA/0.27 (https://github.com/clearclown/pizza)",
                        "Accept": "application/json",
                    },
                )
            if resp.status_code != 200:
                _logger.warning(
                    "Overpass API error %d: %s",
                    resp.status_code,
                    resp.text[:200],
                )
                return []
            data = resp.json()
        except Exception as exc:  # 通信エラー等は空返し
            _logger.warning("Overpass API failure: %s", exc)
            return []

        out: list[OSMPlace] = []
        for el in data.get("elements", []) or []:
            if el.get("type") != "node":
                continue
            tags = el.get("tags") or {}
            out.append(
                OSMPlace(
                    osm_id=int(el.get("id", 0)),
                    name=str(tags.get("name", "") or ""),
                    lat=float(el.get("lat") or 0.0),
                    lng=float(el.get("lon") or 0.0),
                    tags=tags,
                )
            )
        return out

    def _build_query(
        self, tag: str, bbox: tuple[float, float, float, float]
    ) -> str:
        """Overpass QL (Overpass クエリ言語) 生成。

        tag 例: 'leisure=fitness_centre' / 'shop=convenience'
        """
        min_lat, min_lng, max_lat, max_lng = bbox
        return (
            f'[out:json][timeout:30];'
            f'node[{tag}]({min_lat},{min_lng},{max_lat},{max_lng});'
            f'out body;'
        )


# ─── Recall KPI helper ────────────────────────────────────────────────


def compute_recall_ratio(
    *, places_count: int, reference_count: int
) -> float | None:
    """Places 取得件数 / 参照件数 (OSM or e-Stat) で recall 算出。

    reference が 0 なら比較不能で None 返却。
    """
    if reference_count <= 0:
        return None
    return round(places_count / reference_count, 3)
