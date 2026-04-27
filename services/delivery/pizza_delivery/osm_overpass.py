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

import asyncio
import json as json_mod
import logging
import re
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
    osm_type: str = "node"

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


_BRAND_TO_NAMES: dict[str, list[str]] = {
    "カーブス": ["カーブス", "Curves"],
    "モスバーガー": ["モスバーガー", "MOS BURGER", "Mos Burger"],
    "業務スーパー": ["業務スーパー", "Gyomu Super"],
    "Itto個別指導学院": [
        "ITTO個別指導学院",
        "ITTO 個別指導学院",
        "ITTO個別指導",
        "ITTO 個別指導",
        "Itto個別指導学院",
        "個別指導塾ITTO",
    ],
    "エニタイムフィットネス": [
        "エニタイムフィットネス",
        "ANYTIME FITNESS",
        "Anytime Fitness",
    ],
    "コメダ珈琲": ["コメダ珈琲", "コメダ珈琲店", "Komeda"],
    "シャトレーゼ": ["シャトレーゼ", "Chateraise"],
    "ハードオフ": ["ハードオフ", "HARD OFF", "Hard Off"],
    "オフハウス": ["オフハウス", "OFF HOUSE", "Off House"],
    "Kids Duo": ["Kids Duo", "キッズデュオ"],
    "アップガレージ": ["アップガレージ", "UP GARAGE", "Up Garage"],
    "カルビ丼とスン豆腐専門店韓丼": ["カルビ丼とスン豆腐専門店韓丼", "韓丼"],
    "Brand off": ["BRAND OFF", "Brand Off", "ブランドオフ"],
    "TSUTAYA": ["TSUTAYA", "Tsutaya", "ツタヤ"],
}


def brand_to_osm_tags(brand: str) -> list[str]:
    """brand 名から OSM tag list を返す。未登録は空 list。"""
    return list(_BRAND_TO_TAGS.get(brand, []))


def brand_to_osm_names(brand: str) -> list[str]:
    """brand 名から OSM 上で使われやすい name/brand 表記を返す。"""
    names = list(_BRAND_TO_NAMES.get(brand, [brand]))
    return [n for n in names if n]


def brand_to_osm_name_pattern(brand: str) -> str:
    """Overpass name/brand regex 用の安全な alternation を返す。"""
    return "|".join(re.escape(n) for n in brand_to_osm_names(brand))


def _regex_literal(value: str) -> str:
    return re.escape(value).replace("\\ ", " ")


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
        name_pattern: str | None = None,
    ) -> list[OSMPlace]:
        """OSM tag + bbox (min_lat, min_lng, max_lat, max_lng) で n/w/r を取得。"""
        query = self._build_query(tag, bbox, name_pattern=name_pattern)
        return await self._query(query)

    async def query_by_key_pattern(
        self,
        *,
        key: str,
        pattern: str,
        bbox: tuple[float, float, float, float],
    ) -> list[OSMPlace]:
        """OSM key regex + bbox で n/w/r を取得する。

        全国 bbox では `amenity=...` などの broad tag と regex を複合すると
        Overpass 側で timeout しやすいため、ブランド名 discovery では key 単独
        regex を小分けに発行する。
        """
        query = self._build_key_pattern_query(key=key, pattern=pattern, bbox=bbox)
        return await self._query(query)

    async def _query(self, query: str) -> list[OSMPlace]:
        kwargs: dict[str, Any] = {"timeout": self.timeout}
        if self.transport is not None:
            kwargs["transport"] = self.transport
        data = None
        for attempt in range(2):
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
                if resp.status_code == 429 and attempt == 0:
                    if self.transport is None:
                        await asyncio.sleep(8.0)
                    continue
                if resp.status_code != 200:
                    _logger.warning(
                        "Overpass API error %d: %s",
                        resp.status_code,
                        resp.text[:200],
                    )
                    return []
                data = resp.json()
                break
            except Exception as exc:  # 通信エラー等は空返し
                if attempt == 0:
                    if self.transport is None:
                        await asyncio.sleep(3.0)
                    continue
                _logger.warning("Overpass API failure: %s", exc)
                return []
        if data is None:
            return []

        if remark := data.get("remark"):
            _logger.warning("Overpass API remark: %s", str(remark)[:200])
            return []

        out: list[OSMPlace] = []
        for el in data.get("elements", []) or []:
            osm_type = str(el.get("type") or "")
            if osm_type not in {"node", "way", "relation"}:
                continue
            tags = el.get("tags") or {}
            center = el.get("center") or {}
            lat = el.get("lat", center.get("lat"))
            lon = el.get("lon", center.get("lon"))
            if lat is None or lon is None:
                continue
            out.append(
                OSMPlace(
                    osm_id=int(el.get("id", 0)),
                    name=str(tags.get("name", "") or ""),
                    lat=float(lat),
                    lng=float(lon),
                    tags=tags,
                    osm_type=osm_type,
                )
            )
        return out

    def _build_query(
        self,
        tag: str,
        bbox: tuple[float, float, float, float],
        *,
        name_pattern: str | None = None,
    ) -> str:
        """Overpass QL (Overpass クエリ言語) 生成。

        tag 例: 'leisure=fitness_centre' / 'shop=convenience'
        """
        min_lat, min_lng, max_lat, max_lng = bbox
        selector = self._tag_selector(tag)
        bbox_expr = f'({min_lat},{min_lng},{max_lat},{max_lng})'
        if name_pattern:
            pattern = name_pattern.replace("\\", "\\\\").replace('"', '\\"')
            name_filters = (
                "name",
                "name:ja",
                "brand",
                "brand:ja",
                "operator",
                "operator:ja",
            )
            body = "".join(
                f'nwr{selector}["{key}"~"{pattern}",i]{bbox_expr};'
                for key in name_filters
            )
            return f'[out:json][timeout:30];({body});out center tags;'
        return (
            f'[out:json][timeout:30];'
            f'nwr{selector}{bbox_expr};'
            f'out center tags;'
        )

    def _build_key_pattern_query(
        self,
        *,
        key: str,
        pattern: str,
        bbox: tuple[float, float, float, float],
    ) -> str:
        min_lat, min_lng, max_lat, max_lng = bbox
        key = key.replace("\\", "\\\\").replace('"', '\\"')
        pattern = _regex_literal(pattern).replace("\\", "\\\\").replace('"', '\\"')
        return (
            f'[out:json][timeout:30];'
            f'nwr["{key}"~"{pattern}"]({min_lat},{min_lng},{max_lat},{max_lng});'
            f'out center tags;'
        )

    def _tag_selector(self, tag: str) -> str:
        """`key=value` を colon key でも安全な Overpass selector にする。"""
        if "=" not in tag:
            return f"[{tag}]"
        key, value = tag.split("=", 1)
        key = key.replace("\\", "\\\\").replace('"', '\\"')
        value = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'["{key}"="{value}"]'


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
