"""Phase 16: 業種別 Territory 半径の知識 + 2 店舗 pair 判定 + カバーマップ。

territory_radius.yaml (internal/dough/knowledge/) を load し、
 - brand → industry → (dominant_min_m, dominant_typical_m, territory_max_m)
 - 2 店舗 pair を距離で分類 (DUPLICATE_SUSPECT / DOMINANT_CLUSTER / INDEPENDENT)
 - 既知 store 群から Places の未探索領域を判定する CoverMap

Top-down + Bottom-up 統合戦略の核:
  既知 store 群の territory 内は既発見とみなし、bottom-up scan を省略する。
"""

from __future__ import annotations

import enum
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import yaml


# ─── Data types ──────────────────────────────────────────────────────


class TerritoryClass(str, enum.Enum):
    DUPLICATE_SUSPECT = "duplicate_suspect"  # 距離が dominant_min_m 未満
    DOMINANT_CLUSTER = "dominant_cluster"    # min-max 範囲内 (ドミナント戦略の典型)
    INDEPENDENT = "independent"              # territory_max_m 超過 = 別商圏
    UNKNOWN = "unknown"                      # 未登録 brand 等、判定保留


@dataclass
class TerritoryRadius:
    brand: str
    industry: str
    strategy: str
    dominant_min_m: float
    dominant_typical_m: float
    territory_max_m: float


@dataclass
class TerritoryKnowledge:
    """territory_radius.yaml をパースした結果。"""
    industries: dict[str, dict]
    brands: dict[str, dict]


# ─── Loader ──────────────────────────────────────────────────────────


def _default_yaml_path() -> Path:
    here = Path(__file__).resolve()
    root = here.parents[3]
    return root / "internal" / "dough" / "knowledge" / "territory_radius.yaml"


def load_territory_knowledge(path: Path | str | None = None) -> TerritoryKnowledge:
    p = Path(path) if path else _default_yaml_path()
    with p.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return TerritoryKnowledge(
        industries=raw.get("industries") or {},
        brands=raw.get("brands") or {},
    )


# 起動時に 1 度 load (module-level cache)
_KB: TerritoryKnowledge | None = None


def _kb() -> TerritoryKnowledge:
    global _KB
    if _KB is None:
        _KB = load_territory_knowledge()
    return _KB


# ─── brand → radius lookup ─────────────────────────────────────────────


def territory_radius(brand: str) -> TerritoryRadius | None:
    """brand から industry を解決し、dominant_min_m 等を返す。

    優先順:
      1. brand の override (dominant_min_m が明記されていれば)
      2. brand の industry default
      3. brand が登録されていない → None
    """
    kb = _kb()
    b = kb.brands.get(brand)
    if b is None:
        return None
    industry = b.get("industry", "")
    ind = kb.industries.get(industry, {})

    def _pick(key: str) -> float:
        v = b.get(key)
        if v is None:
            v = ind.get(key)
        return float(v) if v is not None else 0.0

    return TerritoryRadius(
        brand=brand,
        industry=industry,
        strategy=str(ind.get("strategy", "unknown")),
        dominant_min_m=_pick("dominant_min_m"),
        dominant_typical_m=_pick("dominant_typical_m"),
        territory_max_m=_pick("territory_max_m"),
    )


# ─── distance ────────────────────────────────────────────────────────


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Haversine 距離 (m)。"""
    R = 6371000.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlng / 2) ** 2
    )
    return R * 2 * math.asin(math.sqrt(a))


# ─── pair classification ─────────────────────────────────────────────


def check_pair(
    *,
    brand: str,
    lat1: float, lng1: float,
    lat2: float, lng2: float,
) -> TerritoryClass:
    """2 店舗が同じ operator か別 territory かを territory 半径で分類。"""
    r = territory_radius(brand)
    d = _haversine_m(lat1, lng1, lat2, lng2)
    if r is None:
        # fallback: generic なしきい値
        if d < 50:
            return TerritoryClass.DUPLICATE_SUSPECT
        if d < 2000:
            return TerritoryClass.DOMINANT_CLUSTER
        return TerritoryClass.INDEPENDENT
    if d < r.dominant_min_m:
        return TerritoryClass.DUPLICATE_SUSPECT
    if d >= r.territory_max_m:
        return TerritoryClass.INDEPENDENT
    return TerritoryClass.DOMINANT_CLUSTER


# ─── CoverMap (既知店舗から territory 内かの判定) ───────────────────


@dataclass
class CoverMap:
    """既知店舗の緯度経度リスト + 1 店舗あたりの cover 半径 (m)。

    is_covered(lat, lng) が True なら「既に territory 内にある (既知店舗の近隣)」。
    Bottom-up scan でこれら領域は省略できる設計。
    """

    centers: list[tuple[float, float]]
    radius_m: float

    def is_covered(self, lat: float, lng: float) -> bool:
        for clat, clng in self.centers:
            if _haversine_m(clat, clng, lat, lng) <= self.radius_m:
                return True
        return False


def compute_cover_map(
    brand: str,
    stores: Iterable[dict],
) -> CoverMap:
    """brand の territory_typical_m を使って既知店舗群の cover map を生成。

    stores 各 dict は {'lat': ..., 'lng': ...} が必須。
    """
    r = territory_radius(brand)
    radius_m = r.dominant_typical_m if r else 1000.0
    centers: list[tuple[float, float]] = []
    for s in stores:
        lat = s.get("lat")
        lng = s.get("lng")
        if lat is None or lng is None:
            continue
        centers.append((float(lat), float(lng)))
    return CoverMap(centers=centers, radius_m=radius_m)
