"""Google Places API (New) クライアント (Phase 5 広域芋づる式展開用)。

pizza_delivery 側は Python 層。Go 側の internal/dough/places.go と並列に
存在するのは、Research Pipeline が直接 Places を叩くケースがあるため:

  - operator 発見後の広域店舗探索 (Text Search で "OperatorName 店舗" )
  - CrossVerifier の alt URL 取得 (place details の websiteUri)

環境変数:
  GOOGLE_MAPS_API_KEY  — .env から読む (Go と同じキーを使う)

API 参考:
  https://developers.google.com/maps/documentation/places/web-service/text-search
  https://developers.google.com/maps/documentation/places/web-service/place-details
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import httpx


# ─── Constants ─────────────────────────────────────────────────────────


DEFAULT_BASE_URL = "https://places.googleapis.com/v1"

# 店舗情報抽出に必要な FieldMask
DEFAULT_FIELD_MASK = (
    "places.id,"
    "places.displayName,"
    "places.formattedAddress,"
    "places.location,"
    "places.websiteUri,"
    "places.nationalPhoneNumber,"
    "places.internationalPhoneNumber,"
    "nextPageToken"
)


# ─── Data classes ──────────────────────────────────────────────────────


@dataclass
class PlaceRaw:
    """Places API /searchText レスポンスの 1 件分。"""

    place_id: str
    name: str
    address: str
    lat: float
    lng: float
    website_uri: str = ""
    phone: str = ""


@dataclass
class PlacesSearchResult:
    places: list[PlaceRaw] = field(default_factory=list)
    next_page_token: str = ""


# ─── Errors ────────────────────────────────────────────────────────────


class PlacesAPIError(Exception):
    """Places API が非 2xx または parsing エラー。"""

    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"Places API error status={status} body={body[:300]}")
        self.status = status
        self.body = body


# ─── Client ────────────────────────────────────────────────────────────


@dataclass
class PlacesClient:
    """Google Places API (New) 軽量クライアント。"""

    api_key: str = ""
    base_url: str = DEFAULT_BASE_URL
    field_mask: str = DEFAULT_FIELD_MASK
    language_code: str = "ja"
    region_code: str = "JP"
    timeout: float = 30.0

    def __post_init__(self) -> None:
        if not self.api_key:
            self.api_key = os.getenv("GOOGLE_MAPS_API_KEY", "")

    def _ensure_paid_google_enabled(self) -> None:
        """Block accidental Google Maps Platform spend unless explicitly enabled."""
        if self.base_url.rstrip("/") != DEFAULT_BASE_URL.rstrip("/"):
            return
        if os.getenv("PIZZA_ENABLE_PAID_GOOGLE_APIS") == "1":
            return
        raise PlacesAPIError(
            0,
            "paid Google Places API disabled; set PIZZA_ENABLE_PAID_GOOGLE_APIS=1 to opt in",
        )

    def _headers(self) -> dict[str, str]:
        if not self.api_key:
            raise PlacesAPIError(0, "GOOGLE_MAPS_API_KEY not set")
        self._ensure_paid_google_enabled()
        return {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": self.field_mask,
        }

    async def search_text(
        self,
        query: str,
        *,
        max_result_count: int = 20,
        page_token: str = "",
        location_bias: dict | None = None,
    ) -> PlacesSearchResult:
        """Text Search API を叩き、店舗一覧を返す。

        query は自然言語 (例: "株式会社AFJ Project 運営店舗" or "セブン-イレブン 東京都")。
        """
        if not query:
            raise ValueError("search_text: query is required")
        body: dict[str, Any] = {
            "textQuery": query,
            "languageCode": self.language_code,
            "regionCode": self.region_code,
            "maxResultCount": max(1, min(max_result_count, 20)),
        }
        if page_token:
            body["pageToken"] = page_token
        if location_bias:
            body["locationBias"] = location_bias

        url = f"{self.base_url.rstrip('/')}/places:searchText"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(url, headers=self._headers(), json=body)
        if resp.status_code < 200 or resp.status_code >= 300:
            raise PlacesAPIError(resp.status_code, resp.text)

        data = resp.json()
        places_raw = data.get("places", []) or []
        out = PlacesSearchResult(next_page_token=data.get("nextPageToken", ""))
        for p in places_raw:
            loc = p.get("location") or {}
            dn = p.get("displayName") or {}
            out.places.append(
                PlaceRaw(
                    place_id=p.get("id", ""),
                    name=(dn.get("text") or ""),
                    address=(p.get("formattedAddress") or ""),
                    lat=float(loc.get("latitude") or 0.0),
                    lng=float(loc.get("longitude") or 0.0),
                    website_uri=(p.get("websiteUri") or ""),
                    phone=(
                        p.get("nationalPhoneNumber")
                        or p.get("internationalPhoneNumber")
                        or ""
                    ),
                )
            )
        return out

    async def get_place_details(
        self,
        place_id: str,
        *,
        fields: str = "",
    ) -> PlaceRaw | None:
        """単一 place_id の詳細 (phone / website / reviews 等) を取得。

        Google Places API Details `/places/{id}` endpoint。FC 加盟店特定で
        重要な phone / websiteUri / formattedAddress / displayName を取る。

        fields 省略時は FC 特定用の標準 field mask を使用。
        """
        if not place_id:
            return None
        if not fields:
            fields = (
                "id,displayName,formattedAddress,nationalPhoneNumber,"
                "internationalPhoneNumber,websiteUri,location,businessStatus,"
                "types,primaryType,primaryTypeDisplayName"
            )
        self._ensure_paid_google_enabled()
        url = f"{self.base_url.rstrip('/')}/places/{place_id}"
        headers = {
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": fields,
            "Accept-Language": self.language_code,
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(url, headers=headers)
        if resp.status_code == 404:
            return None
        if resp.status_code < 200 or resp.status_code >= 300:
            raise PlacesAPIError(resp.status_code, resp.text)
        p = resp.json() or {}
        loc = p.get("location") or {}
        dn = p.get("displayName") or {}
        return PlaceRaw(
            place_id=p.get("id", place_id),
            name=(dn.get("text") or ""),
            address=(p.get("formattedAddress") or ""),
            lat=float(loc.get("latitude") or 0.0),
            lng=float(loc.get("longitude") or 0.0),
            website_uri=(p.get("websiteUri") or ""),
            phone=(
                p.get("nationalPhoneNumber")
                or p.get("internationalPhoneNumber")
                or ""
            ),
        )

    async def search_by_operator(
        self,
        operator_name: str,
        *,
        area_hint: str = "",
        max_result_count: int = 20,
    ) -> list[PlaceRaw]:
        """operator 名で関連店舗/拠点を検索する (広域芋づる式の入口)。

        area_hint があれば "<operator> 店舗 <area>" の形で検索範囲を絞る。
        """
        if not operator_name:
            return []
        parts = [operator_name, "店舗"]
        if area_hint:
            parts.append(area_hint)
        query = " ".join(parts)
        result = await self.search_text(query, max_result_count=max_result_count)
        return result.places

    async def close(self) -> None:
        """httpx は AsyncClient を関数内で open/close しているため現状は no-op。"""
        return
