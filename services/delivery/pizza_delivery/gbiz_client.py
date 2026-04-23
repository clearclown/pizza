"""gBizINFO REST API クライアント (経済産業省)。

Houjin Web-API (国税庁) の APP_ID は発行に 1 か月かかる一方、
gBizINFO は Web フォーム即時発行で token が取れる補完経路。

API:
  - endpoint: https://info.gbiz.go.jp/hojin/v1/hojin/{corporateNumber}
              https://info.gbiz.go.jp/hojin/v1/hojin?name=...
  - 認証:   X-hojinInfo-api-token: <GBIZ_API_TOKEN>
  - 無料、登録即時
  - rate limit: 公開されていないが 2秒に 1 リクエスト推奨 (運用慣習)

認証が無い環境ではメソッドが空結果を返す。運用者は GBIZ_API_TOKEN env を
設定するだけで有効化できる。

参考:
  - https://info.gbiz.go.jp/hojin/api/index.html
  - 登録: https://info.gbiz.go.jp/hojin/api/api-reference/
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any

import httpx


DEFAULT_BASE = "https://info.gbiz.go.jp/hojin/v1"


@dataclass
class GBizRecord:
    """gBizINFO が返す法人 1 件 (公開フィールドのみ)。"""

    corporate_number: str
    name: str
    postal_code: str = ""
    address: str = ""
    representative_name: str = ""
    representative_title: str = ""
    capital_stock: str = ""
    employee_number: str = ""
    business_summary: str = ""
    kind: str = ""        # 法人種別
    update_date: str = ""
    close_date: str = ""  # 閉鎖日 (ある場合)


@dataclass
class GBizSearchResult:
    query: str
    records: list[GBizRecord] = field(default_factory=list)

    @property
    def found(self) -> bool:
        return bool(self.records)


# ─── Client ─────────────────────────────────────────────────────────


@dataclass
class GBizClient:
    token: str = ""
    base_url: str = DEFAULT_BASE
    timeout: float = 20.0
    transport: Any = None  # httpx.MockTransport 注入用

    def __post_init__(self) -> None:
        if not self.token:
            self.token = os.getenv("GBIZ_API_TOKEN", "")

    def ready(self) -> bool:
        return bool(self.token)

    def _headers(self) -> dict[str, str]:
        if not self.token:
            raise ValueError("GBIZ_API_TOKEN が未設定")
        return {
            "Accept": "application/json",
            "X-hojinInfo-api-token": self.token,
        }

    async def get_by_corporate_number(self, corporate_number: str) -> GBizRecord | None:
        """13 桁 法人番号で詳細を取得。未登録/エラー時 None。"""
        cn = (corporate_number or "").strip()
        if not cn or not cn.isdigit() or len(cn) != 13:
            return None
        if not self.token:
            return None
        url = f"{self.base_url.rstrip('/')}/hojin/{cn}"
        kwargs: dict[str, Any] = {"timeout": self.timeout}
        if self.transport is not None:
            kwargs["transport"] = self.transport
        async with httpx.AsyncClient(**kwargs) as client:
            r = await client.get(url, headers=self._headers())
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json() or {}
        return _record_from_hojin_detail(data.get("hojin-infos", [{}])[0])

    async def search_by_name(
        self, name: str, *, limit: int = 10
    ) -> GBizSearchResult:
        """法人名で部分一致検索。"""
        if not name or not name.strip() or not self.token:
            return GBizSearchResult(query=name, records=[])
        url = f"{self.base_url.rstrip('/')}/hojin"
        params = {"name": name.strip(), "limit": str(limit)}
        kwargs: dict[str, Any] = {"timeout": self.timeout}
        if self.transport is not None:
            kwargs["transport"] = self.transport
        async with httpx.AsyncClient(**kwargs) as client:
            r = await client.get(url, headers=self._headers(), params=params)
        if r.status_code >= 400:
            return GBizSearchResult(query=name, records=[])
        data = r.json() or {}
        records = [
            _record_from_hojin_detail(item)
            for item in (data.get("hojin-infos") or [])
            if item.get("corporate_number")
        ]
        return GBizSearchResult(query=name, records=records)


def _record_from_hojin_detail(h: dict) -> GBizRecord:
    """gBizINFO hojin-info dict → GBizRecord 変換。"""
    return GBizRecord(
        corporate_number=str(h.get("corporate_number") or "").strip(),
        name=str(h.get("name") or "").strip(),
        postal_code=str(h.get("postal_code") or "").strip(),
        address=str(h.get("location") or "").strip(),
        representative_name=str(h.get("representative_name") or "").strip(),
        representative_title=str(h.get("representative_title") or "").strip(),
        capital_stock=str(h.get("capital_stock") or "").strip(),
        employee_number=str(h.get("employee_number") or "").strip(),
        business_summary=str(h.get("business_summary") or "").strip(),
        kind=str(h.get("kind") or "").strip(),
        update_date=str(h.get("update_date") or "").strip(),
        close_date=str(h.get("close_date") or "").strip(),
    )


# ─── verify_operator (互換 I/F) ──────────────────────────────────────


async def verify_operator_via_gbiz(
    name: str,
    client: GBizClient | None = None,
) -> dict:
    """houjin_bangou.verify_operator と同じ dict 形式。gBizINFO 経由。

    GBIZ_API_TOKEN 未設定ならすべて False で返す (graceful)。
    """
    from pizza_delivery.houjin_bangou import _name_similarity

    client = client or GBizClient()
    if not client.ready():
        return {
            "exists": False,
            "name_similarity": 0.0,
            "best_match_name": "",
            "best_match_number": "",
            "active": False,
            "source": "gbiz_skipped",
        }
    result = await client.search_by_name(name, limit=10)
    if not result.found:
        return {
            "exists": False,
            "name_similarity": 0.0,
            "best_match_name": "",
            "best_match_number": "",
            "active": False,
            "source": "gbiz",
        }
    best_score = 0.0
    best: GBizRecord | None = None
    for rec in result.records:
        s = _name_similarity(name, rec.name)
        if s > best_score:
            best_score = s
            best = rec
    return {
        "exists": True,
        "name_similarity": best_score,
        "best_match_name": best.name if best else "",
        "best_match_number": best.corporate_number if best else "",
        "active": bool(best and not best.close_date),
        "source": "gbiz",
    }


# ─── CLI ────────────────────────────────────────────────────────────


def _main() -> None:
    import argparse
    import json

    ap = argparse.ArgumentParser(description="gBizINFO API クライアント")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_detail = sub.add_parser("detail", help="法人番号で詳細取得")
    p_detail.add_argument("--number", required=True, help="13 桁 法人番号")

    p_search = sub.add_parser("search", help="法人名検索")
    p_search.add_argument("--name", required=True)
    p_search.add_argument("--limit", type=int, default=10)

    p_ready = sub.add_parser("ready", help="GBIZ_API_TOKEN 設定確認")

    args = ap.parse_args()
    client = GBizClient()

    if args.cmd == "ready":
        print("ready:", client.ready())
        return

    async def run() -> None:
        if args.cmd == "detail":
            r = await client.get_by_corporate_number(args.number)
            print(json.dumps(r.__dict__ if r else None, ensure_ascii=False, indent=2))
        elif args.cmd == "search":
            res = await client.search_by_name(args.name, limit=args.limit)
            for rec in res.records:
                print(f"  {rec.corporate_number}  {rec.name}  [{rec.address}]")

    asyncio.run(run())


if __name__ == "__main__":
    _main()
