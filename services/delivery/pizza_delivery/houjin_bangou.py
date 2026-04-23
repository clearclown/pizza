"""国税庁 法人番号公表サイト Web API v4 クライアント。

抽出した operator 名 (例: "株式会社Fast Fitness Japan") が本当に実在するかを
決定論的に検証するための Layer D 層。LLM の fabrication / 誤抽出を
「法人として実在する」という外部 ground-truth で弾く。

API:
  - Endpoint: https://api.houjin-bangou.nta.go.jp/4/name
  - 要 application ID (環境変数 HOUJIN_BANGOU_APP_ID)
  - type=12 を指定すると XML UTF-8 が返る (このクライアントは XML 固定)
  - history=0 は現時点の登録情報のみ
  - process コードで 現存 / 吸収合併消滅 / 商号変更 等を区別できる

参考: https://www.houjin-bangou.nta.go.jp/webapi/
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any
from xml.etree import ElementTree as ET

import httpx

from pizza_delivery.normalize import canonical_key


DEFAULT_BASE = "https://api.houjin-bangou.nta.go.jp/4"


# process code の active 判定 (国税庁仕様)
# 01: 新規、11-13: 商号変更 / 本店移転 / 代表者変更、31: 国外本店移転
# 71-72: 吸収合併による消滅 / 解散 → inactive
ACTIVE_PROCESS_CODES = frozenset({"01", "11", "12", "13", "21", "22", "31"})


@dataclass
class HoujinRecord:
    """1 件の法人レコード。"""

    corporate_number: str
    name: str
    address: str
    process: str
    update: str

    @property
    def active(self) -> bool:
        return self.process in ACTIVE_PROCESS_CODES


@dataclass
class HoujinSearchResult:
    query: str
    records: list[HoujinRecord] = field(default_factory=list)

    @property
    def found(self) -> bool:
        return len(self.records) > 0

    @property
    def active_records(self) -> list[HoujinRecord]:
        return [r for r in self.records if r.active]


@dataclass
class HoujinBangouClient:
    app_id: str = ""
    base_url: str = DEFAULT_BASE
    timeout: float = 10.0
    transport: Any = None  # httpx.MockTransport 注入用

    def __post_init__(self) -> None:
        if not self.app_id:
            self.app_id = os.getenv("HOUJIN_BANGOU_APP_ID", "")

    async def search_by_name(self, name: str) -> HoujinSearchResult:
        """法人名で検索し、ヒットした法人一覧を返す。

        空クエリは API を叩かず空結果で返す。APP_ID 未設定時は ValueError。
        """
        if not name or not name.strip():
            return HoujinSearchResult(query=name, records=[])
        if not self.app_id:
            raise ValueError("HOUJIN_BANGOU_APP_ID is not set")
        params = {
            "id": self.app_id,
            "name": name.strip(),
            "type": "12",  # XML UTF-8
            "history": "0",
        }
        url = f"{self.base_url.rstrip('/')}/name"
        kwargs: dict[str, Any] = {"timeout": self.timeout}
        if self.transport is not None:
            kwargs["transport"] = self.transport
        async with httpx.AsyncClient(**kwargs) as client:
            resp = await client.get(url, params=params)
        resp.raise_for_status()
        return HoujinSearchResult(query=name, records=_parse_xml(resp.text))


def _parse_xml(text: str) -> list[HoujinRecord]:
    """国税庁 API の XML レスポンスから HoujinRecord 配列を生成。"""
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []
    out: list[HoujinRecord] = []
    for corp in root.iter("corporation"):
        def _t(tag: str) -> str:
            return (corp.findtext(tag) or "").strip()

        address = _t("prefectureName") + _t("cityName") + _t("streetNumber")
        out.append(
            HoujinRecord(
                corporate_number=_t("corporateNumber"),
                name=_t("name"),
                address=address,
                process=_t("process"),
                update=_t("updateDate"),
            )
        )
    return out


# ─── verification glue ─────────────────────────────────────────────────


def _bigrams(s: str) -> list[str]:
    if len(s) < 2:
        return []
    return [s[i : i + 2] for i in range(len(s) - 1)]


def _name_similarity(a: str, b: str) -> float:
    """operator 名 vs 法人レコード name の類似度 [0, 1]。

    canonical_key (株式会社/㈱ 正規化 + 小文字) 後に完全一致なら 1.0。
    それ以外は bi-gram Jaccard。
    """
    ka, kb = canonical_key(a), canonical_key(b)
    if not ka or not kb:
        return 0.0
    if ka == kb:
        return 1.0
    # 包含関係 (片方がもう片方の substring) は高スコア
    if ka in kb or kb in ka:
        return 0.9
    ga, gb = set(_bigrams(ka)), set(_bigrams(kb))
    if not ga or not gb:
        return 0.0
    union = ga | gb
    if not union:
        return 0.0
    return len(ga & gb) / len(union)


def verify_operator(name: str, result: HoujinSearchResult) -> dict[str, Any]:
    """operator 抽出結果を HoujinSearchResult と照合し、検証情報を返す。

    戻り値:
      {
        exists: bool,              # active な法人がヒットしたか
        name_similarity: float,    # 最も近い active レコードとの類似度 [0, 1]
        best_match_name: str,
        best_match_number: str,    # 13 桁 法人番号
        active: bool,
      }
    """
    actives = result.active_records
    if not actives:
        return {
            "exists": False,
            "name_similarity": 0.0,
            "best_match_name": "",
            "best_match_number": "",
            "active": False,
        }
    best_score = 0.0
    best_rec: HoujinRecord | None = None
    for rec in actives:
        s = _name_similarity(name, rec.name)
        if s > best_score:
            best_score = s
            best_rec = rec
    return {
        "exists": True,
        "name_similarity": best_score,
        "best_match_name": best_rec.name if best_rec else "",
        "best_match_number": best_rec.corporate_number if best_rec else "",
        "active": bool(best_rec and best_rec.active),
    }
