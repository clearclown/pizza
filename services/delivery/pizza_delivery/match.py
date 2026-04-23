"""決定論的突合エンジン (Phase 8)。

Places API の結果 (Top-down) と SQLite stores (Bottom-up) を 3 段階で突合:

  1. place_id 完全一致 — 最強 (score=1.0)
  2. 住所 normalize + bi-gram Jaccard — 表記揺れに頑健
  3. 緯度経度 Haversine — 住所文字列が全く違う場合の最後の救済

LLM 不使用、すべて決定論。merge_all で 3 段を上から適用、前段でマッチしたものは
後段のループから除外する。
"""

from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable


# ─── address normalization ────────────────────────────────────────────


# 末尾建物情報のトークン (これらを含む token 丸ごと skip)
_BUILDING_HINTS = ("ビル", "マンション", "タワー", "階", "ハイツ", "コーポ", "プラザ")
# 末尾フロア表記
_FLOOR_RE = re.compile(r"^\d+[FB]?F?$|^B\d+F$")

# 末尾フラグメント「X号」の「号」を削る
_TRIM_SUFFIX_RE = re.compile(r"(\d+)号$")


def normalize_address(raw: str) -> str:
    """住所を突合しやすい形に正規化。

    - 〒XXX-XXXX を除去
    - 全角→半角 (NFKC)
    - 「X丁目」「X番」→ "X-"、末尾「X号」→ "X"
    - 空白 token に建物/階情報を含むものは除外
    - 連結
    """
    if not raw:
        return ""
    s = raw.strip()
    s = unicodedata.normalize("NFKC", s)
    # 〒
    s = re.sub(r"〒\s*\d{3}-?\d{4}\s*", "", s)

    tokens = re.split(r"\s+", s)
    cleaned: list[str] = []
    for tok in tokens:
        if not tok:
            continue
        # 建物ヒント含む token を skip
        if any(h in tok for h in _BUILDING_HINTS):
            continue
        # 純フロア表記 (例: "2F", "B1F") skip
        if _FLOOR_RE.match(tok):
            continue
        cleaned.append(tok)

    out = "".join(cleaned)
    # 丁目 / 番 / 号 の正規化
    out = re.sub(r"(\d+)丁目", r"\1-", out)
    out = re.sub(r"(\d+)番地?", r"\1-", out)
    out = _TRIM_SUFFIX_RE.sub(r"\1", out)
    out = out.rstrip("-").strip()
    return out


# ─── ParsedAddress + parse_address (Phase 10) ─────────────────────────


@dataclass
class ParsedAddress:
    pref: str           # 都道府県 ("東京都" / "大阪府" / "北海道" / "XX県")
    city: str           # 市区町村 (政令市は "〇〇市〇〇区" まで保持)
    rest: str           # 町名 + 番地 以降
    raw_normalized: str  # normalize_address 済の文字列全体


_PREFS = (
    "北海道",
    "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)

# 政令指定都市 (〇〇市〇〇区 で city とみなす)
_SEIREI_CITIES = (
    "札幌市", "仙台市", "さいたま市", "千葉市", "横浜市", "川崎市", "相模原市",
    "新潟市", "静岡市", "浜松市", "名古屋市", "京都市", "大阪市", "堺市",
    "神戸市", "岡山市", "広島市", "北九州市", "福岡市", "熊本市",
)

_CITY_SUFFIX_RE = re.compile(r"^(.+?(?:市|区|郡|町|村))(.*)$")


def parse_address(raw: str) -> ParsedAddress:
    """住所を pref + city + rest に分割する決定論 parser。

    - 都道府県は 47 の前方一致で検出
    - 政令指定都市はさらに区まで city として扱う (例: 大阪市中央区)
    - 東京 23 区は pref="東京都" + city="渋谷区" 等
    """
    norm = normalize_address(raw)
    if not norm:
        return ParsedAddress(pref="", city="", rest="", raw_normalized="")

    pref = ""
    after_pref = norm
    for p in _PREFS:
        if norm.startswith(p):
            pref = p
            after_pref = norm[len(p):]
            break

    # 政令指定都市の場合、"〇〇市〇〇区" まで吸収
    city = ""
    rest = after_pref
    for seirei in _SEIREI_CITIES:
        if after_pref.startswith(seirei):
            # seirei の後に「区」があるか見る
            after_seirei = after_pref[len(seirei):]
            m = re.match(r"^([^0-90-9\-‐-―−]+?区)(.*)$", after_seirei)
            if m:
                city = seirei + m.group(1)
                rest = m.group(2)
            else:
                city = seirei
                rest = after_seirei
            break

    if not city:
        # 一般市区町村: 最初の (市|区|郡|町|村) までを city とする
        m = _CITY_SUFFIX_RE.match(after_pref)
        if m:
            city = m.group(1)
            rest = m.group(2)

    return ParsedAddress(pref=pref, city=city, rest=rest.strip(), raw_normalized=norm)


# ─── bi-gram Jaccard ──────────────────────────────────────────────────


def _bigram_jaccard(a: str, b: str) -> float:
    if len(a) < 2 or len(b) < 2:
        return 0.0
    ga = {a[i : i + 2] for i in range(len(a) - 1)}
    gb = {b[i : i + 2] for i in range(len(b) - 1)}
    if not ga or not gb:
        return 0.0
    return len(ga & gb) / len(ga | gb)


# ─── Haversine ─────────────────────────────────────────────────────────


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """2 点間の球面距離 (m)。"""
    R = 6371000.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    return R * c


# ─── match candidate / result ─────────────────────────────────────────


@dataclass
class MatchCandidate:
    top_id: str
    bottom_id: str
    score: float        # 1.0 最強。proximity のときは距離 (m)
    strategy: str       # place_id | address | proximity


@dataclass
class MergeResult:
    matches: list[MatchCandidate]
    unmatched_top: list[dict]
    unmatched_bottom: list[dict]


# ─── match strategies ─────────────────────────────────────────────────


def match_by_place_id(
    top: Iterable[dict], bottom: Iterable[dict]
) -> list[MatchCandidate]:
    """place_id 完全一致。"""
    bottom_by_id = {b.get("place_id", ""): b for b in bottom if b.get("place_id")}
    out: list[MatchCandidate] = []
    for t in top:
        tid = t.get("place_id", "")
        if tid and tid in bottom_by_id:
            out.append(
                MatchCandidate(top_id=tid, bottom_id=tid, score=1.0, strategy="place_id")
            )
    return out


def match_by_address(
    top: Iterable[dict],
    bottom: Iterable[dict],
    *,
    threshold: float = 0.8,
) -> list[MatchCandidate]:
    """住所 normalize + pref/city strict gate + bi-gram Jaccard。

    Phase 10 強化:
      - pref (都道府県) と city (市区町村) が両方特定できた場合、
        両者の完全一致を必須 gate とする (false positive を大幅削減)
      - pref or city が不明な側はこの gate を skip (下位互換)
    最高スコアを採用 (1 top : 1 bottom)。
    """
    bottom_list = list(bottom)
    out: list[MatchCandidate] = []
    taken_bottom: set[str] = set()
    for t in top:
        t_parsed = parse_address(t.get("address", ""))
        if not t_parsed.raw_normalized:
            continue
        best: MatchCandidate | None = None
        for b in bottom_list:
            bid = b.get("place_id", "")
            if bid in taken_bottom:
                continue
            b_parsed = parse_address(b.get("address", ""))
            if not b_parsed.raw_normalized:
                continue
            # pref strict gate (両方特定できた場合)
            if t_parsed.pref and b_parsed.pref and t_parsed.pref != b_parsed.pref:
                continue
            # city strict gate (両方特定できた場合)
            if t_parsed.city and b_parsed.city and t_parsed.city != b_parsed.city:
                continue
            score = _bigram_jaccard(t_parsed.raw_normalized, b_parsed.raw_normalized)
            if score >= threshold and (best is None or score > best.score):
                best = MatchCandidate(
                    top_id=t.get("place_id", ""),
                    bottom_id=bid,
                    score=score,
                    strategy="address",
                )
        if best:
            out.append(best)
            taken_bottom.add(best.bottom_id)
    return out


def match_by_proximity(
    top: Iterable[dict],
    bottom: Iterable[dict],
    *,
    radius_m: float = 100.0,
) -> list[MatchCandidate]:
    """緯度経度 Haversine。半径内で距離最短を採用。"""
    bottom_list = list(bottom)
    out: list[MatchCandidate] = []
    taken_bottom: set[str] = set()
    for t in top:
        tlat = t.get("lat")
        tlng = t.get("lng")
        if tlat is None or tlng is None:
            continue
        best: MatchCandidate | None = None
        best_d: float = radius_m + 1
        for b in bottom_list:
            bid = b.get("place_id", "")
            if bid in taken_bottom:
                continue
            blat, blng = b.get("lat"), b.get("lng")
            if blat is None or blng is None:
                continue
            d = haversine_m(tlat, tlng, blat, blng)
            if d <= radius_m and d < best_d:
                best = MatchCandidate(
                    top_id=t.get("place_id", ""),
                    bottom_id=bid,
                    score=d,  # proximity は距離
                    strategy="proximity",
                )
                best_d = d
        if best:
            out.append(best)
            taken_bottom.add(best.bottom_id)
    return out


# ─── combined merge ───────────────────────────────────────────────────


def merge_all(
    top: Iterable[dict],
    bottom: Iterable[dict],
    *,
    addr_threshold: float = 0.8,
    radius_m: float = 100.0,
) -> MergeResult:
    """3 段階突合を上から適用。前段の結果は後段から除外。"""
    top_list = list(top)
    bottom_list = list(bottom)

    matched_top: set[str] = set()
    matched_bottom: set[str] = set()
    matches: list[MatchCandidate] = []

    # Layer 1: place_id
    for m in match_by_place_id(top_list, bottom_list):
        matches.append(m)
        matched_top.add(m.top_id)
        matched_bottom.add(m.bottom_id)

    # Layer 2: address
    rt = [t for t in top_list if t.get("place_id", "") not in matched_top]
    rb = [b for b in bottom_list if b.get("place_id", "") not in matched_bottom]
    for m in match_by_address(rt, rb, threshold=addr_threshold):
        matches.append(m)
        matched_top.add(m.top_id)
        matched_bottom.add(m.bottom_id)

    # Layer 3: proximity
    rt = [t for t in top_list if t.get("place_id", "") not in matched_top]
    rb = [b for b in bottom_list if b.get("place_id", "") not in matched_bottom]
    for m in match_by_proximity(rt, rb, radius_m=radius_m):
        matches.append(m)
        matched_top.add(m.top_id)
        matched_bottom.add(m.bottom_id)

    unmatched_top = [
        t for t in top_list if t.get("place_id", "") not in matched_top
    ]
    unmatched_bottom = [
        b for b in bottom_list if b.get("place_id", "") not in matched_bottom
    ]
    return MergeResult(
        matches=matches,
        unmatched_top=unmatched_top,
        unmatched_bottom=unmatched_bottom,
    )
