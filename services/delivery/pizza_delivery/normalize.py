"""Operator 名の正規化ユーティリティ。

2 つの異なる表記 ("株式会社 FIT PLACE" と "株式会社FIT PLACE" 等) が同じ法人を
指すかを安定判定する。Phase 5 ChainDiscovery のグルーピング、CrossVerifier の
operator match 判定、ledger の dedupe 全てで使う。

core principles:
  - 決定的 (LLM 非依存)
  - idempotent: normalize(normalize(x)) == normalize(x)
  - symmetry: match(a, b) == match(b, a)
"""

from __future__ import annotations

import re
import unicodedata


# 会社サフィックス (株式会社の別表記)
_LEGACY_SUFFIXES = [
    ("（株）", "株式会社"),
    ("(株)", "株式会社"),
    ("㈱", "株式会社"),
    ("（有）", "有限会社"),
    ("(有)", "有限会社"),
    ("㈲", "有限会社"),
]

# 半角統一マップ (全角→半角)
_ZEN_HAN_MAP = {
    "　": " ",
    "！": "!",
    "？": "?",
    "（": "(",
    "）": ")",
    "・": "・",  # keep
}


def normalize_operator_name(name: str) -> str:
    """operator 名を正規化する。

    操作:
      1. 前後 trim
      2. (株) / ㈱ / （株） → 株式会社
      3. Unicode NFKC 正規化 (全角英数 → 半角、など)
      4. 連続空白 → 1 つ
      5. 末尾のみ: 句読点・記号を除去
      6. 法人接頭/接尾 (株式会社) 前後の余分な空白を取り除く

    例:
      "株式会社 FIT PLACE" → "株式会社FIT PLACE"
      "（株） コメダ" → "株式会社コメダ"
      "㈱テスト　" → "株式会社テスト"
      "株式会社 テスト 所在地 東京都..." → この関数は所在地切り取りは行わない
    """
    if not name:
        return ""

    s = name.strip()

    # Unicode NFKC: 全角→半角、濁点正規化
    s = unicodedata.normalize("NFKC", s)

    # 全角空白マップ
    for zen, han in _ZEN_HAN_MAP.items():
        s = s.replace(zen, han)

    # (株) / ㈱ → 株式会社
    for src, dst in _LEGACY_SUFFIXES:
        s = s.replace(src, dst)

    # 連続空白を 1 つに
    s = re.sub(r"\s+", " ", s)

    # 株式会社 の前後の空白を除去 ("株式会社 X" → "株式会社X")
    s = re.sub(r"株式会社\s+", "株式会社", s)
    s = re.sub(r"\s+株式会社", "株式会社", s)
    s = re.sub(r"有限会社\s+", "有限会社", s)
    s = re.sub(r"\s+有限会社", "有限会社", s)

    # 末尾の句読点除去
    s = s.rstrip("、。,.・")
    s = s.strip()

    return s


def operators_match(a: str, b: str) -> bool:
    """2 つの operator 名が同じ法人を指すかを判定する。

    - 正規化後に完全一致
    - または一方が他方の substring (3 文字以上)
    """
    na = normalize_operator_name(a)
    nb = normalize_operator_name(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    # 部分一致 (株式会社名の省略表記をマージ)
    if len(na) >= 3 and na in nb:
        return True
    if len(nb) >= 3 and nb in na:
        return True
    return False


def canonical_key(name: str) -> str:
    """operator グルーピング用の canonical key を生成。

    normalize_operator_name の結果を小文字化して、
    大文字小文字違いを吸収したキーを返す。
    """
    return normalize_operator_name(name).lower()
