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

    Phase 9: neologdn を優先的に使用 (mecab-neologd 準拠、全角/半角/繰り返し
    記号/波ダッシュ等を広くカバー)。未インストール時は従来の NFKC。

    操作:
      1. 前後 trim
      2. neologdn.normalize または Unicode NFKC (全角→半角、濁点正規化)
      3. (株) / ㈱ / （株） → 株式会社
      4. 連続空白 → 1 つ
      5. 末尾のみ: 句読点・記号を除去
      6. 法人接頭/接尾 (株式会社) 前後の余分な空白を取り除く
    """
    if not name:
        return ""

    s = name.strip()

    # neologdn が入っていれば優先、無ければ NFKC
    try:
        import neologdn  # noqa

        s = neologdn.normalize(s)
    except ImportError:
        s = unicodedata.normalize("NFKC", s)
    else:
        # neologdn 後に念のため NFKC (㈱→(株) を確実に)
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


def _strip_kabushiki(name: str) -> str:
    """前後の '株式会社' / '有限会社' を strip して core 部分を返す。

    normalize_operator_name 適用後に呼ぶ想定 (㈱ → 株式会社 に統一済)。
    """
    s = name
    for tag in ("株式会社", "有限会社"):
        while s.startswith(tag):
            s = s[len(tag):]
        while s.endswith(tag):
            s = s[: -len(tag)]
    return s.strip()


def operators_match(a: str, b: str) -> bool:
    """2 つの operator 名が同じ法人を指すかを判定する。

    Phase 9 強化:
      1. normalize_operator_name 後の完全一致
      2. 株式会社 を前後 strip した core 部分で完全一致 (prefix/suffix ゆれ吸収)
         例: "株式会社川勝商事" ⇔ "川勝商事株式会社"
      3. 一方が他方の substring (3 文字以上)
      4. rapidfuzz.token_set_ratio / ratio で部分類似度 ≥ 88
    """
    na = normalize_operator_name(a)
    nb = normalize_operator_name(b)
    if not na or not nb:
        return False
    if na == nb:
        return True

    # 2. 株式会社 を剥がしたコア部分で比較
    core_a = _strip_kabushiki(na)
    core_b = _strip_kabushiki(nb)
    if core_a and core_b and core_a == core_b:
        return True

    # 3. 部分一致 (株式会社名の省略表記をマージ)
    if len(na) >= 3 and na in nb:
        return True
    if len(nb) >= 3 and nb in na:
        return True

    # 4. rapidfuzz による類似度判定
    try:
        from rapidfuzz import fuzz  # optional, 未インストール時は fallback

        if len(na) >= 4 and len(nb) >= 4:
            # token_set_ratio: 空白区切り token の集合一致 (語順違い吸収)
            if fuzz.token_set_ratio(na, nb) >= 88:
                return True
            # core 同士の character-level ratio
            if core_a and core_b and fuzz.ratio(core_a, core_b) >= 88:
                return True
    except ImportError:
        pass
    return False


def canonical_key(name: str) -> str:
    """operator グルーピング用の canonical key を生成。

    normalize_operator_name の結果を小文字化して、
    大文字小文字違いを吸収したキーを返す。
    """
    return normalize_operator_name(name).lower()
