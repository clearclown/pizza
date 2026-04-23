"""neologdn + rapidfuzz 統合後の normalize.operators_match 強化テスト。"""

from __future__ import annotations

import pytest

from pizza_delivery.normalize import (
    canonical_key,
    normalize_operator_name,
    operators_match,
)


# ─── neologdn: 全角/半角/繰り返し記号の吸収 ──────────────────────────


@pytest.mark.parametrize(
    "raw, normalized_contains",
    [
        # 全角記号 → 半角
        ("株式会社　テスト", "株式会社テスト"),    # 全角空白
        ("ＡＢＣ株式会社", "ABC株式会社"),             # 全角英字
        ("株式会社 テスト", "株式会社テスト"),          # 半角空白は 株式会社 後の空白除去
        # 波ダッシュ・長音類
        ("株式会社スーパーマーケット", "株式会社スーパーマーケット"),
    ],
)
def test_neologdn_normalizes_variants(raw, normalized_contains) -> None:
    got = normalize_operator_name(raw)
    assert normalized_contains in got, f"got {got!r}"


# ─── rapidfuzz: token_set_ratio でゆれマッチ ─────────────────────────


@pytest.mark.parametrize(
    "a, b, should_match",
    [
        # 表記ゆれ (半角/全角)
        ("株式会社アトラクト", "株式会社ＡＴＴＲＡＣＴ", False),  # カタカナとアルファベットは別物扱い OK
        # 完全一致
        ("株式会社エムデジ", "株式会社エムデジ", True),
        # 短縮形
        ("川勝商事株式会社", "川勝商事", True),
        ("㈱川勝商事", "川勝商事株式会社", True),
        # 語順違い (rapidfuzz token_set_ratio で吸収)
        ("株式会社 Fast Fitness Japan", "Fast Fitness Japan 株式会社", True),
    ],
)
def test_operators_match_with_rapidfuzz(a, b, should_match) -> None:
    got = operators_match(a, b)
    assert got == should_match, (
        f"operators_match({a!r}, {b!r}) = {got}, want {should_match}"
    )


def test_operators_match_rejects_different_companies() -> None:
    assert operators_match("株式会社アトラクト", "株式会社エムデジ") is False
    assert operators_match("株式会社トピーレック", "株式会社アズ") is False


def test_canonical_key_is_lowercase() -> None:
    # canonical_key は小文字化するので、大文字違い operator も同一 key
    k1 = canonical_key("株式会社ABC")
    k2 = canonical_key("株式会社abc")
    assert k1 == k2
