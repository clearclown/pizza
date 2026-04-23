"""Unit tests for operator name normalization."""

from __future__ import annotations

import pytest

from pizza_delivery.normalize import (
    canonical_key,
    normalize_operator_name,
    operators_match,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("株式会社 FIT PLACE", "株式会社FIT PLACE"),
        ("株式会社  FIT PLACE", "株式会社FIT PLACE"),
        ("株式会社 コメダ", "株式会社コメダ"),
        ("（株） コメダ", "株式会社コメダ"),
        ("(株)コメダ", "株式会社コメダ"),
        ("㈱コメダ", "株式会社コメダ"),
        ("株式会社コメダ", "株式会社コメダ"),
        # 全角英数 → 半角 (NFKC)
        ("株式会社ABC", "株式会社ABC"),
        # 全角空白 → 半角 → 除去
        ("株式会社　コメダ", "株式会社コメダ"),
        # 前後 trim
        ("  株式会社コメダ  ", "株式会社コメダ"),
        # 空入力
        ("", ""),
        # 有限会社
        ("(有)テストホーム", "有限会社テストホーム"),
        # Suffix pattern (先に "株式会社" が後に来る)
        ("日本マクドナルド株式会社", "日本マクドナルド株式会社"),
        ("日本マクドナルド 株式会社", "日本マクドナルド株式会社"),
        # 末尾句読点
        ("株式会社テスト、", "株式会社テスト"),
        ("株式会社テスト。", "株式会社テスト"),
    ],
)
def test_normalize_operator_name(raw: str, expected: str) -> None:
    assert normalize_operator_name(raw) == expected


def test_normalize_is_idempotent() -> None:
    for s in ["株式会社A", "(株)B", "㈱C", "  株式会社 D  "]:
        once = normalize_operator_name(s)
        twice = normalize_operator_name(once)
        assert once == twice


@pytest.mark.parametrize(
    "a, b, expected",
    [
        ("株式会社 FIT PLACE", "株式会社FIT PLACE", True),
        ("(株)コメダ", "株式会社コメダ", True),
        ("㈱AFJ Project", "株式会社AFJ Project", True),
        ("株式会社AFJ Project", "AFJ Project", True),  # 部分一致
        ("株式会社A", "株式会社B", False),
        ("株式会社 コメダ", "株式会社プレナス", False),
        ("", "株式会社A", False),
    ],
)
def test_operators_match(a: str, b: str, expected: bool) -> None:
    assert operators_match(a, b) is expected


def test_operators_match_is_symmetric() -> None:
    pairs = [
        ("株式会社 FIT PLACE", "株式会社FIT PLACE"),
        ("(株)A", "株式会社A"),
        ("株式会社AFJ Project", "AFJ Project"),
    ]
    for a, b in pairs:
        assert operators_match(a, b) == operators_match(b, a)


def test_canonical_key_case_insensitive() -> None:
    k1 = canonical_key("株式会社AFJ Project")
    k2 = canonical_key("株式会社afj project")
    assert k1 == k2


def test_canonical_key_dedupes_same_operator() -> None:
    keys = {
        canonical_key("株式会社 FIT PLACE"),
        canonical_key("株式会社FIT PLACE"),
        canonical_key("(株)FIT PLACE"),
        canonical_key("㈱FIT PLACE"),
    }
    assert len(keys) == 1, f"all 4 should normalize to same key, got {keys}"
