"""revenue_extractor の決定論抽出テスト (Phase 25)。"""

from __future__ import annotations

from pizza_delivery.sources.revenue_extractor import (
    RevenueFinding,
    extract_revenue_from_html,
)


def test_revenue_empty_html() -> None:
    r = extract_revenue_from_html("")
    assert r.empty is True
    assert r.current_jpy == 0


def test_revenue_two_years_inline() -> None:
    html = "売上高: 2024年3月期 846億円\n売上高: 2023年3月期 798億円"
    r = extract_revenue_from_html(html)
    assert r.current_jpy == 84_600_000_000
    assert r.previous_jpy == 79_800_000_000
    assert "2024" in r.observed_at


def test_revenue_hyakuman_en() -> None:
    html = "売上高 1,234 百万円"
    r = extract_revenue_from_html(html)
    assert r.current_jpy == 1_234_000_000


def test_revenue_oku_en() -> None:
    html = "当社の売上高: 50億円 でした"
    r = extract_revenue_from_html(html)
    assert r.current_jpy == 5_000_000_000


def test_revenue_rejects_sub_oku() -> None:
    """1 億円未満は filter で除外 (FC 本部としてありえない)。"""
    html = "売上高: 500 万円"
    r = extract_revenue_from_html(html)
    assert r.empty is True


def test_revenue_rejects_too_large() -> None:
    """10 兆円超えは桁間違い扱いで除外。"""
    html = "売上高: 100,000,000 億円"  # = 10 京円
    r = extract_revenue_from_html(html)
    assert r.empty is True


def test_revenue_sort_by_year() -> None:
    """複数年順不同でも新しい年が current に来る。"""
    html = "売上高: 2022年3月期 500億円 売上高: 2024年3月期 846億円 売上高: 2023年3月期 600億円"
    r = extract_revenue_from_html(html)
    assert r.current_jpy == 84_600_000_000  # 2024
    assert "2024" in r.observed_at
