"""fc_recruitment の FC 募集 LP 抽出テスト (Phase 25)。"""

from __future__ import annotations

from pizza_delivery.sources.fc_recruitment import extract_fc_recruitment_url


def test_fc_lp_empty_html() -> None:
    r = extract_fc_recruitment_url("")
    assert r.empty is True


def test_fc_lp_high_score_anchor() -> None:
    html = '<a href="/franchise/">加盟店募集</a>'
    r = extract_fc_recruitment_url(html, base_url="https://example.co.jp")
    assert r.url == "https://example.co.jp/franchise/"
    assert r.confidence >= 0.7  # anchor HIGH 0.5 + href HIGH 0.3


def test_fc_lp_rejects_employment_ad() -> None:
    html = '<a href="/recruit/new-grad">新卒採用</a>'
    r = extract_fc_recruitment_url(html, base_url="https://example.co.jp")
    assert r.empty is True  # 採用情報 blocklist


def test_fc_lp_owner_recruitment_alt() -> None:
    html = '<a href="/owner/">独立開業</a>'
    r = extract_fc_recruitment_url(html, base_url="https://example.co.jp")
    assert r.url.endswith("/owner/")
    assert r.confidence >= 0.7


def test_fc_lp_picks_best_score() -> None:
    html = (
        '<a href="/contract/">加盟店</a>'
        '<a href="/franchise/">加盟店募集</a>'
    )
    r = extract_fc_recruitment_url(html, base_url="https://x.com")
    # 加盟店募集 (HIGH) + /franchise/ (HIGH) = 0.8 beats
    # 加盟店 (MED) + /contract/ (HIGH) = 0.5
    assert "/franchise/" in r.url
