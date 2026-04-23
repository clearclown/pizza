"""scrapling_fetcher: 決定論的 operator 抽出のテスト。

ネットワークは叩かず、HTML string を直接 extract する形で検証。
"""

from __future__ import annotations

import pytest

from pizza_delivery.scrapling_fetcher import (
    ExtractedOperator,
    _body_to_text,
    _clean_operator_name,
    _looks_rendered,
    build_google_lookup_url,
    extract_operator_from_html,
)


# ─── extract_operator_from_html ─────────────────────────


def test_extract_operator_match_unei_gaisha() -> None:
    html = "<html><body>運営会社: 株式会社サンプル</body></html>"
    r = extract_operator_from_html(html, source_url="https://x")
    assert r.name == "株式会社サンプル"
    assert r.pattern == "運営会社"
    assert r.confidence >= 0.9


def test_extract_operator_match_shamei() -> None:
    html = "<html><body><p>社名:株式会社テスト商事</p></body></html>"
    r = extract_operator_from_html(html)
    assert r.name == "株式会社テスト商事"
    assert r.pattern == "社名"


def test_extract_operator_match_kameiten() -> None:
    html = "店舗情報 加盟店 株式会社ABC商店 所在地..."
    r = extract_operator_from_html(html)
    assert r.name.startswith("株式会社ABC")


def test_extract_operator_blocks_franchisor() -> None:
    """本部 (モスフードサービス等) 単発 match は reject される。"""
    html = "運営: 株式会社モスフードサービス"
    r = extract_operator_from_html(html)
    assert r.name == ""  # ブロックリスト hit で空


def test_extract_operator_bare_pattern_fallback() -> None:
    """運営 context 無くとも 『株式会社XXX』があれば低 confidence で返す。"""
    html = "<p>店舗概要: 株式会社カネヤマ運営の支店です。</p>"
    r = extract_operator_from_html(html)
    assert r.name
    assert r.confidence <= 0.5


def test_extract_operator_extracts_phone_and_address() -> None:
    html = """
    <html>
      <div>運営会社: 株式会社ヤマダ</div>
      <div>電話: 03-1234-5678</div>
      <div>〒100-0001 東京都千代田区千代田1-1</div>
    </html>
    """
    r = extract_operator_from_html(html)
    assert r.name == "株式会社ヤマダ"
    assert r.phone == "03-1234-5678"
    assert "東京都" in r.address


def test_extract_operator_extracts_corporate_number() -> None:
    """社名 + 法人番号 併記を同時抽出。"""
    html = "運営会社: 株式会社テスト 法人番号: 1234567890123"
    r = extract_operator_from_html(html)
    assert r.name == "株式会社テスト"
    assert r.corporate_number == "1234567890123"


def test_extract_operator_empty_html() -> None:
    assert extract_operator_from_html("").empty is True


def test_extract_operator_caps_long_name() -> None:
    """regex の character class 上限 (25 chars) で社名が安全に cut される。"""
    long_name = "株式会社" + "あ" * 60
    html = f"運営会社: {long_name}"
    r = extract_operator_from_html(html)
    # 完全な 60 あ は取らず、regex cap で最大 29 文字程度に収まる
    assert r.name.startswith("株式会社")
    assert len(r.name) <= 30


# ─── _clean_operator_name ──────────────────────────────


def test_clean_operator_name_strips_symbols() -> None:
    assert _clean_operator_name("  「株式会社X」 ") == "株式会社X"


def test_clean_operator_name_removes_whitespace() -> None:
    assert _clean_operator_name("株式 会社 X") == "株式会社X"


def test_clean_operator_name_empty() -> None:
    assert _clean_operator_name("") == ""


# ─── _body_to_text ─────────────────────────────────────


def test_body_to_text_bytes_utf8() -> None:
    assert _body_to_text("こんにちは".encode()) == "こんにちは"


def test_body_to_text_bytes_cp932() -> None:
    b = "あいう".encode("cp932")
    out = _body_to_text(b)
    # cp932 なら decode に失敗して utf-8 replace で char が壊れるが crash はしない
    assert isinstance(out, str)


def test_body_to_text_none() -> None:
    assert _body_to_text(None) == ""


def test_body_to_text_str() -> None:
    assert _body_to_text("plain") == "plain"


# ─── _looks_rendered ─────────────────────────────────


def test_looks_rendered_empty_false() -> None:
    assert _looks_rendered("") is False


def test_looks_rendered_small_html_false() -> None:
    assert _looks_rendered("<html><body>Hi</body></html>") is False


def test_looks_rendered_japanese_text_true() -> None:
    # 500 日本語文字以上で rendered 判定
    html = "<html><body>" + ("あいうえお日本語" * 500) + "</body></html>"
    assert _looks_rendered(html) is True


# ─── build_google_lookup_url ─────────────────────────


def test_build_google_lookup_url_phone() -> None:
    url = build_google_lookup_url(phone="03-1234-5678", brand="モス")
    assert url.startswith("https://www.google.com/search?q=")
    # url-encoded 後でも phone と brand が含まれる
    assert "03-1234-5678" in url or "03-1234-5678".replace("-", "%2D") in url


def test_build_google_lookup_url_name_and_brand() -> None:
    url = build_google_lookup_url(name="モス六本木店", brand="モスバーガー")
    assert "google.com/search" in url
    assert "hl=ja" in url
