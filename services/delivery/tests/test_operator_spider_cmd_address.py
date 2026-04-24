"""operator_spider_cmd._normalize_japan_address / _address_key / _address_prefix_key の
住所正規化回帰テスト。

これらは pipeline stores.address (Places API 由来) と公式 HP scrape 由来の
住所 (表記揺れ多) を同一 key に揃えるためのもの。false positive を出さない
(= 違う住所を同一視しない) ことが最優先。
"""

from __future__ import annotations

import pytest

from pizza_delivery.commands.operator_spider_cmd import (
    _address_key,
    _address_prefix_key,
    _normalize_japan_address,
)


class TestNormalizeJapanAddress:
    def test_postal_code_removed(self):
        assert _normalize_japan_address("〒100-0001 東京都千代田区千代田1-1-1") \
            == "東京都千代田区千代田1-1-1"

    def test_postal_code_without_symbol(self):
        assert _normalize_japan_address("100-0001 東京都千代田区千代田1-1-1") \
            == "東京都千代田区千代田1-1-1"

    def test_chome_banchi_gou_normalized(self):
        assert _normalize_japan_address("東京都千代田区千代田1丁目1番1号") \
            == "東京都千代田区千代田1-1-1"

    def test_chome_only(self):
        assert _normalize_japan_address("愛知県名古屋市中区栄3丁目") \
            == "愛知県名古屋市中区栄3"

    def test_banchi_no(self):
        assert _normalize_japan_address("北海道札幌市中央区北1条西2番地の3") \
            == "北海道札幌市中央区北1条西2-3"

    def test_kanji_numbers(self):
        assert _normalize_japan_address("大阪府大阪市中央区本町二丁目3番4号") \
            == "大阪府大阪市中央区本町2-3-4"

    def test_kanji_ten(self):
        assert _normalize_japan_address("兵庫県神戸市中央区加納町十丁目") \
            == "兵庫県神戸市中央区加納町10"

    def test_zenkaku_numbers(self):
        assert _normalize_japan_address("福岡県福岡市博多区博多駅東１丁目１番１号") \
            == "福岡県福岡市博多区博多駅東1-1-1"

    def test_whitespace_removed(self):
        assert _normalize_japan_address("東京都 千代田区 千代田 1-1-1") \
            == "東京都千代田区千代田1-1-1"

    def test_hyphen_variants_unified(self):
        for dash in "‐‑‒–—―−ー":
            addr = f"東京都千代田区千代田1{dash}1{dash}1"
            assert _normalize_japan_address(addr) == "東京都千代田区千代田1-1-1", \
                f"failed for dash {dash!r}"

    def test_building_name_stripped(self):
        assert _normalize_japan_address("東京都港区芝浦3-9-1 芝浦ルネサイトタワー11F") \
            == "東京都港区芝浦3-9-1"

    def test_floor_only(self):
        assert _normalize_japan_address("東京都新宿区西新宿1-25-1 新宿センタービル42階") \
            == "東京都新宿区西新宿1-25-1"

    def test_empty_passthrough(self):
        assert _normalize_japan_address("") == ""

    def test_no_address_pattern(self):
        # pref 検出失敗でも crash しない
        out = _normalize_japan_address("雑文字列")
        assert isinstance(out, str)


class TestAddressKey:
    def test_same_after_building(self):
        """同一住所で建物名が違うだけのケースは同 key になる。"""
        a = _address_key("東京都港区芝浦3-9-1 芝浦ルネサイトタワー11F")
        b = _address_key("東京都港区芝浦3-9-1")
        assert a == b
        assert "東京都港区芝浦" in a
        assert a.endswith("3-9-1")

    def test_chome_vs_hyphen(self):
        """丁目表記とハイフン表記は同 key。"""
        a = _address_key("東京都千代田区千代田1丁目1番1号")
        b = _address_key("東京都千代田区千代田1-1-1")
        assert a == b

    def test_different_banchi_different_key(self):
        """番地が違えば別 key (false positive 防止)。"""
        a = _address_key("東京都千代田区千代田1-1-1")
        b = _address_key("東京都千代田区千代田2-2-2")
        assert a != b

    def test_different_pref_different_key(self):
        a = _address_key("東京都港区芝浦3-9-1")
        b = _address_key("大阪府大阪市北区芝浦3-9-1")
        assert a != b

    def test_empty(self):
        assert _address_key("") == ""


class TestAddressPrefixKey:
    def test_depth2_matches_block(self):
        """depth=2 で "1-2-3" と "1-2-4" が同一 block key になる。"""
        a = _address_prefix_key("東京都千代田区千代田1-2-3", depth=2)
        b = _address_prefix_key("東京都千代田区千代田1-2-4", depth=2)
        assert a == b
        assert a.endswith("1-2")

    def test_depth1_matches_chome(self):
        """depth=1 なら "1丁目" で同 block key (町丁目レベル fallback)。"""
        a = _address_prefix_key("東京都千代田区千代田1-2-3", depth=1)
        b = _address_prefix_key("東京都千代田区千代田1-9-9", depth=1)
        assert a == b
        assert a.endswith("1")

    def test_different_chome_different_key(self):
        a = _address_prefix_key("東京都千代田区千代田1-2-3", depth=1)
        b = _address_prefix_key("東京都千代田区千代田2-2-3", depth=1)
        assert a != b

    def test_different_town_different_key(self):
        """町名違い (town) は depth に関わらず別 key。"""
        a = _address_prefix_key("東京都千代田区千代田1-2-3", depth=2)
        b = _address_prefix_key("東京都千代田区丸の内1-2-3", depth=2)
        assert a != b
