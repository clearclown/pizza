"""pizza import-megajii-csv の pure 関数 (brand split / section parse / address)
回帰テスト。LLM / 国税庁 / ORM 経路は integration test 側で確認する。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pizza_delivery.commands.import_megajii_csv import (
    canonicalize_brand,
    parse_int_plain,
    parse_int_yen_thousand,
    prefecture_from_address,
    read_tsv,
    split_brands,
)


class TestBrandNormalization:
    def test_alias_mapped(self):
        assert canonicalize_brand("ITTO") == "Itto個別指導学院"
        assert canonicalize_brand("ITTO個別指導学院") == "Itto個別指導学院"
        assert canonicalize_brand("HARD OFF") == "ハードオフ"
        assert canonicalize_brand("BRAND OFF") == "Brand off"
        assert canonicalize_brand("Chateraise") == "シャトレーゼ"

    def test_unknown_passthrough(self):
        assert canonicalize_brand("モスバーガー") == "モスバーガー"
        assert canonicalize_brand("ユニークブランド") == "ユニークブランド"

    def test_strip_whitespace(self):
        assert canonicalize_brand("  モスバーガー  ") == "モスバーガー"


class TestSplitBrands:
    def test_middot_separator(self):
        assert split_brands("モスバーガー・ミスタードーナツ・築地銀だこ") == [
            "モスバーガー", "ミスタードーナツ", "築地銀だこ",
        ]

    def test_mixed_separators(self):
        assert split_brands("KFC・ピザハット、カプリチョーザ") == [
            "KFC", "ピザハット", "カプリチョーザ",
        ]

    def test_X_filtered(self):
        assert split_brands("X") == []
        assert split_brands("") == []

    def test_dedup(self):
        assert split_brands("モスバーガー・モスバーガー") == ["モスバーガー"]

    def test_alias_applied(self):
        assert "ハードオフ" in split_brands("HARD OFF・BOOKOFF")


class TestIntParsing:
    def test_yen_thousand_to_yen(self):
        assert parse_int_yen_thousand("650,000") == 650_000_000

    def test_yen_thousand_empty(self):
        assert parse_int_yen_thousand("") == 0
        assert parse_int_yen_thousand("   ") == 0

    def test_yen_thousand_invalid(self):
        assert parse_int_yen_thousand("不明") == 0
        assert parse_int_yen_thousand("ー") == 0

    def test_plain_int(self):
        assert parse_int_plain("23") == 23
        assert parse_int_plain("1,234") == 1234
        assert parse_int_plain("") == 0


class TestPrefecture:
    def test_tokyo(self):
        assert prefecture_from_address("東京都港区芝浦2-14-4") == "東京都"

    def test_hokkaido(self):
        assert prefecture_from_address("北海道苫小牧市若草町5-3-5") == "北海道"

    def test_osaka_fu(self):
        assert prefecture_from_address("大阪府大阪市北区芝田2-3-19") == "大阪府"

    def test_prefecture_kanji(self):
        assert prefecture_from_address("愛知県名古屋市中区錦二丁目") == "愛知県"
        assert prefecture_from_address("神奈川県平塚市八重咲町") == "神奈川県"

    def test_empty(self):
        assert prefecture_from_address("") == ""


class TestReadTSV:
    def test_parse_two_sections(self, tmp_path: Path):
        p = tmp_path / "sample.tsv"
        p.write_text(
            "# section: megajii\n"
            "大和フーヅ\tパン小売業\t69\t山崎\t埼玉県熊谷市\t7630476\t642797\t5891334\thttps://example.jp/\tミスタードーナツ・モスバーガー・築地銀だこ\n"
            "# section: franchisor\n"
            "株式会社モスフードサービス\tモスバーガー\tハンバーガー店\t1266\t中村\t東京都品川区\t69153000\t66281000\t59751000\thttps://mos.jp/\tモスバーガー\thttps://mos.jp/fc/\n",
            encoding="utf-8",
        )
        rows = read_tsv(p)
        assert len(rows) == 2

        a = rows[0]
        assert a.section == "megajii"
        assert a.raw_name == "大和フーヅ"
        assert a.store_count == 69
        assert a.revenue_current_jpy == 7630476 * 1000
        assert a.raw_brands.startswith("ミスタードーナツ")

        b = rows[1]
        assert b.section == "franchisor"
        assert b.raw_name == "株式会社モスフードサービス"
        assert b.brand_name == "モスバーガー"
        assert b.store_count == 1266
        assert b.recruit_url == "https://mos.jp/fc/"

    def test_skip_X_brand(self, tmp_path: Path):
        p = tmp_path / "sample.tsv"
        p.write_text(
            "# section: megajii\n"
            "ある会社\t業種\t20\t代表\t住所\t0\t0\t0\t\tX\n"
            "別の会社\t業種\t20\t代表\t住所\t0\t0\t0\t\tモスバーガー\n",
            encoding="utf-8",
        )
        rows = read_tsv(p)
        assert len(rows) == 1
        assert rows[0].raw_name == "別の会社"

    def test_skip_header_row(self, tmp_path: Path):
        p = tmp_path / "sample.tsv"
        p.write_text(
            "# section: megajii\n"
            "企業名\t業態\t店舗数\t代表\t住所\t当期\t前期\t前々期\tHPURL\t加盟ブランド\n"
            "会社A\t業種\t20\t代表\t住所\t0\t0\t0\t\tモスバーガー\n",
            encoding="utf-8",
        )
        rows = read_tsv(p)
        assert len(rows) == 1
        assert rows[0].raw_name == "会社A"
