from __future__ import annotations

from pizza_delivery.jfa_disclosure import (
    parse_disclosure_index_html,
    parse_disclosure_pdf_text,
)


def test_parse_disclosure_index_html_skips_comment_links() -> None:
    html = """
    <table class="tbl_kaiji">
      <tr><th class="thColor01" colspan="2">学習塾・カルチャースクール</th></tr>
      <tr><th>会社名</th><th>チェーン店</th></tr>
      <tr>
        <td>（株）カーブスホールディングス</td>
        <td><a href="/fc-g-misc/pdf/217-1.pdf">カーブス</a></td>
      </tr>
      <!-- <a href="/fc-g-misc/pdf/old-1.pdf">旧PDF</a> -->
    </table>
    """
    links = parse_disclosure_index_html(
        html,
        source_url="https://www.jfa-fc.or.jp/particle/3614.html",
    )
    assert len(links) == 1
    assert links[0].franchisor_name == "株式会社カーブスホールディングス"
    assert links[0].brand_name == "カーブス"
    assert links[0].industry == "学習塾・カルチャースクール"
    assert links[0].pdf_url == "https://www.jfa-fc.or.jp/fc-g-misc/pdf/217-1.pdf"


def test_parse_disclosure_pdf_text_fc_rc_total_table() -> None:
    text = """
    株式会社モスフードサービス
    ５．出店状況：FC店・RC店別（モスバーガー事業）
    店舗数推移 （数値は全て年度末時点）
    FC店 RC店 合 計
    2022 年度 1,248 44 1,292
    2023 年度 1,266 45 1,311
    2024 年度 1,273 45 1,318
    """
    metrics = parse_disclosure_pdf_text(text)
    assert metrics.franchisor_name == "株式会社モスフードサービス"
    assert metrics.observed_at == "2024年度"
    assert metrics.fc_store_count == 1273
    assert metrics.rc_store_count == 45
    assert metrics.total_store_count == 1318
    assert metrics.best_store_count == 1318
    assert metrics.extraction_method == "fc_rc_total_table"


def test_parse_disclosure_pdf_text_history_store_count() -> None:
    text = """
    株式会社カーブスジャパン
    2023 年 8 月 店舗数 1,962 店舗 会員数 777,000 名
    2024 年 8 月 店舗数 1,978 店舗 会員数 817,000 名
    2025 年 8 月 店舗数 1,996 店舗 会員数 863,000 名
    """
    metrics = parse_disclosure_pdf_text(text)
    assert metrics.franchisor_name == "株式会社カーブスジャパン"
    assert metrics.observed_at == "2025-08"
    assert metrics.total_store_count == 1996
    assert metrics.extraction_method == "history_store_count"


def test_parse_disclosure_pdf_text_brand_row_table() -> None:
    text = """
    株式会社コメダ
    ・店舗数推移
      年度              2023/2                           2024/2                            2025/2
                加盟店   直営店            合計      加盟店       直営店        合計        加盟店          直営店         合計
    コメダ珈琲店      897     34           931       939      26        965         986          22        1008
     おかげ庵        6      7            13         7        6            13       8           8          16
    """
    metrics = parse_disclosure_pdf_text(text, brand_name="コメダ珈琲店")
    assert metrics.franchisor_name == "株式会社コメダ"
    assert metrics.observed_at == "2025-02"
    assert metrics.fc_store_count == 986
    assert metrics.rc_store_count == 22
    assert metrics.total_store_count == 1008
    assert metrics.extraction_method == "brand_store_count_row"


def test_parse_disclosure_pdf_text_direct_first_year_table() -> None:
    text = """
    株式会社 やる気スイッチグループ
    （２）教   室    数   推   移                               （ 校 ）
        年 度             直営校         フランチャイズ校       合計
      2021 年度                  73          120            193
      2022 年度                  73          139            212
      2023 年度                  77          137            214
      2024 年度                  83          135            218
    """
    metrics = parse_disclosure_pdf_text(text, brand_name="Kids Duo")
    assert metrics.franchisor_name == "株式会社やる気スイッチグループ"
    assert metrics.observed_at == "2024年度"
    assert metrics.fc_store_count == 135
    assert metrics.rc_store_count == 83
    assert metrics.total_store_count == 218
    assert metrics.extraction_method == "year_store_count_table"


def test_parse_disclosure_pdf_text_current_sentence() -> None:
    text = """
    株式会社テスト
    2025 年 8 月末日現在、現在は 123 店舗のクラブを展開しています。
    """
    metrics = parse_disclosure_pdf_text(text)
    assert metrics.franchisor_name == "株式会社テスト"
    assert metrics.observed_at == "2025-08"
    assert metrics.total_store_count == 123
    assert metrics.extraction_method == "current_store_count_sentence"
