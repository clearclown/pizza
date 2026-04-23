"""houjin_csv: 国税庁 CSV ローカル取込 + 検索のユニットテスト。

APP_ID 無しで動作することを保証。CSV サンプルを直接流し込んで SQLite
index 経由の検索をテスト。
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pytest

from pizza_delivery.houjin_csv import (
    HoujinCSVIndex,
    HoujinCSVRecord,
    _iter_csv_text,
    iter_records,
    verify_operator_via_csv,
)


# ─── フィクスチャ: 国税庁仕様の CSV 1 行 ────────────────────────────
#
# 列構造 (0-indexed): seq, corporateNumber, process, ...
# 抜粋して最低限列数を満たすダミー行。

# 国税庁 CSV 実レイアウト (全 30 列):
#   [0] seq [1] corp_num [2] process [3] correct [4] update [5] change
#   [6] name [7] furigana [8] 国名code [9] prefecture [10] city [11] street
#   [12] 国外 [13] pref_code [14] city_code [15] postal ...
_SAMPLE_ROW_MOS = (
    "1,"
    "3010701019707,"  # 法人番号
    "01,"              # process (新規)
    "0,"               # 訂正区分
    "2023-09-15,"      # 更新年月日
    "2023-09-15,"      # 変更年月日
    "株式会社モスストアカンパニー,"   # 商号
    "カブシキガイシャモスストアカンパニー,"  # フリガナ
    "101,"             # 国名 code (101=日本)
    "東京都,"          # prefecture NAME
    "品川区,"          # city NAME
    "大崎2-1-1,"       # street
    ",13,13109,,,,,,,,,,,,\n"  # 以降 pref_code/city_code 等
)

_SAMPLE_ROW_VIENS = (
    "2,5010401003406,01,0,2024-03-01,2024-03-01,"
    "株式会社ヴィアン,カブシキガイシャヴィアン,"
    "101,東京都,港区,芝浦2-14-4,,13,13103,,,,,,,,,,,,\n"
)

_SAMPLE_ROW_DEAD = (
    # process=71 = 吸収合併消滅
    "3,9999999999999,71,0,2022-01-01,2022-01-01,"
    "株式会社消滅社,カブシキガイシャショウメツシャ,"
    "101,東京都,港区,芝浦X-X-X,,13,13103,,,,,,,,,,,,\n"
)


# ─── iter_records ──────────────────────────────────────────────


def test_iter_csv_text_parses_minimal_row() -> None:
    recs = list(_iter_csv_text(_SAMPLE_ROW_MOS))
    assert len(recs) == 1
    r = recs[0]
    assert r.corporate_number == "3010701019707"
    assert r.name == "株式会社モスストアカンパニー"
    assert r.process == "01"
    assert r.prefecture == "東京都"
    assert r.city == "品川区"
    assert r.address == "東京都品川区大崎2-1-1"
    assert r.active is True


def test_iter_csv_text_skips_malformed_rows() -> None:
    text = "\n1,short\n" + _SAMPLE_ROW_MOS
    recs = list(_iter_csv_text(text))
    assert len(recs) == 1


def test_iter_records_handles_zip(tmp_path: Path) -> None:
    """CSV が zip 内にあっても展開できる。"""
    zp = tmp_path / "sample.zip"
    csv_bytes = (_SAMPLE_ROW_MOS + _SAMPLE_ROW_VIENS).encode("utf-8")
    with zipfile.ZipFile(zp, "w") as zf:
        zf.writestr("13_202603.csv", csv_bytes)
    recs = list(iter_records(zp))
    assert len(recs) == 2
    names = {r.name for r in recs}
    assert "株式会社モスストアカンパニー" in names
    assert "株式会社ヴィアン" in names


# ─── HoujinCSVIndex ─────────────────────────────────────────────


def test_ingest_and_search(tmp_path: Path) -> None:
    db = tmp_path / "registry.sqlite"
    csv = tmp_path / "sample.csv"
    csv.write_text(
        _SAMPLE_ROW_MOS + _SAMPLE_ROW_VIENS + _SAMPLE_ROW_DEAD, encoding="utf-8"
    )
    idx = HoujinCSVIndex(db)
    n = idx.ingest_csv(csv)
    assert n == 3
    assert idx.count() == 3

    # active only で検索 → 消滅社は除外
    r = idx.search_by_name("株式会社ヴィアン")
    assert len(r) == 1
    assert r[0].corporate_number == "5010401003406"

    r = idx.search_by_name("株式会社")
    active_names = {x.name for x in r}
    assert "株式会社モスストアカンパニー" in active_names
    assert "株式会社消滅社" not in active_names

    # active_only=False なら消滅社も返る
    r_all = idx.search_by_name("株式会社消滅社", active_only=False)
    assert len(r_all) == 1
    assert r_all[0].active is False


def test_ingest_is_upsert(tmp_path: Path) -> None:
    """同じ corporate_number を二回 ingest しても重複しない。"""
    db = tmp_path / "registry.sqlite"
    csv = tmp_path / "sample.csv"
    csv.write_text(_SAMPLE_ROW_MOS, encoding="utf-8")
    idx = HoujinCSVIndex(db)
    idx.ingest_csv(csv)
    idx.ingest_csv(csv)  # 再 import
    assert idx.count() == 1


def test_search_empty_name_returns_empty(tmp_path: Path) -> None:
    idx = HoujinCSVIndex(tmp_path / "r.sqlite")
    assert idx.search_by_name("") == []
    assert idx.search_by_name("   ") == []


# ─── verify_operator_via_csv ────────────────────────────────────


def test_verify_operator_via_csv_hit(tmp_path: Path) -> None:
    db = tmp_path / "r.sqlite"
    csv = tmp_path / "s.csv"
    csv.write_text(_SAMPLE_ROW_MOS, encoding="utf-8")
    idx = HoujinCSVIndex(db)
    idx.ingest_csv(csv)

    result = verify_operator_via_csv("株式会社モスストアカンパニー", idx=idx)
    assert result["exists"] is True
    assert result["best_match_number"] == "3010701019707"
    assert result["name_similarity"] >= 0.9
    assert result["active"] is True
    assert result["source"] == "houjin_csv"


def test_verify_operator_via_csv_miss(tmp_path: Path) -> None:
    db = tmp_path / "r.sqlite"
    idx = HoujinCSVIndex(db)  # 空 DB
    result = verify_operator_via_csv("存在しない会社株式会社", idx=idx)
    assert result["exists"] is False
    assert result["best_match_number"] == ""
    assert result["source"] == "houjin_csv"
