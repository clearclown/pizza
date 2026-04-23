"""verify_pipeline: Layer D fallback チェーンのテスト。

env 状況と各 backend の可否を mock して fallback 順序を検証。
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from pizza_delivery.verify_pipeline import VerifyPipeline


# ─── available_paths ────────────────────────────────────────────


def test_available_paths_empty_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """env 無し + csv DB 無し → 有効経路 0。"""
    monkeypatch.delenv("HOUJIN_BANGOU_APP_ID", raising=False)
    monkeypatch.delenv("GBIZ_API_TOKEN", raising=False)
    pipe = VerifyPipeline(csv_db_path=str(tmp_path / "empty.sqlite"))
    assert pipe.available_paths() == []


def test_available_paths_with_csv_index(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """csv index に 1 件でも登録があれば csv が有効。"""
    monkeypatch.delenv("HOUJIN_BANGOU_APP_ID", raising=False)
    monkeypatch.delenv("GBIZ_API_TOKEN", raising=False)

    from pizza_delivery.houjin_csv import HoujinCSVIndex

    db = tmp_path / "r.sqlite"
    csv = tmp_path / "s.csv"
    csv.write_text(
        "1,3010701019707,01,0,2023-09,2023-09,"
        "株式会社モスストアカンパニー,カ,東京都,品川区,大崎,,,,13,13109,,,,\n",
        encoding="utf-8",
    )
    HoujinCSVIndex(db).ingest_csv(csv)
    pipe = VerifyPipeline(csv_db_path=str(db))
    assert "houjin_csv" in pipe.available_paths()


def test_available_paths_with_gbiz_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("HOUJIN_BANGOU_APP_ID", raising=False)
    monkeypatch.setenv("GBIZ_API_TOKEN", "tok")
    pipe = VerifyPipeline(csv_db_path=str(tmp_path / "none.sqlite"))
    assert "gbiz" in pipe.available_paths()


# ─── verify() ──────────────────────────────────────────────────


def test_verify_empty_name_returns_empty(tmp_path: Path) -> None:
    pipe = VerifyPipeline(csv_db_path=str(tmp_path / "n.sqlite"))
    r = asyncio.run(pipe.verify(""))
    assert r["exists"] is False
    assert r["source"] == "skipped"


def test_verify_hits_csv_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """CSV index にヒットすれば csv 経由で exists=True が返る。"""
    monkeypatch.delenv("HOUJIN_BANGOU_APP_ID", raising=False)
    monkeypatch.delenv("GBIZ_API_TOKEN", raising=False)

    from pizza_delivery.houjin_csv import HoujinCSVIndex

    db = tmp_path / "r.sqlite"
    csv = tmp_path / "s.csv"
    csv.write_text(
        "1,5010401089998,01,0,2024-08,2024-08,"
        "大和フーヅ株式会社,ダイワフーヅ,埼玉県,熊谷市,筑波,,,,11,11202,,,,\n",
        encoding="utf-8",
    )
    HoujinCSVIndex(db).ingest_csv(csv)
    pipe = VerifyPipeline(csv_db_path=str(db))
    r = asyncio.run(pipe.verify("大和フーヅ株式会社"))
    assert r["exists"] is True
    assert r["best_match_number"] == "5010401089998"
    assert r["source"] == "houjin_csv"


def test_verify_falls_back_from_csv_to_gbiz(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """CSV が miss でも gbiz がヒットすれば gbiz 結果を返す。"""
    monkeypatch.delenv("HOUJIN_BANGOU_APP_ID", raising=False)
    monkeypatch.setenv("GBIZ_API_TOKEN", "tok")

    from pizza_delivery.houjin_csv import HoujinCSVIndex

    db = tmp_path / "r.sqlite"
    csv = tmp_path / "s.csv"
    # CSV には別の会社のみ
    csv.write_text(
        "1,1111111111111,01,0,2024-08,2024-08,"
        "無関係株式会社,ムカンケイ,東京都,港区,XX,,,,13,13103,,,,\n",
        encoding="utf-8",
    )
    HoujinCSVIndex(db).ingest_csv(csv)

    # gbiz の mock
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "hojin-infos": [
                    {
                        "corporate_number": "5010401003406",
                        "name": "株式会社ヴィアン",
                        "location": "東京都港区",
                    }
                ]
            },
        )

    from pizza_delivery.gbiz_client import GBizClient

    pipe = VerifyPipeline(csv_db_path=str(db))
    pipe._gbiz_client = GBizClient(token="tok", transport=httpx.MockTransport(handler))

    r = asyncio.run(pipe.verify("株式会社ヴィアン"))
    assert r["exists"] is True
    assert r["source"] == "gbiz"
    assert r["best_match_number"] == "5010401003406"


def test_verify_all_miss_returns_not_exists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """全経路 miss → exists False、source は最後の試行結果。"""
    monkeypatch.delenv("HOUJIN_BANGOU_APP_ID", raising=False)
    monkeypatch.setenv("GBIZ_API_TOKEN", "tok")

    from pizza_delivery.houjin_csv import HoujinCSVIndex

    db = tmp_path / "r.sqlite"
    csv = tmp_path / "s.csv"
    csv.write_text(
        "1,1111111111111,01,0,2024-08,2024-08,"
        "別会社,ベツガイシャ,東京都,港区,A,,,,13,13103,,,,\n",
        encoding="utf-8",
    )
    HoujinCSVIndex(db).ingest_csv(csv)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"hojin-infos": []})

    from pizza_delivery.gbiz_client import GBizClient

    pipe = VerifyPipeline(csv_db_path=str(db))
    pipe._gbiz_client = GBizClient(token="tok", transport=httpx.MockTransport(handler))

    r = asyncio.run(pipe.verify("存在しない社"))
    assert r["exists"] is False
