"""Coverage boost: 未カバー branch を systematic に踏む。

対象の低カバ module:
  - verify_pipeline.py  (60%)
  - integrate.py       (64%)
  - jfa_fetcher.py     (69%)
  - evaluator.py       (72%)
  - llm_cleanser.py    (76%)
  - houjin_csv.py      (77%)

全てネットワーク遮断 (httpx.MockTransport) + in-memory DB。
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import httpx
import pytest
from sqlalchemy import create_engine

from pizza_delivery.evaluator import evaluate
from pizza_delivery.gbiz_client import GBizClient
from pizza_delivery.houjin_csv import (
    HoujinCSVIndex,
    _decode_bytes,
    iter_records,
)
from pizza_delivery.integrate import (
    _extract_japanese_prefix,
    export_unified_csv,
    hydrate_corporate_numbers,
    import_pipeline_operators,
    integrate_all,
)
from pizza_delivery.jfa_fetcher import (
    JFAFetcher,
    _canonicalize_operator,
    _looks_like_industry,
    _split_brand_industry,
)
from pizza_delivery.llm_cleanser import (
    CleanseResult,
    _invoke_structured,
    canonicalize_operator_name,
)
from pizza_delivery.orm import (
    create_all,
    link_brand_operator,
    make_session,
    upsert_brand,
    upsert_operator,
)
from pizza_delivery.verify_pipeline import VerifyPipeline


# ─── houjin_csv edge cases ──────────────────────────────────


def test_decode_bytes_bom_utf8() -> None:
    data = b"\xef\xbb\xbfhello"
    text, enc = _decode_bytes(data, "cp932")
    assert text == "hello"
    assert enc == "utf-8-sig"


def test_decode_bytes_cp932_fallback() -> None:
    # 明らかに utf-8 で decode できない cp932 の 'あ' = 0x82 0xa0
    data = b"\x82\xa0"
    text, enc = _decode_bytes(data, "utf-8")
    assert text == "あ"
    assert enc == "cp932"


def test_decode_bytes_replace_fallback() -> None:
    # 全ての encoding で invalid なら replace で decode
    data = b"\xff\xfe\xff\xfe"
    text, enc = _decode_bytes(data, "utf-8")
    assert isinstance(text, str)  # 例外なし


def test_iter_records_skips_non_numeric_corp(tmp_path: Path) -> None:
    """corp_number が 13 桁数字でない行は無視。"""
    csv = tmp_path / "x.csv"
    csv.write_text(
        "0,,,,,,,,101,,,,,\n"  # 法人番号空
        "1,ABCDEFGHIJKLM,01,0,2024-01,2024-01,badcorp,,101,東京都,渋谷区,A,,,,,,,,,,,,\n"
        "2,3010701019707,01,0,2024-01,2024-01,正常,,101,東京都,品川区,X,,,,,,,,,,,,\n",
        encoding="utf-8",
    )
    recs = list(iter_records(csv))
    assert len(recs) == 1
    assert recs[0].corporate_number == "3010701019707"


def test_houjin_csv_search_tiered_lookup(tmp_path: Path) -> None:
    """exact → prefix → substring の 3 段階を踏む。"""
    db = tmp_path / "r.sqlite"
    csv = tmp_path / "c.csv"
    csv.write_text(
        "1,3010701019707,01,0,2024-01,2024-01,"
        "株式会社モスストアカンパニー,カ,101,東京都,品川区,A,,,,,,,,,,,,\n",
        encoding="utf-8",
    )
    HoujinCSVIndex(db).ingest_csv(csv)
    idx = HoujinCSVIndex(db)
    # exact
    r = idx.search_by_name("株式会社モスストアカンパニー")
    assert len(r) == 1
    # prefix (末尾に空白)
    r = idx.search_by_name("株式会社モス")
    assert len(r) == 1
    # substring
    r = idx.search_by_name("モスストア")
    assert len(r) == 1


# ─── verify_pipeline edge cases ─────────────────────────────


def test_verify_pipeline_web_api_fail_falls_back(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Web-API ルートが例外で失敗しても次 backend にフォールバック。"""
    monkeypatch.setenv("HOUJIN_BANGOU_APP_ID", "dummy")
    monkeypatch.delenv("GBIZ_API_TOKEN", raising=False)

    # CSV hit あり
    db = tmp_path / "r.sqlite"
    csv = tmp_path / "c.csv"
    csv.write_text(
        "1,5010401089998,01,0,2024-01,2024-01,"
        "大和フーヅ株式会社,カ,101,埼玉県,熊谷市,A,,,,,,,,,,,,\n",
        encoding="utf-8",
    )
    HoujinCSVIndex(db).ingest_csv(csv)

    # Web-API が例外 → CSV fallback
    from pizza_delivery import houjin_bangou

    class _BrokenWeb:
        def __init__(self) -> None:
            pass
        async def search_by_name(self, name: str):
            raise RuntimeError("simulated web-api failure")

    pipe = VerifyPipeline(csv_db_path=str(db))
    pipe._web_client = _BrokenWeb()
    r = asyncio.run(pipe.verify("大和フーヅ株式会社"))
    assert r["exists"] is True
    assert r["source"] == "houjin_csv"


def test_verify_pipeline_llm_cleanse_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """同じ入力を 2 回 verify しても LLM は 1 回しか呼ばれない (cache)。"""
    monkeypatch.delenv("HOUJIN_BANGOU_APP_ID", raising=False)
    monkeypatch.delenv("GBIZ_API_TOKEN", raising=False)

    calls = {"n": 0}

    class _StubLLM:
        async def ainvoke(self, messages, output_format=None):
            calls["n"] += 1
            class _R:
                completion = CleanseResult(canonical="株式会社X", confidence=0.9)
            return _R()

    pipe = VerifyPipeline(llm=_StubLLM())
    asyncio.run(pipe.verify("㈱X"))
    asyncio.run(pipe.verify("㈱X"))  # 2 回目
    assert calls["n"] == 1, "LLM は cache で 1 回しか呼ばれないはず"


# ─── jfa_fetcher edge ─────────────────────────────────────


def test_split_brand_industry_industry_only() -> None:
    """brand 無し、業種説明のみの cell は brand 空で industry 格納。"""
    from bs4 import BeautifulSoup

    td = BeautifulSoup("<td>ATMの設置推進及びATMサービスの展開</td>", "lxml").find("td")
    brand, industry = _split_brand_industry(td)
    assert brand == ""
    assert "ATM" in industry


def test_split_brand_industry_long_first_line_is_industry() -> None:
    """1 行目が 40 文字超 → brand 空扱い。"""
    from bs4 import BeautifulSoup

    long_line = "あ" * 50  # 確実に 40 文字超
    td = BeautifulSoup(f"<td>{long_line}</td>", "lxml").find("td")
    brand, industry = _split_brand_industry(td)
    assert brand == ""
    assert len(industry) > 0


def test_split_brand_industry_empty_td() -> None:
    from bs4 import BeautifulSoup

    td = BeautifulSoup("<td></td>", "lxml").find("td")
    brand, industry = _split_brand_industry(td)
    assert brand == ""
    assert industry == ""


def test_looks_like_industry_positive() -> None:
    assert _looks_like_industry("ATMの設置推進及びサービスの展開")
    assert _looks_like_industry("ITソリューションの提供")
    assert not _looks_like_industry("モスバーガー")


def test_canonicalize_operator_strips_english() -> None:
    assert (
        _canonicalize_operator("株式会社モスフードサービス MOS FOOD SERVICES INC.")
        == "株式会社モスフードサービス"
    )


def test_canonicalize_operator_handles_abbrev() -> None:
    assert _canonicalize_operator("（株）テスト") == "株式会社テスト"
    assert _canonicalize_operator("上島珈琲貿易（株）") == "上島珈琲貿易株式会社"


# ─── integrate edge ───────────────────────────────────────


@pytest.fixture
def mem_sess():
    engine = create_engine("sqlite:///:memory:", future=True)
    create_all(engine)
    s = make_session(engine)
    yield s
    s.close()


def test_integrate_all_handles_missing_pipeline_db(
    tmp_path: Path, mem_sess,
) -> None:
    stats = integrate_all(
        pipeline_db_path=tmp_path / "nope.sqlite",
        orm_session=mem_sess,
    )
    # missing DB は warning + 0 added で graceful
    assert stats.brand_links_added == 0


def test_import_pipeline_operators_blocks_franchisor(tmp_path: Path, mem_sess) -> None:
    """cross-brand 本部混入 (ドムドム on モス brand) は blocklist で除外される。"""
    db = tmp_path / "p.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE operator_stores (
          operator_name TEXT, place_id TEXT, brand TEXT,
          operator_type TEXT, confidence REAL,
          discovered_via TEXT, corporate_number TEXT,
          PRIMARY KEY (operator_name, place_id)
        );
        """
    )
    conn.executemany(
        "INSERT INTO operator_stores VALUES (?,?,?,?,?,?,?)",
        [
            ("株式会社ドムドムフードサービス", "p1", "モスバーガー", "unknown", 0.5, "chain", ""),
            ("株式会社モスフードサービス", "p2", "モスバーガー", "franchisor", 1.0, "registry", ""),
            ("株式会社実在FC", "p3", "モスバーガー", "franchisee", 1.0, "per_store", ""),
        ],
    )
    conn.commit()
    conn.close()

    n = import_pipeline_operators(mem_sess, db)
    # ドムドム + モスフード は除外、実在 FC のみ残る
    assert n == 1
    from pizza_delivery.orm import OperatorCompany

    ops = {o.name for o in mem_sess.query(OperatorCompany).all()}
    assert "株式会社実在FC" in ops
    assert "株式会社ドムドムフードサービス" not in ops
    assert "株式会社モスフードサービス" not in ops


# ─── evaluator edge ───────────────────────────────────────


def test_evaluator_same_source_hits_all(mem_sess) -> None:
    """truth と pipeline に同じ link を作れば recall=1.0。"""
    b = upsert_brand(mem_sess, "X", source="jfa")
    o = upsert_operator(mem_sess, name="株式会社X", source="jfa")
    mem_sess.flush()
    link_brand_operator(mem_sess, brand=b, operator=o, source="jfa")
    link_brand_operator(mem_sess, brand=b, operator=o, source="pipeline")
    mem_sess.commit()

    r = evaluate(truth_source="jfa", pipeline_source="pipeline", orm_session=mem_sess)
    assert r.link_hits == 1
    assert r.link_recall == 1.0


# ─── llm_cleanser edge ────────────────────────────────────


def test_invoke_structured_dict_fallback_parse() -> None:
    """LLM が dict を返すと schema でバリデート。"""
    class _StubLLM:
        async def ainvoke(self, messages, output_format=None):
            class _R:
                completion = {"canonical": "株式会社X", "confidence": 0.8}
            return _R()
    r = asyncio.run(
        _invoke_structured(_StubLLM(), "sys", "user", CleanseResult)
    )
    assert r is not None
    assert r.canonical == "株式会社X"


def test_invoke_structured_json_string_fallback() -> None:
    class _StubLLM:
        async def ainvoke(self, messages, output_format=None):
            class _R:
                completion = '{"canonical": "㈱Y", "confidence": 0.5}'
            return _R()
    r = asyncio.run(
        _invoke_structured(_StubLLM(), "sys", "user", CleanseResult)
    )
    assert r is not None
    assert r.canonical == "㈱Y"


def test_invoke_structured_invalid_returns_none() -> None:
    class _StubLLM:
        async def ainvoke(self, messages, output_format=None):
            class _R:
                completion = "this is not json"
            return _R()
    r = asyncio.run(
        _invoke_structured(_StubLLM(), "sys", "user", CleanseResult)
    )
    assert r is None
