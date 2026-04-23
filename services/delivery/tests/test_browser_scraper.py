"""browser_scraper: ネットワーク遮断 unit test。

browser_use.Agent を stub して 呼び出しロジックと JSON parsing を検証。
実 Playwright は動かさない。
"""

from __future__ import annotations

import asyncio

import pytest

from pizza_delivery.browser_scraper import (
    BrowserScraper,
    OperatorInfo,
    _OperatorJSON,
    _build_operator_task,
    _parse_operator_json,
)


# ─── enabled() ─────────────────────────────────────────


def test_enabled_false_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ENABLE_BROWSER_FALLBACK", raising=False)
    assert BrowserScraper().enabled() is False


def test_enabled_false_without_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    """env=1 でも LLM provider が ready でなければ enabled=False。"""
    monkeypatch.setenv("ENABLE_BROWSER_FALLBACK", "1")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert BrowserScraper().enabled() is False


def test_enabled_true_with_stub_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_BROWSER_FALLBACK", "1")

    class _Stub:
        pass

    s = BrowserScraper(llm=_Stub())
    assert s.enabled() is True


# ─── _parse_operator_json ────────────────────────────


def test_parse_operator_json_dict() -> None:
    r = _parse_operator_json({"operator_name": "株式会社X", "confidence": 0.7})
    assert r is not None
    assert r.operator_name == "株式会社X"
    assert r.confidence == 0.7


def test_parse_operator_json_string() -> None:
    r = _parse_operator_json('{"operator_name": "株式会社Y"}')
    assert r is not None
    assert r.operator_name == "株式会社Y"


def test_parse_operator_json_invalid() -> None:
    assert _parse_operator_json("not json") is None
    assert _parse_operator_json(None) is None


def test_parse_operator_json_instance() -> None:
    j = _OperatorJSON(operator_name="Z")
    r = _parse_operator_json(j)
    assert r is j


# ─── _build_operator_task ─────────────────────────────


def test_build_operator_task_contains_url() -> None:
    t = _build_operator_task(
        url="https://example.com/s", brand_hint="モス", store_name="ABC"
    )
    assert "https://example.com/s" in t
    assert "モス" in t
    assert "operator_name" in t


# ─── scrape_operator_from_url ─────────────────────────


def test_scrape_returns_none_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ENABLE_BROWSER_FALLBACK", raising=False)
    s = BrowserScraper()
    r = asyncio.run(s.scrape_operator_from_url("https://example.com"))
    assert r is None


def test_scrape_returns_none_for_empty_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENABLE_BROWSER_FALLBACK", "1")

    class _Stub:
        pass

    s = BrowserScraper(llm=_Stub())
    r = asyncio.run(s.scrape_operator_from_url(""))
    assert r is None


def test_scrape_parses_stub_agent_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """browser_use.Agent を monkey patch して stub 返却を parse。"""
    monkeypatch.setenv("ENABLE_BROWSER_FALLBACK", "1")

    class _StubHistory:
        final_result = {
            "operator_name": "株式会社テスト商事",
            "corporate_number": "1234567890123",
            "address": "東京都新宿区",
            "phone": "03-1234-5678",
            "confidence": 0.85,
            "reasoning": "公式サイト運営会社欄より",
        }

    class _StubAgent:
        def __init__(self, task, llm) -> None:
            self.task = task
            self.llm = llm

        async def run(self, max_steps: int = 10):
            return _StubHistory()

    # browser_use 全体を stub module に
    import sys
    import types

    stub_mod = types.ModuleType("browser_use")
    stub_mod.Agent = _StubAgent  # type: ignore
    monkeypatch.setitem(sys.modules, "browser_use", stub_mod)

    s = BrowserScraper(llm=object(), rate_limit_sec=0.0)
    r = asyncio.run(
        s.scrape_operator_from_url(
            "https://www.mos.jp/shop/detail/?shop_cd=02232",
            brand_hint="モスバーガー",
            store_name="秋葉原末広町店",
        )
    )
    assert r is not None
    assert r.name == "株式会社テスト商事"
    assert r.corporate_number == "1234567890123"
    assert r.source_url.startswith("https://www.mos.jp/")
    assert r.confidence == 0.85


def test_scrape_returns_none_on_agent_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENABLE_BROWSER_FALLBACK", "1")

    class _BrokenAgent:
        def __init__(self, task, llm) -> None:
            pass
        async def run(self, max_steps: int = 10):
            raise RuntimeError("playwright crashed")

    import sys, types
    stub_mod = types.ModuleType("browser_use")
    stub_mod.Agent = _BrokenAgent  # type: ignore
    monkeypatch.setitem(sys.modules, "browser_use", stub_mod)

    s = BrowserScraper(llm=object(), rate_limit_sec=0.0)
    r = asyncio.run(s.scrape_operator_from_url("https://x"))
    assert r is None


# ─── lookup_operator_by_phone ────────────────────────


def test_lookup_phone_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ENABLE_BROWSER_FALLBACK", raising=False)
    s = BrowserScraper()
    r = asyncio.run(s.lookup_operator_by_phone("03-1234-5678"))
    assert r is None


def test_lookup_phone_with_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_BROWSER_FALLBACK", "1")

    class _StubHistory:
        final_result = {
            "operator_name": "株式会社山田商店",
            "address": "東京都渋谷区",
            "phone": "03-1234-5678",
            "confidence": 0.7,
        }

    class _StubAgent:
        def __init__(self, task, llm) -> None: ...
        async def run(self, max_steps: int = 10):
            return _StubHistory()

    import sys, types
    stub_mod = types.ModuleType("browser_use")
    stub_mod.Agent = _StubAgent  # type: ignore
    monkeypatch.setitem(sys.modules, "browser_use", stub_mod)

    s = BrowserScraper(llm=object(), rate_limit_sec=0.0)
    r = asyncio.run(s.lookup_operator_by_phone("03-1234-5678"))
    assert r is not None
    assert r.name == "株式会社山田商店"
    assert r.phone == "03-1234-5678"
