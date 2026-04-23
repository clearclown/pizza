"""browser-use (Playwright + LLM) を使った汎用 scraper。

モス公式サイト等 SPA で JS 実行後でないと operator 情報が見えないページ、
および iタウンページ等 電話番号 → 会社名 逆引きに使う。

設計原則:
  - LLM は「ブラウザを操作する agent」として使い、**出力 JSON は structured**
  - 失敗時 graceful に None を返す
  - ENABLE_BROWSER_FALLBACK=1 でのみ起動
  - 1 ホストに短期間に叩かない rate limit 付き

使い方:
    scraper = BrowserScraper()
    operator = await scraper.scrape_operator_from_url(
        url="https://www.mos.jp/shop/detail/?shop_cd=02232",
        brand_hint="モスバーガー",
    )
    # operator は OperatorInfo(name, source_url, reasoning) or None
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ─── データ ─────────────────────────────────────────────────


@dataclass
class OperatorInfo:
    """browser 抽出の結果。"""

    name: str
    corporate_number: str = ""
    address: str = ""
    phone: str = ""
    source_url: str = ""
    reasoning: str = ""
    confidence: float = 0.0


class _OperatorJSON(BaseModel):
    """LLM に structured output で返してもらう schema。"""

    operator_name: str = Field(default="")
    corporate_number: str = Field(default="")
    address: str = Field(default="")
    phone: str = Field(default="")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    reasoning: str = Field(default="")


# ─── Scraper ───────────────────────────────────────────────


@dataclass
class BrowserScraper:
    """browser-use.Agent を軽く wrap する scraper。

    Attributes:
      llm:           browser_use.llm.Chat* instance。None なら providers 経由で
                     env から auto 解決。
      max_steps:     1 scrape あたりの step 上限 (default 10)
      rate_limit_sec: 同一 host に対する最小間隔 (default 2.0)
    """

    llm: Any = None
    max_steps: int = 10
    rate_limit_sec: float = 2.0
    _last_host_at: dict[str, float] | None = None

    def __post_init__(self) -> None:
        if self._last_host_at is None:
            self._last_host_at = {}

    def _get_llm(self) -> Any | None:
        if self.llm is not None:
            return self.llm
        try:
            from pizza_delivery.providers import get_provider

            provider_name = os.getenv("LLM_PROVIDER", "anthropic")
            provider = get_provider(provider_name)
            if not provider.ready():
                return None
            return provider.make_llm()
        except Exception as e:
            logger.debug("provider resolve failed: %s", e)
            return None

    async def _rate_limit(self, url: str) -> None:
        from urllib.parse import urlparse

        host = urlparse(url).hostname or ""
        now = asyncio.get_event_loop().time()
        last = self._last_host_at.get(host, 0.0)
        wait = (last + self.rate_limit_sec) - now
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_host_at[host] = asyncio.get_event_loop().time()

    def enabled(self) -> bool:
        """ENABLE_BROWSER_FALLBACK=1 かつ LLM 利用可能か。"""
        if os.getenv("ENABLE_BROWSER_FALLBACK", "0") != "1":
            return False
        return self._get_llm() is not None

    async def scrape_operator_from_url(
        self,
        url: str,
        *,
        brand_hint: str = "",
        store_name: str = "",
    ) -> OperatorInfo | None:
        """店舗 URL を実 browser で訪問して、運営会社情報を取得。

        失敗時は None を返し呼出側でフォールバック可能。
        """
        if not url or not self.enabled():
            return None
        llm = self._get_llm()
        if llm is None:
            return None

        await self._rate_limit(url)
        task = _build_operator_task(url=url, brand_hint=brand_hint, store_name=store_name)
        try:
            from browser_use import Agent
        except ImportError:
            logger.warning("browser_use not installed — skipping browser scrape")
            return None

        try:
            agent = Agent(task=task, llm=llm)
            history = await agent.run(max_steps=self.max_steps)
        except Exception as e:
            logger.debug("browser agent failed for %s: %s", url, e)
            return None

        raw = getattr(history, "final_result", None) or getattr(history, "result", None)
        parsed = _parse_operator_json(raw)
        if parsed is None:
            return None
        if not parsed.operator_name:
            return None
        return OperatorInfo(
            name=parsed.operator_name,
            corporate_number=parsed.corporate_number,
            address=parsed.address,
            phone=parsed.phone,
            source_url=url,
            reasoning=parsed.reasoning,
            confidence=parsed.confidence,
        )

    async def lookup_operator_by_phone(
        self,
        phone: str,
        *,
        brand_hint: str = "",
    ) -> OperatorInfo | None:
        """電話番号 → 会社名 の逆引きを iタウンページ等で試す。"""
        if not phone or not self.enabled():
            return None
        llm = self._get_llm()
        if llm is None:
            return None

        task = _build_phone_lookup_task(phone=phone, brand_hint=brand_hint)
        try:
            from browser_use import Agent
        except ImportError:
            return None

        try:
            agent = Agent(task=task, llm=llm)
            history = await agent.run(max_steps=self.max_steps)
        except Exception as e:
            logger.debug("phone lookup agent failed for %s: %s", phone, e)
            return None

        raw = getattr(history, "final_result", None) or getattr(history, "result", None)
        parsed = _parse_operator_json(raw)
        if parsed is None:
            return None
        if not parsed.operator_name:
            return None
        return OperatorInfo(
            name=parsed.operator_name,
            corporate_number=parsed.corporate_number,
            address=parsed.address,
            phone=phone,
            source_url="",
            reasoning=parsed.reasoning,
            confidence=parsed.confidence,
        )


# ─── task prompt 構築 ────────────────────────────────────


def _build_operator_task(
    *, url: str, brand_hint: str = "", store_name: str = ""
) -> str:
    hint = f"(brand: {brand_hint}, store: {store_name})" if brand_hint else ""
    return (
        f"以下の URL を開き、この店舗の『運営会社』を JSON で返してください {hint}\n"
        f"  {url}\n\n"
        f"手順:\n"
        f"1. ページが完全に描画されるまで待つ (最大 5 秒)\n"
        f"2. 『運営会社』『会社概要』『店舗運営者』『○○株式会社』等の明示記述を探す\n"
        f"3. 見つからなければ「会社概要」「特定商取引法に基づく表記」「運営会社」等の\n"
        f"   リンクを辿って開く\n"
        f"4. それでも不明なら operator_name を空文字列 で返す\n\n"
        f"JSON schema:\n"
        f"  {{operator_name: str,\n"
        f"    corporate_number: str (13 桁、分からなければ空),\n"
        f"    address: str, phone: str,\n"
        f"    confidence: 0-1, reasoning: 50 字以内}}\n"
    )


def _build_phone_lookup_task(*, phone: str, brand_hint: str = "") -> str:
    return (
        f"電話番号 {phone} の会社名・住所を iタウンページ (itp.ne.jp) 等で検索して\n"
        f"JSON で返してください。{brand_hint and f'(brand: {brand_hint})'}\n\n"
        f"手順:\n"
        f"1. https://itp.ne.jp/?q={phone} 相当の検索ページを開く\n"
        f"2. 上位ヒットの会社名・住所を読み取る\n"
        f"3. 会社名が取れなければ google 検索で電話番号を叩く\n"
        f"4. 複数候補ある場合 brand hint に最も一致するものを採用\n\n"
        f"JSON schema:\n"
        f"  {{operator_name: str, corporate_number: str, address: str,\n"
        f"    phone: str, confidence: 0-1, reasoning: str}}\n"
    )


# ─── JSON parse ────────────────────────────────────────


def _parse_operator_json(raw: Any) -> _OperatorJSON | None:
    if raw is None:
        return None
    if isinstance(raw, _OperatorJSON):
        return raw
    if isinstance(raw, dict):
        try:
            return _OperatorJSON.model_validate(raw)
        except Exception:
            return None
    if isinstance(raw, str):
        import json

        try:
            return _OperatorJSON.model_validate_json(raw)
        except Exception:
            pass
        try:
            return _OperatorJSON.model_validate(json.loads(raw))
        except Exception:
            return None
    return None
