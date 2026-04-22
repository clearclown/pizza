"""browser-use + LLM による FC 判定エージェント (browser fallback 付き)。

処理フロー:
  1. Markdown + 店舗 context を LLM に渡し structured output で JudgeJSON 取得
  2. confidence が BROWSER_FALLBACK_THRESHOLD (default 0.4) 未満 & official_url が
     ある場合、browser_use.Agent を起動して実ブラウザで公式サイトを訪問
  3. browser 結果で JudgeReply を上書き (confidence 加算)

browser fallback は:
  - ENABLE_BROWSER_FALLBACK=1 env で有効化
  - テスト時は Agent factory を inject して mock 可能

テスト戦略:
  - Unit: mock LLM / mock Agent で fallback ロジックを検証
  - Live: RUN_LIVE_ACCURACY=1 で実 Claude、ENABLE_BROWSER_FALLBACK=1 で実 Playwright
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Protocol

import yaml
from pydantic import BaseModel, Field


# ─── Data classes ──────────────────────────────────────────────────────


@dataclass
class JudgeRequest:
    place_id: str
    brand: str
    name: str
    markdown: str
    address: str = ""
    official_url: str = ""
    candidate_urls: list[str] = field(default_factory=list)
    provider_hint: str = ""


@dataclass
class JudgeReply:
    place_id: str
    is_franchise: bool
    operator_name: str
    store_count_estimate: int
    confidence: float
    llm_provider: str
    llm_model: str
    reasoning: str = ""
    used_browser_fallback: bool = False
    # Phase 4: 事業会社定義 (docs/operator-definition.md)
    operation_type: str = "unknown"   # direct | franchisee | mixed | unknown
    franchisor_name: str = ""         # 本部会社 (例: セブン-イレブン・ジャパン)
    franchisee_name: str = ""         # 加盟店運営会社 (例: 株式会社○○商事)
    judge_mode: str = ""              # llm-only | browser | hybrid


# operation_type の許容値
OPERATION_TYPES = frozenset({"direct", "franchisee", "mixed", "unknown"})


class JudgeJSON(BaseModel):
    """LLM から structured output で返してほしい JSON スキーマ v3。

    Phase 4: 事業会社定義 (docs/operator-definition.md) に基づき拡張。
    franchisor と franchisee を明示的に区別し、operation_type を 4 値に。
    既存の is_franchise / operator_name は後方互換として残す (optional default)。
    """

    # Phase 4 主フィールド
    operation_type: str = Field(default="unknown")
    franchisor_name: str = Field(default="")
    franchisee_name: str = Field(default="")
    store_count_estimate: int = Field(default=0, ge=0)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    reasoning: str = Field(default="")

    # Phase 2-3 後方互換フィールド (LLM が返さなくても derive で埋まる)
    is_franchise: bool = Field(default=False)
    operator_name: str = Field(default="")

    def derive_legacy(self) -> "JudgeJSON":
        """v3 と v2 フィールド間の相互導出 (bi-directional fill)。

        - LLM が v3 フィールドだけ返した場合: operation_type → is_franchise,
          franchisee_name or franchisor_name → operator_name
        - LLM が v2 レガシーだけ返した場合: is_franchise → operation_type,
          operator_name → franchisee_name (推測)
        """
        ot = (self.operation_type or "unknown").lower()
        if ot not in OPERATION_TYPES:
            ot = "unknown"

        # v2→v3 方向: operation_type が "unknown" で is_franchise が明示されている場合、
        # is_franchise から operation_type を推論する
        if ot == "unknown" and (self.franchisor_name or self.franchisee_name or self.operator_name):
            if not self.is_franchise:
                ot = "direct"
            else:
                ot = "franchisee"
        elif ot == "unknown":
            # names もなく is_franchise のみ: direct/franchisee に倒す
            ot = "direct" if not self.is_franchise else "franchisee"

        self.operation_type = ot

        # v3→v2 方向: is_franchise を operation_type から導出
        self.is_franchise = ot != "direct"

        # operator_name: franchisee 優先 → franchisor → 既存値を維持
        if not self.operator_name:
            self.operator_name = self.franchisee_name or self.franchisor_name
        # operator_name が既にあって franchisee_name が空なら推測で埋める
        if self.operator_name and not self.franchisee_name and not self.franchisor_name:
            if ot == "franchisee":
                self.franchisee_name = self.operator_name
            elif ot == "direct":
                self.franchisor_name = self.operator_name
        return self


# ─── LLM Protocol (for mocking) ────────────────────────────────────────


class LLMClient(Protocol):
    """browser_use.llm.Chat* が満たすべき最小 interface。"""

    async def ainvoke(
        self,
        messages: list[Any],
        output_format: Optional[type] = None,
        **kwargs: Any,
    ) -> Any: ...


# Agent factory type: (task: str, llm: Any) -> awaitable of JudgeJSON-ish dict
BrowserAgentFn = Callable[[str, Any, str], Awaitable[JudgeJSON]]


# ─── Thresholds / knobs ────────────────────────────────────────────────


DEFAULT_BROWSER_FALLBACK_THRESHOLD = 0.4


def _browser_threshold() -> float:
    try:
        return float(os.getenv("BROWSER_FALLBACK_THRESHOLD", DEFAULT_BROWSER_FALLBACK_THRESHOLD))
    except ValueError:
        return DEFAULT_BROWSER_FALLBACK_THRESHOLD


def _browser_fallback_enabled() -> bool:
    return os.getenv("ENABLE_BROWSER_FALLBACK", "0") == "1"


# ─── Prompt loading ────────────────────────────────────────────────────

_PROMPT_PATH = Path(__file__).parent / "prompts" / "judge.yaml"


def _load_prompt() -> dict[str, str]:
    with _PROMPT_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ─── Browser fallback (default: real browser_use.Agent) ────────────────


async def _default_browser_agent(task: str, llm: Any, url: str) -> JudgeJSON:
    """実 browser_use.Agent で url を訪問して JudgeJSON を返す。"""
    from browser_use import Agent  # lazy import: Playwright 依存

    agent = Agent(task=task, llm=llm)
    history = await agent.run(max_steps=10)
    # browser-use の history から final output を抽出
    final = getattr(history, "final_result", None) or getattr(history, "result", None)
    if final is None:
        raise RuntimeError("browser_use.Agent returned no final_result")
    if isinstance(final, JudgeJSON):
        return final
    if isinstance(final, dict):
        return JudgeJSON.model_validate(final)
    if isinstance(final, str):
        return _parse_json_str(final)
    raise RuntimeError(f"unexpected final_result type: {type(final).__name__}")


def _build_browser_task(req: JudgeRequest) -> str:
    urls = req.candidate_urls or ([req.official_url] if req.official_url else [])
    return (
        f"以下の店舗について、公式サイトを訪問して運営会社と店舗数を確認してください:\n"
        f"  ブランド: {req.brand}\n"
        f"  店舗名: {req.name}\n"
        f"  公式 URL: {req.official_url or '(不明)'}\n"
        f"  追加候補: {', '.join(urls)}\n\n"
        f"会社概要ページや運営会社情報へ遷移し、以下を JSON で返してください:\n"
        f"{{'is_franchise': bool, 'operator_name': str, 'store_count_estimate': int,\n"
        f" 'confidence': float (0-1), 'reasoning': str (200 字以内)}}\n"
        f"FC と直営が混在するチェーンでは、該当店舗の運営形態を確認して下さい。"
    )


# ─── Core judge_franchise ──────────────────────────────────────────────


async def judge_franchise(
    req: JudgeRequest,
    *,
    llm: LLMClient | None = None,
    provider_name: str = "",
    model_name: str = "",
    browser_agent: BrowserAgentFn | None = None,
    enable_browser_fallback: bool | None = None,
) -> JudgeReply:
    """browser-use LLM で FC 判定する。

    enable_browser_fallback が True (or None かつ ENABLE_BROWSER_FALLBACK=1) で
    confidence < BROWSER_FALLBACK_THRESHOLD のとき browser_agent を呼ぶ。
    browser_agent が None なら実 browser_use.Agent を使う。

    llm None 時は provider registry から解決。
    """
    if llm is None:
        from pizza_delivery.providers import get_provider

        provider_name = provider_name or os.getenv("LLM_PROVIDER", "anthropic")
        provider = get_provider(provider_name)
        llm = provider.make_llm()
        if not model_name:
            model_name = getattr(llm, "model", "") or getattr(llm, "model_name", "")

    from browser_use.llm.messages import SystemMessage, UserMessage

    prompt = _load_prompt()
    system_msg = SystemMessage(content=prompt["system"])
    user_msg = UserMessage(
        content=prompt["task"].format(
            brand=req.brand,
            name=req.name,
            address=req.address or "(不明)",
            official_url=req.official_url or "(不明)",
            markdown=req.markdown[:5000] if req.markdown else "(Markdown 未取得)",
            candidate_urls="\n".join(req.candidate_urls) or "(なし)",
        )
    )
    completion = await llm.ainvoke([system_msg, user_msg], output_format=JudgeJSON)
    parsed = _extract_judge_json(completion).derive_legacy()
    used_browser = False

    # Browser fallback
    should_try_browser = enable_browser_fallback
    if should_try_browser is None:
        should_try_browser = _browser_fallback_enabled()
    if (
        should_try_browser
        and parsed.confidence < _browser_threshold()
        and (req.official_url or req.candidate_urls)
    ):
        try:
            fn = browser_agent or _default_browser_agent
            task = _build_browser_task(req)
            browser_result = (await fn(task, llm, req.official_url)).derive_legacy()
            # browser の結果で上書き (confidence を少し底上げ)
            parsed = JudgeJSON(
                operation_type=browser_result.operation_type,
                franchisor_name=browser_result.franchisor_name,
                franchisee_name=browser_result.franchisee_name,
                is_franchise=browser_result.is_franchise,
                operator_name=browser_result.operator_name,
                store_count_estimate=browser_result.store_count_estimate,
                confidence=min(1.0, browser_result.confidence + 0.1),
                reasoning=f"[browser] {browser_result.reasoning}",
            ).derive_legacy()
            used_browser = True
        except Exception as exc:  # noqa: BLE001 — log and fall through
            # browser 失敗時は LLM 単独結果を返す (判定は捨てない)
            parsed = JudgeJSON(
                operation_type=parsed.operation_type,
                franchisor_name=parsed.franchisor_name,
                franchisee_name=parsed.franchisee_name,
                is_franchise=parsed.is_franchise,
                operator_name=parsed.operator_name,
                store_count_estimate=parsed.store_count_estimate,
                confidence=parsed.confidence,
                reasoning=f"{parsed.reasoning} [browser fallback failed: {type(exc).__name__}]",
            ).derive_legacy()

    return JudgeReply(
        place_id=req.place_id,
        is_franchise=parsed.is_franchise,
        operator_name=parsed.operator_name,
        store_count_estimate=parsed.store_count_estimate,
        confidence=parsed.confidence,
        llm_provider=provider_name or "unknown",
        llm_model=model_name or "unknown",
        reasoning=parsed.reasoning,
        used_browser_fallback=used_browser,
        operation_type=parsed.operation_type,
        franchisor_name=parsed.franchisor_name,
        franchisee_name=parsed.franchisee_name,
        judge_mode="browser" if used_browser else "llm-only",
    )


def _parse_json_str(s: str) -> JudgeJSON:
    s = s.strip()
    start = s.find("{")
    end = s.rfind("}")
    if start >= 0 and end > start:
        s = s[start : end + 1]
    return JudgeJSON.model_validate(json.loads(s))


def _extract_judge_json(completion: Any) -> JudgeJSON:
    """ChatInvokeCompletion から JudgeJSON を抽出する。"""
    payload = getattr(completion, "completion", completion)
    if isinstance(payload, JudgeJSON):
        return payload
    if isinstance(payload, str):
        return _parse_json_str(payload)
    if isinstance(payload, dict):
        return JudgeJSON.model_validate(payload)
    raise ValueError(f"cannot extract JudgeJSON from {type(payload).__name__}: {payload!r}")
