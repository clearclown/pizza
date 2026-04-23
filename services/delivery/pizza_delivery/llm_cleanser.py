"""LLM による operator 名のクレンジング。

用途:
  1. Pre-cleanse (`canonicalize_operator_name`)
     Per-store 抽出 / 人間入力の揺らぐ operator 名を、検索しやすい
     標準形 (例: "㈱モス" → "株式会社モス") に LLM が直して返す。
     国税庁 CSV の LIKE 検索前の前処理に使う。

  2. Rerank (`rerank_candidates`)
     検索結果が複数ある場合に、LLM が「入力名と最も一致する候補」を
     選んで index を返す。fuzzy match 後の再判定用。

設計方針:
  - LLM 誤答耐性のため JSON スキーマで structured output を強制。
  - provider は providers.py で抽象化済みの ChatAnthropic / ChatGemini に
    依存、そのまま browser_use.llm のインタフェースに合わせる。
  - 失敗時は graceful に原文を返す (絶対に例外を投げない)。
"""

from __future__ import annotations

import json
import logging
from typing import Any, Protocol

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ─── 入出力スキーマ ────────────────────────────────────────────


class CleanseResult(BaseModel):
    """LLM に返してほしい JSON。"""

    canonical: str = Field(
        default="",
        description="国税庁法人番号 DB 検索に適した標準表記 (株式会社 プレフィックス含む)",
    )
    is_legal_entity: bool = Field(
        default=True,
        description="法人として扱えるか (支店・個人店舗なら False)",
    )
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    note: str = Field(default="")


class RerankPick(BaseModel):
    """LLM 再順位付けの結果。"""

    best_index: int = Field(default=-1, description="候補 0-indexed、無ければ -1")
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    reason: str = Field(default="")


# ─── LLM Protocol (browser_use.llm.Chat* 互換) ────────────────


class LLMChat(Protocol):
    """browser_use.llm の最小限 interface。"""

    async def ainvoke(self, messages: Any, output_format: Any | None = None) -> Any:
        ...


async def _invoke_structured(
    llm: LLMChat, system: str, user: str, schema: type[BaseModel]
) -> BaseModel | None:
    """structured output 付きで LLM を呼ぶ。失敗時 None。"""
    try:
        from browser_use.llm.messages import SystemMessage, UserMessage
    except ImportError:
        # テスト mock 用に dict ベースで試す
        messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    else:
        messages = [SystemMessage(content=system), UserMessage(content=user)]

    try:
        reply = await llm.ainvoke(messages, output_format=schema)
    except Exception as e:
        logger.debug("llm structured invoke failed: %s", e)
        return None

    # 多くの wrapper は .completion / .content / .parsed を持つ
    parsed = getattr(reply, "completion", None) or getattr(reply, "parsed", None) or reply
    if isinstance(parsed, schema):
        return parsed
    if isinstance(parsed, dict):
        try:
            return schema.model_validate(parsed)
        except Exception:
            return None
    if isinstance(parsed, str):
        try:
            return schema.model_validate_json(parsed)
        except Exception:
            try:
                return schema.model_validate(json.loads(parsed))
            except Exception:
                return None
    return None


# ─── Pre-cleanse ────────────────────────────────────────────


_CANONICAL_SYSTEM = """あなたは日本の法人名表記の正規化担当。
与えられた会社名を、国税庁『法人番号公表サイト』に登録されている形式に揃えて返す。

ルール:
- `㈱` / `(株)` / `（株）` は 「株式会社」 に展開
- `㈲` / `(有)` は 「有限会社」 に展開
- 前後空白・全角空白を取り除く
- カタカナ / 英字 はそのまま (全角/半角は半角に統一しない)
- 支店名 / 店舗名 (例: 「株式会社A 渋谷店」) は法人名のみに削る
- 商標のみ (例: 「モスバーガー」) は法人格が付いていない → is_legal_entity=False、canonical は空

JSON 必須: {canonical, is_legal_entity, confidence(0-1), note}。
"""


async def canonicalize_operator_name(name: str, llm: LLMChat) -> CleanseResult:
    """operator 名を LLM で検索用正規形にクレンジング。

    失敗時は CleanseResult(canonical=name, confidence=0) を返して graceful。
    """
    if not name or not name.strip():
        return CleanseResult(canonical="", is_legal_entity=False, confidence=0.0)
    user = f"以下の会社名を正規化してください:\n  入力: {name.strip()}"
    r = await _invoke_structured(llm, _CANONICAL_SYSTEM, user, CleanseResult)
    if r is None:
        return CleanseResult(canonical=name.strip(), is_legal_entity=True, confidence=0.0)
    if not r.canonical:
        r.canonical = name.strip()
    return r


# ─── Post-rerank ────────────────────────────────────────────


_RERANK_SYSTEM = """あなたは法人名マッチングの専門家。
入力された operator 名に対し、候補 list の中から「最も同じ法人を指す」ものを選ぶ。

考慮事項:
- 表記ゆれ (株式会社/㈱/(株))、長音 (ダッシュ/ハイフン/マクロン)、半角/全角
- **子会社やグループ会社は別法人** (例: 「モスフードサービス」と「モスストアカンパニー」は別)
- 候補が全て外れと判断できる場合、best_index=-1 を返す

JSON 必須: {best_index (0-indexed or -1), confidence(0-1), reason(50字以内)}。
"""


async def rerank_candidates(
    input_name: str,
    candidates: list[str],
    llm: LLMChat,
) -> RerankPick:
    """候補 list から最も一致する 1 件を LLM に選ばせる。

    candidates が空 or LLM 失敗なら best_index=-1 を返す。
    """
    if not input_name or not candidates:
        return RerankPick(best_index=-1, confidence=0.0)
    listing = "\n".join(f"  [{i}] {c}" for i, c in enumerate(candidates))
    user = (
        f"入力: {input_name}\n\n"
        f"候補:\n{listing}\n\n"
        f"入力と同じ法人を指す候補の index を返してください (無ければ -1)。"
    )
    r = await _invoke_structured(llm, _RERANK_SYSTEM, user, RerankPick)
    if r is None:
        return RerankPick(best_index=-1, confidence=0.0)
    # index 範囲チェック
    if r.best_index >= len(candidates):
        r.best_index = -1
    return r
