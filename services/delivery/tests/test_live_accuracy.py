"""🔬 Live LLM accuracy benchmark — Classification Accuracy 実測。

実 Claude / GPT / Gemini に対して golden dataset を流し、
Phase 3 DoD-6 (≥ 90%) の達成状況を記録する。

実行条件:
  - RUN_LIVE_ACCURACY=1 (env gate) かつ
  - 選択した provider の API キーが env にセット済

通常 pytest では skip される (コスト節約)。

使い方:
  # Anthropic Claude で測定 (default)
  cd services/delivery
  set -a; source ../../.env; set +a
  RUN_LIVE_ACCURACY=1 uv run pytest tests/test_live_accuracy.py -s

  # OpenAI で測定
  RUN_LIVE_ACCURACY=1 LLM_PROVIDER=openai uv run pytest tests/test_live_accuracy.py -s

  # 件数を絞る (コスト抑制)
  RUN_LIVE_ACCURACY=1 LIVE_MAX_SAMPLES=10 uv run pytest ...
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import pytest

from pizza_delivery.agent import JudgeRequest, judge_franchise
from pizza_delivery.providers import get_provider


_GOLDEN_PATH = (
    Path(__file__).resolve().parents[3] / "test" / "fixtures" / "judgement-golden.csv"
)
_TARGET_ACCURACY = 0.90


def _should_run() -> tuple[bool, str]:
    if os.getenv("RUN_LIVE_ACCURACY") != "1":
        return False, "set RUN_LIVE_ACCURACY=1 to enable"
    if not _GOLDEN_PATH.exists():
        return False, f"golden CSV missing: {_GOLDEN_PATH}"
    provider_name = os.getenv("LLM_PROVIDER", "anthropic")
    try:
        provider = get_provider(provider_name)
    except ValueError as e:
        return False, str(e)
    if not provider.ready():
        return False, f"provider {provider_name} not ready (API key missing)"
    return True, ""


def _load_golden() -> list[dict[str, str]]:
    with _GOLDEN_PATH.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@pytest.mark.asyncio
async def test_live_classification_accuracy_on_golden() -> None:
    ok, reason = _should_run()
    if not ok:
        pytest.skip(reason)

    provider_name = os.getenv("LLM_PROVIDER", "anthropic")
    max_samples = int(os.getenv("LIVE_MAX_SAMPLES", "0") or 0)

    provider = get_provider(provider_name)
    llm = provider.make_llm()
    model = getattr(llm, "model", "") or getattr(llm, "model_name", "")

    rows = _load_golden()
    if max_samples > 0:
        rows = rows[:max_samples]

    total = len(rows)
    assert total >= 5, "golden must have ≥5 samples for meaningful accuracy"

    correct = 0
    wrong: list[dict[str, str]] = []
    for row in rows:
        req = JudgeRequest(
            place_id=row["place_id"],
            brand=row["brand"],
            name=row["name"],
            official_url=row.get("official_url", ""),
            # Markdown は未取得 — Phase 3.5 で Firecrawl 連携
            markdown=(
                f"# {row['brand']} 店舗情報\n\n"
                f"**店舗名**: {row['name']}\n\n"
                f"**公式 URL**: {row.get('official_url', '')}\n\n"
                f"(コメント: {row.get('notes', '')})"
            ),
        )
        reply = await judge_franchise(
            req, llm=llm, provider_name=provider_name, model_name=model
        )
        true_fc = row["true_is_franchise"].strip().lower() == "true"
        is_correct = reply.is_franchise == true_fc
        if is_correct:
            correct += 1
        else:
            wrong.append(
                {
                    "brand": row["brand"],
                    "name": row["name"],
                    "expected": "FC" if true_fc else "直営",
                    "predicted": "FC" if reply.is_franchise else "直営",
                    "confidence": f"{reply.confidence:.2f}",
                    "operator": reply.operator_name,
                    "reasoning": reply.reasoning[:100],
                }
            )
        print(
            f"  [{'✅' if is_correct else '❌'}] {row['brand']:20s} "
            f"pred={'FC' if reply.is_franchise else '直営'} "
            f"(conf={reply.confidence:.2f}) "
            f"op={reply.operator_name[:30] if reply.operator_name else '(なし)'}"
        )

    accuracy = correct / total
    print(f"\n{'='*60}")
    print(f"LIVE accuracy: {correct}/{total} = {accuracy*100:.1f}%")
    print(f"  provider={provider_name}  model={model}")
    print(f"  target={_TARGET_ACCURACY*100:.0f}%")
    if wrong:
        print(f"\n間違い一覧 ({len(wrong)} 件):")
        for w in wrong:
            print(
                f"  - {w['brand']}: expected={w['expected']} predicted={w['predicted']} "
                f"conf={w['confidence']} op={w['operator']}"
            )
    print(f"{'='*60}")

    # Phase 3 DoD-6 を本 assertion で強制しない (初回測定時の下限調整用)
    # ユーザーはログから accuracy を見て判断する
    # 参考: 2026-04 時点では 30 件 golden に対し Claude で 85-95% が期待値
    assert accuracy >= 0.5, (
        f"accuracy {accuracy*100:.1f}% is below sanity baseline 50% — "
        f"prompt may be broken"
    )
    # Phase 3 目標への進捗チェック (これは soft assertion として記録のみ)
    if accuracy < _TARGET_ACCURACY:
        print(
            f"\n⚠️  Phase 3 target ≥{_TARGET_ACCURACY*100:.0f}% 未達。"
            f"prompts/judge.yaml のチューニングが必要"
        )
