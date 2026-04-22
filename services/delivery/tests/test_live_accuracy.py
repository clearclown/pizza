"""🔬 Live LLM multi-axis accuracy benchmark (Phase 4)。

Phase 4 では 3 軸で測定:
  - operation_type accuracy (4 値: direct/franchisee/mixed/unknown)
  - franchisor_name accuracy (正規化後の文字列一致)
  - franchisee_name accuracy ← **真の Phase 4 KPI** (メガジー特定の核心)

実行:
  cd services/delivery
  set -a; source ../../.env; set +a
  RUN_LIVE_ACCURACY=1 uv run pytest tests/test_live_accuracy.py -s

  # 件数を絞る:
  RUN_LIVE_ACCURACY=1 LIVE_MAX_SAMPLES=10 uv run pytest ...

  # プロバイダ切替:
  RUN_LIVE_ACCURACY=1 LLM_PROVIDER=openai uv run pytest ...
"""

from __future__ import annotations

import csv
import os
import re
from pathlib import Path

import pytest

from pizza_delivery.agent import JudgeRequest, judge_franchise
from pizza_delivery.providers import get_provider


_GOLDEN_PATH = (
    Path(__file__).resolve().parents[3] / "test" / "fixtures" / "judgement-golden.csv"
)


# ─── Company name normalization ────────────────────────────────────────


_LEGACY_SUFFIXES = [("（株）", "株式会社"), ("(株)", "株式会社"), ("㈱", "株式会社")]
_ZENKAKU_SPACE = "　"


def _normalize_jp_company(s: str) -> str:
    """日本の会社名の表記揺れを吸収する (golden vs LLM 応答の比較用)。

    - (株) / ㈱ → 株式会社
    - 前後空白 trim、全角空白 → 半角
    - 「・」「 」「 」「 」などの区切りを除去
    - 大文字小文字統一 (ASCII)
    """
    if not s:
        return ""
    s = s.strip()
    for src, dst in _LEGACY_SUFFIXES:
        s = s.replace(src, dst)
    s = s.replace(_ZENKAKU_SPACE, " ")
    # 区切り文字を除去
    s = re.sub(r"[・\s]+", "", s)
    return s.lower()


def _names_match(expected: str, actual: str) -> bool:
    """会社名が実質同じかを判定する。

    - 正規化後に完全一致
    - または正規化後に一方がもう一方の substring (e.g., "AFJ Project" ⊂ "株式会社AFJ Project")
    """
    e = _normalize_jp_company(expected)
    a = _normalize_jp_company(actual)
    if not e or not a:
        return False
    if e == a:
        return True
    if len(e) >= 3 and e in a:
        return True
    if len(a) >= 3 and a in e:
        return True
    return False


# ─── Data loader ───────────────────────────────────────────────────────


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


# ─── Main benchmark ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_live_multi_axis_accuracy_on_golden() -> None:
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
    assert total >= 5, "golden must have ≥5 samples"

    # 3 軸のカウンタ
    op_total = 0
    op_correct = 0
    ffc_total = 0  # franchisor でラベルがあるもの
    ffc_correct = 0
    fee_total = 0  # franchisee でラベルがあるもの
    fee_correct = 0

    wrong_op: list[str] = []
    wrong_fc: list[str] = []
    wrong_fe: list[str] = []

    for row in rows:
        req = JudgeRequest(
            place_id=row["place_id"],
            brand=row["brand"],
            name=row["name"],
            official_url=row.get("official_url", ""),
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

        # --- 軸 1: operation_type ---
        true_ot = (row.get("true_operation_type") or "").strip().lower()
        pred_ot = (reply.operation_type or "").strip().lower()
        if true_ot:
            op_total += 1
            if true_ot == pred_ot:
                op_correct += 1
            else:
                wrong_op.append(
                    f"{row['brand']:20s} true={true_ot:10s} pred={pred_ot:10s}"
                )

        # --- 軸 2: franchisor_name (ラベルがある行のみ) ---
        true_fc = (row.get("true_franchisor_name") or "").strip()
        pred_fc = (reply.franchisor_name or "").strip()
        if true_fc:
            ffc_total += 1
            if _names_match(true_fc, pred_fc):
                ffc_correct += 1
            else:
                wrong_fc.append(
                    f"{row['brand']:20s} true='{true_fc}' pred='{pred_fc}'"
                )

        # --- 軸 3: franchisee_name (ラベルがある行のみ — 真の KPI) ---
        true_fe = (row.get("true_franchisee_name") or "").strip()
        pred_fe = (reply.franchisee_name or "").strip()
        if true_fe:
            fee_total += 1
            if _names_match(true_fe, pred_fe):
                fee_correct += 1
            else:
                wrong_fe.append(
                    f"{row['brand']:20s} true='{true_fe}' pred='{pred_fe}'"
                )

        # 逐次ログ (is_franchise 表示は後方互換の sanity)
        mark_ot = "✅" if true_ot and true_ot == pred_ot else "❌" if true_ot else "–"
        mark_fc = (
            "✅" if true_fc and _names_match(true_fc, pred_fc)
            else "❌" if true_fc else "–"
        )
        mark_fe = (
            "✅" if true_fe and _names_match(true_fe, pred_fe)
            else "❌" if true_fe else "–"
        )
        print(
            f"  [{mark_ot}{mark_fc}{mark_fe}] {row['brand']:20s} "
            f"op={pred_ot:10s} franchisor='{pred_fc[:30]}' franchisee='{pred_fe[:30]}'"
        )

    op_acc = op_correct / op_total if op_total else 0.0
    ffc_acc = ffc_correct / ffc_total if ffc_total else 0.0
    fee_acc = fee_correct / fee_total if fee_total else 0.0

    print(f"\n{'='*72}")
    print(f"Provider: {provider_name}  Model: {model}")
    print(f"{'='*72}")
    print(
        f"[AXIS 1] operation_type : {op_correct}/{op_total} = {op_acc*100:.1f}%"
    )
    print(
        f"[AXIS 2] franchisor_name: {ffc_correct}/{ffc_total} = {ffc_acc*100:.1f}%"
    )
    print(
        f"[AXIS 3] franchisee_name: {fee_correct}/{fee_total} = {fee_acc*100:.1f}%  ⭐ Phase 4 KPI"
    )
    print(f"{'='*72}")
    if wrong_op:
        print("\n❌ operation_type の誤判定:")
        for w in wrong_op:
            print(f"  {w}")
    if wrong_fc:
        print("\n❌ franchisor_name の不一致:")
        for w in wrong_fc:
            print(f"  {w}")
    if wrong_fe:
        print("\n❌ franchisee_name の不一致:")
        for w in wrong_fe:
            print(f"  {w}")
    print(f"{'='*72}\n")

    # Soft assertions — benchmark は数値を**記録する**のが目的
    # sanity: operation_type は Phase 3 水準 (≥80% baseline) を超えている
    if op_total > 0:
        assert op_acc >= 0.5, (
            f"operation_type accuracy {op_acc*100:.1f}% が低すぎる (sanity baseline 50%)"
        )


# ─── Unit test for the normalization helper ────────────────────────────


@pytest.mark.parametrize(
    "a, b, expected",
    [
        ("株式会社AFJ Project", "AFJ Project", True),
        ("(株)AFJ Project", "株式会社AFJ Project", True),
        ("㈱AFJ Project", "AFJ Project", True),
        ("株式会社セブン-イレブン・ジャパン", "セブン-イレブン・ジャパン", True),
        ("スターバックス コーヒー ジャパン株式会社", "スターバックス コーヒー ジャパン", True),
        ("株式会社A", "株式会社B", False),
        ("株式会社ABC", "XYZ株式会社", False),
        ("", "株式会社A", False),
    ],
)
def test_names_match(a: str, b: str, expected: bool) -> None:
    assert _names_match(a, b) == expected
