"""🔬 Live evidence-based accuracy benchmark (Phase 4 evidence pivot)。

judge_by_evidence を実 URL + 実 LLM に対して走らせる。
LLM 推論ではなく、公式サイトから収集した evidence snippet のみを根拠に判定する。

実行:
  cd services/delivery
  set -a; source ../../.env; set +a
  RUN_LIVE_EVIDENCE=1 LLM_PROVIDER=gemini GEMINI_MODEL=gemini-2.5-flash \
    LIVE_MAX_SAMPLES=10 uv run pytest tests/test_live_evidence_accuracy.py -s
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import pytest

from pizza_delivery.agent import JudgeRequest, judge_by_evidence
from pizza_delivery.providers import get_provider


_GOLDEN_PATH = (
    Path(__file__).resolve().parents[3] / "test" / "fixtures" / "judgement-golden.csv"
)


def _should_run() -> tuple[bool, str]:
    if os.getenv("RUN_LIVE_EVIDENCE") != "1":
        return False, "set RUN_LIVE_EVIDENCE=1 to enable"
    if not _GOLDEN_PATH.exists():
        return False, f"golden CSV missing: {_GOLDEN_PATH}"
    provider_name = os.getenv("LLM_PROVIDER", "gemini")
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
async def test_live_evidence_accuracy_on_golden() -> None:
    ok, reason = _should_run()
    if not ok:
        pytest.skip(reason)

    provider_name = os.getenv("LLM_PROVIDER", "gemini")
    max_samples = int(os.getenv("LIVE_MAX_SAMPLES", "0") or 0)

    provider = get_provider(provider_name)
    llm = provider.make_llm()
    model = getattr(llm, "model", "") or getattr(llm, "model_name", "")

    rows = _load_golden()
    if max_samples > 0:
        rows = rows[:max_samples]

    total = len(rows)
    assert total >= 3

    # Metrics
    op_total = 0
    op_correct = 0
    ffc_total = 0
    ffc_correct = 0
    evidence_count_total = 0
    unknown_count = 0

    def _norm(s: str) -> str:
        import re

        if not s:
            return ""
        s = s.strip()
        s = s.replace("（株）", "株式会社").replace("(株)", "株式会社").replace("㈱", "株式会社")
        return re.sub(r"[・\s　]+", "", s).lower()

    def _match(a: str, b: str) -> bool:
        na, nb = _norm(a), _norm(b)
        if not na or not nb:
            return False
        if na == nb:
            return True
        if len(na) >= 3 and (na in nb or nb in na):
            return True
        return False

    print(f"\n{'='*78}")
    print(f"EVIDENCE-BASED accuracy: provider={provider_name} model={model}")
    print(f"{'='*78}")

    for row in rows:
        req = JudgeRequest(
            place_id=row["place_id"],
            brand=row["brand"],
            name=row["name"],
            markdown="",
            official_url=row.get("official_url", ""),
        )
        try:
            reply = await judge_by_evidence(
                req, llm=llm, provider_name=provider_name, model_name=model
            )
        except Exception as exc:  # noqa: BLE001
            print(f"  [ERR] {row['brand']:20s} {type(exc).__name__}: {exc}")
            continue

        # evidence 数を解析 ("(evidence=N sources)" from reasoning)
        import re as _re

        m = _re.search(r"evidence=(\d+) sources", reply.reasoning)
        ev_count = int(m.group(1)) if m else 0
        evidence_count_total += ev_count
        if reply.operation_type == "unknown":
            unknown_count += 1

        # AXIS 1: operation_type
        true_ot = (row.get("true_operation_type") or "").strip().lower()
        pred_ot = (reply.operation_type or "").strip().lower()
        if true_ot:
            op_total += 1
            if true_ot == pred_ot:
                op_correct += 1
        ok_ot = true_ot and true_ot == pred_ot

        # AXIS 2: franchisor
        true_fc = (row.get("true_franchisor_name") or "").strip()
        pred_fc = (reply.franchisor_name or "").strip()
        if true_fc:
            ffc_total += 1
            if _match(true_fc, pred_fc):
                ffc_correct += 1
        ok_fc = true_fc and _match(true_fc, pred_fc)

        mark_ot = "✅" if ok_ot else ("❌" if true_ot else "–")
        mark_fc = "✅" if ok_fc else ("❌" if true_fc else "–")
        print(
            f"  [{mark_ot}{mark_fc}] {row['brand']:18s} "
            f"ev={ev_count:2d} op={pred_ot:10s} "
            f"franchisor='{pred_fc[:35]}'"
        )

    op_acc = op_correct / op_total if op_total else 0.0
    ffc_acc = ffc_correct / ffc_total if ffc_total else 0.0

    print(f"\n{'='*78}")
    print(f"[AXIS 1] operation_type : {op_correct}/{op_total} = {op_acc*100:.1f}%")
    print(f"[AXIS 2] franchisor_name: {ffc_correct}/{ffc_total} = {ffc_acc*100:.1f}%")
    print(f"[EVIDENCE] total = {evidence_count_total}, unknown rows = {unknown_count}")
    print(f"{'='*78}")

    # Sanity: at least some evidence was collected overall
    assert evidence_count_total > 0, "evidence が 1 件も取れなかった"
