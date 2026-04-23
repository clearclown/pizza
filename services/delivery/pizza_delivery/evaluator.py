"""JFA 会員一覧 を truth set として pipeline 出力の品質を評価する。

supervised learning 的な改良ループの metric 基盤。

算出指標:
  - Brand precision/recall    (JFA 掲載 brand の pipeline 側検出率)
  - Franchisor precision/recall (JFA 掲載 本部社 の検出率)
  - Brand-operator link 整合率 (JFA link と pipeline link の重なり)

LLM は一切使わない (決定論、canonical_key で名寄せ)。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from sqlalchemy.orm import Session, joinedload

from pizza_delivery.normalize import canonical_key
from pizza_delivery.orm import (
    BrandOperatorLink,
    FranchiseBrand,
    OperatorCompany,
    make_session,
)


@dataclass
class EvalReport:
    """1 回の評価結果。"""

    truth_brand_count: int = 0
    truth_operator_count: int = 0
    truth_link_count: int = 0
    pipeline_brand_count: int = 0
    pipeline_operator_count: int = 0
    pipeline_link_count: int = 0
    brand_hits: int = 0
    brand_misses: list[str] = field(default_factory=list)
    operator_hits: int = 0
    operator_misses: list[str] = field(default_factory=list)
    link_hits: int = 0         # truth link が pipeline 側でも存在する数
    link_only_in_truth: list[tuple[str, str]] = field(default_factory=list)

    @property
    def brand_recall(self) -> float:
        return self.brand_hits / self.truth_brand_count if self.truth_brand_count else 0.0

    @property
    def operator_recall(self) -> float:
        return (
            self.operator_hits / self.truth_operator_count
            if self.truth_operator_count
            else 0.0
        )

    @property
    def link_recall(self) -> float:
        return self.link_hits / self.truth_link_count if self.truth_link_count else 0.0


# ─── 評価本体 ─────────────────────────────────────────────


def evaluate(
    *,
    truth_source: str = "jfa",
    pipeline_source: str = "pipeline",
    orm_session: Session | None = None,
    max_misses: int = 20,
) -> EvalReport:
    """truth_source の brand / operator / link が pipeline_source 側に
    どれだけ現れているか集計して EvalReport を返す。

    `source` 列は ORM の `BrandOperatorLink.source` を使用。
    truth と pipeline は同じ ORM DB を share する (複数 source を source 列で区別)。
    """
    sess = orm_session or make_session()
    try:
        truth_links = (
            sess.query(BrandOperatorLink)
            .options(
                joinedload(BrandOperatorLink.brand),
                joinedload(BrandOperatorLink.operator),
            )
            .filter(BrandOperatorLink.source == truth_source)
            .all()
        )
        pipe_links = (
            sess.query(BrandOperatorLink)
            .options(
                joinedload(BrandOperatorLink.brand),
                joinedload(BrandOperatorLink.operator),
            )
            .filter(BrandOperatorLink.source == pipeline_source)
            .all()
        )

        truth_brands = {_k(link.brand.name) for link in truth_links if link.brand}
        truth_operators = {_k(link.operator.name) for link in truth_links if link.operator}
        truth_link_keys = {
            (_k(link.brand.name), _k(link.operator.name))
            for link in truth_links
            if link.brand and link.operator
        }

        pipe_brands = {_k(link.brand.name) for link in pipe_links if link.brand}
        pipe_operators = {_k(link.operator.name) for link in pipe_links if link.operator}
        pipe_link_keys = {
            (_k(link.brand.name), _k(link.operator.name))
            for link in pipe_links
            if link.brand and link.operator
        }

        brand_hits = truth_brands & pipe_brands
        brand_misses = sorted(truth_brands - pipe_brands)
        op_hits = truth_operators & pipe_operators
        op_misses = sorted(truth_operators - pipe_operators)
        link_hits = truth_link_keys & pipe_link_keys
        link_miss = sorted(truth_link_keys - pipe_link_keys)

    finally:
        if orm_session is None:
            sess.close()

    return EvalReport(
        truth_brand_count=len(truth_brands),
        truth_operator_count=len(truth_operators),
        truth_link_count=len(truth_link_keys),
        pipeline_brand_count=len(pipe_brands),
        pipeline_operator_count=len(pipe_operators),
        pipeline_link_count=len(pipe_link_keys),
        brand_hits=len(brand_hits),
        brand_misses=brand_misses[:max_misses],
        operator_hits=len(op_hits),
        operator_misses=op_misses[:max_misses],
        link_hits=len(link_hits),
        link_only_in_truth=link_miss[:max_misses],
    )


def _k(s: str) -> str:
    """名寄せ key (canonical_key で表記ゆれ吸収)。"""
    return canonical_key(s or "")


# ─── CLI ────────────────────────────────────────────────────


def _main() -> None:
    import argparse
    import json

    ap = argparse.ArgumentParser(
        description="JFA (truth) × pipeline (observed) の突合 metric 算出"
    )
    ap.add_argument("--truth", default="jfa", help="truth source (default jfa)")
    ap.add_argument("--pipeline", default="pipeline", help="pipeline source")
    ap.add_argument("--out", default="", help="JSON レポート出力 (空で stdout)")
    args = ap.parse_args()

    r = evaluate(truth_source=args.truth, pipeline_source=args.pipeline)
    report = {
        "truth_brand_count": r.truth_brand_count,
        "pipeline_brand_count": r.pipeline_brand_count,
        "brand_hits": r.brand_hits,
        "brand_recall": round(r.brand_recall, 4),
        "truth_operator_count": r.truth_operator_count,
        "operator_hits": r.operator_hits,
        "operator_recall": round(r.operator_recall, 4),
        "truth_link_count": r.truth_link_count,
        "link_hits": r.link_hits,
        "link_recall": round(r.link_recall, 4),
        "brand_misses_sample": r.brand_misses,
        "operator_misses_sample": r.operator_misses,
        "link_only_in_truth_sample": [list(p) for p in r.link_only_in_truth],
    }
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.out:
        from pathlib import Path

        Path(args.out).write_text(text, encoding="utf-8")
        print(f"✅ wrote {args.out}")
    else:
        print(text)


if __name__ == "__main__":
    _main()
