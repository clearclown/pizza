"""evaluator: truth × pipeline 突合 metric のテスト。

in-memory ORM に truth (JFA) と pipeline 両方を seed して、
brand_recall / operator_recall / link_recall を検証。
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine

from pizza_delivery.evaluator import evaluate
from pizza_delivery.orm import (
    create_all,
    link_brand_operator,
    make_session,
    upsert_brand,
    upsert_operator,
)


@pytest.fixture
def sess():
    engine = create_engine("sqlite:///:memory:", future=True)
    create_all(engine)
    s = make_session(engine)
    yield s
    s.close()


def _seed(sess, source: str, brand: str, operator: str) -> None:
    b = upsert_brand(sess, brand, source=source)
    o = upsert_operator(sess, name=operator, source=source)
    sess.flush()
    link_brand_operator(sess, brand=b, operator=o, source=source)


def test_evaluate_counts_hits_and_misses(sess) -> None:
    # truth
    _seed(sess, "jfa", "モスバーガー", "株式会社モスフードサービス")
    _seed(sess, "jfa", "マクドナルド", "日本マクドナルド株式会社")
    _seed(sess, "jfa", "ミスタードーナツ", "株式会社ダスキン")
    sess.commit()
    # pipeline: 2 ブランドのみ検出
    _seed(sess, "pipeline", "モスバーガー", "株式会社モスフードサービス")
    _seed(sess, "pipeline", "マクドナルド", "日本マクドナルド株式会社")
    sess.commit()

    r = evaluate(truth_source="jfa", pipeline_source="pipeline", orm_session=sess)
    assert r.truth_brand_count == 3
    assert r.pipeline_brand_count == 2
    assert r.brand_hits == 2
    assert round(r.brand_recall, 2) == round(2 / 3, 2)
    # miss に ミスタードーナツ (canonical_key 適用後) が入る
    assert any("ミスタードーナツ" in m or "ドーナツ" in m for m in r.brand_misses)
    assert r.link_hits == 2
    assert r.truth_link_count == 3


def test_evaluate_empty_sources(sess) -> None:
    r = evaluate(truth_source="jfa", pipeline_source="pipeline", orm_session=sess)
    assert r.truth_brand_count == 0
    assert r.brand_recall == 0.0
