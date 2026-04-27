from __future__ import annotations

from pizza_delivery.megafranchisee_clean_export import canonical_brand


def test_canonical_brand_normalizes_common_aliases() -> None:
    assert canonical_brand("ケンタッキーフライドチキン") == "ケンタッキー"
    assert canonical_brand("セブンイレブン") == "セブン-イレブン"
    assert canonical_brand("珈琲所コメダ珈琲店") == "コメダ珈琲"
    assert canonical_brand("韓丼") == "カルビ丼とスン豆腐専門店韓丼"
