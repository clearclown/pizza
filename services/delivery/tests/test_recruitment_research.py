"""recruitment_research の決定論 gate テスト。

ネットワーク/Gemini は叩かず、求人ページ本文の evidence 判定だけ検証する。
"""

from __future__ import annotations

from pizza_delivery.recruitment_research import (
    _load_target_rows,
    apply_accepted_from_proposal_json,
    build_store_keys,
    check_recruitment_evidence,
    parse_recruitment_candidates,
    write_recruitment_sidecars,
)


def test_build_store_keys_removes_brand_prefix() -> None:
    keys = build_store_keys(
        "モスバーガー 梅ヶ丘駅前店",
        "東京都世田谷区梅丘1-1-1",
        "03-1234-5678",
        "モスバーガー",
    )
    assert "03-1234-5678" in keys
    assert "0312345678" in keys
    assert "梅ヶ丘駅前" in keys
    assert "梅ヶ丘駅前店" in keys


def test_parse_recruitment_candidates_dedups_and_requires_url() -> None:
    data = {
        "candidates": [
            {
                "operator_name": "株式会社サンプル",
                "evidence_urls": ["https://jobs.example/a"],
                "source_type": "job_site",
                "confidence": 0.8,
            },
            {
                "operator_name": "株式会社サンプル",
                "evidence_urls": ["https://jobs.example/a"],
            },
            {"operator_name": "株式会社URLなし"},
        ]
    }
    got = parse_recruitment_candidates(data)
    assert len(got) == 1
    assert got[0].operator_name == "株式会社サンプル"
    assert got[0].confidence == 0.8


def test_recruitment_evidence_accepts_employer_label_and_store_key() -> None:
    keys = ["梅ヶ丘駅前", "03-1234-5678"]
    html = """
    <html><body>
      <h1>モスバーガー梅ヶ丘駅前店 アルバイト求人</h1>
      <dl>
        <dt>会社名</dt><dd>株式会社サンプルフーズ</dd>
        <dt>勤務地</dt><dd>モスバーガー梅ヶ丘駅前店</dd>
      </dl>
      <p>パート・アルバイトを募集しています。</p>
    </body></html>
    """
    ev = check_recruitment_evidence(
        html,
        source_url="https://jobs.example/a",
        operator_name="株式会社サンプルフーズ",
        store_keys=keys,
    )
    assert ev.found is True
    assert ev.url == "https://jobs.example/a"
    assert ev.matched_label == "会社名"
    assert ev.matched_store_key in keys


def test_recruitment_evidence_rejects_without_employer_label() -> None:
    html = """
    <html><body>
      <h1>モスバーガー梅ヶ丘駅前店 求人</h1>
      <p>株式会社サンプルフーズは地域で事業を展開しています。</p>
    </body></html>
    """
    ev = check_recruitment_evidence(
        html,
        source_url="https://jobs.example/a",
        operator_name="株式会社サンプルフーズ",
        store_keys=["梅ヶ丘駅前"],
    )
    assert ev.found is False
    assert ev.reject_reason == "operator_without_employer_label"


def test_recruitment_evidence_rejects_non_job_page() -> None:
    html = """
    <html><body>
      <p>会社名 株式会社サンプルフーズ</p>
      <p>モスバーガー梅ヶ丘駅前店</p>
    </body></html>
    """
    ev = check_recruitment_evidence(
        html,
        source_url="https://example.co.jp",
        operator_name="株式会社サンプルフーズ",
        store_keys=["梅ヶ丘駅前"],
    )
    assert ev.found is False
    assert ev.reject_reason == "not_job_or_recruit_page"


def test_write_recruitment_sidecars_keeps_failed_and_unverified_urls(tmp_path) -> None:
    out = tmp_path / "sample.json"
    proposals = [
        {
            "brand": "シャトレーゼ",
            "place_id": "p1",
            "store_name": "シャトレーゼ七光台店",
            "address": "千葉県野田市七光台4-2",
            "phone": "04-7190-5454",
            "accepted": False,
            "reject_reason": "houjin_no_exact_match",
            "final_operator": "",
            "final_corp": "",
            "candidates": [
                {
                    "operator_name": "東京日食株式会社",
                    "confidence": 1.0,
                    "source_type": "job_site",
                    "evidence_urls": ["https://jobs.example/a"],
                }
            ],
            "evidence_attempts": [
                {
                    "candidate_operator": "東京日食株式会社",
                    "candidate_confidence": 1.0,
                    "source_type": "job_site",
                    "url": "https://jobs.example/a",
                    "fetched": True,
                    "found": False,
                    "reject_reason": "houjin_no_exact_match",
                    "matched_store_key": "七光台",
                    "matched_label": "採用企業",
                    "snippet": "シャトレーゼ七光台店 東京日食株式会社",
                }
            ],
        }
    ]
    paths = write_recruitment_sidecars(out, proposals)

    failed = (tmp_path / "sample-failed-urls.csv").read_text(encoding="utf-8")
    unverified = (tmp_path / "sample-unverified.csv").read_text(encoding="utf-8")
    candidates = (tmp_path / "sample-candidates.csv").read_text(encoding="utf-8")

    assert paths["failed_urls"].endswith("sample-failed-urls.csv")
    assert "https://jobs.example/a" in failed
    assert "houjin_no_exact_match" in failed
    assert "東京日食株式会社" in unverified
    assert "シャトレーゼ七光台店" in candidates


def test_write_recruitment_sidecars_accepted_uses_final_evidence_only(tmp_path) -> None:
    out = tmp_path / "sample.json"
    proposals = [
        {
            "brand": "韓丼",
            "place_id": "p1",
            "store_name": "韓丼 橿原店",
            "address": "奈良県橿原市葛本町",
            "phone": "0744-47-0818",
            "accepted": True,
            "reject_reason": "",
            "final_operator": "大和物産株式会社",
            "final_corp": "9050001023334",
            "candidates": [
                {
                    "operator_name": "株式会社グリフィンホールディングス",
                    "confidence": 0.9,
                    "source_type": "job_site",
                    "evidence_urls": ["https://jobs.example/wrong"],
                },
                {
                    "operator_name": "大和物産株式会社",
                    "confidence": 0.5,
                    "source_type": "job_site",
                    "evidence_urls": ["https://jobs.example/final"],
                },
            ],
            "evidence": {
                "url": "https://jobs.example/final",
                "matched_store_key": "橿原",
                "matched_label": "企業名",
                "snippet": "韓丼 橿原店 大和物産株式会社",
            },
        }
    ]
    write_recruitment_sidecars(out, proposals)

    accepted = (tmp_path / "sample-accepted.csv").read_text(encoding="utf-8")
    assert "大和物産株式会社" in accepted
    assert "https://jobs.example/final" in accepted
    assert "株式会社グリフィンホールディングス" not in accepted
    assert "https://jobs.example/wrong" not in accepted


def test_load_target_rows_supports_offset(tmp_path) -> None:
    db = tmp_path / "pizza.sqlite"
    import sqlite3

    conn = sqlite3.connect(db)
    try:
        conn.execute(
            "CREATE TABLE stores (place_id TEXT, brand TEXT, name TEXT, address TEXT, phone TEXT)"
        )
        conn.execute(
            "CREATE TABLE operator_stores (place_id TEXT, operator_name TEXT, operator_type TEXT)"
        )
        for i in range(5):
            conn.execute(
                "INSERT INTO stores VALUES (?, 'シャトレーゼ', ?, '東京都渋谷区1-1', '03-0000-0000')",
                (f"p{i}", f"シャトレーゼ{i}店"),
            )
        conn.commit()
    finally:
        conn.close()

    rows = _load_target_rows(db, "シャトレーゼ", max_stores=2, offset=2)
    assert [r[0] for r in rows] == ["p2", "p3"]


def test_apply_accepted_from_proposal_json(tmp_path) -> None:
    import json
    import sqlite3

    db = tmp_path / "pizza.sqlite"
    conn = sqlite3.connect(db)
    try:
        conn.execute(
            """
            CREATE TABLE operator_stores (
              operator_name TEXT, place_id TEXT, brand TEXT, operator_type TEXT,
              confidence REAL, discovered_via TEXT, verification_score REAL,
              corporate_number TEXT, verification_source TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    proposals = tmp_path / "proposal.json"
    proposals.write_text(json.dumps({
        "proposals": [
            {
                "place_id": "p1",
                "store_name": "シャトレーゼ七光台店",
                "address": "千葉県野田市",
                "phone": "04-7190-5454",
                "brand": "シャトレーゼ",
                "accepted": True,
                "final_operator": "東京日食株式会社",
                "final_corp": "6040001019311",
                "evidence": {"found": True, "url": "https://jobs.example/final"},
            }
        ]
    }, ensure_ascii=False), encoding="utf-8")

    assert apply_accepted_from_proposal_json(db, proposals) == 1
    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT operator_name, corporate_number FROM operator_stores WHERE place_id='p1'"
        ).fetchone()
    finally:
        conn.close()
    assert row == ("東京日食株式会社", "6040001019311")
