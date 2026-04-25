"""EDINET API v2 client + 関係会社 / 重要契約先 parser (Phase 27)。

金融庁 EDINET (有価証券報告書等電子開示) から listed company の
有価証券報告書を取得し、関係会社・重要な契約先を extract する。
FC 本部 (モスフードサービス E03384 等) の 有報 には **法定開示義務で**
FC 契約先の一部が 関係会社 section に記載される。

**ハルシネ 0 設計**:
  - EDINET は法定開示データ = truth set 同等
  - 抽出した各 company 名は必ず 国税庁 CSV で法人番号検証
  - 国税庁 に存在しなければ reject

**API v2 要件**:
  https://api.edinet-fsa.go.jp/api/v2 は Subscription-Key header 必須。
  key は https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/
         WZEK0110.html で無料登録可能。
  未設定時は graceful error (skip)、pipeline は続行。

CLI:
  pizza edinet-sync --edinet-code E03384 --brand モスバーガー \
      --date-from 2024-01-01 --out var/phase27/mos-edinet.csv
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import re
import zipfile
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)


EDINET_BASE = "https://api.edinet-fsa.go.jp/api/v2"

# 有価証券報告書の書類タイプコード (EDINET document type code)
DOC_TYPE_SECURITIES_REPORT = "120"  # 有価証券報告書


@dataclass
class EDINETDocument:
    """EDINET document metadata (search 結果 1 件分)。"""

    doc_id: str
    submit_date: str
    edinet_code: str
    filer_name: str
    doc_type_code: str
    doc_description: str = ""
    sec_code: str = ""
    period_end: str = ""


@dataclass
class EDINETCompany:
    """有報から抽出された 関係会社 / 重要契約先 1 社。"""

    name: str
    relationship: str = ""         # "subsidiary" / "affiliate" / "major_contract"
    corporate_number: str = ""
    source_doc_id: str = ""
    business_summary: str = ""


@dataclass
class EDINETSyncStats:
    docs_found: int = 0
    companies_extracted: int = 0
    companies_verified: int = 0    # 国税庁 verify pass
    orm_inserted: int = 0
    errors: list[str] = field(default_factory=list)


# ─── API client ─────────────────────────────────────────────


@dataclass
class EDINETClient:
    """EDINET API v2 の最小 wrapper。"""

    api_key: str = ""
    timeout: float = 30.0

    def __post_init__(self) -> None:
        if not self.api_key:
            self.api_key = os.getenv("EDINET_API_KEY", "")

    def ready(self) -> bool:
        return bool(self.api_key)

    async def list_documents(
        self,
        target_date: str,
    ) -> list[dict]:
        """指定日の提出書類一覧を取得。target_date は 'YYYY-MM-DD'。

        EDINET API v2 は日付単位 query のため、複数日範囲は個別呼び出し。
        """
        if not self.api_key:
            raise RuntimeError("EDINET_API_KEY 未設定")
        url = f"{EDINET_BASE}/documents.json"
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.get(
                url,
                headers={"Ocp-Apim-Subscription-Key": self.api_key},
                params={"date": target_date, "type": "2"},
            )
        if r.status_code == 401:
            raise RuntimeError("EDINET_API_KEY 認証失敗 (key が無効)")
        r.raise_for_status()
        data = r.json() or {}
        return list(data.get("results") or [])

    async def download_xbrl_zip(self, doc_id: str) -> bytes | None:
        """有報 XBRL zip をダウンロード (type=1)。"""
        if not self.api_key:
            return None
        url = f"{EDINET_BASE}/documents/{doc_id}"
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.get(
                url,
                headers={"Ocp-Apim-Subscription-Key": self.api_key},
                params={"type": "1"},
            )
        if r.status_code >= 400:
            return None
        return r.content


# ─── 関係会社 section parser ────────────────────────────────


# 関係会社等の状況 section で使われる典型的な heading
_RELATED_HEADING_PATTERNS = (
    "関係会社の状況",
    "関係会社等の状況",
    "主要な関係会社",
    "重要な関係会社",
)

# 株式会社 / 有限会社 / 合同会社 の会社名 regex (operator_spider と同じ文字集合)
_COMPANY_NAME_RE = re.compile(
    r"((?:株式会社|有限会社|合同会社)[一-龥ぁ-んァ-ヶA-Za-z0-9・\-ー]{2,25}"
    r"|"
    r"[一-龥ぁ-んァ-ヶA-Za-z0-9・\-ー]{2,25}(?:株式会社|有限会社|合同会社))"
)


def _extract_xbrl_asr_html(zip_bytes: bytes) -> str:
    """有報 zip 内の本文 HTML (jpcrp030000-asr-XXX.htm) を抽出。

    EDINET XBRL zip には複数ファイルが入っており、本文は
    `XBRL/PublicDoc/*.htm` の中の 最大 HTML。
    """
    if not zip_bytes:
        return ""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            # 本文 HTML 候補 (jpcrp 始まりが有報本体)
            candidates = [
                n for n in zf.namelist()
                if n.lower().endswith((".htm", ".html"))
                and "PublicDoc" in n
            ]
            if not candidates:
                return ""
            # 最大 size のが本文
            candidates.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
            with zf.open(candidates[0]) as f:
                return f.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.debug("xbrl zip extract failed: %s", e)
        return ""


def _extract_related_section(html: str) -> str:
    """有報 HTML から 関係会社 section 部分のみを切り出す。"""
    if not html:
        return ""
    # heading 見つけて そこから次の h1/h2 まで
    for pat in _RELATED_HEADING_PATTERNS:
        idx = html.find(pat)
        if idx < 0:
            continue
        # 次の大見出し (関係会社 section が 3000 chars くらい続く想定)
        end = idx + 15000
        return html[idx:end]
    return ""


def extract_companies_from_asr_html(
    html: str, doc_id: str = "",
) -> list[EDINETCompany]:
    """有報 HTML から 関係会社 list を extract。"""
    if not html:
        return []
    section = _extract_related_section(html) or html
    try:
        from bs4 import BeautifulSoup

        text = BeautifulSoup(section, "lxml").get_text(" ", strip=True)
    except Exception:
        text = re.sub(r"<[^>]+>", " ", section)
    seen: set[str] = set()
    out: list[EDINETCompany] = []
    for m in _COMPANY_NAME_RE.finditer(text):
        name = m.group(1).strip()
        if len(name) < 5 or name in seen:
            continue
        seen.add(name)
        out.append(EDINETCompany(
            name=name, relationship="related",
            source_doc_id=doc_id,
        ))
    return out


# ─── Orchestrator ───────────────────────────────────────────


async def sync_edinet_for_edinet_code(
    *,
    edinet_code: str,
    brand: str,
    date_from: str = "",
    date_to: str = "",
    doc_type_code: str = DOC_TYPE_SECURITIES_REPORT,
    dry_run: bool = False,
) -> tuple[EDINETSyncStats, list[EDINETCompany]]:
    """指定 edinet_code の有報を 1 件ダウンロードして関係会社を ORM 登録。

    - date_from / date_to 指定で期間内、省略時は過去 1 年
    - 1 回の sync で最新 1 件の有報のみ処理 (費用控えめ)
    """
    stats = EDINETSyncStats()
    companies: list[EDINETCompany] = []

    client = EDINETClient()
    if not client.ready():
        stats.errors.append(
            "EDINET_API_KEY 未設定 — https://disclosure2dl.edinet-fsa.go.jp/"
            "guide/static/disclosure/WZEK0110.html で無料登録し、.env に "
            "EDINET_API_KEY=... を追加してください"
        )
        return stats, companies

    # 日付範囲 (default 過去 1 年)
    today = date.today()
    if date_to:
        dto = date.fromisoformat(date_to)
    else:
        dto = today
    if date_from:
        dfrom = date.fromisoformat(date_from)
    else:
        dfrom = dto - timedelta(days=400)

    # 日単位で documents.json を叩き、該当 edinet_code + doc_type を探す
    # (コスト: 有報は年 1 回しか出ないので、ほぼ 1 日だけ hit する)
    found_docs: list[EDINETDocument] = []
    d = dto
    while d >= dfrom:
        try:
            results = await client.list_documents(d.isoformat())
        except Exception as e:
            stats.errors.append(f"list {d}: {e}")
            d -= timedelta(days=1)
            continue
        for r in results:
            if str(r.get("edinetCode") or "") != edinet_code:
                continue
            if str(r.get("docTypeCode") or "") != doc_type_code:
                continue
            found_docs.append(EDINETDocument(
                doc_id=str(r.get("docID") or ""),
                submit_date=str(r.get("submitDateTime") or "")[:10],
                edinet_code=str(r.get("edinetCode") or ""),
                filer_name=str(r.get("filerName") or ""),
                doc_type_code=str(r.get("docTypeCode") or ""),
                doc_description=str(r.get("docDescription") or ""),
                sec_code=str(r.get("secCode") or ""),
                period_end=str(r.get("periodEnd") or ""),
            ))
        d -= timedelta(days=1)
        if found_docs:  # 最新 1 件を取ったら止める
            break

    stats.docs_found = len(found_docs)
    if not found_docs:
        stats.errors.append(
            f"no securities report for {edinet_code} in {dfrom}..{dto}"
        )
        return stats, companies

    # 最新 1 件の有報を download + parse
    target = found_docs[0]
    zip_bytes = await client.download_xbrl_zip(target.doc_id)
    if not zip_bytes:
        stats.errors.append(f"xbrl download failed: {target.doc_id}")
        return stats, companies

    html = _extract_xbrl_asr_html(zip_bytes)
    raw_companies = extract_companies_from_asr_html(html, doc_id=target.doc_id)
    stats.companies_extracted = len(raw_companies)

    # 国税庁 CSV verify (各 company name を search)
    from pizza_delivery.houjin_csv import HoujinCSVIndex

    idx = HoujinCSVIndex()
    verified: list[EDINETCompany] = []
    for c in raw_companies:
        recs = idx.search_by_name(c.name, limit=2, active_only=True)
        if not recs:
            recs = idx.search_by_name(c.name, limit=2, active_only=False)
        exact = None
        for r in recs:
            if r.name == c.name:
                exact = r
                break
        if exact is None:
            continue  # 国税庁 未登録 → skip
        c.corporate_number = exact.corporate_number
        c.name = exact.name  # 正規化
        verified.append(c)
    stats.companies_verified = len(verified)
    companies = verified

    # ORM 登録
    if not dry_run and verified:
        from pizza_delivery.orm import (
            link_brand_operator, make_session, upsert_brand, upsert_operator,
        )

        sess = make_session()
        try:
            brand_obj = upsert_brand(sess, brand, source="edinet")
            sess.flush()
            for c in verified:
                op = upsert_operator(
                    sess, name=c.name,
                    corporate_number=c.corporate_number,
                    kind="franchisee",
                    source="edinet",
                    note=f"edinet_doc={c.source_doc_id}",
                )
                sess.flush()
                link_brand_operator(
                    sess, brand=brand_obj, operator=op,
                    operator_type="franchisee",
                    source="edinet",
                    source_url=f"https://disclosure2.edinet-fsa.go.jp/WEEK0040.aspx?doc={c.source_doc_id}",
                    note=f"auto_edinet_{target.submit_date}",
                )
                stats.orm_inserted += 1
            sess.commit()
        finally:
            sess.close()
    return stats, companies


def _main() -> None:
    import argparse
    import csv
    import sys

    ap = argparse.ArgumentParser(
        description="EDINET 有価証券報告書 → 関係会社 → 国税庁 verify → ORM 登録"
    )
    ap.add_argument("--edinet-code", required=True,
                    help="例: E03384 (モスフードサービス)")
    ap.add_argument("--brand", required=True,
                    help="紐付ける brand 名 (FranchiseBrand)")
    ap.add_argument("--date-from", default="")
    ap.add_argument("--date-to", default="")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--out", default="", help="extracted CSV 出力")
    args = ap.parse_args()

    stats, companies = asyncio.run(
        sync_edinet_for_edinet_code(
            edinet_code=args.edinet_code,
            brand=args.brand,
            date_from=args.date_from,
            date_to=args.date_to,
            dry_run=args.dry_run,
        )
    )
    print(f"✅ edinet-sync {'dry-run' if args.dry_run else 'apply'}")
    print(f"   edinet_code         = {args.edinet_code}")
    print(f"   docs_found          = {stats.docs_found}")
    print(f"   companies_extracted = {stats.companies_extracted}")
    print(f"   companies_verified  = {stats.companies_verified}")
    if not args.dry_run:
        print(f"   orm_inserted        = {stats.orm_inserted}")
    for e in stats.errors[:5]:
        print(f"   ⚠  {e}", file=sys.stderr)

    if args.out and companies:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["operator_name", "corporate_number",
                        "relationship", "source_doc_id"])
            for c in companies:
                w.writerow([c.name, c.corporate_number,
                            c.relationship, c.source_doc_id])
        print(f"📄 companies CSV: {args.out}")


if __name__ == "__main__":
    _main()
