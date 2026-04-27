"""JFA 情報開示書面 index / PDF を ORM に取り込む。

JFA 会員一覧 (`jfa_fetcher.py`) は brand × franchisor の存在確認に強いが、
店舗数は持たない。本モジュールは JFA の「情報開示書面」ページから PDF
リンクを拾い、PDF 本文から公開店舗数を決定論的に抽出する。

LLM は使わない。PDF の抽出結果は source_url と note に根拠を残す。
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup, Comment

from pizza_delivery.jfa_fetcher import _canonicalize_operator
from pizza_delivery.orm import (
    BrandOperatorLink,
    FranchiseBrand,
    OperatorCompany,
    link_brand_operator,
    make_session,
    upsert_brand,
    upsert_operator,
)

logger = logging.getLogger(__name__)


DEFAULT_JFA_DISCLOSURE_INDEX = "https://www.jfa-fc.or.jp/particle/3614.html"
SOURCE_NAME = "jfa_disclosure"
DISCLOSURE_BRAND_ALIASES = {
    "モスバーガーチェーン": "モスバーガー",
    "コメダ珈琲店": "コメダ珈琲",
}


@dataclass(frozen=True)
class JFADisclosureLink:
    """情報開示書面 index の brand × PDF 1 件。"""

    franchisor_name: str
    brand_name: str
    industry: str = ""
    pdf_url: str = ""
    source_url: str = DEFAULT_JFA_DISCLOSURE_INDEX


@dataclass
class JFADisclosureMetrics:
    """PDF 本文から抽出した店舗数情報。"""

    franchisor_name: str = ""
    observed_at: str = ""
    fc_store_count: int = 0
    rc_store_count: int = 0
    total_store_count: int = 0
    extraction_method: str = ""

    @property
    def best_store_count(self) -> int:
        """CSV 集計で使う代表店舗数。合計があれば合計、なければ FC 店舗数。"""
        return self.total_store_count or self.fc_store_count


def parse_disclosure_index_html(
    html: str,
    *,
    source_url: str = DEFAULT_JFA_DISCLOSURE_INDEX,
) -> list[JFADisclosureLink]:
    """JFA 情報開示書面ページから PDF link を抽出する。

    表構造:
      <table class="tbl_kaiji">
        <th colspan=2>業種名</th>
        <tr><td>会社名</td><td><a href="/fc-g-misc/pdf/...">チェーン店</a></td></tr>

    HTML comment 内に古い PDF link が残っているため、Comment node は事前に除外する。
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    for c in soup.find_all(string=lambda text: isinstance(text, Comment)):
        c.extract()

    rows: list[JFADisclosureLink] = []
    seen: set[tuple[str, str, str]] = set()
    for table in soup.find_all("table", class_="tbl_kaiji"):
        th = table.find("th", class_="thColor01")
        industry = th.get_text(" ", strip=True) if th else ""
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            raw_company = tds[0].get_text(" ", strip=True)
            if not raw_company or raw_company == "-":
                continue
            franchisor = _canonicalize_operator(raw_company)
            if not franchisor:
                continue
            for a in tds[1].find_all("a"):
                href = a.get("href", "")
                brand = a.get_text(" ", strip=True)
                if not isinstance(href, str) or ".pdf" not in href.lower():
                    continue
                if not brand or brand == "-":
                    continue
                pdf_url = urljoin(source_url, href)
                key = (franchisor, brand, pdf_url)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    JFADisclosureLink(
                        franchisor_name=franchisor,
                        brand_name=brand,
                        industry=industry,
                        pdf_url=pdf_url,
                        source_url=source_url,
                    )
                )
    return rows


_CORP_RE = re.compile(
    r"((?:株式会社|有限会社|合同会社|一般社団法人|公益社団法人)"
    r"[一-龥ぁ-んァ-ヶA-Za-z0-9・\-ー（）()]+|"
    r"[一-龥ぁ-んァ-ヶA-Za-z0-9・\-ー（）()]+"
    r"(?:株式会社|有限会社|合同会社))"
)

_CORP_REJECT_SUBSTRINGS = (
    "日本フランチャイズチェーン",
    "フランチャイズシステム",
)

_FULLWIDTH_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")
_NUM_TOKEN_RE = re.compile(r"[0-9０-９][0-9０-９,，]*")


def _to_int(s: str) -> int:
    return int((s or "0").translate(_FULLWIDTH_DIGITS).replace(",", "").replace("，", "").strip() or "0")


def _compact_spaces(text: str) -> str:
    return re.sub(r"[ \t\u3000]+", " ", text or "")


def _int_tokens(line: str) -> list[int]:
    return [_to_int(m.group(0)) for m in _NUM_TOKEN_RE.finditer(line or "")]


def _compact_key(s: str) -> str:
    return re.sub(r"\s+", "", s or "").translate(_FULLWIDTH_DIGITS)


def _store_table_order(window: str) -> str:
    """店舗数 table の 3 列順を推定する。

    Returns:
      franchise_first: 加盟店/FC, 直営/RC, 合計
      direct_first:    直営/RC, 加盟店/FC, 合計
    """
    w = window or ""
    direct_positions = [p for term in ("直営", "RC", "ＲＣ") if (p := w.find(term)) >= 0]
    franchise_positions = [
        p for term in ("フランチャイズ", "加盟店", "加盟者", "FC", "ＦＣ")
        if (p := w.find(term)) >= 0
    ]
    if direct_positions and franchise_positions and min(direct_positions) < min(franchise_positions):
        return "direct_first"
    return "franchise_first"


def _assign_store_counts(metrics: JFADisclosureMetrics, *, first: int, second: int, total: int, order: str) -> None:
    if order == "direct_first":
        metrics.rc_store_count = first
        metrics.fc_store_count = second
    else:
        metrics.fc_store_count = first
        metrics.rc_store_count = second
    metrics.total_store_count = total


def _valid_store_triple(first: int, second: int, total: int) -> bool:
    return (
        0 <= first <= 10000
        and 0 <= second <= 10000
        and 0 < total <= 10000
        and abs((first + second) - total) <= 1
    )


def _extract_company_name(text: str) -> str:
    """PDF 先頭付近から開示主体の会社名を拾う。"""
    head = text[:5000]
    for line in head.splitlines():
        key = re.sub(r"\s+", "", line)
        if not key or any(rej in key for rej in _CORP_REJECT_SUBSTRINGS):
            continue
        for m in _CORP_RE.finditer(key):
            name = _canonicalize_operator(m.group(1))
            # 「株式会社」単体のような法人格だけの誤抽出は捨てる。
            if name in {"株式会社", "有限会社", "合同会社", "一般社団法人", "公益社団法人"}:
                continue
            if name and not any(rej in name for rej in _CORP_REJECT_SUBSTRINGS):
                return name
    return ""


def parse_disclosure_pdf_text(text: str, *, brand_name: str = "") -> JFADisclosureMetrics:
    """JFA 開示 PDF text から店舗数を抽出する pure parser。

    優先順:
      1. 「FC店 RC店 合計」表の最新年度
      2. 「店舗数推移」内の brand 別 row
      3. 「店舗数/教室数推移」の年度 row
      4. 「2025年8月 店舗数 1,996 店舗」形式の沿革
      5. 「現在は 1,996 店舗」形式の本文
    """
    raw = text or ""
    compact = _compact_spaces(raw)
    metrics = JFADisclosureMetrics(franchisor_name=_extract_company_name(compact))

    # 1) FC店 / RC店 / 合計 table。モスなどで最も構造化されている。
    fc_header = re.search(r"FC\s*店\s+RC\s*店\s+合\s*計", compact)
    if fc_header:
        window = compact[fc_header.start():fc_header.start() + 1600]
        best: tuple[int, int, int, int] | None = None
        for m in re.finditer(
            r"(20\d{2})\s*(?:年\s*)?年度\s+([0-9,，]+)\s+([0-9,，]+)\s+([0-9,，]+)",
            window,
        ):
            year = int(m.group(1))
            fc = _to_int(m.group(2))
            rc = _to_int(m.group(3))
            total = _to_int(m.group(4))
            if best is None or year > best[0]:
                best = (year, fc, rc, total)
        if best:
            year, fc, rc, total = best
            metrics.observed_at = f"{year}年度"
            metrics.fc_store_count = fc
            metrics.rc_store_count = rc
            metrics.total_store_count = total
            metrics.extraction_method = "fc_rc_total_table"
            return metrics

    # 2) コメダ等の「年度 column × brand row」形式。
    #    brand_name が無い場合は誤抽出防止のため適用しない。
    if brand_name:
        lines = raw.splitlines()
        normalized_brand = _compact_key(brand_name)
        for i, line in enumerate(lines):
            if "店舗数推移" not in line and "教室" not in line:
                continue
            window_lines = lines[i:i + 30]
            order = _store_table_order("\n".join(window_lines))
            for row_idx, row in enumerate(window_lines):
                if normalized_brand not in _compact_key(row):
                    continue
                nums = _int_tokens(row)
                if len(nums) < 3:
                    continue
                first, second, total = nums[-3], nums[-2], nums[-1]
                # 店舗数 table として不自然な値は弾く。売上表の誤拾い対策。
                if not _valid_store_triple(first, second, total):
                    continue
                _assign_store_counts(metrics, first=first, second=second, total=total, order=order)
                header_text = "\n".join(window_lines[:row_idx + 1])
                ym = re.findall(r"(20\d{2})\s*/\s*([0-9]{1,2})", header_text)
                if ym:
                    year, month = ym[-1]
                    metrics.observed_at = f"{int(year)}-{int(month):02d}"
                metrics.extraction_method = "brand_store_count_row"
                return metrics

    # 3) Kids Duo 等の「年度 row × 直営/フランチャイズ/合計」形式。
    for header in ("店舗数推移", "教室数推移", "教 室 数 推 移"):
        pos = compact.find(header)
        if pos < 0:
            continue
        window = compact[pos:pos + 2200]
        order = _store_table_order(window)
        best: tuple[int, int, int, int] | None = None
        for m in re.finditer(
            r"(20\d{2})\s*(?:年\s*)?年度\s+([0-9,，]+)\s+([0-9,，]+)\s+([0-9,，]+)",
            window,
        ):
            year = int(m.group(1))
            first = _to_int(m.group(2))
            second = _to_int(m.group(3))
            total = _to_int(m.group(4))
            if not _valid_store_triple(first, second, total):
                continue
            if best is None or year > best[0]:
                best = (year, first, second, total)
        if best:
            year, first, second, total = best
            metrics.observed_at = f"{year}年度"
            _assign_store_counts(metrics, first=first, second=second, total=total, order=order)
            metrics.extraction_method = "year_store_count_table"
            return metrics

    # 4) 沿革などの「2025年8月 店舗数 1,996 店舗」。
    best_month: tuple[int, int, int] | None = None
    for m in re.finditer(
        r"(20\d{2})\s*年\s*([0-9]{1,2})\s*月\s+店舗数\s+([0-9,，]+)\s*店舗",
        compact,
    ):
        year = int(m.group(1))
        month = int(m.group(2))
        stores = _to_int(m.group(3))
        if best_month is None or (year, month) > (best_month[0], best_month[1]):
            best_month = (year, month, stores)
    if best_month:
        year, month, stores = best_month
        metrics.observed_at = f"{year}-{month:02d}"
        metrics.total_store_count = stores
        metrics.extraction_method = "history_store_count"
        return metrics

    # 5) 本文の「現在は 1,996 店舗」。日付は別途近傍から拾う。
    m = re.search(r"現在(?:は|、)?\s*([0-9,，]+)\s*店舗", compact)
    if m:
        metrics.total_store_count = _to_int(m.group(1))
        date_m = re.search(r"(20\d{2})\s*年\s*([0-9]{1,2})\s*月末日現在", compact)
        if date_m:
            metrics.observed_at = f"{int(date_m.group(1))}-{int(date_m.group(2)):02d}"
        metrics.extraction_method = "current_store_count_sentence"
    return metrics


async def fetch_index_html(
    *,
    url: str = DEFAULT_JFA_DISCLOSURE_INDEX,
    timeout: float = 20.0,
    transport: Any = None,
) -> str:
    kwargs: dict[str, Any] = {
        "timeout": timeout,
        "follow_redirects": True,
        "headers": {"User-Agent": "PI-ZZA-research/1.0"},
    }
    if transport is not None:
        kwargs["transport"] = transport
    async with httpx.AsyncClient(**kwargs) as client:
        r = await client.get(url)
    r.raise_for_status()
    return r.text


async def fetch_pdf_bytes(url: str, *, timeout: float = 30.0) -> bytes:
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": "PI-ZZA-research/1.0"},
    ) as client:
        r = await client.get(url)
    r.raise_for_status()
    return r.content


def pdf_bytes_to_text(pdf: bytes) -> str:
    """PDF bytes を text 化する。

    `pdftotext` があればそれを優先し、無ければ pypdf を試す。どちらも無ければ空。
    """
    if not pdf:
        return ""

    pdftotext = shutil.which("pdftotext")
    if pdftotext:
        with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
            f.write(pdf)
            f.flush()
            proc = subprocess.run(
                [pdftotext, "-layout", f.name, "-"],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        if proc.returncode == 0:
            return proc.stdout.decode("utf-8", errors="replace")
        logger.debug("pdftotext failed: %s", proc.stderr.decode("utf-8", errors="replace"))

    try:
        from pypdf import PdfReader
        import io

        reader = PdfReader(io.BytesIO(pdf))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as e:
        logger.debug("pypdf fallback failed: %s", e)
        return ""


def _target_filter(links: list[JFADisclosureLink], brands_csv: str = "") -> list[JFADisclosureLink]:
    if not brands_csv:
        return links
    targets = {b.strip() for b in brands_csv.split(",") if b.strip()}
    if not targets:
        return links
    return [
        ln
        for ln in links
        if ln.brand_name in targets
        or DISCLOSURE_BRAND_ALIASES.get(ln.brand_name, ln.brand_name) in targets
    ]


def _note_for_metrics(
    metrics: JFADisclosureMetrics,
    *,
    raw_brand_name: str = "",
    brand_name: str = "",
) -> str:
    parts = [f"method={metrics.extraction_method or 'none'}"]
    if metrics.fc_store_count:
        parts.append(f"fc_stores={metrics.fc_store_count}")
    if metrics.rc_store_count:
        parts.append(f"rc_stores={metrics.rc_store_count}")
    if metrics.total_store_count:
        parts.append(f"total_stores={metrics.total_store_count}")
    if metrics.observed_at:
        parts.append(f"observed_at={metrics.observed_at}")
    if raw_brand_name and brand_name and raw_brand_name != brand_name:
        parts.append(f"raw_brand={raw_brand_name}")
    return "; ".join(parts)


def _get_or_create_franchisor(sess: Any, name: str) -> OperatorCompany:
    """JFA disclosure 側に法人番号が無いため、同名既存 operator を優先する。"""
    existing = (
        sess.query(OperatorCompany)
        .filter(OperatorCompany.name == name)
        .order_by(OperatorCompany.corporate_number.desc())
        .first()
    )
    if existing is not None:
        if not existing.kind:
            existing.kind = "franchisor"
        return existing
    return upsert_operator(
        sess,
        name=name,
        kind="franchisor",
        source=SOURCE_NAME,
    )


async def sync_to_orm(
    *,
    url: str = DEFAULT_JFA_DISCLOSURE_INDEX,
    brands_csv: str = "",
    fetch_pdfs: bool = False,
    max_pdfs: int = 0,
    rate_limit_sec: float = 0.5,
) -> tuple[int, int]:
    """JFA disclosure index/PDF を ORM に upsert。

    Returns:
      (index_link_count, pdf_metric_count)
    """
    html = await fetch_index_html(url=url)
    links = _target_filter(parse_disclosure_index_html(html, source_url=url), brands_csv)
    if max_pdfs > 0:
        links = links[:max_pdfs]

    sess = make_session()
    metrics_count = 0
    try:
        # 再実行時に古い PDF 抽出結果や誤抽出 operator link を残さない。
        stale_links = sess.query(BrandOperatorLink).filter_by(source=SOURCE_NAME)
        if brands_csv:
            brand_names = [b.strip() for b in brands_csv.split(",") if b.strip()]
            brand_names.extend(
                DISCLOSURE_BRAND_ALIASES.get(b, b)
                for b in list(brand_names)
            )
            brand_ids = [
                bid for (bid,) in sess.query(FranchiseBrand.id)
                .filter(FranchiseBrand.name.in_(brand_names))
                .all()
            ]
            if brand_ids:
                stale_links = stale_links.filter(BrandOperatorLink.brand_id.in_(brand_ids))
            else:
                stale_links = stale_links.filter(False)
        stale_links.delete(synchronize_session=False)
        sess.flush()
        sess.query(OperatorCompany).filter_by(source=SOURCE_NAME).filter(
            ~OperatorCompany.links.any()
        ).delete(synchronize_session=False)
        sess.flush()

        for i, link in enumerate(links):
            metrics = JFADisclosureMetrics()
            if fetch_pdfs:
                try:
                    pdf = await fetch_pdf_bytes(link.pdf_url)
                    text = pdf_bytes_to_text(pdf)
                    metrics = parse_disclosure_pdf_text(text, brand_name=link.brand_name)
                    if metrics.best_store_count:
                        metrics_count += 1
                except Exception as e:
                    logger.warning("jfa disclosure pdf failed: %s %s", link.pdf_url, e)
                if rate_limit_sec > 0 and i < len(links) - 1:
                    await asyncio.sleep(rate_limit_sec)

            franchisor = (
                metrics.franchisor_name
                if metrics.best_store_count and metrics.franchisor_name
                else link.franchisor_name
            )
            brand_name = DISCLOSURE_BRAND_ALIASES.get(link.brand_name, link.brand_name)
            brand = upsert_brand(
                sess,
                brand_name,
                source=SOURCE_NAME,
                industry=link.industry,
                master_franchisor_name=franchisor,
                jfa_member=True,
            )
            op = _get_or_create_franchisor(sess, franchisor)
            sess.flush()
            link_brand_operator(
                sess,
                brand=brand,
                operator=op,
                estimated_store_count=metrics.best_store_count,
                observed_at=metrics.observed_at,
                operator_type="franchisor",
                source=SOURCE_NAME,
                source_url=link.pdf_url,
                note=_note_for_metrics(
                    metrics,
                    raw_brand_name=link.brand_name,
                    brand_name=brand_name,
                ),
            )
        sess.commit()
    finally:
        sess.close()
    return len(links), metrics_count


def export_index_csv(links: list[JFADisclosureLink], out_path: str | Path) -> None:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["brand_name", "industry", "franchisor_name", "pdf_url", "source_url"],
        )
        w.writeheader()
        for link in links:
            w.writerow({
                "brand_name": link.brand_name,
                "industry": link.industry,
                "franchisor_name": link.franchisor_name,
                "pdf_url": link.pdf_url,
                "source_url": link.source_url,
            })


def _main() -> None:
    ap = argparse.ArgumentParser(description="JFA 情報開示書面 index/PDF の取込")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_export = sub.add_parser("export-index", help="JFA disclosure index を CSV に出力")
    p_export.add_argument("--url", default=DEFAULT_JFA_DISCLOSURE_INDEX)
    p_export.add_argument("--out", required=True)
    p_export.add_argument("--brands", default="", help="カンマ区切り brand filter")

    p_sync = sub.add_parser("sync", help="JFA disclosure index/PDF を ORM に upsert")
    p_sync.add_argument("--url", default=DEFAULT_JFA_DISCLOSURE_INDEX)
    p_sync.add_argument("--brands", default="", help="カンマ区切り brand filter")
    p_sync.add_argument("--fetch-pdfs", action="store_true", help="PDF から店舗数も抽出")
    p_sync.add_argument("--max-pdfs", type=int, default=0, help="0 なら制限なし")
    p_sync.add_argument("--rate-limit-sec", type=float, default=0.5)

    args = ap.parse_args()
    if args.cmd == "export-index":
        html = asyncio.run(fetch_index_html(url=args.url))
        links = _target_filter(parse_disclosure_index_html(html, source_url=args.url), args.brands)
        export_index_csv(links, args.out)
        print(f"✅ exported {len(links)} JFA disclosure links → {args.out}")
        return
    if args.cmd == "sync":
        links, metrics = asyncio.run(
            sync_to_orm(
                url=args.url,
                brands_csv=args.brands,
                fetch_pdfs=args.fetch_pdfs,
                max_pdfs=args.max_pdfs,
                rate_limit_sec=args.rate_limit_sec,
            )
        )
        print(f"✅ synced {links} JFA disclosure links ({metrics} with store counts)")
        return


if __name__ == "__main__":
    _main()
