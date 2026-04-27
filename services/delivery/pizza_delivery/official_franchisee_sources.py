"""Deterministic official-source crawl for thin target brands.

This module is intentionally narrow.  It ingests operator evidence only when the
operator name is written in an official franchisor/operator page or in an
official press release body.  Search snippets and LLM knowledge are not used as
source data.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterable

import httpx

SOURCE = "official_franchisee_page"


@dataclass(frozen=True)
class SourceSpec:
    brand: str
    url: str
    parser: str


@dataclass
class EvidenceRow:
    brand: str
    operator_name: str
    source_url: str
    parser: str
    operator_type: str = "franchisee"
    estimated_store_count: int = 1
    store_names: list[str] = field(default_factory=list)
    store_address: str = ""
    context_prefecture: str = ""
    context_city: str = ""
    corporate_number: str = ""
    head_office: str = ""
    prefecture: str = ""
    verification_status: str = "unverified"
    note: str = ""


@dataclass
class CrawlStats:
    fetched: int = 0
    parsed: int = 0
    verified: int = 0
    ambiguous: int = 0
    no_match: int = 0
    applied_links: int = 0
    cleaned_links: int = 0


DEFAULT_SOURCES = (
    SourceSpec(
        brand="Brand off",
        url="https://www.brandoff.co.jp/company/",
        parser="brand_off_company",
    ),
    SourceSpec(
        brand="Brand off",
        url="https://www.brandoff.co.jp/fc/",
        parser="brand_off_fc",
    ),
    SourceSpec(
        brand="カルビ丼とスン豆腐専門店韓丼",
        url="https://prtimes.jp/main/html/rd/p/000000004.000050301.html",
        parser="kandon_prtimes",
    ),
    SourceSpec(
        brand="カルビ丼とスン豆腐専門店韓丼",
        url="https://prtimes.jp/main/html/rd/p/000000005.000050301.html",
        parser="kandon_prtimes",
    ),
    SourceSpec(
        brand="カルビ丼とスン豆腐専門店韓丼",
        url="https://prtimes.jp/main/html/rd/p/000000006.000050301.html",
        parser="kandon_prtimes",
    ),
    SourceSpec(
        brand="カルビ丼とスン豆腐専門店韓丼",
        url="https://prtimes.jp/main/html/rd/p/000000007.000050301.html",
        parser="kandon_prtimes",
    ),
    SourceSpec(
        brand="カルビ丼とスン豆腐専門店韓丼",
        url="https://prtimes.jp/main/html/rd/p/000000009.000050301.html",
        parser="kandon_prtimes",
    ),
    SourceSpec(
        brand="カルビ丼とスン豆腐専門店韓丼",
        url="https://harika-yame.jp/",
        parser="haricom_company",
    ),
    SourceSpec(
        brand="カルビ丼とスン豆腐専門店韓丼",
        url="https://www.birion.com/group/kandon/",
        parser="birion_company",
    ),
)


_PREFS = (
    "北海道",
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "茨城県",
    "栃木県",
    "群馬県",
    "埼玉県",
    "千葉県",
    "東京都",
    "神奈川県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
    "山梨県",
    "長野県",
    "岐阜県",
    "静岡県",
    "愛知県",
    "三重県",
    "滋賀県",
    "京都府",
    "大阪府",
    "兵庫県",
    "奈良県",
    "和歌山県",
    "鳥取県",
    "島根県",
    "岡山県",
    "広島県",
    "山口県",
    "徳島県",
    "香川県",
    "愛媛県",
    "高知県",
    "福岡県",
    "佐賀県",
    "長崎県",
    "熊本県",
    "大分県",
    "宮崎県",
    "鹿児島県",
    "沖縄県",
)


def _text_from_html(raw_html: str, *, include_title: bool = True) -> str:
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(raw_html or "", "lxml")
        parts: list[str] = []
        if include_title and soup.title and soup.title.string:
            parts.append(soup.title.string)
        parts.append(soup.get_text("\n", strip=True))
        return "\n".join(parts)
    except Exception:
        return re.sub(r"<[^>]+>", "\n", raw_html or "")


def _uniq(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        v = re.sub(r"\s+", " ", html.unescape(raw or "")).strip(" 　、,。")
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _clean_operator_name(raw: str) -> str:
    from pizza_delivery.normalize import normalize_operator_name

    s = html.unescape(raw or "")
    s = re.sub(r"\s+", " ", s).strip(" 　:：、,。")
    return normalize_operator_name(s)


def _extract_pref_city(address: str) -> tuple[str, str]:
    pref = ""
    for p in _PREFS:
        if p in address:
            pref = p
            break
    city = ""
    if pref:
        rest = address.split(pref, 1)[1]
        for pat in (
            r"([一-龥ぁ-んァ-ヶA-Za-z0-9]+?市(?:[一-龥ぁ-んァ-ヶA-Za-z0-9]+?区)?)",
            r"([一-龥ぁ-んァ-ヶA-Za-z0-9]+?郡[一-龥ぁ-んァ-ヶA-Za-z0-9]+?町)",
            r"([一-龥ぁ-んァ-ヶA-Za-z0-9]+?区)",
            r"([一-龥ぁ-んァ-ヶA-Za-z0-9]+?町)",
            r"([一-龥ぁ-んァ-ヶA-Za-z0-9]+?村)",
        ):
            m = re.search(pat, rest)
            if m:
                city = m.group(1)
                break
    return pref, city


def _extract_prtimes_payload(raw_html: str) -> str:
    m = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        raw_html or "",
        flags=re.S,
    )
    if not m:
        return _text_from_html(raw_html)
    try:
        data = json.loads(html.unescape(m.group(1)))
        release = data["props"]["pageProps"]["pressRelease"]
        parts = [
            release.get("title") or "",
            release.get("subtitle") or "",
            release.get("head") or "",
            release.get("text") or "",
        ]
        return _text_from_html("\n".join(parts), include_title=False)
    except Exception:
        return _text_from_html(raw_html)


def parse_brand_off_company(raw_html: str, spec: SourceSpec) -> list[EvidenceRow]:
    text = _text_from_html(raw_html)
    if "株式会社K-ブランドオフ" not in text and "株式会社 K-ブランドオフ" not in text:
        return []
    count = 0
    m = re.search(r"店舗数\s*(\d+)\s*店舗", text)
    if m:
        count = int(m.group(1))
    return [
        EvidenceRow(
            brand=spec.brand,
            operator_name="株式会社K-ブランドオフ",
            source_url=spec.url,
            parser=spec.parser,
            operator_type="franchisor",
            estimated_store_count=count or 0,
            note="official company profile",
        )
    ]


def parse_brand_off_fc(raw_html: str, spec: SourceSpec) -> list[EvidenceRow]:
    text = _text_from_html(raw_html)
    rows: list[EvidenceRow] = []
    matches = list(re.finditer(r"加盟者名\s*[：:]\s*([^\n]+)", text))
    for i, m in enumerate(matches):
        op = _clean_operator_name(m.group(1))
        if not op:
            continue
        end = matches[i + 1].start() if i + 1 < len(matches) else m.start() + 900
        window = text[m.start(): end]
        stores = _uniq(
            s for s in re.findall(r"BRAND\s*OFF[^\n、,。]*?店", window)
            if s.strip() != "BRAND OFF買取専門店"
        )
        if not stores:
            stores = _uniq(
                s for s in re.findall(r"BRAND\s*OFF[^\n、,。]*?店", text)
                if s.strip() != "BRAND OFF買取専門店"
            )
        pref, city = "", ""
        if "仙台" in window:
            pref, city = "宮城県", "仙台市"
        elif "弘前" in window or "青森" in window:
            pref, city = "青森県", ""
        elif "函館" in window or "北海道" in window:
            pref, city = "北海道", "函館市"
        rows.append(
            EvidenceRow(
                brand=spec.brand,
                operator_name=op,
                source_url=spec.url,
                parser=spec.parser,
                estimated_store_count=max(1, len(stores)),
                store_names=stores,
                context_prefecture=pref,
                context_city=city,
                note="official FC owner voice",
            )
        )
    return rows


def parse_kandon_prtimes(raw_html: str, spec: SourceSpec) -> list[EvidenceRow]:
    text = _extract_prtimes_payload(raw_html)
    if "韓丼" not in text:
        return []
    m_op = re.search(r"[◇◆]\s*運営会社\s*[：:\s]*([^\n◇■<]+)", text)
    if not m_op:
        m_op = re.search(r"運営会社\s*[：:\s]*([^\n◇■<]+)", text)
    if not m_op:
        return []
    op = _clean_operator_name(m_op.group(1))
    m_store = re.search(r"[◇◆]\s*店名\s*[：:\s]*([^\n]+)", text)
    if not m_store:
        m_store = re.search(r"店名\s*[：:\s]*([^\n]+)", text)
    m_addr = re.search(r"[◇◆]\s*所在地\s*[：:\s]*([^\n]+)", text)
    if not m_addr:
        m_addr = re.search(r"所在地\s*[：:\s]*([^\n]+)", text)
    address = re.sub(r"\s+", "", m_addr.group(1)) if m_addr else ""
    pref, city = _extract_pref_city(address)
    return [
        EvidenceRow(
            brand=spec.brand,
            operator_name=op,
            source_url=spec.url,
            parser=spec.parser,
            estimated_store_count=1,
            store_names=_uniq([m_store.group(1)] if m_store else []),
            store_address=address,
            context_prefecture=pref,
            context_city=city,
            note="official franchisor press release",
        )
    ]


def parse_haricom_company(raw_html: str, spec: SourceSpec) -> list[EvidenceRow]:
    text = _text_from_html(raw_html)
    if "韓丼八女店" not in text:
        return []
    m = re.search(r"会社名\s*\n?\s*([^\n]+)", text)
    op = _clean_operator_name(m.group(1)) if m else ""
    stores = _uniq(re.findall(r"韓丼[^\n、,。]*?店", text))
    if not op or not stores:
        return []
    corp = ""
    m_corp = re.search(r"法人番号\s*\n?\s*([0-9]{13})", text)
    if m_corp:
        corp = m_corp.group(1)
    return [
        EvidenceRow(
            brand=spec.brand,
            operator_name=op,
            source_url=spec.url,
            parser=spec.parser,
            estimated_store_count=len(stores),
            store_names=stores,
            context_prefecture="福岡県",
            context_city="八女市",
            corporate_number=corp,
            note="official operator company profile",
        )
    ]


def parse_birion_company(raw_html: str, spec: SourceSpec) -> list[EvidenceRow]:
    text = _text_from_html(raw_html)
    if "カルビ丼とスン豆腐専門店" not in text or "ビリオンフーズハヤシ" not in text:
        return []
    m = re.search(r"株式会社\s*ビリオンフーズハヤシ", text)
    op = _clean_operator_name(m.group(0)) if m else ""
    stores = _uniq(re.findall(r"(?:大宮|鯖江|丸山)店", text))
    if not op or not stores:
        return []
    return [
        EvidenceRow(
            brand=spec.brand,
            operator_name=op,
            source_url=spec.url,
            parser=spec.parser,
            estimated_store_count=len(stores),
            store_names=stores,
            context_prefecture="福井県",
            operator_type="franchisee",
            note="official operator brand page",
        )
    ]


_PARSERS = {
    "brand_off_company": parse_brand_off_company,
    "brand_off_fc": parse_brand_off_fc,
    "kandon_prtimes": parse_kandon_prtimes,
    "haricom_company": parse_haricom_company,
    "birion_company": parse_birion_company,
}


_HOUJIN_NAME_ALIASES = {
    "株式会社K-ブランドオフ": ["株式会社Ｋ－ブランドオフ", "株式会社Kーブランドオフ"],
}


def parse_source(raw_html: str, spec: SourceSpec) -> list[EvidenceRow]:
    parser = _PARSERS.get(spec.parser)
    if parser is None:
        return []
    return parser(raw_html, spec)


def _record_by_corp(corp: str, houjin_db: str | Path) -> tuple[str, str, str, str] | None:
    if not corp:
        return None
    conn = sqlite3.connect(houjin_db)
    try:
        row = conn.execute(
            """
            SELECT name, prefecture, city, street
            FROM houjin_registry
            WHERE corporate_number = ?
            """,
            (corp,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    return row[0], row[1], row[2], row[3]


def verify_rows(
    rows: list[EvidenceRow],
    *,
    houjin_db: str | Path = "var/houjin/registry.sqlite",
) -> list[EvidenceRow]:
    from pizza_delivery.houjin_csv import HoujinCSVIndex

    idx = HoujinCSVIndex(houjin_db)
    for row in rows:
        preserve_source_name = row.operator_name in _HOUJIN_NAME_ALIASES
        if row.corporate_number:
            rec = _record_by_corp(row.corporate_number, houjin_db)
            if rec is not None:
                name, pref, city, street = rec
                if not preserve_source_name:
                    row.operator_name = name
                row.prefecture = pref
                row.head_office = f"{pref}{city}{street}"
                row.verification_status = "houjin_corp_number"
                continue
            row.verification_status = "houjin_corp_number_missing"
            continue

        candidates = idx.search_by_name(row.operator_name, limit=50, allow_substring=False)
        inactive_fallback = False
        if not candidates:
            for alias in _HOUJIN_NAME_ALIASES.get(row.operator_name, []):
                candidates = idx.search_by_name(alias, limit=50, allow_substring=False)
                if candidates:
                    break
        if not candidates:
            candidates = idx.search_by_name(
                row.operator_name,
                limit=50,
                active_only=False,
                allow_substring=False,
            )
            inactive_fallback = bool(candidates)
        if not candidates:
            for alias in _HOUJIN_NAME_ALIASES.get(row.operator_name, []):
                candidates = idx.search_by_name(
                    alias,
                    limit=50,
                    active_only=False,
                    allow_substring=False,
                )
                if candidates:
                    inactive_fallback = True
                    break
        if len(candidates) == 1:
            rec = candidates[0]
            if not preserve_source_name:
                row.operator_name = rec.name
            row.corporate_number = rec.corporate_number
            row.prefecture = rec.prefecture
            row.head_office = rec.address
            row.verification_status = (
                "houjin_unique_name_inactive_fallback"
                if inactive_fallback else "houjin_unique_name"
            )
            continue

        filtered = candidates
        if row.context_prefecture:
            pref_matches = [r for r in filtered if r.prefecture == row.context_prefecture]
            if row.context_city:
                city_matches = [r for r in pref_matches if r.city.startswith(row.context_city)]
                if len(city_matches) == 1:
                    filtered = city_matches
                elif city_matches:
                    filtered = city_matches
                else:
                    filtered = pref_matches
            elif pref_matches:
                filtered = pref_matches
        if len(filtered) == 1:
            rec = filtered[0]
            if not preserve_source_name:
                row.operator_name = rec.name
            row.corporate_number = rec.corporate_number
            row.prefecture = rec.prefecture
            row.head_office = rec.address
            row.verification_status = (
                "houjin_context_match_inactive_fallback"
                if inactive_fallback else "houjin_context_match"
            )
        elif candidates:
            row.verification_status = "houjin_ambiguous"
        else:
            row.verification_status = "houjin_no_match"
    return rows


def dedupe_rows(rows: list[EvidenceRow]) -> list[EvidenceRow]:
    merged: dict[tuple[str, str, str, str, str], EvidenceRow] = {}
    for row in rows:
        key = (
            row.brand,
            row.corporate_number or row.operator_name,
            row.source_url,
            row.parser,
            row.operator_type,
        )
        existing = merged.get(key)
        if existing is None:
            merged[key] = row
            continue
        existing.estimated_store_count = max(
            int(existing.estimated_store_count or 0),
            int(row.estimated_store_count or 0),
        )
        existing.store_names = _uniq([*existing.store_names, *row.store_names])
        if not existing.corporate_number and row.corporate_number:
            existing.corporate_number = row.corporate_number
            existing.head_office = row.head_office
            existing.prefecture = row.prefecture
            existing.verification_status = row.verification_status
        if not existing.context_prefecture and row.context_prefecture:
            existing.context_prefecture = row.context_prefecture
        if not existing.context_city and row.context_city:
            existing.context_city = row.context_city
    return list(merged.values())


def fetch_sources(
    sources: list[SourceSpec],
    *,
    timeout: float = 15.0,
) -> tuple[list[EvidenceRow], CrawlStats]:
    stats = CrawlStats()
    rows: list[EvidenceRow] = []
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        for spec in sources:
            resp = client.get(
                spec.url,
                headers={
                    "User-Agent": "PI-ZZA/0.28 official-franchisee-sources",
                    "Accept": "text/html,application/xhtml+xml",
                },
            )
            resp.raise_for_status()
            stats.fetched += 1
            parsed = parse_source(resp.text, spec)
            stats.parsed += len(parsed)
            rows.extend(parsed)
    return rows, stats


def apply_rows(
    rows: list[EvidenceRow],
    *,
    orm_db: str | Path = "var/pizza-registry.sqlite",
    dry_run: bool = False,
) -> int:
    if dry_run:
        return 0
    from sqlalchemy import create_engine

    from pizza_delivery.orm import link_brand_operator, make_session, upsert_brand, upsert_operator

    engine = create_engine(f"sqlite:///{Path(orm_db)}", future=True)
    sess = make_session(engine)
    applied = 0
    try:
        observed_at = date.today().isoformat()
        for row in rows:
            if not row.operator_name:
                continue
            brand = upsert_brand(sess, row.brand, source=SOURCE)
            op = upsert_operator(
                sess,
                name=row.operator_name,
                corporate_number=row.corporate_number,
                head_office=row.head_office,
                prefecture=row.prefecture,
                kind=row.operator_type,
                source=SOURCE,
                note=row.verification_status,
            )
            sess.flush()
            note_parts = [
                row.note,
                row.verification_status,
                f"stores={';'.join(row.store_names)}" if row.store_names else "",
                f"address={row.store_address}" if row.store_address else "",
            ]
            link_brand_operator(
                sess,
                brand=brand,
                operator=op,
                estimated_store_count=max(0, int(row.estimated_store_count or 0)),
                observed_at=observed_at,
                operator_type=row.operator_type,
                source=SOURCE,
                source_url=row.source_url,
                note="; ".join(p for p in note_parts if p),
            )
            applied += 1
        sess.commit()
    finally:
        sess.close()
    return applied


def cleanup_registry_brand_links(
    *,
    orm_db: str | Path = "var/pizza-registry.sqlite",
    brands: Iterable[str],
    dry_run: bool = False,
) -> int:
    from pizza_delivery.normalize import canonical_key
    from pizza_delivery.purge import _is_structural_garbage

    brand_names = [b for b in brands if b]
    if not brand_names:
        return 0
    conn = sqlite3.connect(orm_db)
    try:
        conn.row_factory = sqlite3.Row
        placeholders = ",".join("?" for _ in brand_names)
        rows = conn.execute(
            f"""
            SELECT l.id AS link_id, o.id AS operator_id, o.name, o.corporate_number
            FROM brand_operator_link l
            JOIN franchise_brand b ON b.id = l.brand_id
            JOIN operator_company o ON o.id = l.operator_id
            WHERE b.name IN ({placeholders})
              AND l.source = 'pipeline'
            """,
            brand_names,
        ).fetchall()
        by_key_has_corp = {
            canonical_key(r["name"])
            for r in rows
            if (r["corporate_number"] or "") and canonical_key(r["name"])
        }
        delete_link_ids: list[int] = []
        for r in rows:
            name = r["name"] or ""
            key = canonical_key(name)
            if _is_structural_garbage(name):
                delete_link_ids.append(int(r["link_id"]))
            elif not (r["corporate_number"] or "") and key in by_key_has_corp:
                delete_link_ids.append(int(r["link_id"]))
        if not dry_run and delete_link_ids:
            ph = ",".join("?" for _ in delete_link_ids)
            conn.execute(f"DELETE FROM brand_operator_link WHERE id IN ({ph})", delete_link_ids)
            conn.commit()
        return len(delete_link_ids)
    finally:
        conn.close()


def _parse_brands(raw: str) -> set[str]:
    return {b.strip() for b in (raw or "").split(",") if b.strip()}


def _filter_sources(sources: Iterable[SourceSpec], brands: set[str]) -> list[SourceSpec]:
    return [s for s in sources if not brands or s.brand in brands]


def run(
    *,
    brands: set[str] | None = None,
    orm_db: str | Path = "var/pizza-registry.sqlite",
    houjin_db: str | Path = "var/houjin/registry.sqlite",
    out: str | Path = "",
    dry_run: bool = False,
    clean_registry: bool = False,
    timeout: float = 15.0,
) -> tuple[CrawlStats, list[EvidenceRow]]:
    selected_brands = brands or set()
    sources = _filter_sources(DEFAULT_SOURCES, selected_brands)
    rows, stats = fetch_sources(sources, timeout=timeout)
    rows = verify_rows(rows, houjin_db=houjin_db)
    rows = dedupe_rows(rows)
    stats.verified = sum(1 for r in rows if r.corporate_number)
    stats.ambiguous = sum(1 for r in rows if r.verification_status == "houjin_ambiguous")
    stats.no_match = sum(1 for r in rows if r.verification_status == "houjin_no_match")
    stats.applied_links = apply_rows(rows, orm_db=orm_db, dry_run=dry_run)
    if clean_registry:
        cleanup_brands = selected_brands or {s.brand for s in DEFAULT_SOURCES}
        stats.cleaned_links = cleanup_registry_brand_links(
            orm_db=orm_db,
            brands=cleanup_brands,
            dry_run=dry_run,
        )
    if out:
        p = Path(out)
        p.parent.mkdir(parents=True, exist_ok=True)
        fields = list(asdict(EvidenceRow("", "", "", "")).keys())
        with p.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for row in rows:
                data = asdict(row)
                data["store_names"] = ";".join(row.store_names)
                w.writerow(data)
    return stats, rows


def _main() -> None:
    ap = argparse.ArgumentParser(
        description="公式FC/運営会社/本部PR本文から薄いブランドのoperator evidenceをORM登録",
    )
    ap.add_argument("--brands", default="", help="カンマ区切り brand filter")
    ap.add_argument("--orm-db", default="var/pizza-registry.sqlite")
    ap.add_argument("--houjin-db", default="var/houjin/registry.sqlite")
    ap.add_argument("--out", default="var/phase28/nationwide-coverage/official-franchisee-sources.csv")
    ap.add_argument("--timeout", type=float, default=15.0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--clean-registry", action="store_true",
                    help="対象brandのpipeline由来 structural garbage/重複corp空linkを削除")
    args = ap.parse_args()

    stats, rows = run(
        brands=_parse_brands(args.brands),
        orm_db=args.orm_db,
        houjin_db=args.houjin_db,
        out=args.out,
        dry_run=args.dry_run,
        clean_registry=args.clean_registry,
        timeout=args.timeout,
    )
    print(f"✅ official-franchisee-sources {'dry-run' if args.dry_run else 'apply'}")
    print(f"   fetched       = {stats.fetched}")
    print(f"   parsed        = {stats.parsed}")
    print(f"   verified      = {stats.verified}")
    print(f"   ambiguous     = {stats.ambiguous}")
    print(f"   no_match      = {stats.no_match}")
    print(f"   applied_links = {stats.applied_links}")
    if args.clean_registry:
        print(f"   cleaned_links = {stats.cleaned_links}")
    print(f"📄 evidence: {args.out}")
    for row in rows[:8]:
        corp = row.corporate_number or "unverified"
        print(f"   {row.brand}: {row.operator_name} ({corp}) stores={row.estimated_store_count}")


if __name__ == "__main__":
    _main()
