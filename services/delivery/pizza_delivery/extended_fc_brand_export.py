"""Export additional FC brand operator tables from user-provided brand seeds.

The seed file is not treated as franchisee ground truth.  It only defines which
new brands should be reported and which franchisor row may be shown as seed
evidence.  Franchisee/operator links still come from existing ORM exports such
as JFA, manual Megajii, pipeline, and official-page evidence.
"""

from __future__ import annotations

import argparse
import csv
import re
import shutil
import sqlite3
import unicodedata
from dataclasses import dataclass
from pathlib import Path

from pizza_delivery.megafranchisee_clean_export import (
    TARGET_BRAND_SET,
    _dedupe_link_rows,
    canonical_brand,
)
from pizza_delivery.normalize import canonical_key, normalize_operator_name


SEED_SOURCE = "user_fc_brand_seed_2026_04_27"
DEFAULT_SEED_PATH = Path("test/fixtures/megafranchisee/fc-brand-seeds-2026-04-27.tsv")
DEFAULT_FC_LINKS_PATH = Path("test/fixtures/megafranchisee/fc-links.csv")
DEFAULT_OUT = Path("test/fixtures/megafranchisee/extended-brand-links.csv")
DEFAULT_SUMMARY_OUT = Path("test/fixtures/megafranchisee/extended-brand-summary.csv")
DEFAULT_BY_BRAND_DIR = Path("test/fixtures/megafranchisee/by-view/extended-by-brand")
DEFAULT_FC_OUT = Path("test/fixtures/megafranchisee/extended-fc-operator-links.csv")
DEFAULT_FC_BY_BRAND_DIR = Path("test/fixtures/megafranchisee/by-view/extended-fc-by-brand")
DEFAULT_ALL_FC_OUT = Path("test/fixtures/megafranchisee/all-fc-operator-links.csv")
DEFAULT_ALL_FC_BY_BRAND_DIR = Path("test/fixtures/megafranchisee/by-view/all-fc-by-brand")
DEFAULT_ALL_FC_MIN2_BY_BRAND_DIR = Path(
    "test/fixtures/megafranchisee/by-view/all-fc-by-brand-min2"
)
DEFAULT_ALL_FC_BRAND_INDEX_OUT = Path(
    "test/fixtures/megafranchisee/by-view/all-fc-brand-index.csv"
)
DEFAULT_ALL_FC_SINGLETONS_OUT = Path(
    "test/fixtures/megafranchisee/by-view/all-fc-singleton-brands.csv"
)
DEFAULT_ALL_FC_CANDIDATES_OUT = Path(
    "test/fixtures/megafranchisee/all-fc-operator-candidates.csv"
)

BASE_LINK_FIELDS = [
    "brand_name",
    "industry",
    "operator_name",
    "corporate_number",
    "head_office",
    "prefecture",
    "operator_type",
    "estimated_store_count",
    "source",
    "source_url",
    "note",
]
EXTENDED_LINK_FIELDS = [
    *BASE_LINK_FIELDS,
    "seed_brand_name",
    "seed_franchisor_name",
    "match_status",
]
SUMMARY_FIELDS = [
    "brand_name",
    "seed_brand_name",
    "seed_franchisor_name",
    "status",
    "operator_rows",
    "franchisee_rows",
    "franchisor_rows",
    "max_estimated_store_count",
    "sources",
]
ALL_FC_BRAND_INDEX_FIELDS = [
    "brand_name",
    "franchisee_rows",
    "verified_franchisee_rows",
    "estimated_store_sum",
    "max_operator_store_count",
    "largest_operator_name",
    "largest_operator_scope_brand_count",
    "largest_operator_count_basis",
    "sources",
    "review_status",
    "by_brand_csv",
    "min2_by_brand_csv",
    "note",
]
ALL_FC_SINGLETON_FIELDS = [
    *BASE_LINK_FIELDS,
    "operator_scope_brand_count",
    "count_basis",
    "review_status",
    "by_brand_csv",
    "review_note",
]

SEED_BRAND_ALIASES = {
    "女性だけの30分健康体操教室 Curves(カーブス)": "カーブス",
    "Curves": "カーブス",
    "ITTO個別指導学院": "Itto個別指導学院",
    "ITTO個別指導": "Itto個別指導学院",
    "Anytime Fitness": "エニタイムフィットネス",
    "ANYTIME FITNESS": "エニタイムフィットネス",
    "珈琲所コメダ珈琲店": "コメダ珈琲",
    "コメダ珈琲店": "コメダ珈琲",
    "BRAND OFF": "Brand off",
    "ブランドオフ": "Brand off",
    "センチュリー2": "センチュリー21",
    "セブンイレブン": "セブン-イレブン",
    "ケンタッキー": "ケンタッキーフライドチキン",
    "KFC": "ケンタッキーフライドチキン",
    "ケンタッキーフライドチキン": "ケンタッキーフライドチキン",
    "カレーハウスCoCo壱番屋": "カレーハウスCoCo壱番屋",
    "カレーハウスCOCO壱番屋": "カレーハウスCoCo壱番屋",
    "ドトール": "ドトールコーヒーショップ",
    "ドトールコーヒー": "ドトールコーヒーショップ",
    "ドトールコーヒーショップ": "ドトールコーヒーショップ",
    "リンガーハット": "長崎ちゃんぽんリンガーハット",
    "長崎ちゃんぽん リンガーハット": "長崎ちゃんぽんリンガーハット",
    "長崎ちゃんぽんリンガーハット": "長崎ちゃんぽんリンガーハット",
    "宅配寿司 銀のさら": "銀のさら",
    "宅配御膳 釜寅": "釜寅",
    "宅配寿司 すし上等!": "すし上等!",
    "0秒レモンサワー®仙台ホルモン焼肉酒場ときわ亭": "ときわ亭",
    "ACCEA (アクセア)": "ACCEA",
    "ACCEA(アクセア)": "ACCEA",
    "Di PUNTO(ディプント)": "Di PUNTO",
    "BURGER KING": "バーガーキング",
    "FÜRDI": "FURDI",
    "FURDI": "FURDI",
    "Renotta(リノッタ)": "Renotta",
    "Seria(セリア)": "Seria",
    "プライベートジムHPER(ハイパー)": "プライベートジムHPER",
    "メディカルホワイトニングHAKU(次世代型ホワイトニングサロンHAKU)": "メディカルホワイトニングHAKU",
    "蔦屋": "TSUTAYA",
    "CoCo壱番屋": "カレーハウスCoCo壱番屋",
    "韓丼": "カルビ丼とスン豆腐専門店韓丼",
    "スペースクリエイト自遊空間": "スペースクリエイト自遊空間",
    "自遊空間": "スペースクリエイト自遊空間",
    "信州そば 小木曽製粉所": "信州そば 小木曽製粉所",
    "小木曽製粉所": "信州そば 小木曽製粉所",
    "神楽食堂 串家物語": "神楽食堂 串家物語",
    "串家物語": "神楽食堂 串家物語",
    "美容室イレブンカット": "美容室イレブンカット",
    "イレブンカット": "美容室イレブンカット",
    "大戸屋": "大戸屋ごはん処",
    "大戸屋ごはん処": "大戸屋ごはん処",
    "田所商店": "麺場 田所商店",
    "「蔵出し味噌」麺場 田所商店": "麺場 田所商店",
    "「蔵出し味噌」麺場　田所商店": "麺場 田所商店",
    "コミックバスター": "コミック・バスター",
    "コミック・バスター": "コミック・バスター",
    "京進の個別指導 スクール・ワン": "京進の個別指導スクール・ワン",
    "京進の個別指導スクール・ワン": "京進の個別指導スクール・ワン",
    "張替本舗　金沢屋": "張替本舗 金沢屋",
    "張替本舗 金沢屋": "張替本舗 金沢屋",
    "宅配クックワン・ツゥ・スリー": "高齢者専門宅配弁当 宅配クック ワン・ツゥ・スリー",
    "Goncha": "Gong cha",
    "ゴンチャ": "Gong cha",
    "Heart Bread ANTIQUE": "Heart Bread ANTIQUE",
    "HEART BREAD ANTIQUE": "Heart Bread ANTIQUE",
    "サーティワン": "サーティワンアイスクリーム",
    "サーティーワン": "サーティワンアイスクリーム",
    "フィット365": "FIT365",
    "オンデーズ": "OWNDAYS",
    "タリーズ": "タリーズコーヒー",
    "焼肉キング": "焼肉きんぐ",
    "銀だこ": "築地銀だこ",
    "赤カラ": "赤から",
    "魅力屋": "京都北白川ラーメン魁力屋",
}

EXCLUDED_EXISTING_NONSEED_BRAND_NAMES = {
    "ザ",
    "ホームセンター",
    "名代とんかつ",
}


@dataclass(frozen=True)
class SeedBrand:
    brand_name: str
    seed_brand_name: str
    seed_franchisor_name: str
    discovered_from_existing_links: bool = False


@dataclass(frozen=True)
class HoujinMatch:
    corporate_number: str = ""
    head_office: str = ""
    prefecture: str = ""
    note: str = ""


def _nfkc(value: str) -> str:
    return unicodedata.normalize("NFKC", (value or "").strip())


def _brand_key(value: str) -> str:
    s = _nfkc(value).lower()
    return re.sub(r"[\s　・･/／()（）「」『』!！®#・]", "", s)


def _canonical_extended_brand(value: str) -> str:
    raw = _clean_brand(value)
    if raw in SEED_BRAND_ALIASES:
        return SEED_BRAND_ALIASES[raw]
    canonical = canonical_brand(raw)
    return SEED_BRAND_ALIASES.get(canonical, canonical)


def _clean_brand(value: str) -> str:
    s = _nfkc(value)
    s = s.replace("　", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip(" 、。")


def _split_seed_brand_name(brand_name: str) -> list[str]:
    raw = _clean_brand(brand_name)
    if not raw:
        return []
    if raw in {"RE/MAX"}:
        return [raw]
    if raw == "ITTO個別指導学院・みやび個別指導学院":
        return ["ITTO個別指導学院", "みやび個別指導学院"]
    parts = [p.strip() for p in re.split(r"／|/", raw) if p.strip()]
    return parts or [raw]


def _brand_variants(brand_name: str) -> list[str]:
    variants: list[str] = []
    for part in _split_seed_brand_name(brand_name):
        cleaned = _clean_brand(part)
        if cleaned in SEED_BRAND_ALIASES:
            variants.append(SEED_BRAND_ALIASES[cleaned])
            continue
        if cleaned:
            variants.append(cleaned)
        parens = re.findall(r"[（(]([^（）()]+)[）)]", cleaned)
        variants.extend(_clean_brand(p) for p in parens if _clean_brand(p))
        without_paren = re.sub(r"[（(][^（）()]+[）)]", "", cleaned).strip()
        if without_paren and without_paren != cleaned:
            variants.append(without_paren)
    out: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        canonical = _canonical_extended_brand(variant)
        key = _brand_key(canonical)
        if canonical and key not in seen:
            seen.add(key)
            out.append(canonical)
    return out


def load_seed_brands(path: Path) -> list[SeedBrand]:
    rows: list[SeedBrand] = []
    seen: set[tuple[str, str]] = set()
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            franchisor = normalize_operator_name(row.get("franchisor_name", ""))
            seed_brand = _clean_brand(row.get("brand_name", ""))
            if not franchisor or not seed_brand:
                continue
            for brand in _brand_variants(seed_brand):
                if _canonical_extended_brand(brand) in TARGET_BRAND_SET:
                    continue
                key = (_brand_key(brand), canonical_key(franchisor))
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    SeedBrand(
                        brand_name=_canonical_extended_brand(brand),
                        seed_brand_name=seed_brand,
                        seed_franchisor_name=franchisor,
                    )
                )
    return rows


def _load_fc_links(path: Path) -> dict[str, list[dict[str, str]]]:
    by_brand: dict[str, list[dict[str, str]]] = {}
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            brand = _canonical_extended_brand(row.get("brand_name", ""))
            normalized = dict(row)
            normalized["brand_name"] = brand
            by_brand.setdefault(_brand_key(brand), []).append(normalized)
    return by_brand


def _first_franchisor_name(rows: list[dict[str, str]]) -> str:
    for row in rows:
        if row.get("operator_type") == "franchisor":
            return normalize_operator_name(row.get("operator_name") or "")
    return ""


def _append_existing_nonseed_brands(
    seeds: list[SeedBrand], links_by_brand: dict[str, list[dict[str, str]]]
) -> list[SeedBrand]:
    """Add already-evidenced non-14 brands that have franchisee rows.

    User seeds define the requested investigation scope, but the existing
    all-brand FC export already contains additional franchisee evidence from
    JFA/manual/pipeline sources.  This keeps those discovered brands visible
    without treating an LLM or a hand-written list as ground truth.
    """

    out = list(seeds)
    seen = {_brand_key(seed.brand_name) for seed in seeds}
    for key, rows in sorted(links_by_brand.items(), key=lambda item: item[0]):
        if key in seen:
            continue
        if not any(row.get("operator_type") == "franchisee" for row in rows):
            continue
        brand = _canonical_extended_brand(rows[0].get("brand_name") or "")
        if brand in EXCLUDED_EXISTING_NONSEED_BRAND_NAMES:
            continue
        if not brand or _canonical_extended_brand(brand) in TARGET_BRAND_SET:
            continue
        seen.add(key)
        out.append(
            SeedBrand(
                brand_name=brand,
                seed_brand_name=brand,
                seed_franchisor_name=_first_franchisor_name(rows),
                discovered_from_existing_links=True,
            )
        )
    return out


def _resolve_existing_operator(conn: sqlite3.Connection, name: str) -> HoujinMatch:
    rows = conn.execute(
        """
        SELECT corporate_number, head_office, prefecture
        FROM operator_company
        WHERE lower(name) = lower(?)
           OR lower(replace(name, ' ', '')) = lower(replace(?, ' ', ''))
        ORDER BY corporate_number != '' DESC, updated_at DESC
        LIMIT 3
        """,
        (name, name),
    ).fetchall()
    if len(rows) == 1:
        row = rows[0]
        return HoujinMatch(row[0] or "", row[1] or "", row[2] or "", "operator_company_match")
    if rows:
        row = rows[0]
        return HoujinMatch(row[0] or "", row[1] or "", row[2] or "", "operator_company_multiple")
    return HoujinMatch()


def _resolve_houjin(houjin_db: Path | None, name: str) -> HoujinMatch:
    if not houjin_db or not houjin_db.exists():
        return HoujinMatch()
    conn = sqlite3.connect(houjin_db)
    try:
        rows = conn.execute(
            """
            SELECT corporate_number, prefecture, city, street
            FROM houjin_registry
            WHERE normalized_name = ?
            ORDER BY update_date DESC
            LIMIT 5
            """,
            (canonical_key(name),),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return HoujinMatch()
    corporate_numbers = {r[0] for r in rows if r[0]}
    if len(corporate_numbers) != 1:
        return HoujinMatch(note="houjin_ambiguous")
    row = rows[0]
    head_office = "".join(part for part in (row[1], row[2], row[3]) if part)
    return HoujinMatch(row[0] or "", head_office, row[1] or "", "houjin_exact")


def _seed_row(seed: SeedBrand, match: HoujinMatch) -> dict[str, str]:
    note = "seed_only=user_provided_fc_brand"
    if match.note:
        note = f"{note}; {match.note}"
    return {
        "brand_name": seed.brand_name,
        "industry": "",
        "operator_name": seed.seed_franchisor_name,
        "corporate_number": match.corporate_number,
        "head_office": match.head_office,
        "prefecture": match.prefecture,
        "operator_type": "franchisor",
        "estimated_store_count": "0",
        "source": SEED_SOURCE,
        "source_url": "",
        "note": note,
        "seed_brand_name": seed.seed_brand_name,
        "seed_franchisor_name": seed.seed_franchisor_name,
        "match_status": "franchisor_seed",
    }


def _row_score(row: dict[str, str]) -> tuple[int, str]:
    try:
        count = int(row.get("estimated_store_count") or 0)
    except ValueError:
        count = 0
    return (-count, row.get("operator_name") or "")


def _with_seed_context(row: dict[str, str], seed: SeedBrand, status: str) -> dict[str, str]:
    out = {k: row.get(k, "") for k in BASE_LINK_FIELDS}
    out["brand_name"] = seed.brand_name
    out["seed_brand_name"] = seed.seed_brand_name
    out["seed_franchisor_name"] = seed.seed_franchisor_name
    out["match_status"] = status
    return out


def _has_same_operator(rows: list[dict[str, str]], operator_name: str, corporate_number: str) -> bool:
    op_key = canonical_key(operator_name)
    for row in rows:
        if corporate_number and row.get("corporate_number") == corporate_number:
            return True
        if canonical_key(row.get("operator_name") or "") == op_key:
            return True
    return False


def _write_csv(path: Path, rows: list[dict[str, str]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _safe_filename(name: str) -> str:
    safe = re.sub(r'[\\/:*?"<>|]', "_", name)
    safe = safe.strip().strip(".")
    return safe or "unknown"


def _load_canonical_link_rows(
    path: Path, *, operator_types: set[str] | None = None
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            operator_type = row.get("operator_type") or ""
            if operator_types is not None and operator_type not in operator_types:
                continue
            brand = _canonical_extended_brand(row.get("brand_name", ""))
            if brand in EXCLUDED_EXISTING_NONSEED_BRAND_NAMES:
                continue
            operator_name = normalize_operator_name(row.get("operator_name") or "")
            if not brand or not operator_name:
                continue
            out = {field: row.get(field, "") for field in BASE_LINK_FIELDS}
            out["brand_name"] = brand
            out["operator_name"] = operator_name
            rows.append(out)
    return rows


def _write_by_brand_files(
    rows: list[dict[str, str]], by_brand_dir: Path, fields: list[str], *, min_rows: int = 1
) -> int:
    if by_brand_dir.exists():
        shutil.rmtree(by_brand_dir)
    by_brand_dir.mkdir(parents=True, exist_ok=True)
    brands: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        brands.setdefault(row.get("brand_name") or "", []).append(row)
    for brand, brand_rows in brands.items():
        if not brand:
            continue
        if len(brand_rows) < min_rows:
            continue
        brand_rows.sort(key=_row_score)
        _write_csv(by_brand_dir / f"{_safe_filename(brand)}.csv", brand_rows, fields)
    return sum(1 for brand_rows in brands.values() if len(brand_rows) >= min_rows)


def _brand_groups(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    groups: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        brand = row.get("brand_name") or ""
        if brand:
            groups.setdefault(brand, []).append(row)
    return groups


def _store_count(row: dict[str, str]) -> int:
    try:
        return int(row.get("estimated_store_count") or 0)
    except ValueError:
        return 0


def _operator_key(row: dict[str, str]) -> str:
    return row.get("corporate_number") or canonical_key(row.get("operator_name") or "")


def _operator_count_context(rows: list[dict[str, str]]) -> dict[str, dict[str, object]]:
    context: dict[str, dict[str, object]] = {}
    for row in rows:
        key = _operator_key(row)
        if not key:
            continue
        info = context.setdefault(key, {"brands": set(), "counts": {}})
        brands = info["brands"]
        counts = info["counts"]
        assert isinstance(brands, set)
        assert isinstance(counts, dict)
        brands.add(row.get("brand_name") or "")
        count = _store_count(row)
        counts[count] = int(counts.get(count, 0)) + 1
    return context


def _operator_scope_brand_count(
    row: dict[str, str], operator_context: dict[str, dict[str, object]]
) -> int:
    info = operator_context.get(_operator_key(row), {})
    brands = info.get("brands", set())
    return len({b for b in brands if b}) if isinstance(brands, set) else 1


def _count_basis(
    row: dict[str, str], operator_context: dict[str, dict[str, object]]
) -> str:
    info = operator_context.get(_operator_key(row), {})
    counts = info.get("counts", {})
    scope_brand_count = _operator_scope_brand_count(row, operator_context)
    repeated_count_rows = 0
    if isinstance(counts, dict):
        repeated_count_rows = int(counts.get(_store_count(row), 0))
    if (
        row.get("source") == "manual_megajii_2026_04_24"
        and scope_brand_count > 1
        and repeated_count_rows > 1
    ):
        return "operator_total_repeated_not_brand_specific"
    if row.get("source") == "manual_megajii_2026_04_24":
        return "operator_declared_total_single_brand_or_unverified_basis"
    return "source_estimated_or_store_matched_count"


def _all_fc_review_status(row_count: int, max_count: int, count_basis: str = "") -> str:
    if count_basis == "operator_total_repeated_not_brand_specific":
        return "operator_total_count_review"
    if row_count <= 1 and max_count >= 100:
        return "singleton_high_store_count_expand"
    if row_count <= 1:
        return "singleton_expand"
    if row_count == 2:
        return "thin_expand"
    return "multi_operator"


def _relative_csv_path(path: Path) -> str:
    return str(path).replace("\\", "/")


def _all_fc_brand_index_rows(
    rows: list[dict[str, str]],
    *,
    all_fc_by_brand_dir: Path,
    all_fc_min2_by_brand_dir: Path,
    min_operator_rows: int,
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    operator_context = _operator_count_context(rows)
    for brand, brand_rows in _brand_groups(rows).items():
        counts = [_store_count(r) for r in brand_rows]
        largest = max(brand_rows, key=_store_count)
        row_count = len(brand_rows)
        max_count = max(counts) if counts else 0
        filename = f"{_safe_filename(brand)}.csv"
        count_basis = _count_basis(largest, operator_context)
        status = _all_fc_review_status(row_count, max_count, count_basis)
        note = ""
        if count_basis == "operator_total_repeated_not_brand_specific":
            note = (
                "largest operator count is repeated across multiple brands; treat as "
                "operator-level footprint, not brand-specific store count"
            )
        if row_count == 1:
            note = (
                f"{note}; " if note else ""
            ) + "only_one_confirmed_franchisee_operator; keep as evidence, expand via official/JFA/recruiting/operator sources"
        elif row_count == 2:
            note = (
                f"{note}; " if note else ""
            ) + "two_confirmed_franchisee_operators; still thin for brand-level review"
        out.append(
            {
                "brand_name": brand,
                "franchisee_rows": str(row_count),
                "verified_franchisee_rows": str(
                    sum(1 for r in brand_rows if r.get("corporate_number"))
                ),
                "estimated_store_sum": str(sum(counts)),
                "max_operator_store_count": str(max_count),
                "largest_operator_name": largest.get("operator_name") or "",
                "largest_operator_scope_brand_count": str(
                    _operator_scope_brand_count(largest, operator_context)
                ),
                "largest_operator_count_basis": count_basis,
                "sources": ",".join(
                    sorted({r.get("source", "") for r in brand_rows if r.get("source")})
                ),
                "review_status": status,
                "by_brand_csv": _relative_csv_path(all_fc_by_brand_dir / filename),
                "min2_by_brand_csv": (
                    _relative_csv_path(all_fc_min2_by_brand_dir / filename)
                    if row_count >= min_operator_rows else ""
                ),
                "note": note,
            }
        )
    out.sort(
        key=lambda r: (
            int(r["franchisee_rows"]),
            -int(r["max_operator_store_count"] or 0),
            r["brand_name"],
        )
    )
    return out


def _all_fc_singleton_rows(
    rows: list[dict[str, str]], *, all_fc_by_brand_dir: Path
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    operator_context = _operator_count_context(rows)
    for brand, brand_rows in _brand_groups(rows).items():
        if len(brand_rows) != 1:
            continue
        row = dict(brand_rows[0])
        max_count = _store_count(row)
        basis = _count_basis(row, operator_context)
        row["operator_scope_brand_count"] = str(
            _operator_scope_brand_count(row, operator_context)
        )
        row["count_basis"] = basis
        row["review_status"] = _all_fc_review_status(1, max_count, basis)
        row["by_brand_csv"] = _relative_csv_path(
            all_fc_by_brand_dir / f"{_safe_filename(brand)}.csv"
        )
        note = "single confirmed franchisee/operator row; not a complete brand list yet"
        if basis == "operator_total_repeated_not_brand_specific":
            note = (
                "estimated_store_count is repeated for this operator across multiple brands; "
                "do not treat it as brand-specific; "
                + note
            )
        row["review_note"] = note
        out.append(row)
    out.sort(key=lambda r: (-_store_count(r), r.get("brand_name") or ""))
    return out


def export_all_fc_operator_links(
    *,
    fc_links_path: Path,
    all_fc_out: Path,
    all_fc_by_brand_dir: Path,
    all_fc_candidates_out: Path,
    all_fc_brand_index_out: Path = DEFAULT_ALL_FC_BRAND_INDEX_OUT,
    all_fc_singletons_out: Path = DEFAULT_ALL_FC_SINGLETONS_OUT,
    all_fc_min2_by_brand_dir: Path = DEFAULT_ALL_FC_MIN2_BY_BRAND_DIR,
    min_operator_rows: int = 2,
) -> dict[str, int]:
    franchisee_rows = _dedupe_link_rows(
        _load_canonical_link_rows(fc_links_path, operator_types={"franchisee"})
    )
    candidate_rows = _dedupe_link_rows(
        _load_canonical_link_rows(fc_links_path, operator_types={"franchisee", "unknown"})
    )
    franchisee_rows.sort(key=lambda r: (r.get("brand_name") or "", *_row_score(r)))
    candidate_rows.sort(key=lambda r: (r.get("brand_name") or "", *_row_score(r)))

    _write_csv(all_fc_out, franchisee_rows, BASE_LINK_FIELDS)
    _write_csv(all_fc_candidates_out, candidate_rows, BASE_LINK_FIELDS)
    by_brand_count = _write_by_brand_files(franchisee_rows, all_fc_by_brand_dir, BASE_LINK_FIELDS)
    min2_by_brand_count = _write_by_brand_files(
        franchisee_rows,
        all_fc_min2_by_brand_dir,
        BASE_LINK_FIELDS,
        min_rows=min_operator_rows,
    )
    index_rows = _all_fc_brand_index_rows(
        franchisee_rows,
        all_fc_by_brand_dir=all_fc_by_brand_dir,
        all_fc_min2_by_brand_dir=all_fc_min2_by_brand_dir,
        min_operator_rows=min_operator_rows,
    )
    singleton_rows = _all_fc_singleton_rows(
        franchisee_rows,
        all_fc_by_brand_dir=all_fc_by_brand_dir,
    )
    _write_csv(all_fc_brand_index_out, index_rows, ALL_FC_BRAND_INDEX_FIELDS)
    _write_csv(all_fc_singletons_out, singleton_rows, ALL_FC_SINGLETON_FIELDS)
    return {
        "all_fc_operator_links": len(franchisee_rows),
        "all_fc_operator_link_brands": by_brand_count,
        "all_fc_min2_by_brand_files": min2_by_brand_count,
        "all_fc_brand_index_rows": len(index_rows),
        "all_fc_singleton_brands": len(singleton_rows),
        "all_fc_operator_candidates": len(candidate_rows),
        "all_fc_operator_candidate_brands": len(
            {row.get("brand_name") for row in candidate_rows if row.get("brand_name")}
        ),
    }


def export_extended_brands(
    *,
    seed_path: Path,
    fc_links_path: Path,
    orm_db: Path,
    houjin_db: Path | None,
    out: Path,
    summary_out: Path,
    by_brand_dir: Path,
    fc_out: Path,
    fc_by_brand_dir: Path,
    all_fc_out: Path = DEFAULT_ALL_FC_OUT,
    all_fc_by_brand_dir: Path = DEFAULT_ALL_FC_BY_BRAND_DIR,
    all_fc_candidates_out: Path = DEFAULT_ALL_FC_CANDIDATES_OUT,
    all_fc_brand_index_out: Path = DEFAULT_ALL_FC_BRAND_INDEX_OUT,
    all_fc_singletons_out: Path = DEFAULT_ALL_FC_SINGLETONS_OUT,
    all_fc_min2_by_brand_dir: Path = DEFAULT_ALL_FC_MIN2_BY_BRAND_DIR,
    min_operator_rows: int = 2,
) -> dict[str, int]:
    links_by_brand = _load_fc_links(fc_links_path)
    user_seeds = load_seed_brands(seed_path)
    seeds = _append_existing_nonseed_brands(user_seeds, links_by_brand)
    orm = sqlite3.connect(orm_db)
    try:
        all_rows: list[dict[str, str]] = []
        fc_rows_all: list[dict[str, str]] = []
        summary_rows: list[dict[str, str]] = []
        if by_brand_dir.exists():
            shutil.rmtree(by_brand_dir)
        by_brand_dir.mkdir(parents=True, exist_ok=True)
        if fc_by_brand_dir.exists():
            shutil.rmtree(fc_by_brand_dir)
        fc_by_brand_dir.mkdir(parents=True, exist_ok=True)

        for seed in seeds:
            existing_status = (
                "existing_nonseed_link"
                if seed.discovered_from_existing_links
                else "existing_link"
            )
            existing = [
                _with_seed_context(row, seed, existing_status)
                for row in links_by_brand.get(_brand_key(seed.brand_name), [])
            ]
            if seed.seed_franchisor_name and not seed.discovered_from_existing_links:
                op_match = _resolve_existing_operator(orm, seed.seed_franchisor_name)
                if not op_match.corporate_number:
                    op_match = _resolve_houjin(houjin_db, seed.seed_franchisor_name)
                seed_link = _seed_row(seed, op_match)
                if not _has_same_operator(
                    existing, seed.seed_franchisor_name, op_match.corporate_number
                ):
                    existing.append(seed_link)

            deduped = [
                _with_seed_context(row, seed, row.get("match_status") or "existing_link")
                for row in _dedupe_link_rows(existing)
            ]
            deduped.sort(key=_row_score)
            all_rows.extend(deduped)

            franchisee_rows = [r for r in deduped if r.get("operator_type") == "franchisee"]
            if franchisee_rows:
                fc_rows_all.extend(franchisee_rows)
                _write_csv(
                    fc_by_brand_dir / f"{_safe_filename(seed.brand_name)}.csv",
                    franchisee_rows,
                    EXTENDED_LINK_FIELDS,
                )
            franchisor_rows = [r for r in deduped if r.get("operator_type") == "franchisor"]
            counts: list[int] = []
            for row in deduped:
                try:
                    counts.append(int(row.get("estimated_store_count") or 0))
                except ValueError:
                    pass
            status = "operator_links_found" if franchisee_rows else "franchisor_seed_only"
            if franchisee_rows and all(r.get("source") == SEED_SOURCE for r in deduped):
                status = "franchisor_seed_only"
            sources = ",".join(sorted({r.get("source", "") for r in deduped if r.get("source")}))
            summary_rows.append(
                {
                    "brand_name": seed.brand_name,
                    "seed_brand_name": seed.seed_brand_name,
                    "seed_franchisor_name": seed.seed_franchisor_name,
                    "status": status,
                    "operator_rows": str(len(deduped)),
                    "franchisee_rows": str(len(franchisee_rows)),
                    "franchisor_rows": str(len(franchisor_rows)),
                    "max_estimated_store_count": str(max(counts) if counts else 0),
                    "sources": sources,
                }
            )
            _write_csv(by_brand_dir / f"{_safe_filename(seed.brand_name)}.csv", deduped, EXTENDED_LINK_FIELDS)

        all_rows.sort(key=lambda r: (r.get("brand_name") or "", *_row_score(r)))
        fc_rows_all.sort(key=lambda r: (r.get("brand_name") or "", *_row_score(r)))
        summary_rows.sort(key=lambda r: (r["status"] != "operator_links_found", r["brand_name"]))
        _write_csv(out, all_rows, EXTENDED_LINK_FIELDS)
        _write_csv(fc_out, fc_rows_all, EXTENDED_LINK_FIELDS)
        _write_csv(summary_out, summary_rows, SUMMARY_FIELDS)
        stats = {
            "seed_brands": len(user_seeds),
            "existing_nonseed_brands": sum(1 for seed in seeds if seed.discovered_from_existing_links),
            "reported_brands": len(seeds),
            "extended_brand_links": len(all_rows),
            "extended_fc_operator_links": len(fc_rows_all),
            "extended_by_brand_files": len(summary_rows),
            "extended_fc_by_brand_files": sum(
                1 for r in summary_rows if int(r["franchisee_rows"] or 0) > 0
            ),
            "operator_link_brands": sum(1 for r in summary_rows if r["status"] == "operator_links_found"),
            "franchisor_seed_only_brands": sum(
                1 for r in summary_rows if r["status"] == "franchisor_seed_only"
            ),
        }
        stats.update(
            export_all_fc_operator_links(
                fc_links_path=fc_links_path,
                all_fc_out=all_fc_out,
                all_fc_by_brand_dir=all_fc_by_brand_dir,
                all_fc_candidates_out=all_fc_candidates_out,
                all_fc_brand_index_out=all_fc_brand_index_out,
                all_fc_singletons_out=all_fc_singletons_out,
                all_fc_min2_by_brand_dir=all_fc_min2_by_brand_dir,
                min_operator_rows=min_operator_rows,
            )
        )
        return stats
    finally:
        orm.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED_PATH)
    parser.add_argument("--fc-links", type=Path, default=DEFAULT_FC_LINKS_PATH)
    parser.add_argument("--orm-db", type=Path, default=Path("var/pizza-registry.sqlite"))
    parser.add_argument("--houjin-db", type=Path, default=Path("var/houjin/registry.sqlite"))
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--summary-out", type=Path, default=DEFAULT_SUMMARY_OUT)
    parser.add_argument("--by-brand-dir", type=Path, default=DEFAULT_BY_BRAND_DIR)
    parser.add_argument("--fc-out", type=Path, default=DEFAULT_FC_OUT)
    parser.add_argument("--fc-by-brand-dir", type=Path, default=DEFAULT_FC_BY_BRAND_DIR)
    parser.add_argument("--all-fc-out", type=Path, default=DEFAULT_ALL_FC_OUT)
    parser.add_argument("--all-fc-by-brand-dir", type=Path, default=DEFAULT_ALL_FC_BY_BRAND_DIR)
    parser.add_argument("--all-fc-min2-by-brand-dir", type=Path, default=DEFAULT_ALL_FC_MIN2_BY_BRAND_DIR)
    parser.add_argument("--all-fc-brand-index-out", type=Path, default=DEFAULT_ALL_FC_BRAND_INDEX_OUT)
    parser.add_argument("--all-fc-singletons-out", type=Path, default=DEFAULT_ALL_FC_SINGLETONS_OUT)
    parser.add_argument("--min-operator-rows", type=int, default=2)
    parser.add_argument(
        "--all-fc-candidates-out",
        type=Path,
        default=DEFAULT_ALL_FC_CANDIDATES_OUT,
    )
    args = parser.parse_args()
    stats = export_extended_brands(
        seed_path=args.seed,
        fc_links_path=args.fc_links,
        orm_db=args.orm_db,
        houjin_db=args.houjin_db,
        out=args.out,
        summary_out=args.summary_out,
        by_brand_dir=args.by_brand_dir,
        fc_out=args.fc_out,
        fc_by_brand_dir=args.fc_by_brand_dir,
        all_fc_out=args.all_fc_out,
        all_fc_by_brand_dir=args.all_fc_by_brand_dir,
        all_fc_candidates_out=args.all_fc_candidates_out,
        all_fc_brand_index_out=args.all_fc_brand_index_out,
        all_fc_singletons_out=args.all_fc_singletons_out,
        all_fc_min2_by_brand_dir=args.all_fc_min2_by_brand_dir,
        min_operator_rows=args.min_operator_rows,
    )
    for key, value in stats.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
