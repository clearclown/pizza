"""Review 対象メガフランチャイジーの国税庁 CSV 高速照合。

`LIKE '%name%'` を使わず、国税庁 CSV 由来 SQLite の `normalized_name`
完全一致と、ORM にある本社都道府県・住所で候補を絞る。
LLM や web 検索は使わない。
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import unicodedata
from dataclasses import dataclass
from pathlib import Path

from pizza_delivery.houjin_csv import ACTIVE_PROCESS_CODES, HoujinCSVRecord
from pizza_delivery.normalize import canonical_key, normalize_operator_name


DEFAULT_REVIEW_CSV = "var/phase28/nationwide-coverage/mega-franchisee-review-min20.csv"
DEFAULT_OUT = "var/phase28/nationwide-coverage/mega-franchisee-review-houjin-matches.csv"


@dataclass(frozen=True)
class OperatorContext:
    operator_id: int
    name: str
    corporate_number: str
    prefecture: str
    head_office: str


@dataclass(frozen=True)
class HydrationMatch:
    operator_name: str
    total_stores: str
    brand_count: str
    brands_breakdown: str
    status: str
    matched_name: str = ""
    corporate_number: str = ""
    prefecture: str = ""
    address: str = ""
    candidate_count: int = 0
    orm_operator_ids: str = ""
    reason: str = ""

    def to_row(self) -> dict[str, object]:
        return {
            "operator_name": self.operator_name,
            "total_stores": self.total_stores,
            "brand_count": self.brand_count,
            "brands_breakdown": self.brands_breakdown,
            "status": self.status,
            "matched_name": self.matched_name,
            "corporate_number": self.corporate_number,
            "prefecture": self.prefecture,
            "address": self.address,
            "candidate_count": self.candidate_count,
            "orm_operator_ids": self.orm_operator_ids,
            "reason": self.reason,
        }


def _variants(name: str) -> list[str]:
    base = normalize_operator_name(name)
    if not base:
        return []
    out = [base]
    has_designator = base.startswith(("株式会社", "有限会社", "合同会社")) or base.endswith(
        ("株式会社", "有限会社", "合同会社")
    )
    if not has_designator:
        out.append(f"株式会社{base}")
        out.append(f"{base}株式会社")
    return list(dict.fromkeys(out))


def _compact(value: str) -> str:
    text = unicodedata.normalize("NFKC", value or "")
    return "".join(ch for ch in text if ch.isalnum())


def _fetch_houjin_candidates(
    conn: sqlite3.Connection,
    name: str,
    *,
    limit: int = 200,
) -> list[HoujinCSVRecord]:
    active = sorted(ACTIVE_PROCESS_CODES)
    sql = (
        "SELECT corporate_number, process, update_date, name, prefecture, city, street "
        "FROM houjin_registry WHERE normalized_name = ? "
        f"AND process IN ({','.join('?' * len(active))}) LIMIT ?"
    )
    seen: set[str] = set()
    out: list[HoujinCSVRecord] = []
    for variant in _variants(name):
        key = canonical_key(variant)
        if not key:
            continue
        for row in conn.execute(sql, [key, *active, limit]).fetchall():
            corp = str(row[0])
            if corp in seen:
                continue
            seen.add(corp)
            out.append(
                HoujinCSVRecord(
                    corporate_number=corp,
                    process=row[1],
                    update_date=row[2],
                    name=row[3],
                    prefecture=row[4],
                    city=row[5],
                    street=row[6],
                )
            )
    return out


def _load_operator_contexts(
    conn: sqlite3.Connection, operator_name: str
) -> list[OperatorContext]:
    key = canonical_key(operator_name)
    rows = conn.execute(
        """
        SELECT id, name, corporate_number, prefecture, head_office
        FROM operator_company
        WHERE corporate_number = ''
        ORDER BY id
        """
    ).fetchall()
    out: list[OperatorContext] = []
    for row in rows:
        if canonical_key(row[1]) != key:
            continue
        out.append(
            OperatorContext(
                operator_id=int(row[0]),
                name=row[1],
                corporate_number=row[2],
                prefecture=row[3],
                head_office=row[4],
            )
        )
    return out


def _best_context(contexts: list[OperatorContext]) -> OperatorContext | None:
    if not contexts:
        return None
    return max(
        contexts,
        key=lambda c: (
            bool(c.prefecture),
            bool(c.head_office),
            len(c.head_office),
        ),
    )


def _address_score(ctx: OperatorContext | None, rec: HoujinCSVRecord) -> int:
    if ctx is None:
        return 0
    score = 0
    if ctx.prefecture and ctx.prefecture == rec.prefecture:
        score += 50
    hq = _compact(ctx.head_office)
    if hq:
        if _compact(rec.city) and _compact(rec.city) in hq:
            score += 20
        street = _compact(rec.street)
        if street and (street in hq or hq in _compact(rec.address)):
            score += 30
    return score


def _select_match(
    operator_name: str,
    candidates: list[HoujinCSVRecord],
    contexts: list[OperatorContext],
) -> tuple[str, HoujinCSVRecord | None, str]:
    if not candidates:
        return "no_match", None, "normalized_name_no_hit"

    ctx = _best_context(contexts)
    scored = sorted(
        ((rec, _address_score(ctx, rec)) for rec in candidates),
        key=lambda item: (-item[1], item[0].corporate_number),
    )
    best, best_score = scored[0]
    if len(candidates) == 1:
        if ctx and ctx.prefecture and ctx.prefecture != best.prefecture:
            return "prefecture_mismatch", None, "single_name_hit_but_hq_prefecture_differs"
        return "accepted_unique", best, "single_normalized_name_hit"

    if best_score >= 50:
        tied = [rec for rec, score in scored if score == best_score]
        if len(tied) == 1:
            reason = "matched_hq_address" if best_score >= 80 else "matched_hq_prefecture"
            return "accepted_disambiguated", best, reason

    return "ambiguous", None, "multiple_normalized_name_hits"


def find_houjin_matches(
    *,
    review_csv: str | Path = DEFAULT_REVIEW_CSV,
    houjin_db: str | Path = "var/houjin/registry.sqlite",
    orm_db: str | Path = "var/pizza-registry.sqlite",
) -> list[HydrationMatch]:
    review_rows = list(csv.DictReader(Path(review_csv).open(encoding="utf-8")))
    houjin = sqlite3.connect(houjin_db)
    orm = sqlite3.connect(orm_db)
    try:
        matches: list[HydrationMatch] = []
        for row in review_rows:
            if row.get("primary_corporate_number"):
                continue
            name = row["operator_name"]
            candidates = _fetch_houjin_candidates(houjin, name)
            contexts = _load_operator_contexts(orm, name)
            status, rec, reason = _select_match(name, candidates, contexts)
            matches.append(
                HydrationMatch(
                    operator_name=name,
                    total_stores=row.get("operator_total_stores_est", ""),
                    brand_count=row.get("operator_brand_count_est", ""),
                    brands_breakdown=row.get("brands_breakdown", ""),
                    status=status,
                    matched_name=rec.name if rec else "",
                    corporate_number=rec.corporate_number if rec else "",
                    prefecture=rec.prefecture if rec else "",
                    address=rec.address if rec else "",
                    candidate_count=len(candidates),
                    orm_operator_ids="|".join(str(c.operator_id) for c in contexts),
                    reason=reason,
                )
            )
        return matches
    finally:
        houjin.close()
        orm.close()


def _merge_operator(conn: sqlite3.Connection, src_id: int, dst_id: int) -> None:
    links = conn.execute(
        """
        SELECT id, brand_id, source, estimated_store_count
        FROM brand_operator_link
        WHERE operator_id = ?
        """,
        (src_id,),
    ).fetchall()
    for link_id, brand_id, source, count in links:
        existing = conn.execute(
            """
            SELECT id, estimated_store_count
            FROM brand_operator_link
            WHERE operator_id = ? AND brand_id = ? AND source = ?
            """,
            (dst_id, brand_id, source),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE brand_operator_link
                SET estimated_store_count = MAX(estimated_store_count, ?)
                WHERE id = ?
                """,
                (count, existing[0]),
            )
            conn.execute("DELETE FROM brand_operator_link WHERE id = ?", (link_id,))
        else:
            conn.execute(
                "UPDATE brand_operator_link SET operator_id = ? WHERE id = ?",
                (dst_id, link_id),
            )
    conn.execute("DELETE FROM operator_company WHERE id = ?", (src_id,))


def apply_matches(
    matches: list[HydrationMatch],
    *,
    orm_db: str | Path = "var/pizza-registry.sqlite",
) -> dict[str, int]:
    conn = sqlite3.connect(orm_db)
    try:
        applied = 0
        merged = 0
        skipped = 0
        for match in matches:
            if not match.status.startswith("accepted") or not match.corporate_number:
                skipped += 1
                continue
            operator_ids = [int(v) for v in match.orm_operator_ids.split("|") if v]
            if not operator_ids:
                skipped += 1
                continue
            existing = conn.execute(
                """
                SELECT id
                FROM operator_company
                WHERE corporate_number = ?
                ORDER BY id
                LIMIT 1
                """,
                (match.corporate_number,),
            ).fetchone()
            dst_id = int(existing[0]) if existing else 0
            for operator_id in operator_ids:
                if dst_id and dst_id != operator_id:
                    _merge_operator(conn, operator_id, dst_id)
                    merged += 1
                    continue
                conn.execute(
                    """
                    UPDATE operator_company
                    SET name = ?,
                        corporate_number = ?,
                        head_office = ?,
                        prefecture = ?,
                        source = CASE
                            WHEN source LIKE '%houjin_csv%' THEN source
                            WHEN source = '' THEN 'houjin_csv'
                            ELSE source || ',houjin_csv'
                        END
                    WHERE id = ?
                    """,
                    (
                        match.matched_name,
                        match.corporate_number,
                        match.address,
                        match.prefecture,
                        operator_id,
                    ),
                )
                dst_id = operator_id
                applied += 1
        conn.commit()
        return {"applied": applied, "merged": merged, "skipped": skipped}
    finally:
        conn.close()


def write_matches(path: str | Path, matches: list[HydrationMatch]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = [m.to_row() for m in matches]
    fieldnames = list(rows[0].keys()) if rows else list(HydrationMatch("", "", "", "", "").to_row().keys())
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _main() -> None:
    ap = argparse.ArgumentParser(description="review 対象メガジーを国税庁 CSV SQLite で高速 hydrate")
    ap.add_argument("--review-csv", default=DEFAULT_REVIEW_CSV)
    ap.add_argument("--houjin-db", default="var/houjin/registry.sqlite")
    ap.add_argument("--orm-db", default="var/pizza-registry.sqlite")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    matches = find_houjin_matches(
        review_csv=args.review_csv,
        houjin_db=args.houjin_db,
        orm_db=args.orm_db,
    )
    write_matches(args.out, matches)
    counts: dict[str, int] = {}
    for match in matches:
        counts[match.status] = counts.get(match.status, 0) + 1
    print("✅ megafranchisee review houjin hydrate")
    print(f"   matches_csv = {args.out}")
    for key in sorted(counts):
        print(f"   {key} = {counts[key]}")
    if args.apply:
        stats = apply_matches(matches, orm_db=args.orm_db)
        for key, value in stats.items():
            print(f"   {key} = {value}")


if __name__ == "__main__":
    _main()
