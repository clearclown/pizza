"""エニタイムフィットネス公式店舗一覧 sync。

Google API を使わず、公式サイトの都道府県別店舗一覧だけを source として
pipeline `stores` を補完する。operator は公式店舗一覧には出ないため扱わない。
"""

from __future__ import annotations

import argparse
import csv
import html
import re
import sqlite3
import time
import unicodedata
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin, urlparse


BASE_URL = "https://www.anytimefitness.co.jp"
BRAND = "エニタイムフィットネス"

PREFECTURE_PATHS: tuple[tuple[str, str], ...] = (
    ("北海道", "/hokkaido/"),
    ("青森県", "/tohoku/aomori/"),
    ("岩手県", "/tohoku/iwate/"),
    ("宮城県", "/tohoku/miyagi/"),
    ("秋田県", "/tohoku/akita/"),
    ("山形県", "/tohoku/yamagata/"),
    ("福島県", "/tohoku/fukushima/"),
    ("茨城県", "/kanto/ibaraki/"),
    ("栃木県", "/kanto/tochigi/"),
    ("群馬県", "/kanto/gunma/"),
    ("埼玉県", "/kanto/saitama/"),
    ("千葉県", "/kanto/chiba/"),
    ("東京都", "/kanto/tokyo/"),
    ("神奈川県", "/kanto/kanagawa/"),
    ("新潟県", "/chubu/niigata/"),
    ("富山県", "/chubu/toyama/"),
    ("石川県", "/chubu/ishikawa/"),
    ("福井県", "/chubu/fukui/"),
    ("山梨県", "/chubu/yamanashi/"),
    ("長野県", "/chubu/nagano/"),
    ("岐阜県", "/chubu/gifu/"),
    ("静岡県", "/chubu/shizuoka/"),
    ("愛知県", "/chubu/aichi/"),
    ("三重県", "/kinki/mie/"),
    ("滋賀県", "/kinki/shiga/"),
    ("京都府", "/kinki/kyoto/"),
    ("大阪府", "/kinki/osaka/"),
    ("兵庫県", "/kinki/hyogo/"),
    ("奈良県", "/kinki/nara/"),
    ("和歌山県", "/kinki/wakayama/"),
    ("鳥取県", "/chugoku/tottori/"),
    ("島根県", "/chugoku/shimane/"),
    ("岡山県", "/chugoku/okayama/"),
    ("広島県", "/chugoku/hiroshima/"),
    ("山口県", "/chugoku/yamaguchi/"),
    ("徳島県", "/shikoku/tokushima/"),
    ("香川県", "/shikoku/kagawa/"),
    ("愛媛県", "/shikoku/ehime/"),
    ("高知県", "/shikoku/kochi/"),
    ("福岡県", "/kyushu/fukuoka/"),
    ("佐賀県", "/kyushu/saga/"),
    ("長崎県", "/kyushu/nagasaki/"),
    ("熊本県", "/kyushu/kumamoto/"),
    ("大分県", "/kyushu/oita/"),
    ("宮崎県", "/kyushu/miyazaki/"),
    ("鹿児島県", "/kyushu/kagoshima/"),
    ("沖縄県", "/kyushu/okinawa/"),
)


@dataclass(frozen=True)
class OfficialStore:
    prefecture: str
    name: str
    address: str
    url: str
    slug: str

    def place_id(self) -> str:
        return f"anytime-official:{self.slug}"

    def store_name(self) -> str:
        return self.name if BRAND in self.name else f"{BRAND} {self.name}"


def _clean_text(value: str) -> str:
    value = re.sub(r"<br\s*/?>", " ", value, flags=re.I)
    value = re.sub(r"<[^>]+>", "", value)
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def _slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    return path.split("/")[-1]


def _absolute_url(href: str) -> str:
    return urljoin(BASE_URL, href)


def parse_prefecture_page(html_text: str, *, prefecture: str) -> list[OfficialStore]:
    """都道府県別一覧 HTML から店舗カードを抽出する。"""
    rows: list[OfficialStore] = []
    pattern = re.compile(
        r'<li>\s*<a\s+href="(?P<href>/[^"]+/)">(?P<body>.*?)</a>\s*</li>',
        re.S,
    )
    for m in pattern.finditer(html_text):
        body = m.group("body")
        name_m = re.search(r'<p class="name">(?P<value>.*?)</p>', body, re.S)
        addr_m = re.search(r'<p class="address">(?P<value>.*?)</p>', body, re.S)
        if not name_m or not addr_m:
            continue
        name = _clean_text(name_m.group("value"))
        address = _clean_text(addr_m.group("value"))
        if not name or not address:
            continue
        href = m.group("href")
        url = _absolute_url(href)
        slug = _slug_from_url(url)
        if not slug:
            continue
        rows.append(
            OfficialStore(
                prefecture=prefecture,
                name=name,
                address=address,
                url=url,
                slug=slug,
            )
        )
    return rows


def fetch_url(url: str, *, timeout: float = 30.0) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "PI-ZZA/0.28 FC market research",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def fetch_official_stores(*, sleep_sec: float = 0.2) -> list[OfficialStore]:
    rows: list[OfficialStore] = []
    seen: set[str] = set()
    for pref, path in PREFECTURE_PATHS:
        url = _absolute_url(path)
        html_text = fetch_url(url)
        for store in parse_prefecture_page(html_text, prefecture=pref):
            if store.slug in seen:
                continue
            seen.add(store.slug)
            rows.append(store)
        if sleep_sec > 0:
            time.sleep(sleep_sec)
    return rows


def _write_csv(path: str | Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _store_dict(store: OfficialStore) -> dict[str, str]:
    return {
        "prefecture": store.prefecture,
        "store_name": store.store_name(),
        "raw_store_name": store.name,
        "address": store.address,
        "official_url": store.url,
        "slug": store.slug,
        "place_id": store.place_id(),
        "source": "anytime_official_list",
    }


def _is_anytime_store(name: str, official_url: str) -> bool:
    text = f"{name or ''} {official_url or ''}".casefold()
    return (
        "エニタイム" in text
        or "anytime fitness" in text
        or "anytimefitness.co.jp" in text
    )


def _normalise_official_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url.strip())
    if "anytimefitness.co.jp" not in parsed.netloc:
        return url.strip()
    path = parsed.path or "/"
    if path != "/" and not path.endswith("/"):
        path += "/"
    return f"https://www.anytimefitness.co.jp{path}"


def _store_name_key(value: str) -> str:
    text = unicodedata.normalize("NFKC", value or "").casefold()
    text = text.replace("エニタイムフィットネス", "")
    text = text.replace("anytime fitness", "")
    text = text.replace("anytimefitness", "")
    return re.sub(r"[\s\-_・()（）]+", "", text)


def _unique_name_lookup(stores: list[OfficialStore]) -> dict[str, OfficialStore]:
    buckets: dict[str, list[OfficialStore]] = {}
    for store in stores:
        for value in (store.name, store.store_name()):
            key = _store_name_key(value)
            if key:
                buckets.setdefault(key, []).append(store)
    return {key: rows[0] for key, rows in buckets.items() if len({r.url for r in rows}) == 1}


def _operator_link_rows(conn: sqlite3.Connection, place_id: str) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT operator_name, brand, operator_type, confidence, discovered_via,
               corporate_number, verification_source
        FROM operator_stores
        WHERE place_id = ?
        """,
        (place_id,),
    ).fetchall()


def _remap_operator_links(conn: sqlite3.Connection, old_place_id: str, new_place_id: str) -> int:
    if old_place_id == new_place_id:
        return 0
    remapped = 0
    for row in _operator_link_rows(conn, old_place_id):
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO operator_stores
              (operator_name, place_id, brand, operator_type, confidence,
               discovered_via, corporate_number, verification_source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["operator_name"],
                new_place_id,
                row["brand"],
                row["operator_type"],
                row["confidence"],
                row["discovered_via"],
                row["corporate_number"],
                row["verification_source"],
            ),
        )
        remapped += max(cur.rowcount, 0)
    return remapped


def _best_place_id_for_official_url(conn: sqlite3.Connection, official_url: str) -> str:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT s.place_id, COUNT(os.operator_name) AS operator_links
        FROM stores s
        LEFT JOIN operator_stores os ON os.place_id = s.place_id
        WHERE s.brand = ? AND s.official_url = ?
        GROUP BY s.place_id
        ORDER BY operator_links DESC,
                 CASE WHEN s.place_id LIKE 'anytime-official:%' THEN 0 ELSE 1 END,
                 s.place_id
        """,
        (BRAND, official_url),
    ).fetchall()
    return str(rows[0]["place_id"]) if rows else ""


def _official_url_place_ids(conn: sqlite3.Connection, official_url: str) -> list[str]:
    return [
        str(row[0])
        for row in conn.execute(
            """
            SELECT place_id
            FROM stores
            WHERE brand = ? AND official_url = ?
            ORDER BY place_id
            """,
            (BRAND, official_url),
        ).fetchall()
    ]


def _mark_master_franchisee_rows(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        """
        UPDATE operator_stores
        SET operator_type = 'franchisor'
        WHERE brand = ?
          AND operator_name IN ('株式会社Fast Fitness Japan', 'Fast Fitness Japan')
          AND COALESCE(operator_type,'') != 'franchisor'
        """,
        (BRAND,),
    )
    return max(cur.rowcount, 0)


def _find_existing_store(conn: sqlite3.Connection, store: OfficialStore) -> str:
    slug = store.slug
    url_like = f"%/{slug}/%"
    row = conn.execute(
        """
        SELECT place_id
        FROM stores
        WHERE brand = ?
          AND (
            official_url LIKE ?
            OR official_url = ?
            OR address = ?
          )
        ORDER BY CASE WHEN official_url LIKE ? THEN 0 ELSE 1 END, place_id
        LIMIT 1
        """,
        (BRAND, url_like, store.url, store.address, url_like),
    ).fetchone()
    return str(row[0]) if row else ""


def apply_official_stores(
    db_path: str | Path,
    stores: list[OfficialStore],
    *,
    purge_false_positives: bool = False,
    purge_non_official: bool = False,
) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    try:
        inserted = 0
        updated = 0
        remapped = 0
        non_official_deleted = 0
        duplicate_official_deleted = 0
        master_operator_rows_marked = 0
        for store in stores:
            existing_place_id = _find_existing_store(conn, store)
            if existing_place_id:
                conn.execute(
                    """
                    UPDATE stores
                    SET name = ?, address = ?, official_url = ?
                    WHERE place_id = ?
                    """,
                    (store.store_name(), store.address, store.url, existing_place_id),
                )
                updated += 1
                continue
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO stores
                  (place_id, brand, name, address, lat, lng, official_url, phone, grid_cell_id)
                VALUES (?, ?, ?, ?, 0, 0, ?, '', 'official:anytime')
                """,
                (
                    store.place_id(),
                    BRAND,
                    store.store_name(),
                    store.address,
                    store.url,
                ),
            )
            inserted += max(cur.rowcount, 0)

        master_operator_rows_marked = _mark_master_franchisee_rows(conn)

        false_rows = list_false_positive_stores(conn)
        deleted = 0
        if purge_false_positives and false_rows:
            place_ids = [r["place_id"] for r in false_rows]
            conn.executemany(
                "DELETE FROM operator_stores WHERE place_id = ?",
                [(pid,) for pid in place_ids],
            )
            conn.executemany(
                "DELETE FROM stores WHERE place_id = ?",
                [(pid,) for pid in place_ids],
            )
            deleted = len(place_ids)

        if purge_non_official:
            official_by_url = {store.url: store for store in stores}
            official_by_normalised_url = {
                _normalise_official_url(store.url): store for store in stores
            }
            official_by_name = _unique_name_lookup(stores)
            canonical_place_id_by_url: dict[str, str] = {}
            for store in stores:
                keep_place_id = _best_place_id_for_official_url(conn, store.url)
                if not keep_place_id:
                    continue
                canonical_place_id_by_url[store.url] = keep_place_id
                conn.execute(
                    """
                    UPDATE stores
                    SET name = ?, address = ?, official_url = ?
                    WHERE place_id = ?
                    """,
                    (store.store_name(), store.address, store.url, keep_place_id),
                )
                for place_id in _official_url_place_ids(conn, store.url):
                    if place_id == keep_place_id:
                        continue
                    remapped += _remap_operator_links(conn, place_id, keep_place_id)
                    conn.execute("DELETE FROM operator_stores WHERE place_id = ?", (place_id,))
                    conn.execute("DELETE FROM stores WHERE place_id = ?", (place_id,))
                    duplicate_official_deleted += 1

            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT place_id, name, official_url
                FROM stores
                WHERE brand = ?
                ORDER BY place_id
                """,
                (BRAND,),
            ).fetchall()
            official_urls = set(official_by_url)
            for row in rows:
                place_id = str(row["place_id"])
                official_url = row["official_url"] or ""
                if official_url in official_urls:
                    continue
                target_store = official_by_normalised_url.get(
                    _normalise_official_url(official_url)
                )
                if target_store is None:
                    target_store = official_by_name.get(_store_name_key(row["name"]))
                if target_store is not None:
                    target_place_id = canonical_place_id_by_url.get(target_store.url)
                    if target_place_id:
                        remapped += _remap_operator_links(conn, place_id, target_place_id)
                conn.execute("DELETE FROM operator_stores WHERE place_id = ?", (place_id,))
                conn.execute("DELETE FROM stores WHERE place_id = ?", (place_id,))
                non_official_deleted += 1

        conn.commit()
    finally:
        conn.close()
    return {
        "official_stores": len(stores),
        "inserted": inserted,
        "updated": updated,
        "false_positive_candidates": len(false_rows),
        "false_positive_deleted": deleted,
        "non_official_deleted": non_official_deleted,
        "duplicate_official_deleted": duplicate_official_deleted,
        "operator_links_remapped": remapped,
        "master_operator_rows_marked": master_operator_rows_marked,
    }


def list_false_positive_stores(conn: sqlite3.Connection) -> list[dict[str, object]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT place_id, name, address, official_url, lat, lng, phone
        FROM stores
        WHERE brand = ?
        ORDER BY place_id
        """,
        (BRAND,),
    ).fetchall()
    out: list[dict[str, object]] = []
    for row in rows:
        if _is_anytime_store(row["name"], row["official_url"]):
            continue
        out.append(dict(row))
    return out


def list_non_official_stores(
    conn: sqlite3.Connection, official_stores: list[OfficialStore]
) -> list[dict[str, object]]:
    conn.row_factory = sqlite3.Row
    official_urls = {s.url for s in official_stores}
    rows = conn.execute(
        """
        SELECT place_id, name, address, official_url, lat, lng, phone
        FROM stores
        WHERE brand = ?
        ORDER BY place_id
        """,
        (BRAND,),
    ).fetchall()
    url_counts: dict[str, int] = {}
    for row in rows:
        official_url = row["official_url"] or ""
        if official_url in official_urls:
            url_counts[official_url] = url_counts.get(official_url, 0) + 1

    out: list[dict[str, object]] = []
    seen_official_urls: set[str] = set()
    for row in rows:
        official_url = row["official_url"] or ""
        reason = ""
        if official_url not in official_urls:
            reason = "not_in_official_list"
        elif url_counts.get(official_url, 0) > 1:
            if official_url in seen_official_urls:
                reason = "duplicate_official_url"
            else:
                seen_official_urls.add(official_url)
        if not reason:
            continue
        item = dict(row)
        item["reason"] = reason
        out.append(item)
    return out


def export_anytime_official(
    *,
    db_path: str | Path,
    out: str | Path,
    false_positive_out: str | Path,
    non_official_out: str | Path | None = None,
    apply: bool = False,
    purge_false_positives: bool = False,
    purge_non_official: bool = False,
    sleep_sec: float = 0.2,
) -> dict[str, int]:
    stores = fetch_official_stores(sleep_sec=sleep_sec)
    _write_csv(
        out,
        [
            "prefecture",
            "store_name",
            "raw_store_name",
            "address",
            "official_url",
            "slug",
            "place_id",
            "source",
        ],
        [_store_dict(s) for s in stores],
    )
    conn = sqlite3.connect(db_path)
    try:
        false_rows = list_false_positive_stores(conn)
        non_official_rows = list_non_official_stores(conn, stores)
    finally:
        conn.close()
    _write_csv(
        false_positive_out,
        ["place_id", "name", "address", "official_url", "lat", "lng", "phone"],
        false_rows,
    )
    if non_official_out is not None:
        _write_csv(
            non_official_out,
            [
                "place_id",
                "name",
                "address",
                "official_url",
                "lat",
                "lng",
                "phone",
                "reason",
            ],
            non_official_rows,
        )
    stats = {
        "official_stores": len(stores),
        "inserted": 0,
        "updated": 0,
        "false_positive_candidates": len(false_rows),
        "false_positive_deleted": 0,
        "non_official_candidates": len(non_official_rows),
        "non_official_deleted": 0,
        "duplicate_official_deleted": 0,
        "operator_links_remapped": 0,
        "master_operator_rows_marked": 0,
    }
    if apply:
        stats = apply_official_stores(
            db_path,
            stores,
            purge_false_positives=purge_false_positives,
            purge_non_official=purge_non_official,
        )
        conn = sqlite3.connect(db_path)
        try:
            false_rows = list_false_positive_stores(conn)
            remaining_non_official_rows = list_non_official_stores(conn, stores)
        finally:
            conn.close()
        stats["non_official_candidates"] = len(non_official_rows)
        stats["remaining_non_official"] = len(remaining_non_official_rows)
        _write_csv(
            false_positive_out,
            ["place_id", "name", "address", "official_url", "lat", "lng", "phone"],
            false_rows,
        )
    return stats


def _main() -> None:
    ap = argparse.ArgumentParser(description="エニタイム公式店舗一覧を取得して pipeline stores を補完")
    ap.add_argument("--db", default="var/pizza.sqlite")
    ap.add_argument("--out", default="var/phase28/anytime/anytime-official-stores.csv")
    ap.add_argument(
        "--false-positive-out",
        default="var/phase28/anytime/anytime-false-positive-stores.csv",
    )
    ap.add_argument(
        "--non-official-out",
        default="var/phase28/anytime/anytime-non-official-stores.csv",
    )
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--purge-false-positives", action="store_true")
    ap.add_argument("--purge-non-official", action="store_true")
    ap.add_argument("--sleep-sec", type=float, default=0.2)
    args = ap.parse_args()
    stats = export_anytime_official(
        db_path=args.db,
        out=args.out,
        false_positive_out=args.false_positive_out,
        non_official_out=args.non_official_out,
        apply=args.apply,
        purge_false_positives=args.purge_false_positives,
        purge_non_official=args.purge_non_official,
        sleep_sec=args.sleep_sec,
    )
    print("✅ anytime official sync")
    for k, v in stats.items():
        print(f"   {k} = {v}")


if __name__ == "__main__":
    _main()
