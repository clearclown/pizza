"""国税庁 法人番号公表サイト CSV のローカル SQLite 取込 + 検索。

Web-API (houjin_bangou.py) は APP_ID 発行に 1 か月以上かかるため実用しにくい。
国税庁は 全件 CSV を公式に **API キー無料・登録不要** で公開しており、
毎月月末に全件更新 + 日次差分。本モジュールはそれを取り込み、Layer D
(operator 名 → 法人番号 検証) のキーレス代替を提供する。

使い方 (CLI):
    # ダウンロード URL は 公式トップ https://www.houjin-bangou.nta.go.jp/download/
    # から取得 (JS rendering のため直接 URL が露出しない)。
    # ユーザーがブラウザで zip を入手 → 本モジュールに渡す。

    python -m pizza_delivery.houjin_csv import --csv path/to/13_XXXXXX.zip

    # 検索 (キーレス):
    python -m pizza_delivery.houjin_csv search --name "株式会社モスストアカンパニー"

API (Python):
    from pizza_delivery.houjin_csv import HoujinCSVIndex
    idx = HoujinCSVIndex()  # default: var/houjin/registry.sqlite
    result = idx.search_by_name("株式会社モスフードサービス")
    for r in result:
        print(r.corporate_number, r.name, r.address)

スキーマ (CSV: 国税庁 最新仕様 https://www.houjin-bangou.nta.go.jp/download/):
    1: 一連番号  2: 法人番号 (13桁)  3: 処理区分
    4: 訂正区分  5: 更新年月日  6: 変更年月日
    7: 商号又は名称  8: 商号フリガナ
    9: 国内所在地 (都道府県)  10: 市区町村  11: 丁目番地等
    12: 国外所在地  13: 郵便番号  14: 都道府県コード  15: 市区町村コード
    16: 登記記録の閉鎖等年月日  17: 閉鎖事由
    18: 承継先法人番号  19: 変更年月日  20: 代表者情報 (空欄多)
    ...
"""

from __future__ import annotations

import csv
import io
import sqlite3
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator


# CSV カラム位置 (0-indexed、国税庁仕様)
COL_CORPORATE_NUMBER = 1
COL_PROCESS = 2
COL_UPDATE_DATE = 4
COL_NAME = 6
COL_PREFECTURE = 8
COL_CITY = 9
COL_STREET = 10

# process code の active 判定 (国税庁仕様 houjin_bangou.py と同じ)
ACTIVE_PROCESS_CODES = frozenset({"01", "11", "12", "13", "21", "22", "31"})


def _default_db_path() -> Path:
    """repo root / var/houjin/registry.sqlite を返す。"""
    here = Path(__file__).resolve()
    root = here.parents[3]
    return root / "var" / "houjin" / "registry.sqlite"


@dataclass
class HoujinCSVRecord:
    corporate_number: str
    process: str
    update_date: str
    name: str
    prefecture: str
    city: str
    street: str

    @property
    def address(self) -> str:
        return f"{self.prefecture}{self.city}{self.street}".strip()

    @property
    def active(self) -> bool:
        return self.process in ACTIVE_PROCESS_CODES


# ─── CSV/zip 読込 ─────────────────────────────────────────────────────


def iter_records(csv_path: str | Path, *, encoding: str = "utf-8") -> Iterator[HoujinCSVRecord]:
    """CSV or CSV 内包 zip から HoujinCSVRecord を yield。

    encoding は 'utf-8' (Unicode CSV) / 'cp932' (Shift-JIS CSV) を想定。
    """
    p = Path(csv_path)
    if p.suffix.lower() == ".zip":
        with zipfile.ZipFile(p) as zf:
            for name in zf.namelist():
                if not name.lower().endswith(".csv"):
                    continue
                with zf.open(name) as bf:
                    text = bf.read().decode(encoding, errors="replace")
                    for rec in _iter_csv_text(text):
                        yield rec
    else:
        text = p.read_text(encoding=encoding, errors="replace")
        for rec in _iter_csv_text(text):
            yield rec


def _iter_csv_text(text: str) -> Iterator[HoujinCSVRecord]:
    reader = csv.reader(io.StringIO(text))
    for row in reader:
        if len(row) <= COL_STREET:
            # Header or truncated row
            continue
        try:
            cn = row[COL_CORPORATE_NUMBER].strip()
        except IndexError:
            continue
        if not cn or not cn.isdigit() or len(cn) != 13:
            continue
        yield HoujinCSVRecord(
            corporate_number=cn,
            process=row[COL_PROCESS].strip(),
            update_date=row[COL_UPDATE_DATE].strip(),
            name=row[COL_NAME].strip(),
            prefecture=row[COL_PREFECTURE].strip(),
            city=row[COL_CITY].strip(),
            street=row[COL_STREET].strip(),
        )


# ─── SQLite Index ─────────────────────────────────────────────────────


_SCHEMA = """
CREATE TABLE IF NOT EXISTS houjin_registry (
  corporate_number  TEXT PRIMARY KEY,
  process           TEXT,
  update_date       TEXT,
  name              TEXT NOT NULL,
  prefecture        TEXT,
  city              TEXT,
  street            TEXT,
  imported_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_houjin_name ON houjin_registry(name);
CREATE INDEX IF NOT EXISTS idx_houjin_pref ON houjin_registry(prefecture);
"""


class HoujinCSVIndex:
    """SQLite に取込んだ法人番号 registry の検索 API。"""

    def __init__(self, db_path: str | Path | None = None) -> None:
        self.db_path = Path(db_path) if db_path else _default_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _init_schema(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(_SCHEMA)
            conn.commit()
        finally:
            conn.close()

    # ── Import ─────────────────────────────────────────────────────

    def ingest_csv(
        self,
        csv_path: str | Path,
        *,
        encoding: str = "utf-8",
        batch_size: int = 5000,
    ) -> int:
        """CSV を SQLite に upsert。戻り値 = 処理行数。"""
        total = 0
        batch: list[tuple] = []
        conn = sqlite3.connect(self.db_path)
        try:
            for rec in iter_records(csv_path, encoding=encoding):
                batch.append(
                    (
                        rec.corporate_number,
                        rec.process,
                        rec.update_date,
                        rec.name,
                        rec.prefecture,
                        rec.city,
                        rec.street,
                    )
                )
                if len(batch) >= batch_size:
                    self._upsert_batch(conn, batch)
                    total += len(batch)
                    batch.clear()
            if batch:
                self._upsert_batch(conn, batch)
                total += len(batch)
            conn.commit()
        finally:
            conn.close()
        return total

    @staticmethod
    def _upsert_batch(conn: sqlite3.Connection, batch: Iterable[tuple]) -> None:
        conn.executemany(
            """
            INSERT INTO houjin_registry
              (corporate_number, process, update_date, name, prefecture, city, street)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(corporate_number) DO UPDATE SET
              process=excluded.process,
              update_date=excluded.update_date,
              name=excluded.name,
              prefecture=excluded.prefecture,
              city=excluded.city,
              street=excluded.street,
              imported_at=CURRENT_TIMESTAMP
            """,
            batch,
        )

    # ── Search ─────────────────────────────────────────────────────

    def search_by_name(
        self,
        name: str,
        *,
        limit: int = 20,
        active_only: bool = True,
    ) -> list[HoujinCSVRecord]:
        """法人名で前方一致 + like 検索。"""
        if not name or not name.strip():
            return []
        conn = sqlite3.connect(self.db_path)
        try:
            q = "SELECT corporate_number, process, update_date, name, prefecture, city, street FROM houjin_registry WHERE name LIKE ?"
            args: list = [f"%{name.strip()}%"]
            if active_only:
                q += " AND process IN (" + ",".join("?" * len(ACTIVE_PROCESS_CODES)) + ")"
                args.extend(sorted(ACTIVE_PROCESS_CODES))
            q += " LIMIT ?"
            args.append(limit)
            rows = conn.execute(q, args).fetchall()
        finally:
            conn.close()
        return [
            HoujinCSVRecord(
                corporate_number=r[0], process=r[1], update_date=r[2],
                name=r[3], prefecture=r[4], city=r[5], street=r[6],
            )
            for r in rows
        ]

    def count(self) -> int:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute("SELECT COUNT(*) FROM houjin_registry").fetchone()[0]
        finally:
            conn.close()


# ─── verify_operator (互換 I/F) ──────────────────────────────────────


def verify_operator_via_csv(
    name: str,
    idx: HoujinCSVIndex | None = None,
) -> dict:
    """houjin_bangou.verify_operator と同じ dict 形式を返すが CSV index ベース。

    APP_ID 不要、オフラインで動作。
    """
    from pizza_delivery.houjin_bangou import _name_similarity

    idx = idx or HoujinCSVIndex()
    records = idx.search_by_name(name)
    if not records:
        return {
            "exists": False,
            "name_similarity": 0.0,
            "best_match_name": "",
            "best_match_number": "",
            "active": False,
            "source": "houjin_csv",
        }
    best_score = 0.0
    best: HoujinCSVRecord | None = None
    for r in records:
        s = _name_similarity(name, r.name)
        if s > best_score:
            best_score = s
            best = r
    return {
        "exists": True,
        "name_similarity": best_score,
        "best_match_name": best.name if best else "",
        "best_match_number": best.corporate_number if best else "",
        "active": bool(best and best.active),
        "source": "houjin_csv",
    }


# ─── CLI ────────────────────────────────────────────────────────────


def _main() -> None:
    import argparse
    import sys

    ap = argparse.ArgumentParser(description="国税庁 法人番号 CSV ローカル取込 + 検索")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_import = sub.add_parser("import", help="CSV/zip を SQLite に取込む")
    p_import.add_argument("--csv", required=True, help="CSV 又は zip ファイルパス")
    p_import.add_argument("--encoding", default="utf-8", help="CSV エンコーディング (utf-8 or cp932)")
    p_import.add_argument("--db", default="", help="SQLite DB path (default var/houjin/registry.sqlite)")

    p_search = sub.add_parser("search", help="法人名検索")
    p_search.add_argument("--name", required=True)
    p_search.add_argument("--limit", type=int, default=10)
    p_search.add_argument("--db", default="")

    p_count = sub.add_parser("count", help="登録件数")
    p_count.add_argument("--db", default="")

    args = ap.parse_args()
    idx = HoujinCSVIndex(args.db if args.db else None)

    if args.cmd == "import":
        n = idx.ingest_csv(args.csv, encoding=args.encoding)
        print(f"✅ ingested {n} records → {idx.db_path}")
    elif args.cmd == "search":
        for r in idx.search_by_name(args.name, limit=args.limit):
            print(f"  {r.corporate_number}  {r.name}  [{r.process}]  {r.address}")
    elif args.cmd == "count":
        print(idx.count())
    else:
        ap.print_help()
        sys.exit(1)


if __name__ == "__main__":
    _main()
