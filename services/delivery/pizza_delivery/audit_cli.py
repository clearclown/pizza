"""audit CLI — `python -m pizza_delivery.audit_cli`。

Go 側 `pizza audit` から spawn される想定。registry load → PlacesClient 生成 →
BrandAuditor 実行 → CSV + gap CSV を出力。
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from pizza_delivery.audit import run_audit
from pizza_delivery.franchisee_registry import load_registry
from pizza_delivery.places_client import PlacesClient


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pizza-audit",
        description="特定ブランドの Top-down × Bottom-up 突合監査",
    )
    p.add_argument("--db", required=True, help="SQLite DB path (bake 実行済)")
    p.add_argument("--brand", required=True, help="対象ブランド (registry に登録済み)")
    p.add_argument(
        "--areas",
        default="",
        help="カンマ区切り area 指定 (例: '東京都,大阪府')。空文字で全国相当",
    )
    p.add_argument(
        "--out",
        required=True,
        help="メイン CSV 出力パス (-unknown-stores/-missing-operators も同じディレクトリに)",
    )
    p.add_argument("--addr-threshold", type=float, default=0.7)
    p.add_argument("--radius-m", type=float, default=150.0)
    p.add_argument("--max-per-operator", type=int, default=60)
    return p


async def _run(args: argparse.Namespace) -> int:
    db = Path(args.db)
    if not db.exists():
        print(f"[error] DB not found: {db}", file=sys.stderr)
        return 2

    areas = [a.strip() for a in args.areas.split(",") if a.strip()] or [""]

    registry = load_registry()
    places = PlacesClient()

    report = await run_audit(
        registry=registry,
        places_client=places,
        db_path=str(db),
        brand=args.brand,
        areas=areas,
        out_csv=args.out,
    )

    # stderr にヒューマンリーダブルサマリ
    print("", file=sys.stderr)
    print("━" * 78, file=sys.stderr)
    print(
        f"✅ audit done in {report.elapsed_sec:.1f}s  "
        f"brand={report.brand}  areas={report.areas}",
        file=sys.stderr,
    )
    print(
        f"   bottom_up_total={report.bottom_up_total}  "
        f"franchisees={len(report.franchisees)}  "
        f"unknown_stores={len(report.unknown_stores)}  "
        f"missing={len(report.missing_operators)}",
        file=sys.stderr,
    )
    print("━" * 78, file=sys.stderr)

    # 表形式
    if report.franchisees:
        maxlen = max(len(c.operator_name) for c in report.franchisees)
        print(
            f"\n{'企業名':{maxlen}}  Registered  Found  Matched  Coverage%",
            file=sys.stderr,
        )
        print("─" * (maxlen + 40), file=sys.stderr)
        for c in sorted(
            report.franchisees, key=lambda x: x.coverage_pct, reverse=True
        ):
            print(
                f"{c.operator_name:{maxlen}}  "
                f"{c.registered_count:10d}  "
                f"{c.found_count:5d}  "
                f"{c.bottom_up_matched_count:7d}  "
                f"{c.coverage_pct:8.2f}",
                file=sys.stderr,
            )
    if report.missing_operators:
        print("\nMissing operators (Places で 0 件):", file=sys.stderr)
        for op in report.missing_operators:
            print(f"  - {op}", file=sys.stderr)
    print(f"\nCSV: {args.out}", file=sys.stderr)
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
