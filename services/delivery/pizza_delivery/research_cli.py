"""Research CLI — Phase 5 Step F。

`python -m pizza_delivery.research` (or `python -m pizza_delivery.research_cli`)
で SQLite に既に投入済の店舗に対し ResearchPipeline を実行する。

想定ワークフロー:

  # Step 1: M1 Seed を Go CLI で実行して SQLite に店舗を投入
  ./bin/pizza bake --query "エニタイムフィットネス" --area "新宿" --no-kitchen

  # Step 2: Research pipeline で per-store operator 抽出 + 集約 + 永続化
  python -m pizza_delivery.research \\
      --db ./var/pizza.sqlite \\
      --brand "エニタイムフィットネス" \\
      --max-stores 30

  # Step 3: SQLite の operator_stores / mega_franchisees を直接 SELECT
  sqlite3 ./var/pizza.sqlite \\
      "SELECT operator_name, store_count FROM mega_franchisees ORDER BY store_count DESC"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from pizza_delivery.chain_discovery import ChainDiscovery
from pizza_delivery.cross_verifier import CrossVerifier
from pizza_delivery.evidence import EvidenceCollector
from pizza_delivery.per_store import PerStoreExtractor
from pizza_delivery.research_pipeline import (
    ResearchPipeline,
    ResearchRequest,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pizza-research",
        description=(
            "PI-ZZA 🍕 Research Pipeline — 店舗単位の operator 抽出 + "
            "operator ごとのグルーピング + SQLite 永続化"
        ),
    )
    p.add_argument("--db", required=True, help="SQLite DB path (M1 Seed 済)")
    p.add_argument("--brand", default="", help="対象ブランド (空で全ブランド)")
    p.add_argument(
        "--max-stores", type=int, default=0, help="処理する店舗の上限 (0 で全件)"
    )
    p.add_argument(
        "--no-verify", action="store_true", help="CrossVerifier を skip (デバッグ用)"
    )
    p.add_argument("--concurrency", type=int, default=4, help="並行 fetch 数")
    p.add_argument(
        "--max-pages", type=int, default=3, help="1 店舗あたり訪問ページ上限"
    )
    p.add_argument(
        "--mega-threshold",
        type=int,
        default=20,
        help="mega franchisee 判定の店舗数閾値",
    )
    p.add_argument(
        "--json", action="store_true", help="結果を JSON で stdout 出力"
    )
    # Phase 5.1 / 6 拡張フラグ
    p.add_argument(
        "--verify-houjin",
        action="store_true",
        help="国税庁法人番号 API で operator 実在確認 (HOUJIN_BANGOU_APP_ID 必須)",
    )
    p.add_argument(
        "--expand-via-places",
        action="store_true",
        help="Places API で同 operator の他店舗を広域検索 (芋づる式)",
    )
    p.add_argument(
        "--expand-area", default="", help="--expand-via-places 時の area hint"
    )
    p.add_argument(
        "--max-expansion-per-operator",
        type=int,
        default=20,
        help="1 operator あたり広域検索で収集する最大店舗数",
    )
    return p


async def run(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"[error] DB not found: {db_path}", file=sys.stderr)
        return 2

    collector = EvidenceCollector(max_pages=args.max_pages)
    extractor = PerStoreExtractor(collector=collector)
    chain = ChainDiscovery(extractor=extractor, max_concurrency=args.concurrency)
    verifier = CrossVerifier(extractor=extractor)

    # Layer D: 法人番号検証 (HOUJIN_BANGOU_APP_ID 設定時のみ Web-API 直叩き)。
    # APP_ID が無い環境では houjin_client=None を渡し、pipeline 内部で
    # VerifyPipeline (ローカル CSV / gBizINFO) fallback が自動起動する。
    houjin_client = None
    if args.verify_houjin and os.getenv("HOUJIN_BANGOU_APP_ID"):
        from pizza_delivery.houjin_bangou import HoujinBangouClient

        houjin_client = HoujinBangouClient()

    # Step 6.2: Places API expansion client (optional)
    places_client = None
    if args.expand_via_places:
        from pizza_delivery.places_client import PlacesClient

        places_client = PlacesClient()

    pipeline = ResearchPipeline(
        chain=chain, verifier=verifier, houjin_client=houjin_client
    )

    def log(msg: str) -> None:
        print(f"  {msg}", file=sys.stderr)

    req = ResearchRequest(
        brand=args.brand or None,
        db_path=str(db_path),
        max_stores=args.max_stores,
        verify=not args.no_verify,
        verify_houjin=args.verify_houjin,
        max_concurrency=args.concurrency,
    )
    # ResearchRequest 経由で places_client を渡す (未実装フィールドは setattr で注入)
    if places_client is not None:
        setattr(req, "places_client", places_client)
        setattr(req, "expand_via_places", True)
        setattr(req, "expand_area_hint", args.expand_area)
        setattr(req, "max_expansion_per_operator", args.max_expansion_per_operator)

    print("🍕 PI-ZZA Research pipeline", file=sys.stderr)
    print(
        f"   brand={req.brand!r}  db={req.db_path}  "
        f"max_stores={req.max_stores}  verify={req.verify}",
        file=sys.stderr,
    )

    report = await pipeline.run(req, progress=log)

    if args.json:
        # JSON output — evidence URL だけ (snippet は省略してコンパクトに)
        json.dump(
            {
                "brand": report.brand,
                "total_stores": report.total_stores,
                "stores_with_operator": report.stores_with_operator,
                "stores_unknown": report.stores_unknown,
                "elapsed_sec": round(report.elapsed_sec, 2),
                "operators": [
                    {
                        "operator_name": op.operator_name,
                        "store_count": op.store_count,
                        "operator_type": op.operator_type,
                        "avg_confidence": round(op.avg_confidence, 3),
                        "verified_count": op.verified_count,
                        "unverified_count": op.unverified_count,
                        "brands": op.brands,
                        "place_ids": op.place_ids,
                        "is_mega": op.store_count >= args.mega_threshold,
                    }
                    for op in report.operators
                ],
            },
            sys.stdout,
            ensure_ascii=False,
            indent=2,
        )
        sys.stdout.write("\n")
    else:
        # Human-readable
        print("", file=sys.stderr)
        print("━" * 78, file=sys.stderr)
        print(
            f"✅ Done in {report.elapsed_sec:.1f}s  "
            f"stores={report.total_stores}  "
            f"with_operator={report.stores_with_operator}  "
            f"unknown={report.stores_unknown}",
            file=sys.stderr,
        )
        print("━" * 78, file=sys.stderr)
        if not report.operators:
            print("(no operators detected)", file=sys.stderr)
            return 0
        # 降順 store_count
        max_name_len = max(len(op.operator_name) for op in report.operators)
        print(
            f"\n{'Operator':{max_name_len}}  Stores  Verified  Type        Conf  Mega",
            file=sys.stderr,
        )
        print("─" * (max_name_len + 45), file=sys.stderr)
        for op in report.operators:
            mega_mark = "⭐" if op.store_count >= args.mega_threshold else " "
            print(
                f"{op.operator_name:{max_name_len}}  "
                f"{op.store_count:6d}  "
                f"{op.verified_count:8d}  "
                f"{op.operator_type:10s}  "
                f"{op.avg_confidence:.2f}  {mega_mark}",
                file=sys.stderr,
            )
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
