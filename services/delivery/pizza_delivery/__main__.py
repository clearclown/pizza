"""Entry point: python -m pizza_delivery"""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_gen_on_path() -> None:
    """gen/python を sys.path に差し込む (リポジトリルートを辿って探す)。"""
    here = Path(__file__).resolve()
    # services/delivery/pizza_delivery/__main__.py → repo root is ../../../
    candidates = [
        here.parents[3] / "gen" / "python",           # repo root 相対
        Path.cwd() / "gen" / "python",                # 実行時 cwd 相対
    ]
    for c in candidates:
        if (c / "pizza").exists():
            p = str(c.resolve())
            if p not in sys.path:
                sys.path.insert(0, p)
            return


_ensure_gen_on_path()

from pizza_delivery.server import serve  # noqa: E402 — after sys.path adjustment


def main() -> int:
    try:
        serve()
    except KeyboardInterrupt:
        print("\n🛵 delivery-service: shutting down", file=sys.stderr)
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
