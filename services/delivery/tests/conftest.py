"""pytest conftest — gen/python を sys.path に通す。"""

from __future__ import annotations

import sys
from pathlib import Path

# Repo root を基準に gen/python を追加
_ROOT = Path(__file__).resolve().parents[3]
_GEN_PY = _ROOT / "gen" / "python"
if _GEN_PY.exists() and str(_GEN_PY) not in sys.path:
    sys.path.insert(0, str(_GEN_PY))
