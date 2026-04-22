"""📦 PI-ZZA Box — Streamlit BI dashboard.

Phase 0: プレースホルダ。Phase 4 で SQLite 読み込み → 地図可視化 → スコアリング。
"""

from __future__ import annotations

import os

try:
    import streamlit as st

    st.set_page_config(page_title="PI-ZZA 🍕", page_icon="🍕", layout="wide")
    st.title("🍕 PI-ZZA — Box UI")
    st.caption("Process Integration & Zonal Search Agent")

    db_path = os.getenv("PIZZA_DB_PATH", "./var/pizza.sqlite")
    st.info(f"Phase 0 scaffold. DB path: `{db_path}` — Phase 4 で本実装します。")
    st.markdown("- 🗺 地図可視化 (TODO Phase 4)")
    st.markdown("- ⭐ メガジー スコアリング (TODO Phase 4)")
    st.markdown("- 📤 CSV エクスポート (TODO Phase 4)")
except ImportError:
    print("streamlit not installed. Run `uv pip install streamlit`.")
