"""📦 PI-ZZA Box — Streamlit BI dashboard.

Phase 2 最小実装:
  - SQLite (PIZZA_DB_PATH) を読み込み
  - Stores タブ: ブランドフィルタ + 地図プロット + 一覧
  - Mega Franchisees タブ: メガジー view (is_franchise=1 の集計)
  - Judgements タブ: 直近の FC 判定結果
  - CSV エクスポート (ブランドごと)

実行:
    streamlit run cmd/box-ui/app.py
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from pathlib import Path

import pandas as pd
import streamlit as st


# ─── Page config ───────────────────────────────────────────────────────

st.set_page_config(
    page_title="PI-ZZA 🍕 Box",
    page_icon="🍕",
    layout="wide",
    menu_items={
        "About": "PI-ZZA — Process Integration & Zonal Search Agent\n\n"
        "https://github.com/clearclown/pizza"
    },
)


# ─── Data layer ────────────────────────────────────────────────────────


def _db_path() -> str:
    # env → CLI working dir → repo root の順で候補を試す
    candidates = [
        os.getenv("PIZZA_DB_PATH"),
        "./var/pizza.sqlite",
        str(Path(__file__).resolve().parents[2] / "var" / "pizza.sqlite"),
    ]
    for c in candidates:
        if c and Path(c).exists():
            return c
    # 存在しない場合は env の値 (または default) をそのまま返す
    return os.getenv("PIZZA_DB_PATH") or "./var/pizza.sqlite"


@st.cache_data(ttl=30)
def load_stores(db: str, brand: str | None) -> pd.DataFrame:
    with closing(sqlite3.connect(db)) as conn:
        if brand:
            q = "SELECT * FROM stores WHERE brand = ? ORDER BY name"
            return pd.read_sql_query(q, conn, params=(brand,))
        return pd.read_sql_query("SELECT * FROM stores ORDER BY brand, name", conn)


@st.cache_data(ttl=30)
def load_brands(db: str) -> list[str]:
    with closing(sqlite3.connect(db)) as conn:
        cur = conn.execute("SELECT DISTINCT brand FROM stores WHERE brand != '' ORDER BY brand")
        return [r[0] for r in cur.fetchall()]


@st.cache_data(ttl=30)
def load_mega_franchisees(db: str, min_count: int) -> pd.DataFrame:
    with closing(sqlite3.connect(db)) as conn:
        q = """
        SELECT operator_name, store_count, avg_confidence
        FROM mega_franchisees
        WHERE store_count >= ?
        ORDER BY store_count DESC, avg_confidence DESC
        """
        return pd.read_sql_query(q, conn, params=(min_count,))


@st.cache_data(ttl=30)
def load_all_franchisees(db: str) -> pd.DataFrame:
    """mega/medium/small 全規模の加盟店運営会社 (本部は除外)。"""
    with closing(sqlite3.connect(db)) as conn:
        # all_franchisees view は migrations に含まれる
        try:
            return pd.read_sql_query(
                "SELECT operator_name, store_count, size_class, avg_confidence, "
                "brands, discovered_via_methods, corporate_number "
                "FROM all_franchisees ORDER BY store_count DESC",
                conn,
            )
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=30)
def load_franchisors(db: str) -> pd.DataFrame:
    """本部 (マスターフランチャイザー) を別枠で表示。"""
    with closing(sqlite3.connect(db)) as conn:
        try:
            return pd.read_sql_query(
                "SELECT operator_name, found_at_store_count, brands "
                "FROM franchisors ORDER BY found_at_store_count DESC",
                conn,
            )
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=30)
def load_judgements(db: str, brand: str | None, limit: int = 200) -> pd.DataFrame:
    with closing(sqlite3.connect(db)) as conn:
        if brand:
            q = """
            SELECT s.brand, s.name, s.address, j.is_franchise, j.operator_name,
                   j.store_count_estimate, j.confidence, j.llm_provider, j.judged_at
            FROM judgements j JOIN stores s ON s.place_id = j.place_id
            WHERE s.brand = ?
            ORDER BY j.judged_at DESC
            LIMIT ?
            """
            return pd.read_sql_query(q, conn, params=(brand, limit))
        q = """
        SELECT s.brand, s.name, s.address, j.is_franchise, j.operator_name,
               j.store_count_estimate, j.confidence, j.llm_provider, j.judged_at
        FROM judgements j JOIN stores s ON s.place_id = j.place_id
        ORDER BY j.judged_at DESC
        LIMIT ?
        """
        return pd.read_sql_query(q, conn, params=(limit,))


# ─── Sidebar ───────────────────────────────────────────────────────────

st.sidebar.title("🍕 PI-ZZA")
st.sidebar.caption("Process Integration & Zonal Search Agent")

db = _db_path()
st.sidebar.code(db, language=None)
if not Path(db).exists():
    st.sidebar.error(f"DB が見つかりません。\n`pizza bake` を先に実行してください。")
    st.title("PI-ZZA Box 🍕")
    st.warning(
        f"DB `{db}` が未作成です。\n\n"
        "まず `./bin/pizza bake --query ブランド名 --area エリア名` を実行して "
        "SQLite を生成してください。"
    )
    st.stop()

brands = ["(全て)", *load_brands(db)]
selected_brand_label = st.sidebar.selectbox("ブランド", brands, index=0)
selected_brand: str | None = None if selected_brand_label == "(全て)" else selected_brand_label

min_mega = st.sidebar.number_input(
    "メガジー閾値 (min store_count)", min_value=1, max_value=100, value=20, step=1
)

# ─── Main ──────────────────────────────────────────────────────────────

st.title("PI-ZZA Box 🍕")

tab_stores, tab_mega, tab_all, tab_hq, tab_judge = st.tabs(
    [
        "🏪 Stores",
        "⭐ Mega Franchisees",
        "🏢 All Franchisees",
        "🏛 Franchisors (本部)",
        "🛵 Judgements",
    ]
)

with tab_stores:
    df = load_stores(db, selected_brand)
    st.subheader(f"Stores — {len(df):,} 件" + (f" ({selected_brand})" if selected_brand else ""))
    if df.empty:
        st.info("該当する店舗がありません。")
    else:
        # 地図プロット (st.map は lat/lon 列を期待)
        map_df = df[["lat", "lng"]].rename(columns={"lng": "lon"}).dropna()
        if not map_df.empty:
            st.map(map_df, size=30)

        st.dataframe(
            df[["place_id", "brand", "name", "address", "official_url", "phone", "grid_cell_id"]],
            use_container_width=True,
            hide_index=True,
        )
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📤 CSV ダウンロード",
            data=csv_bytes,
            file_name=f"pi-zza-stores-{selected_brand or 'all'}.csv",
            mime="text/csv",
        )

with tab_mega:
    mega_df = load_mega_franchisees(db, int(min_mega))
    st.subheader(f"メガ フランチャイジー (≥ {min_mega} 店舗)")
    if mega_df.empty:
        st.info(
            "メガジー候補がありません。\n\n"
            "判定はまだ mock か、閾値を下げる必要があるかもしれません。"
        )
    else:
        st.dataframe(
            mega_df.style.format({"avg_confidence": "{:.2f}"}),
            use_container_width=True,
            hide_index=True,
        )
        st.bar_chart(mega_df.set_index("operator_name")["store_count"])

with tab_all:
    all_df = load_all_franchisees(db)
    st.subheader(f"All Franchisees — {len(all_df):,} 社 (本部を除外)")
    if all_df.empty:
        st.info(
            "加盟店データがありません。\n\n"
            "`./bin/pizza migrate --with-registry` で Ground Truth を seed するか、"
            "`./bin/pizza research --brand ... --max-stores N` で抽出してください。"
        )
    else:
        # size_class でフィルタ + 色分け表示
        size_filter = st.multiselect(
            "規模フィルタ", ["mega", "medium", "small"],
            default=["mega", "medium", "small"],
        )
        df = all_df[all_df["size_class"].isin(size_filter)]
        st.caption(
            f"Mega: {(all_df['size_class']=='mega').sum()}, "
            f"Medium: {(all_df['size_class']=='medium').sum()}, "
            f"Small: {(all_df['size_class']=='small').sum()}"
        )
        st.dataframe(
            df.style.format({"avg_confidence": "{:.2f}"}),
            use_container_width=True,
            hide_index=True,
        )
        if not df.empty:
            st.bar_chart(df.set_index("operator_name")["store_count"])

with tab_hq:
    hq_df = load_franchisors(db)
    st.subheader(f"Franchisors (本部) — {len(hq_df):,} 社")
    st.caption(
        "本部 (master franchisor) は mega 集計から除外されています。"
        "各本部が PI-ZZA のパイプラインで何店舗の『店舗ページに現れたか』を示します。"
    )
    if hq_df.empty:
        st.info("本部データがありません (research pipeline 未実行 or evidence から本部未検出)。")
    else:
        st.dataframe(hq_df, use_container_width=True, hide_index=True)

with tab_judge:
    j_df = load_judgements(db, selected_brand)
    st.subheader(f"判定履歴 — {len(j_df):,} 件 (直近 200 件まで)")
    if j_df.empty:
        st.info("判定データがまだありません。`pizza bake --with-judge` を実行してください。")
    else:
        # is_franchise を boolean に見せる
        j_df["is_franchise"] = j_df["is_franchise"].astype(bool)
        st.dataframe(
            j_df.style.format({"confidence": "{:.2f}"}),
            use_container_width=True,
            hide_index=True,
        )

st.sidebar.markdown("---")
st.sidebar.markdown(
    "**コマンド例**\n\n"
    "```bash\n"
    "./bin/pizza bake \\\n"
    '  --query "エニタイムフィットネス" \\\n'
    '  --area "新宿" \\\n'
    "  --with-judge\n"
    "```"
)
