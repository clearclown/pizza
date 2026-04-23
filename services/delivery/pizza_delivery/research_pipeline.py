"""ResearchPipeline — 人間リサーチャー複製パイプラインの統合層 (Phase 5 Step E)。

M1 Seed が既に生成した店舗 list (SQLite) を起点に、

  Step 1: PerStoreExtractor で各店舗の operator を抽出
  Step 2: ChainDiscovery で operator ごとにグループ化 (表記揺れ吸収)
  Step 3: (optional) CrossVerifier で primary/alt URL 再抽出による二重確認
  Step 4: SQLite operator_stores + store_evidence に永続化
  Step 5: mega_franchisees (≥ N 店舗) リスト生成

を BFS 的に実行する。"芋づる式" の具体的な形は、
1 pass 目で operator を発見 → operator_stores に記録 → 2 pass 目以降は
同 operator の他店舗を追加探索 (これは Go 側の Places API で実施予定)。

この Python 層は **per-store 抽出 + 集約 + 永続化** が責務で、
Places API での他店舗発見は Go オーケストレータ側で行う形を想定する。
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

from pizza_delivery.chain_discovery import (
    ChainDiscovery,
    ChainDiscoveryReport,
    OperatorSummary,
    StoreInput,
)
from pizza_delivery.cross_verifier import (
    CrossVerifier,
    VerifyCandidate,
    VerifyResult,
    verify_many,
)
from pizza_delivery.per_store import PerStoreExtractor


# ─── Input / Output ────────────────────────────────────────────────────


@dataclass
class ResearchRequest:
    """pipeline 実行リクエスト。"""

    brand: str | None = None        # None で全ブランド
    area_label: str = ""            # ログ用 (SQLite の filter には使わない)
    db_path: str = ""               # SQLite DB path (required)
    max_stores: int = 0             # 0 で全件
    verify: bool = True             # CrossVerifier を走らせるか (デフォルト True)
    verify_houjin: bool = False     # Layer D: 国税庁法人番号 API で operator 実在確認
    max_concurrency: int = 4
    # Step 6.2: Places API 広域芋づる式 (depth=1 固定)
    expand_via_places: bool = False
    places_client: Any | None = None  # PlacesClient インターフェース (search_by_operator)
    expand_area_hint: str = ""
    max_expansion_per_operator: int = 20


@dataclass
class MegaFranchiseeCandidate:
    """集計結果の最終形。"""

    operator_name: str
    store_count: int
    operator_type: str
    avg_confidence: float
    verified_count: int  # CrossVerifier で confirmed されたもの
    unverified_count: int
    brands: list[str]
    place_ids: list[str]
    # Layer D: 法人番号 API 照合結果
    verification_score: float = 0.0   # >0 類似度 / 0 未検証 / -1 非実在
    corporate_number: str = ""         # 13 桁 法人番号 (ヒット時)
    verification_source: str = ""      # houjin_bangou_nta | none

    @property
    def is_mega(self) -> bool:
        return self.verified_count >= 20


@dataclass
class ResearchReport:
    brand: str
    total_stores: int
    stores_with_operator: int
    stores_unknown: int
    operators: list[MegaFranchiseeCandidate] = field(default_factory=list)
    elapsed_sec: float = 0.0


# ─── SQLite I/O helpers ────────────────────────────────────────────────


def _load_stores_from_sqlite(
    db_path: str,
    brand: str | None,
    max_stores: int,
) -> list[StoreInput]:
    """SQLite から StoreInput list を取得。"""
    conn = sqlite3.connect(db_path)
    try:
        if brand:
            query = (
                "SELECT place_id, brand, name, official_url "
                "FROM stores WHERE brand = ? AND official_url != '' "
                "ORDER BY name"
            )
            params: tuple = (brand,)
        else:
            query = (
                "SELECT place_id, brand, name, official_url "
                "FROM stores WHERE official_url != '' "
                "ORDER BY brand, name"
            )
            params = ()
        if max_stores > 0:
            query += " LIMIT ?"
            params = params + (max_stores,)
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()
    return [
        StoreInput(place_id=r[0], brand=r[1], name=r[2], official_url=r[3])
        for r in rows
    ]


def _has_verification_columns(conn: sqlite3.Connection) -> bool:
    """operator_stores テーブルに Phase 5.1 の verification 列があるかチェック。"""
    rows = conn.execute("PRAGMA table_info(operator_stores)").fetchall()
    return any(r[1] == "verification_score" for r in rows)


def _persist_results(
    db_path: str,
    report: ChainDiscoveryReport,
    verified: dict[str, bool] | None = None,
    houjin_verifications: dict[str, dict] | None = None,
) -> None:
    """ChainDiscovery 結果と verify 結果を operator_stores + store_evidence に保存。

    houjin_verifications は operator_name → {verification_score, corporate_number,
    verification_source} の dict。存在する場合は列に書き込む (列が DB にあれば)。
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        has_ver_cols = _has_verification_columns(conn)
        for op in report.operators:
            hv = (houjin_verifications or {}).get(op.operator_name, {})
            v_score = hv.get("verification_score", 0.0)
            v_corp = hv.get("corporate_number", "")
            v_src = hv.get("verification_source", "")
            for st in op.stores:
                # operator_stores upsert
                via = "chain_discovery"
                conf = st.confidence
                if verified is not None:
                    if verified.get(st.place_id) is True:
                        via = "chain_verified"
                        conf = min(1.0, conf + 0.1)
                    elif verified.get(st.place_id) is False:
                        via = "chain_unverified"
                if has_ver_cols:
                    cur.execute(
                        """
                        INSERT INTO operator_stores
                          (operator_name, place_id, brand, operator_type, confidence,
                           discovered_via, verification_score, corporate_number, verification_source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(operator_name, place_id) DO UPDATE SET
                          brand = excluded.brand,
                          operator_type = excluded.operator_type,
                          confidence = MAX(excluded.confidence, operator_stores.confidence),
                          discovered_via = excluded.discovered_via,
                          verification_score = excluded.verification_score,
                          corporate_number = excluded.corporate_number,
                          verification_source = excluded.verification_source,
                          confirmed_at = CURRENT_TIMESTAMP
                        """,
                        (op.operator_name, st.place_id, st.brand, op.operator_type, conf, via,
                         v_score, v_corp, v_src),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO operator_stores
                          (operator_name, place_id, brand, operator_type, confidence, discovered_via)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(operator_name, place_id) DO UPDATE SET
                          brand = excluded.brand,
                          operator_type = excluded.operator_type,
                          confidence = MAX(excluded.confidence, operator_stores.confidence),
                          discovered_via = excluded.discovered_via,
                          confirmed_at = CURRENT_TIMESTAMP
                        """,
                        (op.operator_name, st.place_id, st.brand, op.operator_type, conf, via),
                    )
                # store_evidence: 各 evidence を重複防止で insert
                for ev in st.evidences:
                    sig = ev.snippet[:200] if ev.snippet else ""
                    exists = cur.execute(
                        "SELECT 1 FROM store_evidence WHERE place_id=? AND evidence_url=? "
                        "AND SUBSTR(snippet,1,200)=? LIMIT 1",
                        (st.place_id, ev.source_url, sig),
                    ).fetchone()
                    if exists:
                        continue
                    cur.execute(
                        "INSERT INTO store_evidence "
                        "  (place_id, evidence_url, snippet, reason, keyword) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (st.place_id, ev.source_url, ev.snippet, ev.reason, ev.keyword),
                    )
        conn.commit()
    finally:
        conn.close()


# ─── Pipeline ──────────────────────────────────────────────────────────


@dataclass
class ResearchPipeline:
    chain: ChainDiscovery = field(default_factory=ChainDiscovery)
    verifier: CrossVerifier = field(default_factory=CrossVerifier)
    # Layer D: 国税庁法人番号 API による operator 実在確認 (optional)
    # None の場合は verify_houjin=True であっても Layer D はスキップ。
    houjin_client: "object | None" = None

    async def _expand_via_places(
        self,
        req: "ResearchRequest",
        chain_report: ChainDiscoveryReport,
        log: Callable[[str], None],
    ) -> int:
        """各 operator について Places API 広域検索 → stores テーブルに new 店舗 insert。

        depth=1 固定 (insert された store から再発見される operator では再検索しない)。
        戻り値: 新規に insert された stores 件数。
        """
        added = 0
        for op in chain_report.operators:
            log(f"[expand] Places.search_by_operator({op.operator_name!r})")
            places = await req.places_client.search_by_operator(
                op.operator_name,
                area_hint=req.expand_area_hint,
                max_result_count=req.max_expansion_per_operator,
            )
            if not places:
                continue
            # SQLite に insert (既存 place_id は skip)
            conn = sqlite3.connect(req.db_path)
            try:
                cur = conn.cursor()
                for p in places:
                    if not p.place_id:
                        continue
                    exists = cur.execute(
                        "SELECT 1 FROM stores WHERE place_id=? LIMIT 1",
                        (p.place_id,),
                    ).fetchone()
                    if exists:
                        continue
                    cur.execute(
                        "INSERT INTO stores "
                        "(place_id, brand, name, address, lat, lng, "
                        " official_url, phone) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            p.place_id,
                            req.brand or "",
                            p.name,
                            p.address,
                            p.lat,
                            p.lng,
                            p.website_uri,
                            p.phone,
                        ),
                    )
                    added += 1
                conn.commit()
            finally:
                conn.close()
        return added

    async def run(
        self,
        req: ResearchRequest,
        *,
        progress: Callable[[str], None] | None = None,
    ) -> ResearchReport:
        import time

        t0 = time.time()
        log = progress or (lambda s: None)

        if not req.db_path:
            raise ValueError("ResearchRequest.db_path is required")
        if not Path(req.db_path).exists():
            raise FileNotFoundError(f"DB not found: {req.db_path}")

        # 1. Seed: stores from SQLite
        stores = _load_stores_from_sqlite(req.db_path, req.brand, req.max_stores)
        log(f"[seed] loaded {len(stores)} stores from SQLite")
        if not stores:
            return ResearchReport(
                brand=req.brand or "(all)",
                total_stores=0,
                stores_with_operator=0,
                stores_unknown=0,
                elapsed_sec=time.time() - t0,
            )

        # 2. ChainDiscovery
        log(f"[chain] running PerStoreExtractor on {len(stores)} stores...")
        chain_report = await self.chain.discover(stores)
        log(
            f"[chain] found {len(chain_report.operators)} operator groups, "
            f"{chain_report.stores_with_operator} / {chain_report.total_stores_checked} with operator"
        )

        # 2.5. Places API 広域芋づる式 (Step 6.2, depth=1 固定)
        #   operator を見つけたら同 operator の他店舗を Places API で検索し、
        #   stores テーブルに追加 (既存 place_id は重複 insert しない)。
        if (
            req.expand_via_places
            and req.places_client is not None
            and chain_report.operators
        ):
            new_stores_added = await self._expand_via_places(req, chain_report, log)
            log(f"[expand] added {new_stores_added} new stores to SQLite")

        # 3. CrossVerifier (optional)
        verified: dict[str, bool] = {}
        if req.verify and chain_report.operators:
            log(f"[verify] running CrossVerifier on {chain_report.stores_with_operator} stores...")
            candidates: list[VerifyCandidate] = []
            for op in chain_report.operators:
                for st in op.stores:
                    # find official URL from stores list
                    st_input = next((s for s in stores if s.place_id == st.place_id), None)
                    if not st_input:
                        continue
                    candidates.append(
                        VerifyCandidate(
                            place_id=st.place_id,
                            brand=st.brand,
                            name=st.name,
                            expected_operator=op.operator_name,
                            primary_url=st_input.official_url,
                        )
                    )
            vresults = await verify_many(
                self.verifier, candidates, max_concurrency=req.max_concurrency
            )
            for v in vresults:
                verified[v.place_id] = v.confirmed
            confirmed = sum(1 for v in vresults if v.confirmed)
            log(f"[verify] {confirmed}/{len(vresults)} verified")

        # 3.5. Layer D: 法人番号 API による operator 実在確認 (optional)
        houjin_verifications: dict[str, dict] = {}
        if req.verify_houjin and self.houjin_client is not None and chain_report.operators:
            from pizza_delivery.houjin_bangou import verify_operator

            log(
                f"[houjin] verifying {len(chain_report.operators)} operators "
                "against 国税庁 法人番号 API..."
            )
            for op in chain_report.operators:
                search = await self.houjin_client.search_by_name(op.operator_name)
                v = verify_operator(op.operator_name, search)
                if v["exists"]:
                    houjin_verifications[op.operator_name] = {
                        "verification_score": float(v["name_similarity"]),
                        "corporate_number": v["best_match_number"],
                        "verification_source": "houjin_bangou_nta",
                    }
                else:
                    # 非実在 flag: score=-1
                    houjin_verifications[op.operator_name] = {
                        "verification_score": -1.0,
                        "corporate_number": "",
                        "verification_source": "houjin_bangou_nta",
                    }
            hits = sum(1 for v in houjin_verifications.values() if v["verification_score"] > 0)
            log(f"[houjin] {hits}/{len(houjin_verifications)} operators verified as existing")

        # 4. Persist to SQLite
        log("[persist] writing to operator_stores + store_evidence...")
        _persist_results(
            req.db_path,
            chain_report,
            verified if req.verify else None,
            houjin_verifications=houjin_verifications or None,
        )

        # 5. Build report
        mega_candidates: list[MegaFranchiseeCandidate] = []
        for op in chain_report.operators:
            place_ids = [s.place_id for s in op.stores]
            brands = sorted({s.brand for s in op.stores if s.brand})
            v_count = sum(1 for p in place_ids if verified.get(p) is True)
            u_count = sum(1 for p in place_ids if verified.get(p) is False)
            hv = houjin_verifications.get(op.operator_name, {})
            mega_candidates.append(
                MegaFranchiseeCandidate(
                    operator_name=op.operator_name,
                    store_count=op.store_count,
                    operator_type=op.operator_type,
                    avg_confidence=op.avg_confidence,
                    verified_count=v_count,
                    unverified_count=u_count,
                    brands=brands,
                    place_ids=place_ids,
                    verification_score=hv.get("verification_score", 0.0),
                    corporate_number=hv.get("corporate_number", ""),
                    verification_source=hv.get("verification_source", ""),
                )
            )

        return ResearchReport(
            brand=req.brand or "(all)",
            total_stores=chain_report.total_stores_checked,
            stores_with_operator=chain_report.stores_with_operator,
            stores_unknown=chain_report.stores_unknown,
            operators=mega_candidates,
            elapsed_sec=time.time() - t0,
        )
