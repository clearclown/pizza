-- PI-ZZA Box layer schema
-- 設計思想: is_mega はカラムに持たず operator_totals VIEW で動的に計算する
-- （閾値変更時にマイグレーション不要）

-- operators: 法人単位のマスタ（verifier が houjin_bangou を確定後に書き込む）
CREATE TABLE IF NOT EXISTS operators (
    houjin_bangou   TEXT PRIMARY KEY,
    normalized_name TEXT NOT NULL,
    nta_verified    INTEGER NOT NULL DEFAULT 0,
    review_required INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT NOT NULL
);

-- operator_brands: ブランド別店舗数（複数ブランド対応、B方式）
CREATE TABLE IF NOT EXISTS operator_brands (
    houjin_bangou TEXT NOT NULL REFERENCES operators(houjin_bangou),
    brand_name    TEXT NOT NULL,
    store_count   INTEGER NOT NULL,
    count_source  TEXT NOT NULL CHECK(count_source IN ('edinet','chuusho_kaiji','gmaps_cluster','corporate_site')),
    count_unit    TEXT NOT NULL CHECK(count_unit IN ('franchisee','brand_total','unknown')),
    confidence    REAL NOT NULL DEFAULT 0.0,
    sources       TEXT NOT NULL DEFAULT '{}',  -- JSON (BrandSources)
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (houjin_bangou, brand_name)
);

-- stores: Google Maps から取得した生データ（verifier 通過後に houjin_bangou が埋まる）
CREATE TABLE IF NOT EXISTS stores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    houjin_bangou   TEXT REFERENCES operators(houjin_bangou),  -- nullable（未確認）
    brand_name      TEXT NOT NULL,
    place_id        TEXT UNIQUE,   -- Google Maps Place ID
    name            TEXT NOT NULL, -- 店舗名（生データ）
    address         TEXT,
    prefecture      TEXT,
    city            TEXT,
    phone           TEXT,
    operator_prefix TEXT,          -- ExtractOperatorPrefix の結果
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stores_houjin   ON stores(houjin_bangou);
CREATE INDEX IF NOT EXISTS idx_stores_brand    ON stores(brand_name);
CREATE INDEX IF NOT EXISTS idx_stores_place_id ON stores(place_id);

-- operator_totals VIEW: TotalStoreCount と is_mega を動的に計算
-- 閾値 20 を変えたい場合はこの VIEW を差し替えるだけ（operators テーブル変更不要）
CREATE VIEW IF NOT EXISTS operator_totals AS
SELECT
    o.houjin_bangou,
    o.normalized_name,
    SUM(ob.store_count)               AS total_store_count,
    COUNT(DISTINCT ob.brand_name)     AS brand_count,
    CASE WHEN SUM(ob.store_count) >= 20 THEN 1 ELSE 0 END AS is_mega
FROM operators o
JOIN operator_brands ob USING (houjin_bangou)
GROUP BY o.houjin_bangou, o.normalized_name;

-- nta_cache: 国税庁SQLite検索結果のキャッシュ（TTL 30日）
-- IsCentral 判定は L2(internal/kitchen) の責務のため verifier には含まない
CREATE TABLE IF NOT EXISTS nta_cache (
    corporate_name TEXT PRIMARY KEY,
    result_json    TEXT NOT NULL,   -- VerifyResult の JSON シリアライズ
    cached_at      TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at     TEXT NOT NULL    -- cached_at + 30日
);

-- expires_at インデックス: TTL切れレコードのクリーンアップクエリを高速化
CREATE INDEX IF NOT EXISTS idx_nta_cache_expires ON nta_cache(expires_at);

-- クリーンアップ用クエリ（起動時またはバッチで実行）:
-- DELETE FROM nta_cache WHERE expires_at < datetime('now');

-- =============================================================================
-- Phase 2 実装者へ
-- =============================================================================
--
-- stores → operator_brands 集約クエリ（verifier 通過後に実行）:
-- 詳細は internal/box/box.go の upsertOperatorBrands 定数を参照。
--
-- Aggregate() 実装時の集約クエリ（internal/verifier/aggregate.go Phase 2）:
--   SELECT houjin_bangou, SUM(store_count) as total
--   FROM operator_brands
--   WHERE houjin_bangou = ?
--   GROUP BY houjin_bangou;
--
-- is_mega の判定は operator_totals VIEW が担当（閾値変更時は VIEW のみ修正）:
--   SELECT * FROM operator_totals WHERE is_mega = 1;
-- =============================================================================
