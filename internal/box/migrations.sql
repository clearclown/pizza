-- PI-ZZA 🍕 M4 Box — SQLite schema (v2: Phase 4 事業会社定義対応)
-- Applied idempotently at Open() time. See docs/operator-definition.md for 定義。

CREATE TABLE IF NOT EXISTS stores (
  place_id      TEXT PRIMARY KEY,
  brand         TEXT NOT NULL,
  name          TEXT NOT NULL,
  address       TEXT,
  lat           REAL NOT NULL,
  lng           REAL NOT NULL,
  official_url  TEXT,
  phone         TEXT,
  grid_cell_id  TEXT,
  extracted_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stores_brand ON stores(brand);
CREATE INDEX IF NOT EXISTS idx_stores_geo ON stores(lat, lng);

CREATE TABLE IF NOT EXISTS markdown_docs (
  url         TEXT PRIMARY KEY,
  place_id    TEXT,
  title       TEXT,
  markdown    TEXT NOT NULL,
  fetched_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (place_id) REFERENCES stores(place_id)
);

CREATE INDEX IF NOT EXISTS idx_markdown_place ON markdown_docs(place_id);

-- judgements: Phase 4 で operation_type / franchisor_name / franchisee_name を追加
-- 既存 DB への migration は Go 側の ensureColumn() で ALTER TABLE する。
CREATE TABLE IF NOT EXISTS judgements (
  place_id              TEXT PRIMARY KEY,
  is_franchise          INTEGER NOT NULL,        -- 後方互換: 0=直営, 1=FC
  operator_name         TEXT,                    -- 後方互換: franchisee優先
  store_count_estimate  INTEGER,
  confidence            REAL,
  llm_provider          TEXT,
  llm_model             TEXT,
  -- Phase 4 拡張:
  operation_type        TEXT DEFAULT 'unknown',  -- direct | franchisee | mixed | unknown
  franchisor_name       TEXT,                    -- 本部会社
  franchisee_name       TEXT,                    -- 加盟店運営会社 (メガジー集計キー)
  judge_mode            TEXT,                    -- llm-only | browser | hybrid
  judged_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (place_id) REFERENCES stores(place_id)
);

CREATE INDEX IF NOT EXISTS idx_judgements_operator ON judgements(operator_name);
CREATE INDEX IF NOT EXISTS idx_judgements_franchisee ON judgements(franchisee_name);
CREATE INDEX IF NOT EXISTS idx_judgements_optype ON judgements(operation_type);

-- ────────────────────────────────────────────────────────────────────
-- Phase 5: Research Pipeline (店舗単位 + 芋づる式)
-- ────────────────────────────────────────────────────────────────────

-- operator_stores: 確定した (operator, place_id) のマップ
-- - 1 operator が複数 store を運営する関係を表現
-- - メガジー判定の正しいソース (judgements より上位)
CREATE TABLE IF NOT EXISTS operator_stores (
  operator_name        TEXT NOT NULL,
  place_id             TEXT NOT NULL,
  brand                TEXT,
  operator_type        TEXT,                     -- direct | franchisee | unknown
  confidence           REAL DEFAULT 0.0,
  discovered_via       TEXT DEFAULT 'per_store', -- per_store | chain_discovery | manual | cross_llm_consensus
  -- Phase 5.1: Layer D (法人番号 API) による外部 ground-truth
  verification_score   REAL DEFAULT 0.0,         -- 0.0 未検証 / >0 法人名類似度 / -1 非実在
  corporate_number     TEXT,                     -- 13 桁 法人番号 (国税庁)
  verification_source  TEXT,                     -- houjin_bangou_nta | manual | none
  confirmed_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (operator_name, place_id),
  FOREIGN KEY (place_id) REFERENCES stores(place_id)
);

CREATE INDEX IF NOT EXISTS idx_operator_stores_name  ON operator_stores(operator_name);
CREATE INDEX IF NOT EXISTS idx_operator_stores_brand ON operator_stores(brand);
CREATE INDEX IF NOT EXISTS idx_operator_stores_via   ON operator_stores(discovered_via);

-- store_evidence: 個別店舗について集めた raw 証拠 (URL + snippet)
-- - 判定の再現性・人間レビューのため必須
CREATE TABLE IF NOT EXISTS store_evidence (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  place_id      TEXT NOT NULL,
  evidence_url  TEXT NOT NULL,
  snippet       TEXT NOT NULL,
  reason        TEXT,                 -- operator_keyword | direct_keyword | metadata
  keyword       TEXT,
  collected_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (place_id) REFERENCES stores(place_id)
);

CREATE INDEX IF NOT EXISTS idx_store_evidence_place ON store_evidence(place_id);

-- mega_franchisees view (Phase 5 + 7): operator_stores ベースで集計
-- - 旧: judgements + franchisee_name で集計 (推論混入の可能性)
-- - 新: operator_stores の確定データのみで集計 (evidence backed)
-- - Phase 7: operator_type='franchisor' (本部) を除外。
--   PI-ZZA の目的は「加盟店運営会社の特定」であり、本部 (Fast Fitness Japan,
--   日本マクドナルド 等) は mega 集計の対象外。franchisors view を別途提供。
DROP VIEW IF EXISTS mega_franchisees;
CREATE VIEW mega_franchisees AS
  SELECT
    operator_name,
    COUNT(DISTINCT place_id)                 AS store_count,
    AVG(confidence)                          AS avg_confidence,
    GROUP_CONCAT(DISTINCT brand)             AS brands,
    GROUP_CONCAT(DISTINCT discovered_via)    AS discovered_via_methods,
    MIN(operator_type)                       AS operator_type
  FROM operator_stores
  WHERE operator_name IS NOT NULL AND operator_name != ''
    AND COALESCE(operator_type, '') != 'franchisor'
  GROUP BY operator_name;

-- franchisors view: 本部 (除外された operator) を別途集計できるように。
-- BI 上「ブランド X の本部会社と、その本部が発見された店舗数」を見るのに使う。
DROP VIEW IF EXISTS franchisors;
CREATE VIEW franchisors AS
  SELECT
    operator_name,
    COUNT(DISTINCT place_id)                 AS found_at_store_count,
    GROUP_CONCAT(DISTINCT brand)             AS brands
  FROM operator_stores
  WHERE operator_type = 'franchisor'
  GROUP BY operator_name;

-- all_franchisees view: メガ閾値に関係なく**全**加盟店運営会社を listing。
-- PI-ZZA は mega (≥20) だけでなく small/medium franchisee の把握も価値がある
-- (例: 数店舗のローカル運営会社、将来メガ化する可能性)。
DROP VIEW IF EXISTS all_franchisees;
CREATE VIEW all_franchisees AS
  SELECT
    operator_name,
    COUNT(DISTINCT place_id)                 AS store_count,
    AVG(confidence)                          AS avg_confidence,
    GROUP_CONCAT(DISTINCT brand)             AS brands,
    GROUP_CONCAT(DISTINCT discovered_via)    AS discovered_via_methods,
    MIN(operator_type)                       AS operator_type,
    MAX(verification_score)                  AS best_verification_score,
    MAX(corporate_number)                    AS corporate_number,
    CASE
      WHEN COUNT(DISTINCT place_id) >= 20 THEN 'mega'
      WHEN COUNT(DISTINCT place_id) >= 5  THEN 'medium'
      ELSE 'small'
    END AS size_class
  FROM operator_stores
  WHERE operator_name IS NOT NULL AND operator_name != ''
    AND COALESCE(operator_type, '') != 'franchisor'
  GROUP BY operator_name;

-- Phase 19: 事業会社主語 × brand 別内訳 view。メガジー横断で「この会社は
-- どの業態を何店舗運営しているか」を 1 行で見たいときに使う。
-- JSON で brand_counts を持つので BI ツール (pandas/duckdb/jq) から簡単に pivot 可。
DROP VIEW IF EXISTS mega_franchisees_multi_brand;
CREATE VIEW mega_franchisees_multi_brand AS
  SELECT
    operator_name,
    COUNT(DISTINCT place_id)              AS total_stores,
    COUNT(DISTINCT brand)                 AS brand_count,
    JSON_GROUP_OBJECT(brand, brand_n)     AS brand_counts_json,
    GROUP_CONCAT(DISTINCT discovered_via) AS discovered_via_methods,
    MAX(corporate_number)                 AS corporate_number,
    MIN(operator_type)                    AS operator_type
  FROM (
    SELECT
      operator_name,
      brand,
      COUNT(DISTINCT place_id) AS brand_n,
      MAX(discovered_via)      AS discovered_via,
      MAX(corporate_number)    AS corporate_number,
      MIN(operator_type)       AS operator_type,
      MIN(place_id)            AS place_id
    FROM operator_stores
    WHERE operator_name IS NOT NULL AND operator_name != ''
      AND COALESCE(operator_type, '') != 'franchisor'
    GROUP BY operator_name, brand
  )
  GROUP BY operator_name;

-- legacy 用 (Phase 4 compatible): judgements ベースの view も残す
DROP VIEW IF EXISTS mega_franchisees_legacy;
CREATE VIEW mega_franchisees_legacy AS
  SELECT
    COALESCE(NULLIF(franchisee_name, ''), operator_name) AS operator_name,
    COUNT(*)        AS store_count,
    AVG(confidence) AS avg_confidence
  FROM judgements
  WHERE (
    operation_type = 'franchisee'
    OR (operation_type IS NULL AND is_franchise = 1)
  )
  AND COALESCE(NULLIF(franchisee_name, ''), operator_name) IS NOT NULL
  AND COALESCE(NULLIF(franchisee_name, ''), operator_name) != ''
  GROUP BY COALESCE(NULLIF(franchisee_name, ''), operator_name);

-- review_queue: 人間レビューが必要な法人名寄せ結果を保持（自動解決不能ケース）
-- HumanReviewRequired=true 時にパイプラインが積む。人間が確認後に resolved=1 に更新。
CREATE TABLE IF NOT EXISTS review_queue (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_name   TEXT NOT NULL,          -- 入力された事業者名
    place_id        TEXT,                   -- 対象店舗 place_id（nullable）
    match_level     TEXT,                   -- MatchAmbiguous | MatchNotFound 等
    reason          TEXT,                   -- HumanReviewReason（score_too_close 等）
    candidates_json TEXT,                   -- Candidates[] の JSON シリアライズ
    resolved        INTEGER NOT NULL DEFAULT 0,  -- 0: pending / 1: resolved
    resolved_at     TEXT,                   -- 解決日時
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_review_queue_status ON review_queue(resolved);
CREATE INDEX IF NOT EXISTS idx_review_queue_place  ON review_queue(place_id);

-- retry_queue: L3 browser-use による自動リトライキュー（PR#10 で実装）
-- VerifyWithRetry() が not_found/api_error 時に積む。bridge.go が処理。
CREATE TABLE IF NOT EXISTS retry_queue (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_name   TEXT NOT NULL,          -- 元の検索名
    raw_name        TEXT NOT NULL,          -- RawName（L3取得前の名前）
    refined_name    TEXT,                   -- RefinedName（L3取得後、nullable）
    place_id        TEXT,                   -- 対象店舗 place_id（nullable）
    retry_count     INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending | done | failed
    fail_reason     TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

-- status='pending' を高速検索（バッチ処理時に使用）
CREATE INDEX IF NOT EXISTS idx_retry_queue_status ON retry_queue(status);
CREATE INDEX IF NOT EXISTS idx_retry_queue_place  ON retry_queue(place_id);
