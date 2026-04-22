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
  operator_name    TEXT NOT NULL,
  place_id         TEXT NOT NULL,
  brand            TEXT,
  operator_type    TEXT,              -- direct | franchisee | unknown
  confidence       REAL DEFAULT 0.0,
  discovered_via   TEXT DEFAULT 'per_store',  -- per_store | chain_discovery | manual
  confirmed_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
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

-- mega_franchisees view (Phase 5): operator_stores ベースで集計
-- - 旧: judgements + franchisee_name で集計 (推論混入の可能性)
-- - 新: operator_stores の確定データのみで集計 (evidence backed)
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
