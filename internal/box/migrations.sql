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

-- mega_franchisees view (Phase 4): franchisee_name で集計
-- Phase 2 互換の operator_name ベースにフォールバックするため COALESCE を使う
DROP VIEW IF EXISTS mega_franchisees;
CREATE VIEW mega_franchisees AS
  SELECT
    COALESCE(NULLIF(franchisee_name, ''), operator_name) AS operator_name,
    COUNT(*)        AS store_count,
    AVG(confidence) AS avg_confidence
  FROM judgements
  WHERE (
    operation_type = 'franchisee'
    OR (operation_type IS NULL AND is_franchise = 1)  -- 後方互換
  )
  AND COALESCE(NULLIF(franchisee_name, ''), operator_name) IS NOT NULL
  AND COALESCE(NULLIF(franchisee_name, ''), operator_name) != ''
  GROUP BY COALESCE(NULLIF(franchisee_name, ''), operator_name);
