-- PI-ZZA 🍕 M4 Box — SQLite schema
-- Applied idempotently at Open() time. See docs/architecture.md for the data model.

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

CREATE TABLE IF NOT EXISTS judgements (
  place_id              TEXT PRIMARY KEY,
  is_franchise          INTEGER NOT NULL,    -- 0=直営, 1=FC
  operator_name         TEXT,
  store_count_estimate  INTEGER,
  confidence            REAL,
  llm_provider          TEXT,
  llm_model             TEXT,
  judged_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (place_id) REFERENCES stores(place_id)
);

CREATE INDEX IF NOT EXISTS idx_judgements_operator ON judgements(operator_name);

CREATE VIEW IF NOT EXISTS mega_franchisees AS
  SELECT
    operator_name,
    COUNT(*)           AS store_count,
    AVG(confidence)    AS avg_confidence
  FROM judgements
  WHERE is_franchise = 1 AND operator_name IS NOT NULL AND operator_name != ''
  GROUP BY operator_name;
