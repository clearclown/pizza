# Architecture — 詳細設計

## エンドツーエンドシーケンス

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant CLI as pizza CLI
    participant Oven as 🔥 Oven (Go)
    participant Dough as 🫓 Dough (Go gRPC)
    participant Maps as Google Maps API
    participant Kitchen as 🧀 Firecrawl (TS/REST)
    participant Courier as 🛵 Courier (Py gRPC)
    participant LLM as LLM Provider
    participant Box as 📦 SQLite

    User->>CLI: pizza bake --query Q --area A
    CLI->>Oven: Run(query=Q, area=A)
    Oven->>Dough: SearchStoresInGrid(polygon)
    loop Grid cells (parallel, bounded)
        Dough->>Maps: PlacesNearby / TextSearch
        Maps-->>Dough: Store[]
    end
    Dough-->>Oven: stream Store
    par Markdownize per store URL
        Oven->>Kitchen: POST /v1/scrape {url}
        Kitchen-->>Oven: { markdown, ... }
    end
    loop For each store with official URL
        Oven->>Courier: JudgeFranchiseType(StoreContext)
        Courier->>LLM: provider.chat(prompt)
        LLM-->>Courier: JSON { is_franchise, operator, ... }
        Courier-->>Oven: JudgeResult
    end
    Oven->>Box: UpsertStore / UpsertJudgement
    Box-->>Oven: ok
    Oven-->>CLI: summary + csv path
    CLI-->>User: 🍕 Done
```

## gRPC 契約（v1 スナップショット）

### `pizza.v1.SeedService`

```protobuf
rpc SearchStoresInGrid(GridQuery) returns (stream Store);
```

- `GridQuery`: `{ polygon: Polygon, cell_km: double, brand: string }`
- `Store`: `{ place_id, name, address, lat, lng, official_url, phone, ... }`

### `pizza.v1.KitchenService` (将来の拡張枠)

```protobuf
rpc ConvertToMarkdown(CrawlRequest) returns (MarkdownDoc);
```

- 初期は Go から直接 Firecrawl REST を叩く。gRPC 化は Kitchen が独自ロジックを持つ場合のみ。

### `pizza.v1.DeliveryService`

```protobuf
rpc JudgeFranchiseType(StoreContext) returns (JudgeResult);
rpc BatchJudge(stream StoreContext) returns (stream JudgeResult);
```

- `StoreContext`: `{ store: Store, markdown: string, candidate_urls: repeated string }`
- `JudgeResult`: `{ is_franchise: bool, operator_name: string, store_count_estimate: int32, confidence: double, evidence: repeated Evidence }`

### `pizza.v1.BIService`

```protobuf
rpc QueryMegaFranchisees(Filter) returns (stream Megaji);
rpc ExportCSV(Filter) returns (CSVBlob);
```

## SQLite スキーマ（ドラフト）

```sql
CREATE TABLE stores (
  place_id           TEXT PRIMARY KEY,
  brand              TEXT NOT NULL,
  name               TEXT NOT NULL,
  address            TEXT,
  lat                REAL NOT NULL,
  lng                REAL NOT NULL,
  official_url       TEXT,
  phone              TEXT,
  extracted_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  grid_cell_id       TEXT
);
CREATE INDEX idx_stores_brand ON stores(brand);
CREATE INDEX idx_stores_geo ON stores(lat, lng);

CREATE TABLE markdown_docs (
  url                TEXT PRIMARY KEY,
  place_id           TEXT REFERENCES stores(place_id),
  markdown           TEXT NOT NULL,
  fetched_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE judgements (
  place_id              TEXT PRIMARY KEY REFERENCES stores(place_id),
  is_franchise          INTEGER NOT NULL,       -- 0=直営, 1=FC
  operator_name         TEXT,
  store_count_estimate  INTEGER,
  confidence            REAL,
  llm_provider          TEXT,                   -- anthropic | openai | gemini
  llm_model             TEXT,
  judged_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_judgements_operator ON judgements(operator_name);

CREATE VIEW mega_franchisees AS
  SELECT
    operator_name,
    COUNT(*) AS store_count,
    AVG(confidence) AS avg_confidence
  FROM judgements
  WHERE is_franchise = 1 AND operator_name IS NOT NULL
  GROUP BY operator_name
  HAVING COUNT(*) >= 20;
```

## グリッド生成アルゴリズム

1. 入力ポリゴンのバウンディングボックスを取得
2. `cell_km` （デフォルト 1.0 km）で緯度経度メッシュ化
3. 各セルがポリゴン内 or 境界上にある場合のみ採用
4. セル中心を `location`、`radius = cell_km * √2 / 2 * 1000` m として Google Maps `nearbySearch` を実行
5. 結果の `place_id` を dedupe（同一店舗が隣接セルで重複）

テスト: `internal/grid/grid_test.go` — 100% カバレッジを保証。

## メガジー判定ロジック

```
operator_name  ← Delivery の判定結果 (JSON.operator)
store_count    ← SELECT COUNT(*) FROM judgements WHERE operator_name = ?
is_mega        ← store_count >= MEGA_FRANCHISEE_THRESHOLD  (default 20, env override)
score          ← store_count × avg_confidence × brand_heatmap_weight
```

`internal/scoring/` に実装。閾値は `MEGA_FRANCHISEE_THRESHOLD` 環境変数で調整可能。

## 並列制御

- `MAX_CONCURRENCY` (default 8) で Maps / Firecrawl / LLM 呼び出しの並列数を制御
- 指数バックオフは `internal/oven/retry.go`（Phase 1 で実装）
- gRPC keepalive: `time=10s, timeout=3s`
