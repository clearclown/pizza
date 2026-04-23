# Phase 13-16 統合: 汎用 FC 網羅ツールの完成 (2026-04-23)

## 🎯 原点の再確認

PI-ZZA は **特定ブランドに依存しない、あらゆるフランチャイズ業界** 向けの BI ツール。
エニタイムは検証データが豊富な 1 例に過ぎない。以下は **どのブランドでも** 動作する。

## 実装した 4 つの機能 (Phase 13-16)

### Phase 13: Adaptive Quad-tree Split (Google Places 20 件制約の突破)

**問題**: Places Text Search は 1 req 最大 20 件。密度高い地域で取りこぼし。

**解決** (`internal/grid/quadtree.go` + `internal/dough/searcher.go`):
- bbox → QuadCell root から iterative DFS
- cell で 20 件返ったら 4 分割 (quad-tree split)
- `MaxDepth` / `MinCellMeters` / `SaturationThreshold` で制御
- Places API radius 上限 (50km) 超の cell は必ず先に subdivide

### Phase 14: OperatorSpider (Top-down の核)

**問題**: operator 名 → Google Places に逆引きすると API call が 20+ 必要 & 精度低。

**解決** (`pizza_delivery/operator_spider.py`):
- operator の公式 URL (`registry.source_urls[0]`) を fetch
- 「店舗一覧」link を検出して追従 (最大 3 page)
- HTML から日本住所 regex で店舗候補 (name + address) を一括抽出
- 1 operator あたり fetch 1-3 回で数十店舗の place_id 候補取得

### Phase 15: Multi-brand Discovery (芋づる式拡張)

**問題**: 1 operator が複数 FC を運営している (例: KOHATA HD = エニタイム + CLUB PILATES + CYCLEBAR)。既存 Registry では 1 ブランドしか紐付けない。

**解決** (`operator_spider.py:extract_brand_candidates_from_html`):
- 既知 FC ブランド辞書 (35+ ブランド) で anchor text マッチ
- operator 公式 navigation menu から他ブランド link を検出
- `BrandCandidate` を返し、registry に新ブランド追加候補として提示
- **1 operator 公式 scrape で複数ブランドの店舗リスト同時取得可能**

### Phase 16: Territory Knowledge (近接=仮止め)

**問題**: 業種で territory 半径が違う。コンビニ (100m 近接 OK) と ジム (500m+) を一律扱えない。

**解決** (`internal/dough/knowledge/territory_radius.yaml` + `pizza_delivery/territory.py`):
- Web search agent で 30+ ブランド × 11 業種カテゴリを調査
- JFA 契約書、US FDD、RIZAP IR 等の **公開数値に基づく** 半径 DB
- `territory_radius(brand)` で `(dominant_min_m, dominant_typical_m, territory_max_m)` を取得
- `check_pair` で 2 店舗ペアを `DUPLICATE_SUSPECT / DOMINANT_CLUSTER / INDEPENDENT` に分類
- `compute_cover_map` で既知店舗 × territory 半径から **未探索領域を計算** → bottom-up scan を省略可能

### Territory 知識 DB 抜粋

| 業種 | strategy | dominant_min_m | territory_max_m | 代表ブランド |
|---|---|---|---|---|
| convenience_dominant | dominant | 80 | 1,000 | セブン/ファミマ/ローソン |
| convenience_territory | territory | 150 | 1,500 | セイコーマート |
| gym_24h_territory | territory | 800 | 5,000 | エニタイム |
| gym_24h_dominant | dominant | 100 | 2,000 | chocoZAP |
| gym_large | dispersed | 2,000 | 8,000 | ゴールドジム/ルネサンス |
| fastfood_urban | dominant | 100 | 2,000 | マクドナルド/スタバ/ドトール |
| fastfood_suburban | dispersed | 1,000 | 5,000 | コメダ珈琲 |
| family_restaurant | dispersed | 1,500 | 6,000 | ガスト/サイゼリヤ |
| gyudon_bento | mixed | 200 | 3,000 | すき家/吉野家 |
| drugstore | dominant | 300 | 3,000 | マツキヨ/ウエルシア |
| retail_reuse | dispersed | 1,000 | 8,000 | ブックオフ/TSUTAYA/ゲオ |

### 確定された契約書ベースの数値
- **セイコーマート 150m**: JFA 公開契約書で「本部・第三者共に半径 150m 内は出店しない」
- **Anytime Fitness 800m-4,800m**: US FDD 2024、人口 3 万人以内の protected territory
- **chocoZAP 会員 60% が 1km 圏内**: RIZAP 中期経営計画 + 東洋経済

## 統合アーキテクチャ (最終形)

```
┌─────────────────────────────────────────────────────────────────┐
│  Top-down (Registry + OperatorSpider)                            │
│  ──────────                                                      │
│  1. registry.yaml の operator ごとに:                             │
│     OperatorSpider.discover(operator_name, official_url)         │
│     → 公式サイト店舗一覧 scrape → (name, address) 候補            │
│     → Places 住所逆引きで place_id 解決                            │
│  2. Multi-brand: 同 operator の他ブランド link も発見             │
│     → 新ブランド registry 追加 candidate                           │
│  API cost: 1 operator あたり Places call 数個 (住所逆引きのみ)     │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│  Cover Map (Territory で既探索領域)                               │
│  ──────────                                                       │
│  既知 store 群 + territory_typical_m で円のユニオン                │
│  → この領域は「既にほぼ網羅済」と仮止め                              │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│  Bottom-up (Adaptive Scan) — カバー外領域のみ探索                 │
│  ──────────                                                       │
│  SearchStoresAdaptive(polygon \ cover_map):                       │
│  - 20 件飽和 → quad-tree split                                     │
│  - polygon post-filter + brand filter                             │
│  - 結果: 「map でしか判らない漏れ」店舗の発見                        │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│  Quality: Territory sanity check                                 │
│  ──────────                                                       │
│  全 store ペアで check_pair:                                       │
│  - DUPLICATE_SUSPECT → Places の重複登録疑い (dedupe)              │
│  - DOMINANT_CLUSTER  → ドミナント戦略の正常                         │
│  - INDEPENDENT       → 別 territory                                │
└─────────────────────────────────────────────────────────────────┘
```

## 新 CLI (`pizza bench`)

```bash
./bin/pizza bench \
    --brands "エニタイムフィットネス,モスバーガー,TSUTAYA" \
    --areas "東京都" \
    --cell-km 5.0 \
    --adaptive=true \
    --out-dir var/bench
```

出力: JSON (各ブランドの stores / api_calls / polygon_rej / dup / cells_hit_cap / elapsed)。

## テスト状況

- Go 9 パッケージ all ok (Phase 13 quad-tree 4 件 + dough adaptive 3 件追加)
- Python **300 passed + 6 live skipped**
  - `test_operator_spider.py` 11 件 (Phase 14 7 + Phase 15 4)
  - `test_territory.py` 15 件 (Phase 16)

## 残り未実装 (Phase 17 候補)

1. **OSM Overpass 補完** — Places 漏れを `[leisure=fitness_centre]` 等で補う
2. **e-Stat 経済センサス recall audit** — 市区町村単位の事業所数と突合
3. **CoverMap を bake に組み込み** — 既知 territory 領域を scan skip
4. **Registry 自動拡充 loop** — unknown_stores → Web search → YAML 追加

## 新規/変更ファイル

### 新規
- `internal/grid/quadtree.go` / `quadtree_test.go`
- `internal/dough/adaptive_test.go`
- `internal/dough/knowledge/territory_radius.yaml` (11 業種 + 35 ブランド)
- `services/delivery/pizza_delivery/operator_spider.py`
- `services/delivery/pizza_delivery/territory.py`
- `services/delivery/tests/test_operator_spider.py`
- `services/delivery/tests/test_territory.py`
- `docs/phase13-16-integrated.md` (本書)

### 変更
- `internal/dough/searcher.go` (SearchStoresAdaptive + SearchMetrics + RestrictToPolygon)
- `internal/grid/grid.go` (PointInPolygon exported)
- `cmd/pizza/main.go` (cmdBench)
