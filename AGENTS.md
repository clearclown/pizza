# AGENTS.md — PI-ZZA 🍕 開発 AI Onboarding

このファイルは、**Claude / GPT / Codex / その他開発 AI** が PI-ZZA に
途中参加した際に「即戦力」として動けるようにするための運用ガイド。

具体的な禁止事項・行動規範は [`CLAUDE.md`](./CLAUDE.md) に集約されている。
本ファイルは **目的・全体像・現在地・次の手** を提供する。

---

## 1. プロジェクト目的

PI-ZZA は **日本のフランチャイズ (FC) 業界のメガフランチャイジー
(2 業態以上を運営する有力事業会社) を網羅的に特定する BI ツール**。

### ゴール
- **14 brand × 47 都道府県** の店舗を網羅
- 各店舗の **運営事業会社 (operator)** を法人番号 (国税庁 verified) 付きで特定
- **多業態 operator** (例: 大和フーヅ = モス + ミスド + 築地銀だこ) を cross-brand で集計
- **ハルシネーション 0** — LLM 生成データは ground truth に使わない、pipeline 経由のみ採用

### 14 対象ブランド
カーブス / モスバーガー / 業務スーパー / Itto個別指導学院 / エニタイムフィットネス /
コメダ珈琲 / シャトレーゼ / ハードオフ / オフハウス / Kids Duo / アップガレージ /
カルビ丼とスン豆腐専門店韓丼 / Brand off / TSUTAYA

---

## 2. アーキテクチャ概要

```
┌─────────────────────────────────────────────────────────────┐
│  Outer Sources (Ground Truth)                                │
│  ┌──────────┐ ┌──────────┐ ┌─────────────┐ ┌────────────┐    │
│  │ Places   │ │ JFA 489  │ │ 国税庁 5.7M │ │ OSM       │    │
│  │ API (有償│ │ 協会員   │ │ CSV         │ │ Overpass  │    │
│  │ daily限) │ │          │ │             │ │ (無料)    │    │
│  └────┬─────┘ └────┬─────┘ └──────┬──────┘ └─────┬─────┘    │
└───────┼────────────┼──────────────┼──────────────┼──────────┘
        ▼            ▼              ▼              ▼
   ┌──────────────────────────────────────────────────────┐
   │  pipeline DB (var/pizza.sqlite)                       │
   │  stores / operator_stores / mega_franchisees          │
   └──────────────────────────────────────────────────────┘
        ▲                                          │
        │  pizza integrate --mode run               │  pizza integrate --mode export
        │                                          ▼
   ┌──────────────────────────────────────────────────────┐
   │  ORM (var/pizza-registry.sqlite)                      │
   │  franchise_brand / operator_company / brand_operator_link│
   └──────────────────────────────────────────────────────┘
        ▲
        │  pizza import-megajii-csv (人手 TSV)
        │
   ┌──────────────────────┐
   │ var/external/        │
   │ megajii-manual.tsv   │  (BC誌等 人手集計)
   └──────────────────────┘
```

### Polyglot 構成
| Module | 責務 | 言語 | フォーク元 | License |
|---|---|---|---|---|
| **M1 Seed** | Places API scan | Go | google-maps-services-go | MIT/Apache |
| **M2 Kitchen** | Markdown fetch | TypeScript | mendableai/firecrawl | AGPL (REST 隔離) |
| **M3 Delivery** | LLM judge (gRPC) | Python | browser-use | MIT |
| **M4 Box** | BI 可視化 | Python (Streamlit) | self | — |
| **Oven** | Orchestrator | Go | self | — |

`cmd/pizza` (Go CLI) が全モジュールを束ねる。各モジュールは gRPC で疎結合。

---

## 3. 主要 CLI (`./bin/pizza`)

詳細は `CLAUDE.md` の CLI リファレンスと `pizza help`。

### bottom-up (店舗 → operator)
```bash
pizza bake          # Places API scan → stores
pizza research      # 各店舗 HP per_store extract → operator_stores
pizza scan          # bake + research + audit 一括
pizza bench         # 複数 brand × 複数 area 逐次 (bench 関数)
pizza enrich        # phone 逆引き (iタウン)
pizza address-reverse # 住所 → 国税庁逆引き
pizza operator-spider # ORM operator 公式 HP → 住所 match → pipeline back-fill
pizza deep-research # Gemini + Claude critic + houjin 4-Gate
```

### top-down (operator → brand)
```bash
pizza jfa-sync         # JFA 協会員 scrape → ORM
pizza jfa-disclosure-sync # JFA 情報開示書面 PDF → 本部店舗数を ORM
pizza houjin-import    # 国税庁 CSV → SQLite (5.7M)
pizza edinet-sync      # 有報関係会社 → ORM (要 EDINET_API_KEY)
pizza import-megajii-csv  # 人手 TSV → ORM (LLM cleansing 済)
pizza osm-fetch-all    # OSM Overpass 全国 fetch (Places 不要、無料代替)
pizza official-franchisee-sources # 公式FC/運営会社/本部PR本文 → ORM
```

### クレンジング & 統合
```bash
pizza cleanse        # LLM canonicalize + 国税庁 verify (Gemini/Claude fallback)
pizza purge          # garbage operator 削除 (cross-brand pollution 含む)
pizza integrate      # pipeline ↔ ORM 双方向 (mode=run | export)
pizza fc-directory   # ORM operator 全件 CSV
pizza megafranchisee # cross-brand 集計
pizza brand-profile  # 14 brand profile (6 source 融合)
pizza coverage-export # 14 brand × 47 都道府県 coverage CSV
```

### 設定
```bash
pizza migrate        # DB schema 初期化
pizza serve          # delivery-service 起動 (gRPC)
```

---

## 4. データソース 一覧

### 利用中
| Source | 件数 | 用途 | 制約 |
|---|--:|---|---|
| Google Places API (New) | - | 店舗位置・住所・phone | **daily quota 切れリスク** |
| 国税庁法人番号 CSV | 5,766,406 | 法人番号 verify (truth set) | substring LIKE は遅い |
| JFA 協会員 scrape | 826 | brand × operator link | 運営 brand 部分掲載 |
| JFA 情報開示書面 PDF | 103 | franchisor 公表店舗数 / source_url | PDF 表抽出できたもののみ |
| 人手集計 TSV (BC誌等) | 192 | メガジー master | top 500 のうち 40% のみ |
| OSM Overpass | brand 別 100-3000 | Places 代替 | recall 20-100%、tag 依存 |
| operator 公式 HP (Scrapling) | - | operator-spider 経由 | SPA 非公開多 |
| 公式FC/運営会社/本部PR本文 | 11 | 薄い brand の operator evidence | 本文 gate + 国税庁照合 |

### 未活用 (要追加調査)
| Source | 状態 |
|---|---|
| EDINET 有報関係会社 | API key 未設定 |
| gBizINFO API | API token 未設定 |
| Wikidata SPARQL (subsidiary/operator P355/P137) | 未実装 |
| 求人情報 (Indeed/マイナビ/ハローワーク) | 未実装 |
| 第三者集約 (BizPow/フランチャイズの窓口) | 未実装 |
| 上場 FC 本部の決算説明 PDF | 未実装 |

### Cross-reference (実装済 / 部分実装)
| 経路 | 状態 |
|---|---|
| 公式 brand HP の店舗住所 × 国税庁 同住所法人 | 部分実装 (address-reverse) |
| OSM の `operator:ja` tag 直 capture | 実装済 (未検証 source、法人番号は国税庁照合時のみ付与) |

---

## 5. 主要 DB / 成果物

### Database (var/)
| ファイル | 内容 | 行数 (2026-04-27) |
|---|---|--:|
| `var/pizza.sqlite` | pipeline (stores / operator_stores) | 10,612 stores |
| `var/pizza-registry.sqlite` | ORM (franchise_brand / operator_company / brand_operator_link) | 1,422 operators / 1,432 links |
| `var/houjin/registry.sqlite` | 国税庁 法人番号 5.7M | 5,766,406 |
| `var/external/megajii.sqlite` | 人手 TSV SQLite 化 | 192 |

### test/fixtures/megafranchisee/ (git 管理、CSV master)
| ファイル | 行 | 役割 |
|---|--:|---|
| `megajii-enriched.csv` 👑 | 192 | 人手 TSV master + ORM fusion (17 列) |
| `fc-operators-all.csv` ⭐ | 1,005 | 1 operator 1 行集約 |
| `fc-links.csv` | 1,432 | brand × operator flat |
| `jfa-disclosures.csv` | 103 | JFA 情報開示書面 PDF index |
| `spider-matches.csv` | 57 | pipeline spider-matched snapshot |
| `megajii-raw.csv` | 192 | 生 TSV snapshot |
| `by-view/megajii-ranking.csv` | 123 | 2+業態メガジー ランキング |
| `by-view/by-brand/*.csv` | 14 ファイル | brand 別 operator 一覧 |
| `by-view/tokyo-entering-operators.csv` | 32 | 東京進出 FC 社 |
| `by-view/unverified-63-focus.csv` | 63 | 手動確認候補 |
| `operators-pure-pipeline-2026-04-23.csv` | 26 | 旧 snapshot |

---

## 6. 環境変数 (`.env`)

```
GOOGLE_MAPS_API_KEY=     # Places API (Seed) — 通常は空。課金 API なので明示 opt-in 時のみ設定
PIZZA_ENABLE_PAID_GOOGLE_APIS=0  # 1 の時だけ Google Maps Platform 有料 API を許可
PIZZA_API_KEYS=          # 複数 GCP project key の round-robin pool (原則使わない)
ANTHROPIC_API_KEY=       # Claude (cleanser, critic, panel)
GEMINI_API_KEY=          # Gemini Flash (cleanser, panel worker)
OPENAI_API_KEY=          # 補助
LLM_PROVIDER=anthropic   # cleanse / import-megajii-csv の primary 選択
FIRECRAWL_API_URL=http://localhost:3002  # M2 Kitchen self-host
DELIVERY_SERVICE_ADDR=localhost:50053    # M3 Delivery gRPC
HOUJIN_BANGOU_APP_ID=    # 国税庁 API (1 か月審査)
GBIZ_API_TOKEN=          # gBizINFO (即時発行) — 未設定
EDINET_API_KEY=          # EDINET (即時発行) — 未設定
ENABLE_BROWSER_FALLBACK=1
```

---

## 7. 現在地 (2026-04-27 12:40 JST)

### 完了
- ✅ 14 brand 全 cleanse (Claude / Gemini fallback) → 1,651 corp 付与
- ✅ ORM bug fix: `OperatorCompany.website_url` mapped_column 追加
- ✅ operator_spider default fetcher: ScraplingFetcher
- ✅ OSM Overpass 経由 全国 fetch CLI (`osm_fetch_all.py`、無料、Places 不要)
  - ハードオフ recall **99.6%** (240/241 公表) を実証
- ✅ `pizza import-megajii-csv` (人手 TSV → Gemini canonicalize + Claude critic + houjin verify + ORM upsert)
- ✅ `pizza purge --cross-brand-threshold` (cross-brand pollution 削除)
- ✅ `pizza jfa-disclosure-sync` (JFA 情報開示書面 103 PDF link、14 件店舗数抽出)
- ✅ `pizza coverage-export` (14 brand × 47 都道府県 658 row、pref 近傍座標推定付き)
- ✅ OSM 14 brand 補完 (Google API 不使用、国内 bbox + 近接重複除外 + not:brand 除外)
- ✅ `pizza official-franchisee-sources` (Brand off / 韓丼の公式FC・運営会社・本部PR evidence 11 link)
- ✅ test/fixtures/megafranchisee/ に 7 CSV + by-view/ 派生 17 CSV

### 進行中
- 🔄 14 brand operator 未確定店舗の追加調査 (求人/公式/住所逆引き)
- 🔄 全国 bench (3/14 brand 完走、Places quota 切れで 11 brand 待機、Google API は明示 opt-in 時のみ)

### 待機中
- ⏳ Gemini 2.5 Pro daily quota
- ⏳ ユーザー提供 BC誌 top 500 追加 TSV
- ⏳ EDINET_API_KEY / GBIZ_API_TOKEN 設定

---

## 8. 既知の制約 / 限界

| 制約 | 影響 |
|---|---|
| Places API daily quota | 1 日に途中で切れる、回復 18-24h 待ち |
| Gemini 2.5 Pro daily quota | 同様、Claude にfallback wrapper 実装済 |
| Mos shop-detail SPA 非公開 | per_store extractor が本部名しか取れない (Phase 21 既知) |
| OSM の brand:ja tag は brand 依存 | brand により recall が低い、operator tag は少ない |
| `LIKE '%X%'` (substring) は houjin DB で O(N) | fc-directory が遅い、FTS5 化未対応 |
| BC 誌 top 500 のうち 192 社のみ取込 | 残 308 社は人手 TSV 提供待ち |

---

## 9. 「ハルシネーション 0」設計の要点 (絶対遵守)

### 原則
- **LLM は transformer (cleanser, critic) として使う**。Knowledge source としては使わない
- **国税庁 5.7M CSV** が最終 truth set
- 各 row に `source` タグ (`jfa` / `manual_megajii_*` / `pipeline` / `houjin_csv` / `edinet`) で provenance 明示
- corp 空 operator は `source` に `_unverified` を付ける

### 禁止事項 (CLAUDE.md と同じ)
- 手書きで registry.yaml に企業名を追加
- LLM (Claude / GPT) に「○○ ブランドの加盟店は?」と聞いて結果を貼る
- Web search の結果をそのまま DB に書く

### 許可
- pipeline (`pizza bake/research/scan`) の出力をそのまま DB に書く
- 人手集計 TSV (BC誌由来) を `pizza import-megajii-csv` で取込む (source タグ付き)
- LLM cleansing で raw_name → canonical 変換 (国税庁で verify が必須)

---

## 10. 開発フロー (Red → Green → Refactor)

```bash
# 1. 🔴 Red: 失敗テストから commit
git commit -m "test(scoring): add failing test for X"

# 2. 🟢 Green: 最小実装
git commit -m "feat(scoring): implement X"

# 3. 🔵 Refactor: 構造整える
git commit -m "refactor(scoring): clean up Y"

# 4. PR 作成 (admin-merge は user 認可必須、CI 待つのが基本)
gh pr create --title "..." --body "..."
```

### 必ず実行
```bash
go test ./...                              # Go 9 pkg
cd services/delivery && uv run pytest -q   # Python 540+ tests
go build -o bin/pizza ./cmd/pizza         # CLI build
```

### Co-Authored-By footer
LLM 補助で作った commit には:
```
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

## 11. 「次の一手」の判断軸

### Decision Tree
```
データを増やしたい?
├── 既知 brand の店舗を増やす?
│   ├── Places API 生きてる? → pizza bench
│   ├── Places quota 切れ? → pizza osm-fetch-all (無料代替)
│   └── operator 紐付け? → pizza operator-spider / pizza enrich
│
├── 既知 operator の数値を厚くする?
│   ├── 法人番号空? → pizza cleanse (LLM + houjin)
│   ├── 本社住所空? → houjin JOIN backfill
│   └── 運営 brand 不足? → pizza operator-spider 再走 / Wikidata SPARQL
│
└── 新規 operator を発見したい?
    ├── BC誌 top 500 → 人手 TSV 取込 (pizza import-megajii-csv)
    ├── EDINET 関係会社 → pizza edinet-sync (要 API key)
    ├── JFA 詳細 → pizza jfa-sync (再走で精度上)
    └── Wikidata / OSM operator:ja → 未実装、新 CLI 必要

データを綺麗にしたい?
├── garbage operator 削除? → pizza purge
├── 名前正規化 + 法人番号? → pizza cleanse
└── 重複法人番号 merge? → 未実装、SQL で対応
```

### 「ベスト判断」のルール
1. **API quota 切れ時は OSM** (今回実証、ハードオフ 99.6% recall)
2. **LLM quota 切れ時は fallback** (`_LLMWithFallback` wrapper 既実装)
3. **Mass admin-merge は user 確認必須**
4. **branch protection 守る** (main 直 push 禁止、PR 経由)
5. **fixtures は pipeline 由来のみ** (手書きデータ追加禁止)

---

## 12. リファレンス

| 文書 | 内容 |
|---|---|
| [`README.md`](./README.md) | プロジェクト概要 (人間向け) |
| [`CLAUDE.md`](./CLAUDE.md) | Claude 専用 開発ガイド + 禁止事項 |
| [`AGENTS.md`](./AGENTS.md) | 本ファイル — 開発 AI 全般向け |
| [`ARCHITECTURE.md`](./ARCHITECTURE.md) | システム俯瞰図 |
| [`docs/architecture.md`](./docs/architecture.md) | gRPC 契約 / SQLite schema |
| [`docs/tdd-workflow.md`](./docs/tdd-workflow.md) | TDD Red-Green-Refactor 実例 |
| [`docs/license-compliance.md`](./docs/license-compliance.md) | AGPL Firecrawl 隔離方針 |
| [`test/fixtures/megafranchisee/README.md`](./test/fixtures/megafranchisee/README.md) | fixture CSV 詳細 |

---

## 13. Phase 履歴 (要点のみ)

- **Phase 5-16**: 基本 pipeline (bake/research/audit) + Expert Panel + Territory + CoverMap
- **Phase 17**: OSM Overpass / e-Stat / Registry 自動拡充
- **Phase 18**: URL ドメイン二次 brand filter
- **Phase 19-20**: Cross-brand aggregator
- **Phase 21**: Mos 13 FC ground truth (1 社特定/13 既知)
- **Phase 22**: ORM 集約 + JFA 自動取込 + 3 source 統合
- **Phase 23-24**: enrich (Places Details + browser-use → Scrapling)
- **Phase 25**: brand-profile CLI (6 source 融合、14 brand 並列)
- **Phase 26**: cleanse (LLM canonicalizer + 国税庁 verify) + Scrapling phone lookup
- **Phase 27** (現在): EDINET scraper + import-megajii-csv + cross-brand identity + OSM 代替経路

---

## 14. 引き継ぎ時の Quick Start

```bash
# 1. 環境確認
git status                              # 未 commit 確認
ps aux | grep pizza | grep -v grep      # 実行中 process

# 2. 最新化
git pull --rebase

# 3. ビルド + test
go build -o bin/pizza ./cmd/pizza
go test ./...
cd services/delivery && uv run pytest -q && cd ../..

# 4. 現状把握
sqlite3 var/pizza.sqlite "SELECT brand, COUNT(*) FROM stores GROUP BY brand"
sqlite3 var/pizza-registry.sqlite "SELECT COUNT(*) FROM operator_company"
ls -la test/fixtures/megafranchisee/

# 5. 次手は §11 の Decision Tree
```

---

**最後に**: 本プロジェクトはユーザーの BI ツール。**ハルシネーションは
即廃棄処分。ground truth で動く pipeline を維持すること**。

任意の AI agent は本ファイル + `CLAUDE.md` + `README.md` を読めば
即着手できるよう、これらの 3 ファイルを常に最新に保つこと。
