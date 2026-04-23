# CLAUDE.md — PI-ZZA 🍕 開発ガイド & 役割定義

このリポジトリで Claude が作業するときの **基本方針** と **運用手順**。
LLM が「勝手にデータ収集 / 書き込み / 集計する」のを防ぎ、**PI-ZZA 本体のパイプライン
経由でのみ真実データが流れる** 設計を徹底するために置く。

---

## 🚫 Claude (開発者) の役割・禁止事項

**Claude はこのプロジェクトの開発者 (プログラマ) に限定される**。以下は**絶対に行わない**:

| 禁止行為 | 代わりにやること |
|---|---|
| Web 検索結果 (BC 誌 / gBizINFO / 業界記事 等) を YAML・JSON・DB に **直接貼り付ける** | pipeline (`jfa_fetcher.py` / `houjin_csv.py` / `places_client.py` 等) を実装して **pizza 自身が取得する** |
| 法人番号 / 店舗数 / 住所を**手動で入力**する | `pizza houjin-import` や `pizza jfa-sync` で自動取込する CLI を作る |
| `franchisee_registry.yaml` や test fixture に **LLM 生成の推定データを書く** | 人間レビュー済の source (gBizINFO URL 付) のみ許可。LLM 生成なら `source: llm_unverified` タグ必須 |
| 最終 CSV / docs を**手書きデータ**で作る | `pizza integrate --mode export` で pipeline 由来のデータのみ使用 |
| ユーザー操作 (登録・承認・ダウンロード) を代行する | CLI / docs を書き、**手順をユーザーに提示**して user 自身が実行する |

### 根拠
これまでに LLM agent が「調査した」と称して貼り付けたデータに **6 件の重大エラー**
(モスフードサービス 363 店を加盟店扱い、ドムドムをモス加盟誤認、アズナス 1 店→実態 84 店 等)
が fact-check で判明済。**本プロジェクトは BI ツールなのでハルシネーション侵入は致命的**。

---

## 📐 Claude が手を動かす範囲 (許容)

- Go / Python **コードの実装、リファクタ、テスト追加、バグ修正**
- `cmd/pizza/main.go` の CLI サブコマンド追加 / 改良
- ORM model (SQLAlchemy) / スキーマ (SQLite migration) の設計
- docs の構造設計 (docs/*.md)、ただし**具体的な企業名や店舗数は書かない**
- ユニットテスト (fixture は mock / 人工データのみ。実在社名でのテストは不可)
- 稀な例外: E2E デバッグ中の**疎通確認**目的での 1〜2 件の API コール
  (例: `curl https://places.googleapis.com/...` でキー生存確認)
  実データ収集ではない、**通信確認のみ**

---

## 🏗 アーキテクチャ (層構造)

```
┌────────────────────────────────────────────────────────────────┐
│  外部ソース (Ground Truth)                                    │
│  ┌───────────────┐ ┌──────────────────┐ ┌──────────────────┐   │
│  │ JFA 協会 489社 │ │ 国税庁 CSV 577万 │ │ Places / web ...│   │
│  └──────┬────────┘ └────────┬─────────┘ └───────┬──────────┘   │
│         │                   │                    │              │
│         ▼                   ▼                    ▼              │
└─── pipeline ─────────────────────────────────────────────────────┘
     jfa_fetcher.py       houjin_csv.py         dough.searcher
     (scrape + ORM)       (CSV→SQLite)          (Places 実店舗 scan)
                                                     │
                                                     ▼
                                                research_pipeline
                                                     │
                                                     ▼
                               ┌───────────────── ORM (pizza-registry.sqlite) ─┐
                               │ FranchiseBrand / OperatorCompany              │
                               │ BrandOperatorLink                             │
                               │   (brand × operator × source の多対多)         │
                               └──────────────────┬─────────────────────────────┘
                                                  ▼
                                        integrate.py
                                 (hydrate + export unified CSV)
                                                  │
                                                  ▼
                                        var/fc-operators-unified.csv
```

### Layer D (operator 実在検証) の fallback

`verify_pipeline.VerifyPipeline` が以下の順で試行:
1. 国税庁 Web-API (`HOUJIN_BANGOU_APP_ID` 設定時)
2. 国税庁 ローカル CSV (`houjin_csv` index 件数 > 0)
3. gBizINFO (`GBIZ_API_TOKEN` 設定時)
4. skip (graceful)

---

## 🖥 CLI リファレンス

```bash
# 基盤データ (月 1 程度の更新)
pizza houjin-import --csv <nta_zip>     # 国税庁 CSV 全件 → SQLite
pizza houjin-search --name "株式会社X"   # ローカル検索
pizza jfa-sync                           # 協会会員 scrape + ORM

# ブランド単位のフル pipeline
pizza migrate --with-registry            # DB 初期化 + 手動 registry seed
pizza bake  --query "モスバーガー" --area 東京都 --cell-km 3.0
pizza research --brand モスバーガー --verify-houjin
pizza audit --brand モスバーガー --areas 東京都 --skip-bake --out out.csv
pizza scan  --brand "エニタイム" --areas 東京都 \
            --with-judge --judge-mode panel --with-verify --verify-houjin

# 統合 & 出力
pizza integrate --mode run               # 3 ソース統合 + 法人番号 hydrate
pizza integrate --mode export --out all.csv
pizza megafranchisee --min-total 2 --min-brands 2 --out-csv multi.csv
pizza registry-expand --brand X          # unknown_stores から YAML 候補
```

---

## 🧪 開発ワークフロー (Claude のルーティン)

1. **`TaskCreate` → `in_progress`** にマークしてから着手
2. コード変更 → ユニットテスト追加 → `go test ./...` + `uv run pytest` 緑確認
3. 既存実動作を壊していないか `git diff` で確認
4. **人間レビュー前にデータ追加の commit は打たない** (コード変更のみ)
5. commit 時は Co-Authored-By フッタを付ける:
   ```
   Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
   ```
6. push 前に `git log --oneline -5` + `git status` で最終確認

---

## 🔑 環境変数 (.env)

```
GOOGLE_MAPS_API_KEY=     # Places API (Seed)
ANTHROPIC_API_KEY=       # Claude critic (LLM 判定、cleanser)
GEMINI_API_KEY=          # Expert Panel worker
OPENAI_API_KEY=          # optional
FIRECRAWL_API_URL=http://localhost:3002   # self-host (Kitchen)
DELIVERY_SERVICE_ADDR=localhost:50053     # gRPC (Judge)
HOUJIN_BANGOU_APP_ID=    # 国税庁 API (1 か月審査) — なくても CSV 経路で動く
GBIZ_API_TOKEN=          # gBizINFO API (即時発行) — 補完用
ENABLE_BROWSER_FALLBACK=1 # Panel 低 confidence 時に browser_use.Agent 起動
```

---

## 📦 データ保管と公開可否

- `var/**/*.sqlite` / `var/**/*.csv` → **すべて .gitignore** (ローカル pipeline 実行結果)
- `docs/assets/*.zip` → .gitignore (国税庁 zip 等の外部データ)
- `test/fixtures/**/*.csv` → **pipeline 出力の固定スナップショット**なら OK、
  **手書きデータは不可**
- `docs/*.md` → 設計 / 手順書 / Phase レポート。**企業実名のリスト化は不可**、
  ある場合は `source: pipeline_<YYYY-MM-DD>` を必ず明記
- `internal/dough/knowledge/franchisee_registry.yaml` → ORM 移行中、
  ここに **新規手書き追加禁止**。LLM 由来データは absolutely 禁止

---

## 🧭 質問が来たら

- 「○○ の店舗数は?」→ **答えない**。「`pizza bake` で pipeline 通してください」と誘導
- 「○○ の法人番号は?」→ **答えない**。「`pizza houjin-search --name "○○"` で引いてください」
- 「BC 誌ランキングを教えて」→ **答えない**。「人間レビューで registry に追加してください」
- 「このデータを YAML に書いて」→ **拒否**。「pipeline で取得する CLI を足します」と再設計

---

## 🔁 継続改善ループ (supervised 学習的)

1. JFA 489 社 = **truth set** (brand × operator の最上位層)
2. `pizza scan` で pipeline を回し、`pizza integrate --mode export` で結果取得
3. `evaluator.py` で truth × pipeline 出力を突合し precision / recall を算出
4. 乖離箇所をコード側で修正 (brand filter / operator extraction / matching)
5. テストを追加して regression 防止、再度 loop

本 loop の自動化は段階的に。まずは evaluator の metric を固めてから。

---

## 🗓 Phase 履歴 (2026-04 末時点)

- Phase 5-16: 基本 pipeline (bake/research/audit)、Expert Panel、Territory、CoverMap
- Phase 17: OSM Overpass / e-Stat / Registry 自動拡充
- Phase 18: URL ドメイン二次 brand filter
- Phase 19-20: Cross-brand aggregator / multi_brand_operators YAML (後に revert)
- Phase 21: delivery-service asyncio 修正 / Firecrawl SPA waitFor / Panel browser fallback
- Phase 22: **ORM 集約 + JFA 自動取込 + 3 ソース統合** (本 md 作成時)

---

**Claude が作業するときは必ず本 md を読んでからコードに触れること**。
