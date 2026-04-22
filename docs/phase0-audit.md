# Phase 0 監査 — 原点と現状のアラインメント

## 🎯 原点回帰: PI-ZZA の目的 (README から再構築)

> 「精度の高いデータ、おまち！」
>
> フランチャイズ (FC) 業界における **メガフランチャイジー（20 店舗以上の運営会社）** の特定や、**直営・FC の判別** といった、人間が数週間かけて行う泥臭いリサーチ業務を、**AI エージェントが数時間で完遂** させる。

### 成功とは何か (Definition of Done)

開発工程.md §3 から抽出した具体的受入条件:

| # | 受入条件 | 数値目標 |
|---|---|---|
| DoD-1 | Grid が指定ポリゴンを漏れなくカバーする | **100%** |
| DoD-2 | Firecrawl が会社概要 URL を抽出できる | 主要 FC サイトで成功 |
| DoD-3 | CSV から不正文字・重複が除去される | 100% |
| DoD-4 | E2E が完走する (ブランド名 → CSV 出力) | エラー 0 |
| DoD-5 | API バックオフが正しく動作 | リトライ成功 |
| DoD-6 | 直営/FC 判定の正解一致率 | **≥ 90%** |
| DoD-7 | 指定エリアの店舗抽出網羅率 | **≥ 95%** |
| DoD-8 | メガジー判定 (20 店舗閾値) | 正確 |

---

## 📋 Phase 0 で作った Red テスト × 受入条件 対応表

| DoD | テストファイル | Red 状態 | Phase |
|---|---|---|---|
| DoD-1 | `internal/grid/grid_test.go` | 🔴 Red | 1 |
| DoD-2 | `internal/toppings/parser_test.go` | 🔴 Red | 2 |
| DoD-3 | `internal/slice/dedupe_test.go` | 🔴 Red | 1-2 |
| DoD-4 | `test/e2e/pipeline_test.go` | 🟡 Skeleton (build tag) | 1-4 |
| DoD-5 | `internal/oven/retry_test.go` | 🔴 Red | 1 |
| DoD-6 | `internal/scoring/accuracy_test.go` | 🔴 Red | 3 |
| DoD-7 | `internal/scoring/accuracy_test.go` (RecallRate) | 🔴 Red | 1-3 |
| DoD-8 | `internal/scoring/mega_test.go` | 🔴 Red | 3 |

**全 DoD が Red テストとして表現されている**。Phase 1+ で順に Green に変えていけば、自動的に受入条件を満たすことが保証される。

---

## 🧩 統合監査の結果

### ✅ 動作確認済み

| 要素 | 確認方法 | 結果 |
|---|---|---|
| Places API (New) | `curl` で `https://places.googleapis.com/v1/places:searchText` | エニタイムフィットネス新宿 3 件取得 ✅ |
| browser-use import | `python -c 'import browser_use'` | OK (Agent, Browser, Controller 利用可) |
| anthropic SDK | 0.96.0 | OK |
| openai SDK | 2.32.0 | OK |
| google-genai | importable | OK |
| gRPC proto (Go) | `go build ./...` | OK |
| gRPC proto (Python) | `from pizza.v1 import seed_pb2` | OK (v31 gencode で runtime 6.x に適合) |
| gosom/google-maps-scraper | `go list -m ...@latest` | v1.12.1 取得可能 |
| googlemaps/google-maps-services-go | `go list -m ...@latest` | v1.7.0 取得可能 |

### 🔧 修正した問題

1. **Protobuf 版号不整合** → buf plugin を `v31.1` にピン留めで解決
2. **providers/chat()** が browser-use API と噛み合わない
   → `make_llm()` に変更し、browser-use `Agent(llm=...)` パターンに対応
3. **Firecrawl image path 誤認**
   → 正しくは `ghcr.io/firecrawl/firecrawl` (mendableai ではない)
4. **Firecrawl スタック複雑性**
   → 独立した `deploy/compose.firecrawl.yaml` に分離

### 🚧 残件 (Phase 1+ で実装)

- `deploy/Dockerfile.delivery` の playwright インストール (browser-use 本格利用時)
- Streamlit 本実装
- `services/delivery` を Phase 3 で実サーバ化
- gosom/google-maps-scraper を Go モジュール依存に追加 (M1 実装時)

---

## 📐 設計原則の再確認 (ずれていないか)

| 原則 | 守られているか |
|---|---|
| Go オーケストレータ + polyglot gRPC | ✅ api/ v1 proto, gen/{go,python}, cmd/*, services/ |
| フォーク元 OSS の元言語保持 | ✅ browser-use=Py, Firecrawl=TS (独立コンテナ), gosom=Go |
| AGPL 隔離 (Firecrawl) | ✅ REST 越境のみ。`compose.firecrawl.yaml` で独立スタック |
| 厳格 TDD (Red → Green → Refactor) | ✅ 8 コミットのうち 3 本が Red 専用、追加 Red が本プランで拡張 |
| Conventional Commits | ✅ 全コミットが type(scope): … 形式 |
| マルチ LLM 切替 | ✅ providers/registry.py、make_llm() で browser-use 互換 |
| Firecrawl docker/saas 両対応 | ✅ `FIRECRAWL_MODE=docker|saas` 環境変数 |
| Public OSS | ✅ https://github.com/clearclown/pizza |

---

## 🧭 次の一手

Phase 1 着手の前に以下が完了していれば OK:

- [x] GCP API キー発行 (`.env` に格納済)
- [x] `.env` の gitignore 確認
- [x] 全 Red テストが `go test ./...` / `pytest` で失敗
- [x] Firecrawl 公式 image が pull 可能
- [x] browser-use + LLM SDK が import 可能

Phase 1 (M1 Seed Green 化) の最初のタスク:

1. `internal/grid/grid.go` の `Split()` 実装 → `internal/grid/grid_test.go` を Green
2. `internal/slice/dedupe.go` の `Dedupe()` / `Sanitize()` 実装 → Green
3. `internal/oven/retry.go` の `Retry()` / `DelayFor()` 実装 → Green
4. `cmd/dough-service/main.go` で gRPC サーバを立ち上げ、googlemaps/google-maps-services-go で `SearchStoresInGrid` を実装
