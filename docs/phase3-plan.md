# Phase 3 計画 — コアバリューの実証

## 🎯 目的

Phase 2 で「動く」状態には持っていった。**Phase 3 では「信用できる数値」を出す**:

1. **実 LLM 判定で Classification Accuracy ≥ 90%** (開発工程.md §3.3 DoD-6)
2. **mega_franchisees view が実運営会社を返すシナリオ**を再現可能にする
3. **browser fallback** で Markdown から判定不能ケースをカバー (confidence 底上げ)
4. **Firecrawl live 稼働**で M2 Kitchen の end-to-end が本番同等に

## 📊 Phase 2 終了時点の数値

- Go カバレッジ平均 85%、Python 20/20 pass
- 新宿 25 セル → 72 店舗抽出 5.4s
- golden 10 件 — **mock baseline 60%**
- 実 LLM 精度は未測定 (`DELIVERY_MODE=live` の実行実績なし)

## 🎬 Phase 3 成功条件

| # | 条件 | 検証 |
|---|---|---|
| 3A | live LLM 判定が 30+ 件 golden で ≥90% accuracy | `go test -tags=live ./internal/scoring/... -run LiveAccuracy` |
| 3B | Firecrawl live で実 URL の Markdown 取得成功 | `make up-firecrawl && curl -X POST .../v1/scrape` |
| 3C | `pizza bake --with-judge` + 実 LLM で `mega_franchisees` が実運営会社を返す | SQLite で operator_name に実在企業名が載る |
| 3D | browser fallback が confidence <0.4 のケースで confidence 底上げ | unit test + live test |
| 3E | testcontainers-go で docker compose 起動 → pizza bake → CSV 検証 | `go test -tags=integration ./test/e2e/...` |

## 🧭 作業順 (優先度順)

### 3.1 golden dataset を 30 件に拡張 (🟢 低リスク)

- 10 件 (Phase 2) → 30 件
- FC: セブン-イレブン / ファミマ / ローソン / エニタイム / コメダ / ほっともっと / いきなりステーキ / ガスト / サイゼリヤ / マクドナルド (一部 FC) / 吉野家 (一部 FC) / モスバーガー / ドトール (FC 多め) / 松屋 / ロイヤルホスト / バーミヤン / しゃぶしゃぶ温野菜 / ビッグボーイ / ホテルマイステイズ(FC 有) / カラオケ ビッグエコー
- 直営: スタバ / 無印 / ユニクロ / GU / ZARA / H&M / イオン (直営) / イトーヨーカドー / ライフ / マルエツ

### 3.2 Live LLM accuracy 測定 (🟡 コスト発生)

新規ファイル:
- `internal/scoring/live_accuracy_test.go` — build tag=live
- Golden CSV を読み、各行について:
  - `pb.Store` を組み立てる
  - (Markdown は省略 — Phase 3.5 で Firecrawl 連携)
  - Python mock gRPC 経由ではなく、**Go から直接** Python `services/delivery` を呼ぶ代わりに、Go 側で直接 Anthropic API を叩くか、live mode Python サーバを起動して gRPC で叩く
  
**決定**: Python live server を起動しておき、Go test が gRPC 経由で叩く形が一番キレイ。
ただし E2E すぎると切り分け難しい。まずは **Python 側で直接 golden を舐める live test** を書く:
- `services/delivery/tests/test_live_accuracy.py` (pytest marker=live)
- ANTHROPIC_API_KEY がある時のみ実行
- 10-30 件の golden を judge_franchise() に流し、accuracy を計算してログ出力

### 3.3 プロンプトチューニング (必要なら)

測定後 accuracy < 90% なら:
- `prompts/judge.yaml` の system/task を調整
- few-shot examples を追加 (直営 / FC 各 2 例)
- model を opus → sonnet に切替てコストと速度の比較

### 3.4 Browser fallback (🟡 中リスク)

`agent.py` 拡張:
- judge_franchise の confidence < 0.4 なら browser_use.Agent を起動
- Agent task: "{official_url} を訪問して会社概要ページに移動し運営会社名と店舗数を確認せよ"
- 結果を Pydantic モデルで受け取り直し、confidence を加算

unit test は Agent の mock で。Phase 3 では Agent 実 run は gate して基本 skip、手動で golden 数件だけ回す。

### 3.5 Firecrawl live 稼働 (🟢 低リスク)

- `make up-firecrawl` 起動
- `curl -X POST http://localhost:3002/v1/scrape -d '{"url":"https://www.anytimefitness.co.jp/shinjuku/"}'`
- 成功を確認したら `pizza bake --area 新宿 --with-judge` で Markdown 付き judgement が走る
- docs/architecture.md にスクショ or ログ例を貼る

### 3.6 メガジー成功例 docs (🟢)

`docs/examples/mega-franchisee-walkthrough.md` 新規:
- setup → bake → SQLite 覗く → メガジー抽出 → CSV ダウンロード
- 実データスクリーンショット (CSV 抜粋で代用可)

### 3.7 testcontainers-go E2E (🟡 中リスク)

`test/e2e/pipeline_test.go` (現在 skeleton):
- `testcontainers.NewDockerComposeWith(deploy/compose.yaml + compose.firecrawl.yaml)`
- compose.Wait() で Firecrawl + dough + delivery の健全化待ち
- Go バイナリを exec ではなく oven.Pipeline を直接 in-process で呼び出す形式でもよい
- 結果の SQLite を open して count > 0 を assert

## 🔒 コスト & リスク管理

- LLM 呼び出しは golden 30 件 × 500 tokens 程度 = 数十円
- Browser fallback 実 run は Playwright ブラウザ DL が必要 → skip by default
- Firecrawl image 952MB pull 済 / 追加 pull 不要
- 全ての live テストは build tag または env gate で通常 CI から除外

## Non-goals for Phase 3

- Kubernetes 配備
- 多言語化 (日本語 / 英語だけで十分)
- Streamlit の洗練 (Phase 2 のレベルで十分)
- 47 都道府県の実抽出ベンチ
