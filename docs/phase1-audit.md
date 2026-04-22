# Phase 1 監査 — 初心回帰レビュー

## 🎯 原点再確認

README:
> 「メガフランチャイジー（20 店舗以上の運営会社）の特定や、直営・FC の判別といった、**人間が数週間かけて行う泥臭いリサーチ業務**を、AI エージェントが数時間で完遂させることを目的とする」

**唯一のコアバリュー**: 人間の目視調査を AI が完遂すること。Google Maps 抽出と SQLite 保存は手段であって価値ではない。

---

## ✅ Phase 1 完了項目

| DoD | 実装 | 実動作確認 |
|---|---|---|
| 1. Grid 100% cover | 🟢 `internal/grid` 81% cov | 新宿矩形 25 セル生成 OK |
| 2. Parser URL 抽出 | 🟢 `internal/toppings/parser` 92% cov | unit test |
| 3. Sanitization | 🟢 `internal/slice` 87% cov | unit test |
| 4. E2E Flow | 🟢 CLI `pizza bake` 5.4s 完走 | 新宿 → 72 店舗 → CSV + SQLite |
| 5. API Backoff | 🟢 `internal/oven/retry` 79% cov | unit test |
| 6. Classification ≥90% | 🟡 **ロジックだけ実装**、真の判定は mock | golden dataset 未整備 |
| 7. Recall Rate ≥95% | 🟡 **ロジックだけ実装**、真の実測未実施 | 正解総数不明 |
| 8. メガジー判定 | 🟢 view + IsMegaFranchisee Green | mock データに対しては動く |

### 数値的に何ができたか

- 新宿駅 1km ポリゴン → Places API → **72 店舗抽出** 実データで確認済
- SQLite に `stores / markdown_docs / judgements / mega_franchisees` 4 テーブル永続化
- Python mock gRPC サーバ → Go CLI 接続で 72 判定を返す
- CSV 自動出力 (日本語パス対応)

---

## 🔴 **未達 — コアバリューに直結**

**直営/FC 判定は mock のまま**:
- 全店舗の operator_name = "(mock) 株式会社モック運営"
- confidence = 0.5 固定
- 実際のリサーチ価値は **ゼロ**

これが解消されない限り「PI-ZZA は動くが目的を達成していない」状態。

### 解消に必要な実装

1. **`pizza_delivery/providers/*_provider.py` の `make_llm()` 本実装**
   - `browser_use.llm.ChatAnthropic(api_key=...)` を返すだけ
   - 既に SDK は install 済 (`browser-use>=0.3.0`, `anthropic>=0.40.0`)
2. **`pizza_delivery/agent.py` の `judge_franchise()` 本実装**
   - `browser_use.Agent(task=prompt, llm=llm)` を作って run
   - prompt: Markdown + 店舗名 + 公式 URL から運営会社を抽出させる
3. **`pizza_delivery/server.py` に `RealDeliveryServicer`** を追加し、`DELIVERY_MODE=live` で切替
4. **判定プロンプトを `prompts/judge.yaml` に外出し**
5. **golden dataset を `test/fixtures/judgement-golden.csv` に 5-10 件** 用意し、Phase 3 で 100 件に拡張

### テスト戦略

- **Unit**: mock LLM (ChatFake) を inject して `judge_franchise` のロジックを TDD
- **Live**: build tag=live で実 LLM を叩く (ANTHROPIC_API_KEY あり時のみ)
- **Accuracy**: golden dataset に対する ClassificationAccuracy を測定するベンチマーク

---

## 🟡 残件 (Phase 2-4 スコープ)

- **Firecrawl セルフホスト live 動作** — compose.firecrawl.yaml は書けているが、実際に起動して Go から叩く動作は Phase 2 で検証
- **Streamlit Box UI 実装** — 現状はプレースホルダ、地図可視化・メガジー一覧が欲しい
- **Classification/Recall 実測** — golden dataset 整備 + CI 組込
- **testcontainers-go E2E** — 現状 skeleton のみ
- **フォーク戦略の実行** — 必要になったら third_party/ に subtree 取り込み
- **gRPC gateway (REST)** — 必要なら将来
- **Kubernetes マニフェスト** — deploy/k8s は README のみ

---

## 📐 アーキテクチャ整合性チェック

| 原則 | 守られているか |
|---|---|
| Go オーケストレータ | ✅ `cmd/pizza` + `internal/oven` |
| polyglot gRPC | ✅ Go + Python + (Firecrawl は REST) |
| フォーク元言語保持 | ✅ browser-use=Py, Firecrawl=TS |
| TDD | ✅ 全モジュール Red→Green 履歴 |
| Conventional Commits | ✅ 20 コミット全準拠 |
| AGPL 隔離 (Firecrawl) | ✅ REST 越境、独立 compose |
| マルチ LLM 切替 | 🟡 registry は動くが make_llm 未実装 |
| Public OSS | ✅ github.com/clearclown/pizza |

---

## 🧭 次の優先順位 (Phase 2 着手順)

1. **browser-use + LLM 判定の本実装** (最重要 — コアバリュー)
2. **DELIVERY_MODE=mock|live 切替** (コスト管理しつつ本物に触れる)
3. **golden dataset 最初の 5-10 件** (§3.3 DoD-6 土台)
4. **Streamlit Box UI 最小実装** (価値の表出)
5. **judge プロンプト外出し + A/B 比較可能化** (精度チューニング準備)
6. **README / ARCHITECTURE を Phase 1 完了版に更新**
7. **Firecrawl live 動作** (M2 完全通貫)

**Non-goals for Phase 2**: 本番 k8s 配備、Streamlit の凝った UI、多言語対応
