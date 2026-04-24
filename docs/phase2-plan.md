# Phase 2 実装計画 — Layer D統合 & DELIVERY_MODE=live

作成: 2026-04-24  
ステータス: 計画中 (ablaze の HOUJIN_BANGOU_APP_ID 取得待ち)

---

## 前提: Phase 1残件の引き継ぎ

Phase 1監査より最優先残件:

1. **`DELIVERY_MODE=live` 本実装** (コアバリュー未達)
2. **法人番号検証 live 確認** (PR #8 マージ後)
3. **golden dataset 整備** (5-10件)

---

## Phase 2 タスク一覧

### P0: DELIVERY_MODE=live (最重要)

```
services/delivery/pizza_delivery/providers/*_provider.py
└── make_llm() 本実装
    - browser_use.llm.ChatAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    - OpenAI / Gemini も同様

services/delivery/pizza_delivery/agent.py
└── judge_franchise() 本実装
    - browser_use.Agent(task=prompt, llm=llm).run()
    - prompts/judge.yaml から prompt 読み込み

services/delivery/pizza_delivery/server.py
└── RealDeliveryServicer 追加
    - DELIVERY_MODE=live で切替
```

### P1: 法人番号 live 確認 (PR #8 マージ後)

```bash
# HOUJIN_BANGOU_APP_ID 取得後に実行
go test ./internal/verifier/... -tags live -v
# → XML構造の推定が正しいか確認
# → 実際のAPIレスポンスを fixtures/houjin-sample.xml に保存
```

**確認ポイント:**
- ルート要素が `<corporateInfoList>` か (`<body>` の可能性あり)
- 法人名フィールドが `<name>` か (`<corporateName>` 等の可能性あり)

### P2: prompts/judge.yaml 外出し

```yaml
# prompts/judge.yaml (新規作成)
version: 1
rules:
  - evidence_urls が空なら confidence < 0.5 を強制
  - 「〜と思われる」「おそらく」は confidence -= 0.2
  - 店舗数は実測値(Google Maps)か公式サイト記載値のみ使用
  - 法人番号未検証の operator_name は UNVERIFIED フラグ

judge_prompt: |
  以下の情報を元に、この店舗の運営形態を判定してください。
  
  ## 入力情報
  - ブランド名: {brand}
  - 店舗名: {store_name}
  - 公式URL: {official_url}
  - Markdownコンテンツ: {markdown}
  - Google Maps抽出済み店舗数 (同一operator推定): {store_count}
  - 法人番号検証結果: {houjin_result}
  
  ## 判定ルール
  - 推測で回答してはならない。根拠となるページのテキストを引用すること。
  - 「会社概要」「店舗一覧」「採用情報」に記載された数値のみ使用。
  - 上記情報から判断できない場合は is_determinable: false を返す。
  
  ## 出力形式 (JSON)
  {
    "operator_name": "株式会社XXX",
    "is_franchise": true,
    "store_count": 42,
    "is_mega_franchisee": true,
    "confidence": 0.92,
    "evidence": "会社概要ページに「全国42店舗を運営」と記載",
    "evidence_url": "https://example.com/company",
    "is_determinable": true
  }
```

### P3: golden dataset 整備

```
test/fixtures/judgement-golden.csv
```

| operator_name | brand | store_count | is_mega | source_url | verified_date |
|---|---|---|---|---|---|
| 株式会社Fast Fitness Japan | エニタイムフィットネス | 1200+ | true | 公式IR | 2026-04-24 |
| (TBD) | ... | ... | ... | ... | ... |

最初の10件は人間が手動で確認したデータで固める。

### P4: oven.Pipeline への Verifier 統合

```go
// internal/oven/pipeline.go への追加
type Pipeline struct {
    Seed     SearchBackend
    Kitchen  KitchenBackend
    Judge    JudgeBackend
    Box      BoxStore
    Verifier *verifier.Client // 追加: nil なら skip
    Workers  int
}

// Bake() 内でJudge後に実行
if p.Verifier != nil && result.OperatorName != "" {
    vr := p.Verifier.Verify(ctx, result.OperatorName)
    result.CorporateNumber = vr.CorporateNumber
    result.IsVerified = vr.IsVerified
    result.OfficialName = vr.OfficialName
}
```

---

## 依存関係

```
P0 (live実装) ← ANTHROPIC_API_KEY (既存)
P1 (法人番号live) ← HOUJIN_BANGOU_APP_ID (ablaze取得待ち) ← PR #8マージ
P2 (prompts外出し) ← P0完了後
P3 (golden dataset) ← P0 + P1 完了後 (実データで作成)
P4 (Pipeline統合) ← P1完了後
```

---

## 完了定義 (Phase 2 DoD)

- [ ] `DELIVERY_MODE=live` で実際のLLMがoperator_nameを返す
- [ ] `pizza bake --verify-houjin` がGo verifierを経由して動く
- [ ] golden dataset 10件、judge精度 ≥ 70%
- [ ] `prompts/judge.yaml` がコードから外出し済み
- [ ] `config.yaml` に `name_similarity_threshold: 0.9` が定義済み

---

## 0.9ハードコード対応 (Phase 2スコープ)

ezalbaのレビュー指摘より。Phase 2で `config.yaml` に切り出す:

```yaml
# config/pizza.yaml
verifier:
  name_similarity_threshold: 0.9
  min_confidence_to_verify: 0.6
```

`verifier.Client` に `Config` フィールドを追加して読み込む。
