# Phase 4 方針転換 — Inference-based → Evidence-based

## 🚨 重要な方向転換 (2026-04-23)

ユーザーからの指示:

> 「llm は推論ではなく、データ抽出・データ整理・クレンジングなどのみに用いて。
> browser use や既存の OSS などを用い、極めて安定的に行えるようにして欲しい。
> 推論ではなく証拠とともに。」

この指示により、Phase 3 で達成した **v2 プロンプト 96.7%** や
Phase 4 Step 3-B の **v3 プロンプト 100%** は全て **「推論ベース (inference-based)」** として扱う。
これは LLM のブランド知識 (「コメダはだいたい FC」) に依存する設計であり、
**未知ブランドでは通用せず、新規店舗の運営会社を特定する原点のゴールに届かない**。

## 📐 新アーキテクチャ: Evidence-based

### 責務の再配分

| レイヤ | 役割 | 使うもの |
|---|---|---|
| **Data Acquisition** | 公式サイト訪問、HTML 取得、ページ遷移、Markdown 化 | **browser-use + Firecrawl** (既存 OSS) |
| **Data Extraction** | 取得済の raw テキストから「運営会社:」等のキーワード近傍を切り出す | **LLM (cleansing only)** |
| **Data Structuring** | 切り出したテキストを JudgeJSON に詰める | **LLM (structuring only)** |
| **Judgment** | 抽出された evidence から operation_type を決定 | **deterministic rule** (推論なし) |

### ⭐ LLM の使い道 (**推論禁止**)

**OK**:
- "運営会社: 株式会社AFJ Project" という raw snippet から "株式会社AFJ Project" を抽出
- "弊社100%子会社である○○が運営" → operator="○○"、operation_type="direct"
- 表記揺れ吸収: "(株)コメダ" → "株式会社コメダ"
- 空白正規化、Unicode 正規化
- 2 つの企業名が同じ法人を指すか判定 (似た名前のマージ)

**NG**:
- Markdown に情報がない状態で「コメダはだいたい FC だから franchisee」と推測
- ブランド名だけから operation_type を決める
- ブランド別リファレンスを system prompt に埋め込んで LLM にカンニングさせる

### Evidence の格納

`pb.JudgeResult.evidence` に **必ず 1 件以上** 入る:
```proto
message Evidence {
  string source_url = 1;   // 取得元 URL (例: https://example.com/company/about)
  string snippet = 2;      // 根拠となる raw テキスト (20-500 文字)
  string reason = 3;       // LLM が抽出した構造化根拠
}
```

evidence が空 → `operation_type="unknown"` 強制。判定は出さない。

## 🛠 実装変更点

### agent.py: judge_franchise の再設計

```python
async def judge_franchise(req: JudgeRequest, *, ...) -> JudgeReply:
    # Step 1: 証拠を収集 (browser-use または Firecrawl 必須)
    evidence = await _collect_evidence(req)  # List[Evidence]
    if not evidence:
        return _unknown_reply(req, "no evidence collected")
    
    # Step 2: LLM で evidence の構造化 (推論禁止)
    judge_json = await _structure_evidence(evidence, llm)
    
    # Step 3: deterministic rule で最終判定
    reply = _finalize(req, evidence, judge_json)
    return reply
```

### browser-use Agent の必須化 (mode="browser" default)

- `DELIVERY_JUDGE_MODE` default を `browser` に変更
- `llm-only` mode は **「推論」モードとして deprecated** (存続させるが warning ログ)
- 実装:
  - `_collect_evidence_via_browser()` — browser-use Agent が公式サイト訪問
  - `_collect_evidence_via_firecrawl()` — Firecrawl で Markdown 取得 (browser より軽量)
  - `_extract_operator_snippets(markdown)` — 「運営」「会社概要」「運営会社」近傍の N 行を抽出

### プロンプトの再設計

`prompts/judge.yaml` v4:
- ブランド別リファレンスを **全削除** (推論禁止)
- 「与えられた raw text から抽出」だけにタスク限定
- evidence_snippets をプロンプトに直接渡す
- 情報がなければ unknown を返すよう明示

## 📊 旧 metric との関係

Phase 3 v2: **96.7%** — 推論ベース、LLM のブランド知識依存
Phase 4 v3: **100% (op_type), 90% (franchisor)** — 推論ベース、同上

Phase 4 Step 3-C (本 pivot 後): **evidence-based accuracy** を新メトリクスとして測る
- 目標: operation_type evidence_coverage ≥ 80% (= 80% の店舗で browser-use が evidence を取れた)
- 目標: franchisor_name evidence-exact-match ≥ 80%
- 目標: franchisee_name evidence-exact-match ≥ 50% (browser-use による直接抽出)

**これが PI-ZZA の原点の "泥臭いリサーチ業務を自律ブラウジングで完遂" の定量化**。

## 🧭 残作業 (再優先順)

1. ⭐ Firecrawl self-host 解決 — evidence 取得の核心 (自力で nuq schema 対応)
2. browser-use Agent の実装 — 公式サイト訪問 + 運営情報抽出
3. agent.py の evidence-based 再設計
4. プロンプト v4 (推論禁止) 書き直し
5. live accuracy を evidence-based 指標で再測定
6. 人間レビュー UI で「LLM が出した evidence」を人間が確認できる状態に

## Non-goals (この pivot で明確に棄却)

- ❌ プロンプトにブランド知識を埋め込む改善
- ❌ Self-consistency (3 votes) — 推論の多数決は推論の延長線上
- ❌ Gemini Pro / Claude Opus など高性能モデルで精度上げる — 推論精度が上がるだけ
