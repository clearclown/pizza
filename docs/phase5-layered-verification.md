# Phase 5+ 厳密化: 多層検証アーキテクチャ (2026-04-23)

> 「さらに厳密に。別 LLM API の導入を考え、批判やチェック役を設立する。
>  さらに AI だけではなく古典的アルゴリズムによる機械的な的確な制御もする」

## 🎯 設計方針

**原則**: LLM 1 基に全てを委ねない。決定論アルゴリズム (古典) を最前線に、LLM の抽出結果は別 LLM による批判役と外部 ground-truth でダブルチェックする。

```
┌─────────────────────────────────────────────────────────────┐
│  Input: Places Text Search response (fuzzy match を含む)    │
└──────────────────┬──────────────────────────────────────────┘
                   │
         ┌─────────▼──────────┐
         │ Layer A (Go, 古典)  │  ← 決定論: blocklist + 編集距離 + n-gram Jaccard
         │ brand filter        │     LLM を呼ばずに別ブランドを弾く
         └─────────┬──────────┘
                   │
         ┌─────────▼──────────┐
         │ Layer B (Python)    │  ← 決定論: HTML fetch + 正規表現
         │ EvidenceCollector   │     snippet を抽出 (推論しない)
         └─────────┬──────────┘
                   │
         ┌─────────▼──────────┐
         │ Layer C (Python)    │  ← LLM×2: cross-LLM consensus
         │ CrossLLMCritic      │     primary/critic 独立判定、合意率で confidence 調整
         └─────────┬──────────┘
                   │
         ┌─────────▼──────────┐
         │ Layer D (Python)    │  ← 決定論: 国税庁法人番号 API
         │ HoujinBangouClient  │     抽出された operator が実在するか外部照合
         └─────────┬──────────┘
                   │
         ┌─────────▼──────────┐
         │ Ledger (SQLite)     │  ← operator_stores + store_evidence
         └─────────────────────┘
```

---

## Layer A — 古典ブランドフィルタ (Go)

### ファイル
- `internal/dough/brand_filter.go` — 本体
- `internal/dough/brand_filter_test.go` — 22 ケースのユニットテスト

### 機能

1. **明示 blocklist** (`KnownBrandConflicts`): ブランドごとに「混入しがちな別ブランドの正規化名」を手動メンテする辞書。例:
   - `エニタイムフィットネス` → `["fitplace", "24gym", "whitegym", "vitalgym", "ファストジム", "addictgym", "amazingfitness", "willbefitness", "chocozap", "joyfit"]`
   - `スターバックス コーヒー` → `["ドトール", "タリーズ", "プロント", "エクセルシオール", "ベローチェ", "サンマルク", "コメダ"]`
   - コンビニ大手 3 社相互 blocklist も登録済

2. **編集距離 / bi-gram Jaccard** (`editDistance`, `bigramJaccard`): Unicode rune 単位の実装。
   - `brandSimilarityScore(brand, name) -> [0, 1]` で類似度を数値化
   - 現状は stdout 補助指標。将来的に confidence 算出に取り込む

3. **多層 `matchesBrand()`**:
   1. blocklist ヒット → 即 false
   2. 直接 substring → true
   3. 正規化後 substring → true
   4. 5 文字以上 prefix → true (長い公式名称の短縮対応)

### E2E で実証された効果

| ブランド | 修正前 (件) | 修正後 (件) | 混入 |
|---|---|---|---|
| エニタイムフィットネス | 69 | 62 | **0** (10 件前後の別 FC を除外) |
| スターバックス コーヒー | 79 | 79 | 0 |
| セブン-イレブン | 188 | 188 | 0 |

---

## Layer C — 別 LLM による批判役 (Python)

### ファイル
- `pizza_delivery/critic.py` — `CrossLLMCritic` + `CritiqueReport` + `agreement_rate`
- `tests/test_critic.py` — 7 ケース

### モデル

```python
@dataclass
class CritiqueReport:
    place_id: str
    primary: JudgeReply            # 判定 A (例: Gemini)
    critic: JudgeReply             # 判定 B (例: OpenAI)
    operator_agreement: bool        # operator 名が normalize 後一致
    operation_type_agreement: bool  # direct/franchisee/mixed/unknown 一致
    consensus_operation_type: str
    consensus_franchisor: str
    consensus_franchisee: str
    consensus_confidence: float     # 合意時=min(p,c)  / 不一致時=min(p,c)*0.5
    disagreements: list[str]
```

### 合意ロジック

- **operator 比較**: `pizza_delivery.normalize.operators_match` で `株式会社 XXX` / `㈱XXX` / `(株)XXX` を同一視
- 両方が空 ⇒ 合意 (unknown-unknown)
- 片方だけ空 ⇒ 不一致
- 両方値あり ⇒ `operators_match` 適用

- **operation_type 比較**: exact 一致必須

- **confidence 算出**:
  - 完全合意: `min(primary.confidence, critic.confidence)` (安全倒し)
  - 任一不一致: 上記 × 0.5 + operation_type=unknown 強制

### 使い方 (疑似コード)

```python
from pizza_delivery.providers import get_provider
from pizza_delivery.critic import CrossLLMCritic

primary = get_provider("gemini").make_llm(model="gemini-2.5-flash")
critic  = get_provider("openai").make_llm(model="gpt-4o-mini")

cr = CrossLLMCritic(
    primary_llm=primary, critic_llm=critic,
    primary_name="gemini", critic_name="openai",
)
report = await cr.critique(req)

if report.full_agreement:
    persist_to_ledger(report.consensus_operator)
else:
    flag_for_human_review(report.disagreements)
```

### live での推奨運用

**現行ペア (2026-04-23 実データで検証済)**:
- `primary = Gemini 2.5 Pro` (高精度・高速)
- `critic = Anthropic Claude Haiku 4.5` (独立プロバイダ、最安モデル)

実 API での smoke test 結果 (`RUN_LIVE_CRITIC=1 pytest tests/test_live_critic.py`):
```
primary(gemini):   op=direct  franchisor=スターバックス コーヒー ジャパン 株式会社  conf=0.90
critic(anthropic): op=direct  franchisor=スターバックス コーヒー ジャパン 株式会社  conf=0.95
full_agreement=True  consensus conf=0.90
```

- 合意したケースのみ `operator_stores` に永続化
- 不一致は `store_evidence` に `review_needed=true` で記録 (Streamlit Review タブで人間確認)
- 訓練コーパスが独立なので hallucination の相関が低い

---

## Layer D — 国税庁法人番号 API による実在確認 (Python)

### ファイル
- `pizza_delivery/houjin_bangou.py` — `HoujinBangouClient`, `HoujinRecord`, `verify_operator`
- `tests/test_houjin_bangou.py` — 10 ケース

### API 仕様 (国税庁公式)
- Endpoint: `https://api.houjin-bangou.nta.go.jp/4/name`
- 必須: `id` (application ID, 無料登録) / `name` / `type=12` (XML UTF-8) / `history=0`
- レスポンスの process コードで現存/消滅を判別
- 現存判定: `ACTIVE_PROCESS_CODES = {"01","11","12","13","21","22","31"}`
- 消滅 (吸収合併/解散): `71` / `72` → `active=False`

### `verify_operator(name, result)` の返り値

```json
{
  "exists": true,                          // active なヒットあり
  "name_similarity": 1.0,                  // canonical_key 比較 → 完全一致
  "best_match_name": "株式会社Fast Fitness Japan",
  "best_match_number": "1234567890123",
  "active": true
}
```

### Confidence への反映方針

```
operator exists in 法人番号 API, name_similarity >= 0.9   → +0.2
operator exists but similarity 0.6-0.9                    → +0.1
not found                                                  → -0.3 (false positive 警告)
inactive (消滅)                                            → -0.2
```

この値は Research Pipeline の `operator_stores.verification_score` として記録、mega_franchisees view で優先順位付けに使える。

### 環境変数
```bash
HOUJIN_BANGOU_APP_ID=xxxxxxxxxxx   # https://www.houjin-bangou.nta.go.jp/webapi/ で取得
```

---

## Layered Defense の理論

### なぜ 4 層必要か?

| 単層で起きる失敗 | 4 層後の振る舞い |
|---|---|
| Places API fuzzy で FIT PLACE24 が混入 | **Layer A** が blocklist で即除外 |
| 店舗ページに会社名表記なし | **Layer B** が空 evidence を返し、LLM は `unknown` を強制 |
| Gemini が誤った operator を hallucinate | **Layer C** の critic (OpenAI) が別回答を出し、合意率低下 → confidence 半減 |
| LLM 2 基が同じ「幽霊法人」を出力 | **Layer D** の法人番号 API で non-existent → flag |

### なぜ「AI だけではダメ」なのか?

- LLM は fuzzy matcher として優秀だが、**存在しないものを創る (hallucinate) の確率は常に非 0**
- BI ツールは「正しい確度 × 追跡可能性」が価値
- 古典アルゴリズムは遅くもないしコストもほぼ 0 — 前段で弾ける選択肢は弾くべき
- LLM は「抽出 + 要約」にだけ使う、「推論による boost」は禁止 (Phase 4 pivot で既に合意)

### なぜ別 LLM なのか (同じ LLM を 2 回叩くのでは駄目)

- 同じモデルの self-consistency は **同じバイアスを共有** するため、hallucination に対して無力
- 別プロバイダは訓練コーパスも異なるため、ノイズ相関が低い
- 本実装は Gemini × OpenAI を基本ペアとしつつ、API / model を差し替え可能 (Anthropic Claude も候補)

---

## テスト状況 (2026-04-23)

- Go 9 パッケージ all ok (`internal/dough` 22 ケースへ拡張)
- Python **143 passed** + 2 live skipped (RUN_LIVE_*=0 で gate)
  - `test_critic.py` 7 件
  - `test_houjin_bangou.py` 10 件
  - 既存 126 件 を回帰テストしながら追加

---

---

## Phase 5.2: 組織設計 (Expert Panel) — 2026-04-23 追加

> 「gemini に関しては flash の方が良かったり、flash をツールとして使用したり、
>  claude がクリティカルシンキングを用いて評価をしたりと組織設計を」
>
> 「Layer A との依存問題は、司令塔 (or 批判的司令塔) の claude に
>  判断させれば良いかもしれない」

### 3 者構成

```
Worker A (Gemini Flash, seed A)  ┐
                                  ├──→  Critic (Claude Haiku 4.5)
Worker B (Gemini Flash, seed B)  ┘      ・両 Worker の批判的評価
                                         ・KB hit の overrule 判断
Layer A KB conflict flags (info) ─┘      ・final verdict
```

- **Worker** = 抽出担当 (Gemini Flash ×2)
  - 安価 / 高速、独立 run で self-consistency
  - Gemini を 2 箇所別々で使う (同モデル別インスタンス or 別モデル)
- **Critic** = 評価担当 (Claude Haiku 4.5)
  - critical thinking prompt (`prompts/critic_v1.yaml`)
  - 出力: `verdict` (agree_both/prefer_a/prefer_b/both_wrong/uncertain) +
          `confidence_adjustment` + `kb_conflict_overridden`
- **KB (Layer A) は情報**、絶対 reject の主体ではない。
  Critic が evidence と照らして overrule できる

### 実装
- `pizza_delivery/panel.py` — `ExpertPanel` / `CriticJudgement` / `PanelVerdict`
- `pizza_delivery/claude_critic.py` — `ClaudeCritic` (Claude で critic prompt 実行)
- `pizza_delivery/prompts/critic_v1.yaml` — critic system + task prompt

### KB 依存度を下げた matchesBrand (Go 側)
`brandSimilarityScore` を主判定に昇格。KB は中間帯 (0.20-0.85) の曖昧ケースでのみ参照:
```
1. substring / 正規化 substring          → accept
2. brandSimilarityScore >= 0.85          → accept (KB 見ない)
3. brandSimilarityScore < 0.20           → reject (KB 見ない)
4. prefix 5 文字以上                      → accept
5. (中間帯のみ) KB conflict ヒット         → reject
6. それ以外                               → accept
```

### live 検証 (2026-04-23)

```
=== Live Panel Verdict (Starbucks 直営, evidence 明記) ===
worker_a (gemini-flash):   op=direct  franchisor=スターバックス コーヒー ジャパン 株式会社  conf=0.90
worker_b (gemini-flash):   同上                                                            conf=0.90
critic (claude-haiku-4-5): verdict=agree_both  preferred=both
                           critique=「Worker A、B 共に...完全一致。Evidence に「全店舗を
                                    直営で運営」と明記...推論の飛躍なし。KB conflict なし。」
                           adjust=+0.00
final: op=direct  conf=0.90

=== KB override test (ドトールビル名で誤 flag) ===
critic verdict: agree_both
kb_overridden: False  (Claude が「evidence に別ブランド言及なく、
                       スターバックス公式サイトからの抽出であるため conflict なし」と判断)
final op: direct
```
Claude が KB flag を絶対視せず evidence で正しく判断。

---

## Phase 5.3: 統合状況 (2026-04-23)

| Component | 実装 | cmd/pizza から | delivery-service gRPC | E2E (tag=integration) |
|---|---|---|---|---|
| M1 Seed (internal/dough) | ✅ | ✅ | — | ✅ |
| M2 Kitchen (internal/toppings) | ✅ | ✅ (optional) | — | ✅ (skip if firecrawl off) |
| M3 Delivery base (judge_franchise) | ✅ | ✅ --with-judge | ✅ DELIVERY_MODE=live | ✅ mock |
| M4 Box (internal/box) | ✅ | ✅ | — | ✅ |
| Layer A brand filter | ✅ | ✅ 自動 (dough.Searcher) | — | ✅ |
| Layer A KB (brand_conflicts.yaml) | ✅ | ✅ embedded | — | ✅ |
| Research Pipeline | ✅ | — (独立 CLI `python -m pizza_delivery.research`) | — | — (unit のみ) |
| CrossLLMCritic (Gemini×Claude) | ✅ | — | — | — (live gated) |
| Expert Panel (Gemini Flash×2 + Claude) | ✅ | — | ✅ **DELIVERY_MODE=panel** (NEW) | — (live gated) |
| Houjin Bangou API (Layer D) | ✅ | — | — (Research Pipeline のみ) | — |

### gRPC Panel mode
`DELIVERY_MODE=panel` で起動する `PanelDeliveryServicer` が追加された:
```bash
# delivery-service を panel で起動
cd services/delivery
DELIVERY_MODE=panel \
GEMINI_API_KEY=... ANTHROPIC_API_KEY=... \
PANEL_WORKER_A_MODEL=gemini-2.5-flash \
PANEL_WORKER_B_MODEL=gemini-2.5-flash \
PANEL_CRITIC_MODEL=claude-haiku-4-5 \
uv run python -m pizza_delivery

# Go オーケストレータから呼ぶ
./bin/pizza bake --query "..." --area "..." --with-judge
```

### テスト状況 (2026-04-23 時点)

| Layer | Tests | Coverage |
|---|---|---|
| Go internal/dough (brand filter + KB) | 22 | 86.0% |
| Go internal/menu (env config) | 8 | **100.0%** |
| Go internal/box (SQLite + migrations) | 14 | 79.4% |
| Go E2E (integration tag) | 1 | — (live gated) |
| Python pizza_delivery (total) | **175 passed + 6 live skipped** | **84% total** |
| - panel.py | 7 | 100% |
| - claude_critic.py | 7 | 96% |
| - critic.py (CrossLLMCritic) | 7 | 94% |
| - houjin_bangou.py | 10 | 84% |
| - research_pipeline.py | 9 | 96% |
| - providers/* (Anthropic/Gemini/OpenAI) | 16 | 100% |

---

## 次の作業 (Task #66, #67, #54, #64, #78, #79)

1. Layer D の live テスト (実 API ID を取得、RUN_LIVE_HOUJIN=1 で `株式会社Fast Fitness Japan` 実在確認)
2. Research Pipeline に Layer C/D を組み込み、`operator_stores.verification_score` 列を追加
3. Streamlit Review タブで Layer C 不一致 + Layer D 未検出を表示 (Layer L3)
4. マクドナルド / ファミリーマート で E2E 検証 (mixed / FC-only ブランドの blocklist 成熟度)
