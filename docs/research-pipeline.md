# Research Pipeline — 人間リサーチャーの工程を複製する

## 🎯 ユーザーからの核心的な再定義

> 「徹底的に店舗ごとに調べたり、事業会社を一つ見つけたらその事業会社が運営しているものをも
> 芋蔓式に見つけたり、確定なる証拠を見つけて欲しい。人間がリサーチする工程のパイプラインの
> 複製すると考え直して」

### 人間リサーチャーが実際にやっている工程

1. **ブランド一覧取得** (ex: セブン-イレブンの東京都店舗)
   - Google Maps で全店舗を抽出 → PI-ZZA の M1 Seed 相当

2. **個別店舗の運営会社特定** (per-store deep dive)
   - ある 1 店舗の Google Maps URL/公式サイトを開く
   - "店舗詳細" ページで「運営: 株式会社○○」表記を探す
   - 分からなければ他の手がかり (店舗の問い合わせ先、電話帳、名刺写真 etc.) を探す
   - 記録: `(place_id, 確定した運営会社, 証拠 URL, 引用 snippet)`

3. **芋づる式 (operator-first の連鎖探索)**
   - 「あ、新宿6丁目店の運営は 株式会社AFJ Project か」
   - 「じゃあ AFJ Project が他にどこを運営しているか?」
   - AFJ Project の会社サイトを探し「運営店舗一覧」を見る
   - OR 「AFJ Project 店舗」で Google 検索
   - OR 会社概要に記載された全 store の一覧を取得
   - → operator → [store1, store2, ... storeN] のマップを構築

4. **クロス検証**
   - 発見した新 store が本当に Google Maps に存在するか確認
   - Places API で place_id を取り直し、運営会社記載をクロスチェック
   - 矛盾があれば warn + 人間レビュー

5. **メガジー判定**
   - operator ごとの確定 store 数が 20 以上なら "メガフランチャイジー"
   - 重要: **確定した operator** が集計対象。推測 operator は除外

## 🧩 新アーキテクチャ (component 図)

```
┌──────────────────────────────────────────────────────────────┐
│  M1 Seed (既存)                                               │
│  Places API で brand × area から 店舗 list を取得              │
└────────┬─────────────────────────────────────────────────────┘
         │ stores: [Store]
         ▼
┌──────────────────────────────────────────────────────────────┐
│  PerStoreExtractor ⭐ NEW                                    │
│  - 1 店舗の公式 URL から "その店舗の運営会社" を確定する      │
│  - 見つからなければ unknown (決して推測しない)                │
│  - 出力: Maybe(operator, confidence, evidence)                │
└────────┬─────────────────────────────────────────────────────┘
         │ (store, operator or None, evidences)
         ▼
┌──────────────────────────────────────────────────────────────┐
│  OperatorLedger ⭐ NEW                                       │
│  - operator_name → [confirmed_stores] のマップを保持          │
│  - SQLite に永続化 (既存 judgements + 新 operator_stores)     │
│  - duplicated place_id は 1 件として扱う (evidence は累積)    │
└────────┬─────────────────────────────────────────────────────┘
         │ (追加された operator)
         ▼
┌──────────────────────────────────────────────────────────────┐
│  ChainDiscovery ⭐ NEW (芋づる式)                            │
│  - 新しい operator が見つかるたびに呼ばれる                   │
│  - 1. operator のコーポレートサイトを検索/訪問                │
│  - 2. "運営店舗一覧" / "店舗情報" ページを探索                │
│  - 3. 発見した URL list を Places API で検索・マッチ          │
│  - 4. 新 store を OperatorLedger に candidate 登録            │
└────────┬─────────────────────────────────────────────────────┘
         │ candidate_stores
         ▼
┌──────────────────────────────────────────────────────────────┐
│  CrossVerifier ⭐ NEW                                        │
│  - candidate store の Google Maps Places API 照合             │
│  - 店舗 URL を fetch → 運営会社が期待の operator と一致するか │
│  - 一致 → confirmed として OperatorLedger に格納              │
│  - 不一致 → rejected + 理由 logging                           │
└────────┬─────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────┐
│  MegaFranchiseeReporter                                      │
│  - operator_stores view を読み、store_count >= 20 を抽出      │
│  - 各 operator について全確定 store と evidence URL を CSV に │
└──────────────────────────────────────────────────────────────┘
```

## 📋 データモデル拡張

### 新テーブル: `operator_stores` (operator → stores の確定マップ)

```sql
CREATE TABLE IF NOT EXISTS operator_stores (
    operator_name       TEXT NOT NULL,
    place_id            TEXT NOT NULL,
    brand               TEXT,
    confidence          REAL,
    discovered_via      TEXT,  -- 'store_page' | 'chain_discovery' | 'manual'
    confirmed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (operator_name, place_id),
    FOREIGN KEY (place_id) REFERENCES stores(place_id)
);
CREATE INDEX idx_operator_stores_name  ON operator_stores(operator_name);
CREATE INDEX idx_operator_stores_brand ON operator_stores(brand);
```

### 新テーブル: `store_evidence` (店舗ごとの証拠)

```sql
CREATE TABLE IF NOT EXISTS store_evidence (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    place_id            TEXT NOT NULL,
    evidence_url        TEXT NOT NULL,
    snippet             TEXT NOT NULL,
    reason              TEXT,
    keyword             TEXT,
    collected_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (place_id) REFERENCES stores(place_id)
);
CREATE INDEX idx_store_evidence_place ON store_evidence(place_id);
```

### 既存 `mega_franchisees` view を operator_stores ベースに切り替え

```sql
DROP VIEW IF EXISTS mega_franchisees;
CREATE VIEW mega_franchisees AS
  SELECT
    operator_name,
    COUNT(DISTINCT place_id) AS store_count,
    AVG(confidence) AS avg_confidence,
    GROUP_CONCAT(DISTINCT brand) AS brands
  FROM operator_stores
  GROUP BY operator_name;
```

## 🚀 実装フェーズ (Phase 5)

### Step A: PerStoreExtractor 実装
- `pizza_delivery/per_store.py` 新規
- `extract_operator(store: Store) -> Maybe(operator_name, evidences)`
- **ブランドレベル推論禁止**: store-specific 情報のみ採用
- fallback: browser-use Agent (JS-heavy 用)
- 単体テスト: mock fetch で各種 HTML パターンをカバー

### Step B: OperatorLedger + SQLite migration
- `internal/box/store.go` に:
  - `UpsertOperatorStore(ctx, operator, place_id, brand, conf, via)`
  - `UpsertStoreEvidence(ctx, place_id, url, snippet, reason, keyword)`
  - `QueryOperatorStores(ctx, operator) -> [(place_id, ...)]`
  - `QueryMegaFranchisees` を operator_stores ベースに更新
- migrations.sql に新テーブル追加

### Step C: ChainDiscovery 実装
- `pizza_delivery/chain_discovery.py` 新規
- `discover_stores_by_operator(operator: str) -> [CandidateStore]`
- 実装:
  - Places API で Text Search: "運営会社 operator_name" / "operator_name 店舗"
  - operator が企業サイトを持つか検索 (heuristic: "<operator_name> 公式" で Google → 最初の結果)
  - そのサイトで "店舗一覧" ページを発見 → store list 抽出
- 出力: candidate store の URL と住所

### Step D: CrossVerifier 実装
- `pizza_delivery/cross_verifier.py` 新規
- `verify(candidate_store, expected_operator) -> VerifyResult`
- Places API の Text Search で candidate の URL / 住所から place_id を引く
- その place_id の store page を fetch → PerStoreExtractor 呼び出し
- operator match 確認 → 一致なら confirmed

### Step E: Pipeline Orchestrator
- `internal/oven/research_pipeline.go` (または Python)
- BFS: operator queue → discover stores → verify → ledger に積み上げ
- termination: queue 空 or max_iterations
- 結果を CSV + SQLite に出力

### Step F: CLI 統合
- `pizza research --brand エニタイムフィットネス --area 東京都`
- M1 Seed → PerStoreExtractor → ChainDiscovery → Verifier → MegaFranchisees

## 🔒 鉄則 (この pivot で絶対守る)

1. **推論禁止**: ブランド名だけで operator を推測しない
2. **店舗単位の確定証拠**: 各 place_id に対応する evidence URL + snippet を記録
3. **芋づる式**: 1 operator 発見 → その operator の他店舗を積極的に探索
4. **クロス検証**: 発見 store は必ず Places API + store page で再確認
5. **人間が検証可能**: UI で「なぜこの operator と判定したか」を追跡できる

## Phase 4 の扱い

Phase 4 で作った物のうち、Phase 5 でも使えるもの:
- ✅ `EvidenceCollector` — 1 URL からの snippet 抽出は PerStoreExtractor の基盤
- ✅ `JudgeJSON v3` (operation_type, franchisor, franchisee) — 契約は維持
- ✅ Golden validator — 30 件の既存 golden は Phase 5 でも使える
- ✅ Streamlit UI — operator_stores ベースの新 view に差し替える
- ⚠️ `judge_by_evidence` — Phase 5 の PerStoreExtractor に段階的に吸収
- ⚠️ LLM 推論 prompt v2/v3 — Phase 5 では原則使わない (extraction-only)

## 成功指標 (Phase 5 完了時)

- 新宿で エニタイムフィットネス 30 店舗のうち、**operator を確定できた率 ≥ 50%**
- operator 判定のすべてに evidence URL + snippet が紐付いている
- 少なくとも 1 件の operator について "芋づる式" で他店舗発見 → 全体で 3+ stores 確定
- Streamlit UI で operator → [店舗] を辿れる
