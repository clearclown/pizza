# 事業会社の定義 — PI-ZZA の核心 term

## 🎯 なぜこのドキュメントが必要か

README / 開発工程.md では「運営会社」「operator」という用語を使っていたが、
**フランチャイズ業界では "運営会社" が意味する対象が複数ある**。PI-ZZA の
コアゴールである「メガフランチャイジー特定」のためには、この区別を
プロンプト・データモデル・測定の三層で明示化する必要がある。

---

## 📖 用語定義

### 1. Franchisor (フランチャイザー / 本部)

- ブランドの権利を保有する会社
- 商標・ノウハウ・メニュー・内装基準を供与する側
- **例**: 株式会社セブン-イレブン・ジャパン、日本マクドナルド株式会社

### 2. Franchisee (フランチャイジー / 加盟店)

- franchisor とフランチャイズ契約を結び、**個別店舗を運営する会社**
- 同じブランドを掲げる複数店舗を **同一の franchisee が運営する** ことが多い
- **例**: 株式会社○○商事 (セブン-イレブンを 30 店舗運営)、株式会社△△フーズ (マクドナルドを 15 店舗運営)

### 3. 直営店 (Direct-operated store)

- franchisor 本体、または franchisor の完全子会社が運営する店舗
- この場合、"franchisee" という概念はない。**operator = franchisor**

### 4. 直営・FC 混在チェーン (Mixed)

- ブランド全体として直営店と FC 店が混在する
- **例**: マクドナルド (直営 ~70%, FC ~30%), 吉野家, コメダ珈琲店
- 個別店舗の運営形態は「本部直営」か「加盟店運営」かで分岐する

### 5. ⭐ メガフランチャイジー (Mega Franchisee) — PI-ZZA の検索対象

- **20 店舗以上** を運営する **franchisee** (加盟店会社)
- PI-ZZA のコアバリューはこれを特定すること
- franchisor (本部) は対象外 (メガジーとは呼ばない)

---

## 🧩 データモデル

### JudgeResult の意味論 (Phase 3.5 以降)

| フィールド | 意味 | 例 (セブン-イレブン 新宿東口店) |
|---|---|---|
| `operation_type` | 運営形態 | `"franchisee"` (= 加盟店運営) |
| `franchisor_name` | 本部会社名 | `"株式会社セブン-イレブン・ジャパン"` |
| `franchisee_name` | 加盟店運営会社名 (operation_type=franchisee のとき) | `"株式会社○○商事"` |
| `is_franchise` | 後方互換 (`operation_type != "direct"` 相当) | `true` |
| `operator_name` | 後方互換 (franchisee_name or franchisor_name) | `"株式会社○○商事"` (優先は franchisee) |
| `store_count_estimate` | その operator が運営する推定店舗数 | `30` |

### operation_type の取りうる値

- `"direct"` — 本部直営
- `"franchisee"` — 加盟店が運営
- `"franchisor_direct"` — 本部自体が直接運営 (franchisor=operator で FC 契約なし)
- `"mixed"` — 個別店舗の情報が不足、ブランド全体として混在を表明
- `"unknown"` — 情報不足で判定不可

### なぜ operation_type を 3 値以上にするか

Phase 2 まで `is_franchise: bool` で単純化していたが、以下が失われていた:

- **マクドナルドの新宿店が直営か FC か** は個別店舗の情報 (Markdown) がなければ判定困難
- Phase 3 live accuracy 97% 時点で **唯一の誤判定がマクドナルド** だったのは、
  golden で `true_is_franchise=true` としたが LLM は「約 70% 直営」を選んだ
- `operation_type="mixed"` を許容することで、**正直な不明判定** を可能にする

---

## 📊 メガジー集計ロジック (mega_franchisees view)

Phase 4 で SQL view を以下に改訂予定:

```sql
CREATE VIEW mega_franchisees AS
  SELECT
    franchisee_name AS operator_name,
    COUNT(*) AS store_count,
    AVG(confidence) AS avg_confidence,
    GROUP_CONCAT(DISTINCT brand) AS brands
  FROM judgements
  WHERE operation_type = 'franchisee'
    AND franchisee_name IS NOT NULL
    AND franchisee_name != ''
  GROUP BY franchisee_name
  HAVING COUNT(*) >= 20;
```

ポイント:
- `franchisor_name` ではなく **`franchisee_name`** で集計
- 複数ブランドを運営するメガジーを拾える (GROUP_CONCAT)
- `operation_type = 'franchisee'` フィルタで 直営 / mixed を除外

---

## 🔄 後方互換性

Phase 2 までのコードとデータは `is_franchise` / `operator_name` の 2 フィールドしか
持っていない。Phase 3.5 以降:

- `JudgeJSON` に新フィールドを追加、既存は残す (optional default)
- LLM が新フィールドを返さなくても `_derive_legacy_fields()` で
  operation_type → is_franchise、franchisee_name or franchisor_name → operator_name を導出
- SQLite スキーマに `operation_type` / `franchisor_name` / `franchisee_name` 列を追加
  (migrations で ALTER TABLE、既存 row は NULL 許容)

---

## 🧪 Accuracy 測定の観点も 3 軸に

Phase 3 時点: `is_franchise` の 2 値 accuracy のみ測定 → 97% 到達

Phase 4 以降:
1. **operation_type accuracy** — 4 値 (direct/franchisee/mixed/unknown) の一致率
2. **franchisor accuracy** — 企業名の一致 (表記揺れは normalize 後に比較)
3. **franchisee accuracy** — franchisee 特定率 (情報がある場合)

「**メガジーを特定できた率**」を最終指標に:
- golden に含まれる実在 franchisee が、予測結果でも同じ franchisee 名で返ったか
- 表記揺れは `internal/scoring/normalize.go` で吸収

---

## Non-goals

- 株主構成や持株比率 (SHFO = Standard Holding Form Ontology) の解析は扱わない
- M&A による運営会社変遷の追跡は扱わない (スナップショット判定のみ)
- 業務委託契約・直営子会社の厳密な区別は扱わない (直営扱い)
