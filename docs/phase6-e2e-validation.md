# Phase 6.3 E2E 実データ検証 (2026-04-23)

> 「マクドナルド (mixed 型) / ファミリーマート (FC メガジー) で
>  brand filter + Research Pipeline が機能するか確認」

## 検証環境
- Places API (New) v1 / area="新宿" / cell_km=1.0 / 25 cells
- Go `pizza bake` + Python `pizza research --max-stores 10 --no-verify`
- StrictBrandMatch: true (Layer A filter 有効)
- KB blocklist 済 (brand_conflicts.yaml): マクドナルド/セブン-イレブン/ファミリーマート/ローソン/スタバ/エニタイム

## マクドナルド

### brand filter (`pizza bake`)

```bash
sqlite3 var/pizza.sqlite "DELETE FROM stores WHERE brand='マクドナルド';"
./bin/pizza bake --query "マクドナルド" --area "新宿" --no-kitchen
```

| 指標 | 値 |
|---|---|
| Places Text Search hit | 53 件 |
| 混入 (マクドナルド/McDonald/マック どれも含まない) | **0** ✅ |
| 処理時間 | 5.6s |

- Layer A (KB + similarity + substring) で完全にブランド分離
- `モスバーガー`/`ロッテリア`/`バーガーキング`/`ケンタッキー` 等の KB 登録済 conflict パターンが効いた

### Research Pipeline (`pizza research`)

```
[chain] found 0 operator groups, 0 / 10 with operator
✅ Done in 3.2s  stores=10  with_operator=0  unknown=10  (no operators detected)
```

#### 原因分析: `_domain_root` が同一ホストしか見ない

- Places が返す `official_url` は `https://map.mcdonalds.co.jp/map/13764` のような
  **地図サブドメイン**
- Step 6.1 で追加した `/company/` fallback は `https://map.mcdonalds.co.jp/company/` を
  試すが、地図サブドメインに会社概要ページは存在しない
- 真の会社概要は `https://www.mcdonalds.co.jp/company/` (親ドメイン `www.*`) にある
  が、現行実装はホスト昇格 (`map.* → www.*`) を行わない

#### 未実装: 親ドメイン昇格 fetch (Phase 7 候補)

- `_domain_root` を拡張して `map.*` / `store.*` / `as.*` → `www.*` / apex の試行を追加
  - 例: `map.mcdonalds.co.jp` → `www.mcdonalds.co.jp`
  - 例: `store.starbucks.co.jp` → `www.starbucks.co.jp`
- さらに外部地図サイト (`as.chizumaru.com/famima/`) の対応は別設計必要
  (ブランドごとの「本家 URL」レジストリを持つ、etc.)

---

## ファミリーマート

### brand filter (`pizza bake`)

```bash
sqlite3 var/pizza.sqlite "DELETE FROM stores WHERE brand='ファミリーマート';"
./bin/pizza bake --query "ファミリーマート" --area "新宿" --no-kitchen
```

| 指標 | 値 |
|---|---|
| Places Text Search hit | **192 件** |
| 混入 (ファミマ/FamilyMart/ファミリーマート どれも含まない) | **0** ✅ |
| 処理時間 | 6.2s |

- FC メガブランドとして 192 店舗 (Plan 目標 100+ 達成)
- セブンイレブン/ローソン/デイリーヤマザキ/ミニストップ の KB が効いて混入 0
- 1 cell あたり平均 7.7 店舗、新宿のコンビニ密度を正しく反映

### Research Pipeline

未実行 (operator 情報は外部地図サイト `as.chizumaru.com/famima/` のためマクドナルドと同じ親ドメイン問題に直面)

---

## エニタイム / セブン-イレブン (回帰, 以前の結果と一致)

既存検証の再確認:
- エニタイムフィットネス: 59 件 → 62 件 (Places の日次変動)、混入 0
- セブン-イレブン 新宿: 188 件、混入 0

---

## まとめ

| ブランド | Places 結果 | 混入件数 | Research operator | 主要所見 |
|---|---|---|---|---|
| エニタイムフィットネス (FC) | 62 | 0 | **59/59 (Fast Fitness Japan)** | 店舗ページに operator 明示、理想的ケース |
| スターバックス (直営大手) | 79 | 0 | 0/10 | `store.*` サブドメイン、親サイトに昇格必要 |
| セブン-イレブン (FC 大手) | 188 | 0 | 未測定 | — |
| **マクドナルド (mixed)** | 53 | **0 ✅** | 0/10 (同上の親ドメイン問題) | brand filter は完全、Research は要親昇格 |
| **ファミリーマート (FC 大手)** | 192 | **0 ✅** | 未測定 (外部地図サイト) | brand filter は完全 |

### Layer A (brand filter) の評価: ✅ 完全

- 5 ブランド × 総 574 店舗で混入 **0 件**
- KB (blocklist) + 類似度スコア + substring 正規化の 3 段構成が機能
- Layer A は Phase 6 でも目標 100% 達成

### Research Pipeline (per-store operator 抽出) の課題

直営大手/FC 大手の多くは Places が **店舗検索サブドメイン** (`map.*`, `store.*`,
`as.*` 等、外部サービス) の URL を返す。per_store の現行 fallback は同一ホストに
固定されており、**親ドメインへの昇格 fetch が不足**している。

次の改善 (Phase 7 候補):
1. `_domain_root_candidates(url)` — `map.*` / `store.*` / `as.*` から `www.*` /
   apex への昇格候補を複数返す
2. 外部地図 vendor (chizumaru.com 等) の正規ドメイン辞書を `knowledge/` に追加
3. per_store の fallback loop を候補リストに対して回す

## 次アクション

- Task #66 本件は brand filter 検証として完了
- 親ドメイン昇格 fetch を新 Task として起票 (Phase 7)
- エニタイム operator_stores 広域拡張の live 検証 (Step 6.2 の動作確認) は別途

---

## Phase 7 Step 1 追記 (同日、親ドメイン昇格 + HTML ラベル除去)

E2E で発見した Research の限界 (地図サブドメイン問題) に即対応:

### 追加した決定論的強化

| 改善 | 対象ファイル | 効果 |
|---|---|---|
| `_domain_root_candidates(url)`: `map.*/store.*/as.*` → `www.*` 昇格候補 | `per_store.py` | スタバ/マクドナルドの本家 `/company/` に到達 |
| `_trim_at_particles` に verb 接尾辞 killer 追加 (`について`/`に関する` 等) | `evidence.py` | `"株式会社について紹介します"` 誤抽出を除去 |
| suffix pattern の body セパレータに HTML ラベル (`名称`/`会社名`/`商号`/`会社概要` 等) | `evidence.py` | 会社概要ページ HTML の「会社概要 名称 XXX 株式会社」型から XXX を切り出し |

### 改善後の抽出率

| ブランド | Before (Step 6.1 段階) | After (Phase 7 Step 1) |
|---|---|---|
| マクドナルド (新宿 10 店) | 0/10 unknown | **10/10 = 日本マクドナルド株式会社** ✅ |
| スターバックス (新宿 10 店) | 0/10 unknown | **10/10 = スターバックス コーヒー ジャパン株式会社** ✅ |
| エニタイム (既存) | 59/59 = Fast Fitness Japan | 59/59 (維持) |

これで PI-ZZA の原点「個別店舗の運営会社特定」が直営大手でも成立する。

### 残課題

- `as.chizumaru.com/famima/` のような外部地図サイトは親昇格だけでは本家に到達しない → ブランド辞書 (`knowledge/brand_domain_registry.yaml`) で補助する案 (Phase 7 Step 2 候補)
- operator_type は依然 `unknown`。direct/franchisee 判定は evidence に「直営」「加盟」等の明示があるかで再分類の余地 (Phase 5 既存ロジック)

## 再現コマンド

```bash
set -a; source .env; set +a
./bin/pizza bake --query "マクドナルド" --area "新宿" --no-kitchen
./bin/pizza bake --query "ファミリーマート" --area "新宿" --no-kitchen
./bin/pizza research --brand "マクドナルド" --max-stores 10 --no-verify
./bin/pizza research --brand "ファミリーマート" --max-stores 10 --no-verify
```
