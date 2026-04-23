# Phase 7 Step 2: 真のメガフランチャイジー特定 (2026-04-23)

> 「当初の目的をしっかりと思い直して — PI-ZZA の原点は
>  『個別加盟店運営会社 (事業会社) の特定』」

## 🎯 問題再定義

Phase 7 Step 1 までで、店舗公式ページから operator 名を抽出するパイプラインを完成
させたが、**抽出される名前が本部 (franchisor) ばかりで、真の加盟店会社
(franchisee) が取れていない** ことが判明した。

原因:
- エニタイム店舗ページは「株式会社Fast Fitness Japanは、日本のマスター
  フランチャイジーです。当店はフランチャイジーが運営します」と記載
- 個別の加盟店会社 (例: 川勝商事、アトラクト) は **店舗ページに公開されて
  いない** (FC 契約上のブランド統一感のため)
- mega_franchisees view が `Fast Fitness Japan × 59 店舗 ⭐` と **本部を誤って
  メガ扱い** にしていた

## ✅ 実装した軌道修正 (3 つ)

### (A) `operator_type='franchisor'` の検出 (pizza_delivery/per_store.py)

- `FRANCHISOR_MASTER_HINTS = ["マスターフランチャイジー", "フランチャイザー",
  "本部へのお問い合わせ", ...]` を追加
- extract() に分岐 (c') を追加: 本部ヒントがあれば operator_type を `franchisor` に
- confidence は 0.65 (direct=0.85 / franchisee=0.9 より低く、unknown=0.5 より高い)
- TDD 3 ケース (test_per_store_franchisor.py)

### (B) mega_franchisees view から franchisor を除外 (internal/box/migrations.sql)

```sql
CREATE VIEW mega_franchisees AS
  SELECT ... FROM operator_stores
  WHERE operator_name != ''
    AND COALESCE(operator_type, '') != 'franchisor'  -- NEW
  GROUP BY operator_name;

CREATE VIEW franchisors AS     -- NEW: 本部を別枠で見るための view
  SELECT operator_name, COUNT(DISTINCT place_id) AS found_at_store_count, ...
  FROM operator_stores
  WHERE operator_type = 'franchisor'
  GROUP BY operator_name;
```

### (C) Ground Truth Registry (internal/dough/knowledge/franchisee_registry.yaml)

ファクトチェック agent が確認した 5 社を **法人番号付きで** registry 化:

| 会社名 | 法人番号 (gBizINFO 検証済) | 本社 | 店舗数 |
|---|---|---|---|
| 川勝商事株式会社 | 1120001042383 | 大阪市福島区 | 20 |
| 株式会社アトラクト | 3180001058546 | 名古屋市中区 | 19 |
| 株式会社エムデジ | 6430001050730 | 北海道小樽市 | 17 |
| 株式会社アズ | 8120001073612 | 大阪市中央区 | 13 |
| 株式会社トピーレック | 4010601024954 | 東京都江東区 | 5 |

合計 74 店舗 (Fast Fitness Japan 本部はこれらの集計に含まない)。

`pizza_delivery/franchisee_registry.py` で YAML load + SQLite seed (TDD 3 ケース、
冪等性確認)。

**重要な訂正**: ユーザー入力の "トピークス" は誤記で、正式は **トピーレック**
(トピー工業グループ)。registry YAML の `misquoted_as: ["トピークス"]` で
将来の誤入力も吸収できる形に記録。

### (D) `pizza migrate --with-registry` で seed を CLI 化

```bash
./bin/pizza migrate --with-registry   # schema 更新 + registry seed
```

## 📊 Before / After

### Before (Phase 7 Step 1 まで)
```
Operator                Stores  Type        Mega?
株式会社Fast Fitness Japan      59   unknown     ⭐  ← 誤: 本部をメガ扱い
```

### After (Phase 7 Step 2)
```
=== mega_franchisees (真のメガジー) ===
川勝商事株式会社      20  franchisee  ⭐ メガ達成
株式会社アトラクト    19  franchisee
株式会社エムデジ      17  franchisee
株式会社アズ          13  franchisee
株式会社トピーレック   5  franchisee

=== franchisors (本部、別枠) ===
株式会社Fast Fitness Japan  20  (本部は集計から除外)
```

これで原点「20 店舗以上を運営する加盟店会社の特定」が成立。

## 🔬 Ground Truth 登録の運用ルール

1. **必ず 2 ソース以上で裏取り** (公式サイト + gBizINFO など)
2. **法人番号 (13 桁) を記録** — 同名会社との混同防止
3. **source_urls 複数記録** — 1 つが dead link でも復元可能
4. **verified_at / verified_via をメタ保存** — 監査トレール
5. 追加エントリは PR 化、少なくとも 1 人のレビュー必須

## 🪜 残課題 (次の phase 候補)

1. **他ブランドの registry 拡充**: マクドナルド / ファミマ / ローソンの
   major franchisee (例: マクドナルドは全店直営なので franchisee 無し、
   ファミマ / ローソンは小規模多数)
2. **Places 店舗 → registry 突合**: 店舗の住所 + 緯度経度 で known_franchisees
   の本社と地理的マッチング → 個別 place_id に franchisee を紐付け
3. **法人番号 API live 統合**: HOUJIN_BANGOU_APP_ID 取得後、Layer D で active
   法人を再検証
4. **オーバーライド UI**: Streamlit Review タブで人間が「この店舗は X 社運営」
   を手動登録

## テスト状況 (2026-04-23 Phase 7 Step 2 完了時)

- Go 9 パッケージ all ok
- Python **206 passed + 6 live skipped**
  - `test_per_store_franchisor.py` 3 件 (franchisor 検出)
  - `test_franchisee_registry.py` 3 件 (YAML load + seed + idempotent)

## 再現コマンド

```bash
set -a; source .env; set +a
./bin/pizza migrate --with-registry   # schema + 5社 seed (74 rows)
./bin/pizza bake --query "エニタイムフィットネス" --area "新宿" --no-kitchen
./bin/pizza research --brand "エニタイムフィットネス" --max-stores 20 --no-verify
sqlite3 var/pizza.sqlite "SELECT * FROM mega_franchisees ORDER BY store_count DESC;"
sqlite3 var/pizza.sqlite "SELECT * FROM franchisors;"
```
