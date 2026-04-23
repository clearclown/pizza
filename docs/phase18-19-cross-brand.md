# Phase 18 + 19: URL ドメイン二次 filter + 事業会社主語の横断メガジー集計 (2026-04-23)

## 🎯 初心に戻る

> ユーザー要求: 「1 ブランドだけ見て終わりではない。事業会社を主語に、
> モス 1 店舗しか持たないが BK を 20 店舗運営している、みたいなリストが欲しい」

つまり **「メガジー = 多業態多店舗 運営会社」** を見つけるには、
1 ブランド内の operator 頻度ではなく、**brand を跨いで 1 operator** を
集約した view が必要。

## Phase 18: URL ドメイン二次 brand filter

### 背景
Mos 東京都 scan で `https://www.burgerking.co.jp/` が `official_url`
として紐付いた store が 6 件見つかった。`displayName` ベースの Layer A
brand filter では検出不可 (Places 側登録データのノイズ)。

### 実装
`internal/dough/brand_filter.go`:
- `BrandForeignDomains` マップ (7 ブランド × 混入しやすい他ブランド domain)
- `hasForeignDomain(brand, officialURL) bool`

`internal/dough/searcher.go:matchesBrand`:
- 評価順 step 0 で `hasForeignDomain` を最初にチェック、該当なら即 reject

### カバー範囲 (初版)
```
モスバーガー ⇔ {burgerking, mcdonalds, kfc, lotteria}
マクドナルド ⇔ {mos, burgerking, kfc, lotteria}
エニタイムフィットネス ⇔ {fitplace, chocozap, joyfit}
スターバックス ⇔ {doutor, tullys}
セブン-イレブン ⇔ {family, lawson}
ファミリーマート ⇔ {sej, lawson}
ローソン ⇔ {sej, family}
```

### 効果
Mos 東京都: 6 件の BK URL 混入を決定論で除外。LLM 推論を一切使わない。

## Phase 19: Cross-brand operator aggregator

### 背景
既存 `registry_expander.aggregate_unknown_operators(brand=X)` は
**1 ブランド内** の集計しかできない。しかし:

- 大和フーヅ = モス 18 + ミスド 48 = 合計 66 店舗 (BC 2024 年 45 位)
- みちのくジャパン = モス 2 + ほっかほっか亭 + サーティワン (BC 12 位)
- フジタコーポレーション = モス 5 + 多業態 (BC 77 位)

これらの多業態メガジーは、1 ブランドだけ集計しても「2-18 店の中堅」に
しか見えない。**事業会社主語** で集計して初めて本当の規模がわかる。

### 実装
`services/delivery/pizza_delivery/registry_expander.py`:
- `CrossBrandOperator` データクラス (name, total_stores, brand_counts dict)
- `aggregate_cross_brand_operators(db_path, min_total_stores, min_brands, exclude_franchisor)`
  - operator_stores を `GROUP BY operator_name, brand` で一気に取得
  - 同一 operator の全 brand を dict に集約 → total_stores に加算
- `export_cross_brand_to_csv` / `export_cross_brand_to_yaml`
  - CSV: operator_name, total_stores, brand_count, brands_breakdown, corporate_number 等
  - YAML: operator-first 逆方向 index (既存 brand-first registry と補完関係)

### CLI
```bash
pizza megafranchisee \
    --min-total 5 \
    --min-brands 2 \          # 多業態だけ絞り込み
    --out-csv var/megajii/multi.csv \
    --out-yaml var/megajii/multi.yaml
```

### 初回実行結果 (2026-04-23、registry seed 14+Mos 10 社 = 合計 292 rows)
```
✅ 38 operators (min-total=2)
  418 店  1 業態  株式会社モスストアカンパニー (モスバーガー:418)
  338 店  1 業態  株式会社セーブオン (ローソン:338)
  183 店  1 業態  クォリティフーズ株式会社 (マクドナルド:183)
   67 店  1 業態  株式会社KOHATAホールディングス (エニタイム:67)
   18 店  1 業態  大和フーヅ株式会社 (モス:18)    ← 本当は + ミスド 48 で 66 店のはず
   ...
```

### 設計限界の再確認
現状は全 operator が「1 業態」と出ている。理由:

1. `franchisee_registry.yaml` が **brand-first** 構造で、1 operator を
   複数 brand 配下に書くと重複になる
2. 実際の bake/research は 1 ブランドずつ走るので、operator_stores に
   入るのも 1 operator = 1 brand しか蓄積されない
3. 同じ operator が別 brand でも登録されたときに初めて cross-brand で
   集約される

## Phase 20 候補 (本 phase 外)

### 20.1 operator-first YAML スキーマ
`franchisee_registry.yaml` に `multi_brand_operators:` セクション追加:
```yaml
multi_brand_operators:
  大和フーヅ株式会社:
    corporate_number: "5010401089998"
    head_office: 埼玉県熊谷市筑波3-193
    brands:
      モスバーガー: 18
      ミスタードーナツ: 48
    source_urls: [...]
    verified_via: [gBizINFO, BC2024]
```
migrate 時にこのセクションからも operator_stores へ seed すれば、
cross-brand 集計が正しく多業態として表示される。

### 20.2 BC 誌メガジーランキング全 202 位の YAML 化
bc01.net の 2024 年版を Web search agent にまとめて取得させ、
`multi_brand_operators:` に 200+ 社を seed する。これで 1 ブランド
scan だけでも「その operator は他に何をやっているか」が即座に分かる
(operator 名で JOIN)。

### 20.3 Multi-brand spider 強化
既存 Phase 15 の `multi-brand discovery` (operator の他事業発見) は
operator の公式 URL を scrape していたが、registry に operator が
登録されていないと動けない。Phase 20.2 の BC seed があれば、bake 後に
operator 名 match だけで他ブランド情報を引ける。

### 20.4 gBizINFO 横断 operator verify
BC 誌にない地方の中堅 FC も、gBizINFO で「事業目的にフランチャイズ 運営」
と書いてある法人を逆引きするアプローチ。API 制限があるので batch。

## 現時点のテスト統計

| 層 | Test 数 | 変化 |
|---|---|---|
| Go | 9 パッケージ all ok | + TestHasForeignDomain, TestMatchesBrand_rejectsForeignDomain |
| Python | **332 passed + 6 skipped** | 前 328 → **+4** (cross-brand aggregator 4 ケース) |

## Phase 19 のフォロー課題

- [ ] `franchisee_registry.yaml` に `multi_brand_operators:` section を追加
- [ ] `box/store.go` migrate seed を multi_brand_operators にも対応
- [ ] BC2024 メガジーランキング上位 50 位の seed (web search agent)
- [ ] `pizza megafranchisee` に `--sort-by brand_count` 追加 (多業態度でソート)
