# Franchise Operator Fixtures

**4 CSV** (計 2,344 rows)。すべて pizza pipeline から生成。手書きデータは
`megajii-raw.csv` のみで、これは「人手集計 TSV の SQLite dump」であり生成元
(`var/external/megajii-manual.tsv`) がユーザー提供 snapshot。

## ファイル構成

### ⭐ `fc-operators-all.csv` (1,085 rows, 2026-04-24)
**1 事業会社 1 行** の集約 CSV。これがメインの参照資料。

```bash
# 生成 (ORM 由来、pizza は内部の sqlite に書き込み済)
sqlite3 -csv -header var/pizza-registry.sqlite "
  SELECT oc.name AS operator_name, oc.corporate_number AS corp,
         oc.prefecture AS hq_prefecture, oc.head_office,
         oc.representative_name AS representative, oc.website_url AS url,
         oc.source,
         COUNT(DISTINCT fb.id) AS brand_count,
         GROUP_CONCAT(DISTINCT fb.name) AS brands,
         COALESCE(SUM(bol.estimated_store_count),0) AS total_stores
  FROM operator_company oc
  LEFT JOIN brand_operator_link bol ON bol.operator_id = oc.id
  LEFT JOIN franchise_brand fb ON bol.brand_id = fb.id
  GROUP BY oc.id
  ORDER BY brand_count DESC, total_stores DESC
" > test/fixtures/megafranchisee/fc-operators-all.csv
```

列: `operator_name, corp, hq_prefecture, head_office, representative, url, source, brand_count, brands, total_stores`

### `fc-links.csv` (1,037 rows, 2026-04-24)
**brand × operator の flat link table**。1 operator が複数 brand 運営なら複数行。

```bash
./bin/pizza integrate --mode export --out test/fixtures/megafranchisee/fc-links.csv
```

列: `brand_name, industry, operator_name, corporate_number, head_office, prefecture, operator_type, estimated_store_count, source, source_url, note`

### `megajii-raw.csv` (192 rows, 2026-04-24)
**人手 TSV の SQLite dump**。LLM/クレンジング前の生データ snapshot。

```bash
./bin/pizza import-megajii-csv --csv var/external/megajii-manual.tsv \
    --save-db var/external/megajii.sqlite --dry-run
sqlite3 -csv -header var/external/megajii.sqlite \
    "SELECT line, section, raw_name, industry, store_count, representative,
            address, revenue_current_jpy, website_url, raw_brands, brand_name
     FROM megajii_rows ORDER BY line" \
  > test/fixtures/megafranchisee/megajii-raw.csv
```

### `operators-pure-pipeline-2026-04-23.csv` (26 rows, 前日 snapshot)
東京都 Mos 調査完走時点の pipeline-only メガジー。Phase 27 以前の状態保全。

## 現状の既知制約

### 偏り / 不足 (2026-04-24 時点)
- **operator 総数 1,085 は実態の推定 20-30%**。BC 誌 top 500 FC 運営会社のうち ORM 化は約 1/5
- **エニタイムフィットネス** 公表 957 店舗に対し ORM 29 operator (実態は 100-200 社)
- **モスバーガー** 公表 1,266 店舗に対し ORM 18 operator (Phase 21 既知 13 社 + 他)
- **空 prefecture 524/1,085 (48%)** — houjin JOIN で一部補完済、残は法人番号未取得 operator
- **pipeline observed stores は関東偏重** (東京+神奈川+埼玉+千葉 で全 5,721 stores の 61%)

### 崩れ
- `pizza bench` 大阪 scan は Places API daily quota 切れで空振り
- `stores.address` 集計と公表店舗数の不一致あり (別業態店の誤拾 or place_id 重複)

### 再生成手順 (ゼロから再現)
```bash
pizza migrate --with-registry
pizza jfa-sync
pizza houjin-import --csv <国税庁 zip>
pizza import-megajii-csv --csv var/external/megajii-manual.tsv \
    --save-db var/external/megajii.sqlite
pizza cleanse --brand <14 brand 全て>
pizza integrate --mode run
pizza integrate --mode export --out fc-links.csv
# fc-operators-all.csv は ORM 直接 SQL
```

## 絶対に守ること

- `franchise_brand` に「企業名」「外食」「食品製造販売」等の業種名を入れない
  (JFA scrape の header 誤取り込み対策、purge 済)
- 手書きで YAML/CSV に operator を追加しない。必ず pipeline 経由で
- corporate_number 空 operator は `source` に `_unverified` を付ける
