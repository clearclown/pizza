# Franchise Operator Fixtures

主要 CSV 群。すべて pizza pipeline から生成。手書きデータは
`megajii-raw.csv` のみで、これは「人手集計 TSV の SQLite dump」であり生成元
(`var/external/megajii-manual.tsv`) がユーザー提供 snapshot。

## ファイル構成

### `operator-centric-master-14brand-complete.csv` (527 rows, 2026-04-27)
**事業会社 1 行**を主キーに、14 brand の店舗数・検証状態・リスク・根拠 URL・求人候補
を横持ちした全量 master。未確認候補や 1 店舗候補も捨てず、
`quality_best_tier` / `risk_level` / `risk_flags_all` に明示する。失敗 URL や
未確認 URL も `all_evidence_urls` / `failed_or_unverified_urls` に同梱するため、
この 1 CSV だけでレビューできる。

```bash
uv run --project services/delivery python -m pizza_delivery.operator_master_export \
  --min-total 1 \
  --out test/fixtures/megafranchisee/operator-centric-master-14brand-complete.csv \
  --evidence-out /tmp/operator-centric-evidence-14brand-all.csv \
  --excluded-out /tmp/operator-centric-excluded-below1-14brand.csv
```

### 👑 `megajii-enriched.csv` (192 rows, 2026-04-24) — 人手 TSV master
ユーザー提供の人手集計 TSV を軸に、`import-apply.json` の cleanse 結果 + ORM
の brand_operator_link を JOIN した **最優先の master CSV**。

1 行 = 1 社 (megajii section 179 + franchisor section 13)。すべて 17 列。

### ⭐ `fc-operators-all.csv` (1,005 rows, 2026-04-27)
**1 事業会社 1 行** の集約 CSV。これがメインの参照資料。
同一 operator × canonical brand に複数 source がある場合は、店舗数の最大値だけを
`total_stores` に採用する (`manual_megajii` + `jfa_disclosure` の二重計上防止)。
同名 operator の法人番号あり/なし重複は CSV 生成時に 1 行へ畳み込む。

```bash
# 生成 (ORM 由来、pizza は内部の sqlite に書き込み済)
sqlite3 -csv -header var/pizza-registry.sqlite "
  WITH brand_norm AS (
    SELECT id AS brand_id,
           CASE name
             WHEN 'モスバーガーチェーン' THEN 'モスバーガー'
             WHEN '珈琲所コメダ珈琲店' THEN 'コメダ珈琲'
             WHEN 'コメダ珈琲店' THEN 'コメダ珈琲'
             ELSE name
           END AS brand_name
    FROM franchise_brand
  ),
  link_best AS (
    SELECT bol.operator_id, bn.brand_name,
           MAX(bol.estimated_store_count) AS brand_stores
    FROM brand_operator_link bol
    JOIN brand_norm bn ON bn.brand_id = bol.brand_id
    GROUP BY bol.operator_id, bn.brand_name
  ),
  source_rollup AS (
    SELECT operator_id, GROUP_CONCAT(DISTINCT source) AS sources
    FROM brand_operator_link GROUP BY operator_id
  )
  SELECT oc.name AS operator_name, oc.corporate_number AS corp,
         oc.prefecture AS hq_prefecture, oc.head_office,
         oc.representative_name AS representative, oc.website_url AS url,
         COALESCE(sr.sources, oc.source) AS source,
         COUNT(DISTINCT lb.brand_name) AS brand_count,
         GROUP_CONCAT(DISTINCT lb.brand_name) AS brands,
         COALESCE(SUM(lb.brand_stores),0) AS total_stores
  FROM operator_company oc
  JOIN link_best lb ON lb.operator_id = oc.id
  LEFT JOIN source_rollup sr ON sr.operator_id = oc.id
  GROUP BY oc.id
  ORDER BY total_stores DESC, brand_count DESC, operator_name
" > test/fixtures/megafranchisee/fc-operators-all.csv
```

列: `operator_name, corp, hq_prefecture, head_office, representative, url, source, brand_count, brands, total_stores`

### `megajii-enriched.csv` 列定義
| 列 | 説明 |
|---|---|
| line, section | 元 TSV の行番号 / (megajii/franchisor) |
| input_name | 元 TSV の企業名 |
| canonical_name | Gemini cleanse 後の canonical 名 (空なら input と同じ) |
| corp | 13 桁法人番号 (国税庁 verified、空なら未検証) |
| verified | True = corp 付き、False = 未検証 |
| hq_prefecture / head_office | ORM 経由で houjin JOIN 補完済 |
| representative | TSV 由来の代表者名 |
| declared_stores | TSV 原本の宣言店舗数 (BC 誌等) |
| revenue_current_jpy / revenue_previous_jpy | 当期 / 前期売上 (円) |
| website_url | 公式 HP |
| brands_raw | 元 TSV の加盟ブランド文字列 (中点区切り) |
| brands_orm | ORM で実際に link された brand list (パイプ区切り) |
| orm_brand_count | brands_orm の数 |
| orm_total_stores | ORM link の estimated_store_count 合計 |
| gap_stores | orm_total_stores − declared_stores (+ = ORM 過剰、- = 不足) |

### 生成手順 (再現)
```bash
# 1. 人手 TSV を SQLite 化
./bin/pizza import-megajii-csv --csv var/external/megajii-manual.tsv \
    --save-db var/external/megajii.sqlite --dry-run

# 2. 人手 TSV を LLM クレンジング + ORM 書込
LLM_PROVIDER=anthropic ./bin/pizza import-megajii-csv \
    --csv var/external/megajii-manual.tsv \
    --out var/phase27/orchestrate/import-apply.json

# 3. 3 DB JOIN + CSV 生成 (Python 小スクリプト、README 末尾参照)
```

### `jfa-disclosures.csv` (103 rows, 2026-04-26)
JFA 情報開示書面 index の live PDF link 一覧。HTML comment 内の旧 link は除外。
PDF 本文の店舗数は `pizza jfa-disclosure-sync --fetch-pdfs` で
`brand_operator_link.source = jfa_disclosure` として取り込む。

### `fc-links.csv` (1,432 rows, 2026-04-27)
**brand × operator の flat link table**。1 operator が複数 brand 運営なら複数行。
2026-04-27 追加の `operator_official_brand_link` は、operator 公式 HP の
事業/店舗/ブランドページ上の anchor だけを根拠にした evidence link
(開店告知・休業/閉店・求人/採用・社員インタビュー文脈は自動反映から除外)。
店舗数根拠は別ソースが必要なので `estimated_store_count=0` のまま保持する。

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

### 偏り / 不足 (2026-04-26 時点)
- **brand link 付き operator 801 / 20+店舗 operator 214 / 2+業態かつ20+店舗 128**
- **エニタイムフィットネス** 公表 957 店舗に対し ORM 33 operator (実態は 100-200 社)
- **モスバーガー** 公表 1,318 店舗に対し ORM 179 operator (JFA 開示書面 + pipeline 反映後)
- **空 prefecture 852/1,413 (60%)** — JFA disclosure 由来 franchisor が増えたため、houjin hydrate 余地あり
- **pipeline observed stores は関東偏重** (東京+神奈川+埼玉+千葉 で全 5,721 stores の 61%)

### 崩れ
- `pizza bench` 大阪 scan は Places API daily quota 切れで空振り
- `stores.address` 集計と公表店舗数の不一致あり (別業態店の誤拾 or place_id 重複)

### 再生成手順 (ゼロから再現)
```bash
pizza migrate --with-registry
pizza jfa-sync
pizza jfa-disclosure-sync --fetch-pdfs
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
