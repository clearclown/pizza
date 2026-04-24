# Megafranchisee Pure-Pipeline Fixtures

**原則**: pizza pipeline (migrate → bake → research → audit → megafranchisee)
からのみ生成された CSV。LLM に YAML を書かせた結果を貼り付ける **ハード
コードは禁止**。

## `operators-pure-pipeline-2026-04-23.csv`

### 生成手順 (完全再現可能)
```bash
# Registry は pre-pollution state (3dac0f9) に復元済
rm -f var/pizza.sqlite

# delivery-service 起動 (asyncio 永続 loop 修正版)
cd services/delivery && ENABLE_BROWSER_FALLBACK=1 DELIVERY_MODE=panel \
    uv run python -m pizza_delivery serve --port 50053 &

# pipeline (bake は 10 分 timeout で打ち切り、research verify=True)
./bin/pizza scan --brand "モスバーガー" --areas "東京都" --cell-km 3.0 \
    --with-judge --judge-mode panel --with-verify \
    --max-research 300

# 集計 (本部除外)
./bin/pizza megafranchisee --min-total 1 --min-brands 1 --top 0 \
    --out-csv test/fixtures/megafranchisee/operators-pure-pipeline-2026-04-23.csv
```

### 出力サマリ
- 合計 **26 operators**
- うち Anytime 14 社 (Phase 10.3 既検証)
- ローソン 4 / ファミマ 4 / マクドナルド 3 / モス 1 (registry seed)
- 本部 (株式会社モスフードサービス、ドムドムフードサービス) は blocklist
  で自動除外

### 注意 (pipeline の既知制約)
- Mos 東京都 E2E で per_store 抽出は本部 (モスフードサービス 284 件) と
  別ブランド誤認 (ドムドムフードサービス 1 件) のみで止まる。**SPA 制約**
  により個別 FC 名は公開 HTML に存在しない。これは PI-ZZA のバグではなく
  Mos の情報開示水準の問題
- audit 突合で coverage% 向上には registry に FC を追加する必要があるが、
  **ハードコード貼り付け禁止** のため、今後は以下のいずれかで育てる:
  1. pipeline が per_store で実抽出した operator (本 CSV の chain_verified 列)
  2. 人間が手動でファクトチェックした操作会社 (法人番号 + 公式 URL 必須)
  3. Houjin API 連動の自動検証 (API キー取得後)

### 列定義
| 列 | 説明 |
|---|---|
| operator_name | 事業会社名 (正規化済) |
| total_stores | 全 brand 合計店舗数 |
| brand_count | 運営 brand 数 |
| brands_breakdown | "brand:N; brand:M" 降順 |
| corporate_number | gBizINFO 検証済の 13 桁法人番号 (空可) |
| operator_types | franchisee / franchisor / unknown |
| discovered_vias | registry / registry_mbo / chain_verified / per_store |

### 生成時の環境
- registry YAML: 3dac0f9 (Phase 10.3 直後の state、LLM 貼り付け汚染なし)
- delivery-service: asyncio 永続 loop + panel mode
- Places API: 実データ (Google Cloud key 設定済)
- CrossVerifier: anthropic (claude-haiku-4-5) で各 per_store 結果を検証

## `cross-brand-operators-2026-04-24.csv`

### 生成手順 (Phase 27: 人手 TSV ルート)
```bash
# Claude primary (Gemini quota 切れ時の自動 fallback 実装済)
LLM_PROVIDER=anthropic ./bin/pizza import-megajii-csv \
    --csv var/external/megajii-manual.tsv \
    --out var/phase27/orchestrate/import-apply.json

# ORM から 2+ 業態 operator 抽出
sqlite3 -csv -header var/pizza-registry.sqlite "
  SELECT oc.name, oc.corporate_number, oc.prefecture, oc.head_office,
         oc.website_url, COUNT(DISTINCT fb.id) AS brand_count,
         GROUP_CONCAT(DISTINCT fb.name) AS brands
  FROM operator_company oc
  JOIN brand_operator_link bol ON bol.operator_id = oc.id
  JOIN franchise_brand fb ON bol.brand_id = fb.id
  GROUP BY oc.id HAVING brand_count >= 2
  ORDER BY brand_count DESC, oc.name
" > test/fixtures/megafranchisee/cross-brand-operators-2026-04-24.csv
```

### 出力サマリ
- 合計 **126 operators** (2 業態以上、ORM ベース)
- 最大 **8 業態**: 株式会社プライムウィル (兵庫県芦屋、いきなりステーキ + コメダ + Gong cha + ミスド + かつや + 魅力屋 + 串家物語 + BABY FACE)
- 7 業態: 映クラ / エイコス / グローバルノースジャパン
- 3 業態: 大和フーヅ (モスバーガー + ミスタードーナツ + 築地銀だこ)、ありがとうサービス (ハードオフ + オフハウス + BOOKOFF) 等

### 列定義
| 列 | 説明 |
|---|---|
| operator | ORM canonical 名 (Gemini canonicalize + 国税庁 verify 済) |
| corp | 13 桁法人番号 (国税庁 CSV 検証済、空 = 未検証) |
| hq | 本社所在都道府県 |
| head_office | 本社所在地 (詳細) |
| url | 公式 HP URL |
| brand_count | 運営ブランド数 (franchisor 本部契約も含む) |
| brands | 運営ブランド名 (カンマ区切り) |

### クレンジング方式 (ハルシネ 0 設計)
- Stage 1: Gemini (primary) で canonical 化 — 法人格補正 / 表記揺れ吸収
- Stage 2: 国税庁 CSV (577 万件) で variant 検索 (株式会社X / X株式会社 / 有限会社X)、prefecture filter で絞り込み
- Stage 3: Claude critic が住所付き rerank で同名他社を弁別 (best_index=-1 なら reject)
- 429/quota 切れは `_LLMWithFallback` が runtime 検知して自動で primary→fallback 切替

## `fc-operators-unified-2026-04-24.csv` ⭐ 最包括 CSV

**1,037 link rows** — ブランド × 事業会社 の flat table。ORM の
`franchise_brand` × `brand_operator_link` × `operator_company` を結合した全件 dump。

### 生成手順
```bash
./bin/pizza integrate --mode export \
    --out test/fixtures/megafranchisee/fc-operators-unified-2026-04-24.csv
```

### 列定義
| 列 | 説明 |
|---|---|
| brand_name | ブランド名 (franchise_brand.name) |
| industry | 業種 |
| operator_name | 事業会社名 (canonical) |
| corporate_number | 13 桁法人番号 (国税庁 verified or 空) |
| head_office | 本社所在地 |
| prefecture | 本社所在都道府県 |
| operator_type | franchisor (本部) / franchisee (加盟) / unknown |
| estimated_store_count | 推定店舗数 (brand 単位) |
| source | data source タグ (jfa / manual_megajii_* / houjin_csv / edinet 等) |
| source_url | 出典 URL (空可) |
| note | 備考 |

### 使い所
- 「事業会社 X を運営しているブランドは?」 → operator_name で filter
- 「ブランド X を運営している事業会社は?」 → brand_name で filter
- 両方向のクエリが可能な双方向 index

## `megafranchisee-pipeline-2026-04-24.csv`

**66 operators** — pipeline DB (`pizza.sqlite` の operator_stores) を cross-brand
集計した結果。こちらは実店舗を観測できた operator に限る。

### 生成手順
```bash
./bin/pizza megafranchisee --min-total 1 --include-franchisor \
    --out-csv test/fixtures/megafranchisee/megafranchisee-pipeline-2026-04-24.csv \
    --top 0
```

## `megajii-raw-2026-04-24.csv`

**192 rows** — 人手 TSV (`var/external/megajii-manual.tsv`) を `pizza import-megajii-csv
--save-db` で SQLite 化した `megajii_rows` table をそのまま CSV dump。
LLM 処理を通していない**生データ snapshot**。

### 生成手順
```bash
./bin/pizza import-megajii-csv --csv var/external/megajii-manual.tsv \
    --save-db var/external/megajii.sqlite --dry-run
sqlite3 -csv -header var/external/megajii.sqlite \
    "SELECT line, section, raw_name, industry, store_count, representative,
            address, revenue_current_jpy, website_url, raw_brands, brand_name
     FROM megajii_rows ORDER BY line" \
  > test/fixtures/megafranchisee/megajii-raw-2026-04-24.csv
```

### 列定義
| 列 | 説明 |
|---|---|
| line | 元 TSV の行番号 (PRIMARY KEY) |
| section | megajii (多業態メガジー) / franchisor (本部) |
| raw_name | 元 TSV の企業名 (クレンジング前) |
| industry / store_count / representative / address | BC 誌掲載値そのまま |
| revenue_current_jpy | 当期売上 (千円 → 円に変換済) |
| raw_brands | 加盟ブランド (中点区切り) |
| brand_name | franchisor 行のみ、自社ブランド名 |

## `cleanse-by-brand-2026-04-24.csv`

**58 rows** — `pizza cleanse --brand <14 brand>` の JSON proposals を合算。
pipeline DB の operator_name を Gemini canonicalize → 国税庁 verify した結果の snapshot。

### 生成手順
```bash
LLM_PROVIDER=gemini ./bin/pizza cleanse --brand <brand> \
    --out var/phase27/orchestrate/cleanse/<brand>.json
# 14 brand 全実行後、JSON を merge して CSV 化 (scripts/merge-cleanse.py 相当)
```

### 列定義
| 列 | 説明 |
|---|---|
| brand | 対象ブランド |
| raw_name | pipeline operator_stores の元名前 |
| canonical | Gemini が正規化した canonical 名 |
| corp | 国税庁 CSV で verified された 13 桁法人番号 (空 = 未検証) |
| verified | True = 国税庁一致、False = 未一致 |
| reason | verified / not_in_houjin / not_legal_entity 等 |
| confidence | LLM 信頼度 (0-1) |

### 使い所
- LLM クレンジングの回帰テスト (同 DB / TSV で同結果が返るべき)
- verified=True の corp を pipeline `operator_stores.corporate_number` に back-fill したか検証

## `import-megajii-dry-proposals-2026-04-24.csv`

192 行 TSV (人手集計メガジー 179 + 本部 13) に対する import-megajii-csv dry-run の
全提案 snapshot。**verified=True** は国税庁法人番号で確定した 129 行、**False** は
未検証 (corp 空、略称・独自商号・登記非公開で CSV に一致無し) の 63 行。

### 使い所
- クレンジングパイプラインの回帰テストに使う (同 TSV 入力で同結果が返るはず)
- unverified 63 行は手動ファクトチェック候補

## 絶対に守ること (再汚染防止)

- `franchisee_registry.yaml` を LLM で作成した YAML で直接編集しない
- Web search agent の出力を信頼せず、必ず:
  - gBizINFO 法人番号 → URL で実ページ存在確認
  - 店舗数 → 公式サイト or 決算資料で裏取り
  - ブランド加盟 → 公式 IR or 当該ブランドの FC 公表情報で確認
- この fixture は **pipeline の出力そのもの**。次回更新も pipeline から
  生成し、手編集しない
