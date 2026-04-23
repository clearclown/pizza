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

## 絶対に守ること (再汚染防止)

- `franchisee_registry.yaml` を LLM で作成した YAML で直接編集しない
- Web search agent の出力を信頼せず、必ず:
  - gBizINFO 法人番号 → URL で実ページ存在確認
  - 店舗数 → 公式サイト or 決算資料で裏取り
  - ブランド加盟 → 公式 IR or 当該ブランドの FC 公表情報で確認
- この fixture は **pipeline の出力そのもの**。次回更新も pipeline から
  生成し、手編集しない
