# Megafranchisee Test Fixtures

`pizza megafranchisee` CLI の実行結果を **ゴールデンテスト用 CSV** として
固定化したスナップショット。事業会社主語のクロスブランド集計 (Phase 19/20)
の regression を検出する。

## `operators-2026-04-23.csv`

- 生成コマンド:
  ```bash
  ./bin/pizza migrate --with-registry
  ./bin/pizza megafranchisee --min-total 1 --min-brands 1 \
      --include-franchisor \
      --sort-by total \
      --top 0 \
      --out-csv test/fixtures/megafranchisee/operators-2026-04-23.csv
  ```
- 対象: `franchisee_registry.yaml` から seed した operator_stores 全件
  (known_franchisees + multi_brand_operators)
- 件数: **60 operators** (合計店舗数降順)
- 列:
  - `operator_name` 事業会社名
  - `total_stores` 全ブランド合計店舗数
  - `brand_count` 運営ブランド数 (多業態度)
  - `brands_breakdown` "ブランドA:N; ブランドB:M" 降順
  - `corporate_number` gBizINFO 法人番号 (13 桁、空欄可)
  - `operator_types` franchisee / franchisor / unknown
  - `discovered_vias` registry / registry_mbo / chain_discovery / per_store

## テスト期待値の性質

このスナップショットは:
- **決定論的** - registry YAML に変更がなければ全く同じ CSV が出る
- **法人番号はユニーク key** - 重複 row があれば registry のバグ
- **total_stores >= brand_counts の合計** - 突合整合性

## 更新方針

registry に operator 追加/削除/件数変更する度に再生成:

```bash
# before commit
./bin/pizza megafranchisee ... > test/fixtures/megafranchisee/operators-$(date +%Y-%m-%d).csv
```

日付付きで複数バージョンを残し、時系列で registry 成長を追跡する。
