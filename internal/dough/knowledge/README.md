# PI-ZZA ブランドフィルタ ナレッジベース

## 目的

BI ツールとしての精度を守るために、**Places Text Search の fuzzy match で混入した別ブランド** を ここに 永続蓄積する。いちど発見した混入は `brand_conflicts.yaml` に記録し、`go:embed` で binary に焼き込み、テストで回帰防止する。

これは「再発防止のためのナレッジベース」であり、コードの一部ではなく **データ資産** として扱う。

## ファイル

| File | Role |
|---|---|
| `brand_conflicts.yaml` | ブランドごとの「混入する別ブランド」の正規化パターン辞書 |

## 追加手順 (新しい混入ケースを発見したとき)

1. E2E で「brand X の検索に Y が混入した」ことを実データで確認
   ```bash
   ./bin/pizza bake --query "X" --area "..." --no-kitchen
   sqlite3 var/pizza.sqlite "SELECT name FROM stores WHERE brand='X' AND name NOT LIKE '%X%';"
   ```
2. 該当する `displayName` を `normalizeForBrandMatch` 相当の正規化で変換:
   - 小文字化
   - 空白 / 全角空白 / 中黒 / ASCII ハイフン を除去
   - 長音記号 `ー` は残す (音韻情報を壊さない)
3. `brand_conflicts.yaml` の `brands.<X>.conflicts[]` に新エントリを追加
   ```yaml
   - pattern: "正規化済み文字列"
     first_sighted: "YYYY-MM-DD"
     example: "生 displayName"
     area: "検出した地域"
     note: "どんな別ブランドか"
   ```
4. `internal/dough/brand_filter_test.go` の `TestIsKnownConflict_*` に最低 1 ケース追加
5. ビルド + テスト
   ```bash
   go build -o bin/pizza ./cmd/pizza
   go test ./internal/dough/...
   ```
6. 実データで再検証
   ```bash
   sqlite3 var/pizza.sqlite "DELETE FROM stores WHERE brand='X';"
   ./bin/pizza bake --query "X" --area "..." --no-kitchen
   # 該当 Y が混入していないことを確認
   ```

## スキーマ設計の意図

- `pattern` は **正規化済み** を要求する (実行時の正規化と完全一致させるため)
- `first_sighted` / `area` / `example` / `note` は将来的にデータ分析するためのメタデータ:
  - 「あるエリアだけ頻発する混入」の検出
  - blocklist の自動生成候補推薦 (頻度ベース)
  - LLM に blocklist の妥当性をレビューしてもらう batch に渡す input

## 運用ガイド

- `design-time` は開発時に一括投入した beforehand 類似ブランド (実混入確認ではなく、予防)
- `first_sighted=YYYY-MM-DD` は実混入が確認された日付。優先的に回帰テストする
- `conflict` は追加のみ、削除は「false positive (正当な店舗なのに除外してしまった)」が確認されたときに限る
- その場合は ADR 的に issue を立てて root cause を書く (消すだけでは再発する)

## Embed の仕組み

`internal/dough/brand_filter.go` で:

```go
//go:embed knowledge/brand_conflicts.yaml
var brandConflictsYAML []byte
```

ビルド時にバイナリに焼き込む。ランタイムのディスク依存はない。
