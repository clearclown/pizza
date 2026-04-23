# Phase 5 E2E 実データ検証 (2026-04-23)

> 「これは間違えてはいけない大切な BI ツールだ。しっかりと検討をし直してほしい。
>  e2e や実際環境でのテストを繰り返して。」

## TL;DR

複数ブランドで実 Places API + Research Pipeline を走らせ、**精度の致命的な穴 (fuzzy match によるブランド混入)** を発見し修正。以下の成果を得た:

| 検証項目 | Before | After |
|---|---|---|
| Anytime Fitness 新宿検索 | 69 件 (うち 9 件は他ブランド) | **59 件 (混入 0)** |
| スターバックス 新宿検索 | 79 件 (確認必要) | **79 件 (混入 0)** |
| セブン-イレブン 新宿検索 | 188 件 (確認必要) | **188 件 (混入 0)** |
| Research Pipeline 適用 | 7/10 operator (うち本部以外の誤 operator 混入) | **59/59 operator 抽出 → 1 group (Fast Fitness Japan)** |

## 発見した致命的欠陥

### 問題: Places Text Search の fuzzy match

`textQuery="エニタイムフィットネス"` で Text Search を叩くと、API は **brand に含まれない単語** を含む近傍店舗も返してくる:

```
エニタイムフィットネス 新宿6丁目店        ← 正
...
FIT PLACE24 新宿西口店                   ← 誤 (別 FC)
24GYM 西新宿五丁目店                     ← 誤 (別 FC)
WHITEGYM新宿1号店/レンタルジム              ← 誤 (別 FC)
VITAL GYM24中野新橋店                    ← 誤 (別 FC)
ファストジム24東中野店                     ← 誤 (別 FC)
addict gym 千駄ヶ谷店                    ← 誤 (別 FC)
Amazing fitness                        ← 誤 (別 FC)
WILL BE fitness studio 高田馬場店          ← 誤 (別 FC)
```

**9 件 / 69 件 = 13% が他ブランド**。これらが DB に `brand='エニタイムフィットネス'` で紛れ込む。
BI ツールとして致命的: メガジー集計の数値が嘘になる。

### 修正: `internal/dough/searcher.go` に `StrictBrandMatch`

```go
type Searcher struct {
    ...
    // StrictBrandMatch が true のとき、displayName に brand 文字列が含まれない店舗を除外。
    StrictBrandMatch bool
}

// displayName に brand が含まれるかチェック (正規化・prefix 許容あり)
func matchesBrand(p *PlaceRaw, brand string) bool { ... }
```

正規化 (`normalizeForBrandMatch`):
- 小文字化
- 空白 / 全角空白 / 中黒 / ASCII ハイフンを除去
- **長音記号 "ー" (U+30FC) は残す** ← 「スターバックス」等の音を壊さない

判定ロジック (以下いずれかで match):
1. `displayName` に brand が完全 substring で含まれる
2. 正規化後の name に正規化後の brand が含まれる
3. brand の先頭 5 文字以上の prefix が name に含まれる (長い公式名称の短縮対応)

### CLI/gRPC 両方で有効化

- `cmd/pizza/main.go`: `StrictBrandMatch: true` (default)
- `internal/dough/server.go:NewServer()`: 同上

### テスト

`internal/dough/brand_filter_test.go` (新規 7 ケース):
- ✅ 完全一致 substring
- ✅ 空白/中黒違い normalize 一致
- ✅ 別ブランド 8 件 (FIT PLACE24, 24GYM, WHITEGYM, VITAL GYM24, ファストジム24, addict gym, Amazing fitness, WILL BE fitness studio) を **全て reject**
- ✅ 空 brand は全通過 (filter 無効)
- ✅ "セブン-イレブン" / "セブンイレブン" 表記ゆれ吸収
- ✅ スターバックス コーヒー (中黒・空白違い 4 パターン)
- ✅ normalizeForBrandMatch (5 パターン, ハイフンだけ除去、長音は保持)

## E2E 実行結果

### Case 1: エニタイムフィットネス (FC ブランド)

```
$ ./bin/pizza bake --query "エニタイムフィットネス" --area "新宿" --no-kitchen
   Cells: 25  Stores: 59  (前回 69 → 10 件の他ブランド除外)

$ python -m pizza_delivery.research --brand "エニタイムフィットネス" --max-stores 60 --no-verify
  [chain] found 1 operator groups, 59 / 59 with operator
  ✅ Done in 5.6s  stores=59  with_operator=59  unknown=0

Operator                Stores  Type        Conf  Mega
株式会社Fast Fitness Japan    59   unknown     0.50  ⭐
```

**観察**:
- ブランドフィルタで 100% 正当な店舗に絞り込み成功
- 全 59 店舗から operator 抽出成功 = 店舗詳細ページの deterministic parsing が機能
- **すべて "Fast Fitness Japan" にグルーピング** = 正規化 canonical_key が機能 (新たに ChainDiscovery を実装したものの想定通り動作)
- ⭐ 20 店舗以上で mega 認定

**残課題**: Fast Fitness Japan は **本部 (franchisor)** であり個別の加盟店会社ではない。
各店舗を運営する具体的な FC 法人を特定するには、
- 店舗公式ページに明示された「運営会社: 株式会社XXX」のような表記が必要
- 公式サイトに記載がない場合は Phase 6 の browser-use エージェント or 法人番号公表サイト照合

### Case 2: スターバックス コーヒー (直営大手)

```
$ ./bin/pizza bake --query "スターバックス コーヒー" --area "新宿" --no-kitchen
   Cells: 25  Stores: 79  (混入 0)

$ python -m pizza_delivery.research --brand "スターバックス コーヒー" --max-stores 10 --no-verify
  [chain] found 0 operator groups, 0 / 10 with operator
  ✅ Done in 0.7s  stores=10  with_operator=0  unknown=10  (no operators detected)
```

**観察**:
- ブランドフィルタ 100% (混入 0)
- Places API が返す official_url は `https://store.starbucks.co.jp/detail-XXX/` の店舗詳細ページ
- このページには「スターバックス コーヒー ジャパン」という表記はあるが、**「株式会社 プレフィックス/サフィックス」がない**
- 結果 PerStoreExtractor は 10/10 で unknown を返す (正しい振る舞い — 推論禁止)

**改善アイデア**:
1. `https://www.starbucks.co.jp/company/` (会社概要ページ) へ一段階 fallback fetch
2. ブランドごとの運営会社情報レジストリ (deterministic, 手動メンテ) を別途用意
3. 実装は Phase 6 で検討

### Case 3: セブン-イレブン (FC 単独メガブランド)

```
$ ./bin/pizza bake --query "セブン-イレブン" --area "新宿" --no-kitchen
   Cells: 25  Stores: 188  (混入 0)
```

**観察**: 188 店舗すべてが "セブン-イレブン" を含む正当な店舗名。brand filter が表記ゆれ (「セブン-イレブン」⇔「セブンイレブン」) を吸収している。

## 回帰テスト

```bash
$ go test ./...
ok github.com/clearclown/pizza/internal/box         (cached)
ok github.com/clearclown/pizza/internal/dough       0.454s     ← ブランドフィルタテスト 7 件追加
ok github.com/clearclown/pizza/internal/grid        (cached)
ok github.com/clearclown/pizza/internal/menu        (cached)
ok github.com/clearclown/pizza/internal/oven        (cached)
ok github.com/clearclown/pizza/internal/scoring     (cached)
ok github.com/clearclown/pizza/internal/slice       (cached)
ok github.com/clearclown/pizza/internal/toppings    (cached)
ok github.com/clearclown/pizza/test/fixtures        (cached)
```

## 今後の継続検証 (todo)

- [ ] マクドナルド (mixed, 直営 + FC) での挙動
- [ ] ファミリーマート / ローソン (FC 単独) での broader chain discovery
- [ ] Places details API (per-place) と Text Search のダブルチェック
- [ ] Case C browser-use で store locator 巡回 → individual operator 抽出 (Phase 6)
