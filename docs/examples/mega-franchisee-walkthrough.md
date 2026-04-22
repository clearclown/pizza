# メガジー検出 Walkthrough 🍕

PI-ZZA が本番でどのように**メガフランチャイジー (20 店舗以上の運営会社)** を
特定するか、エンドツーエンドで示すガイド。

## 前提

- `.env` に `GOOGLE_MAPS_API_KEY` が設定済 (Places API New 有効化済)
- (任意) `.env` に `ANTHROPIC_API_KEY` 等 LLM プロバイダの API キー
  → これがあれば `DELIVERY_MODE=live` で真判定、なしなら mock 判定

## シナリオ: 東京近郊のエニタイムフィットネス 運営会社を特定する

### Step 1: 店舗抽出

```bash
cd /Users/ablaze/Projects/pizza
set -a; source .env; set +a   # GOOGLE_MAPS_API_KEY を shell に投入
make build                    # ./bin/pizza をビルド

./bin/pizza bake \
  --query "エニタイムフィットネス" \
  --area "新宿" \
  --cell-km 1.0 \
  --no-kitchen
```

**出力 (5 秒):**
```
🍕 Baking: brand="エニタイムフィットネス" area="新宿" cell_km=1.0
   Kitchen disabled (skip Markdown fetch)
   Judge disabled (no --with-judge)

✅ Done in 5.4s
   Cells:       25
   Stores:      72
   ...
   CSV:  var/output/pizza-エニタイムフィットネス-新宿-...csv
   DB:   ./var/pizza.sqlite
```

SQLite に 72 店舗が格納された。

### Step 2: 判定 (mock モードで流れ確認)

```bash
# 別 shell で Python delivery-service を起動 (default mock)
cd services/delivery
uv run python -m pizza_delivery
# => 🛵 delivery-service (MOCK) listening on 0.0.0.0:50053

# 元 shell で --with-judge 付きで再実行
./bin/pizza bake \
  --query "エニタイムフィットネス" \
  --area "新宿" \
  --with-judge
```

mock では全店 `(mock) 株式会社モック運営` に紐付くので、**この段階では
メガジー検出の"意味"は出ない**。疎通確認のみ。

### Step 3: **実 LLM 判定で差がつく**

`.env` に `ANTHROPIC_API_KEY=sk-ant-...` を入れ、live モードで起動:

```bash
# delivery-service を live で再起動
DELIVERY_MODE=live LLM_PROVIDER=anthropic \
  uv run python -m pizza_delivery
# => 🛵 delivery-service (LIVE) listening on 0.0.0.0:50053

# CLI から再実行 (72 店舗分の LLM call が走る。数十秒〜数分)
./bin/pizza bake \
  --query "エニタイムフィットネス" \
  --area "新宿" \
  --with-judge
```

この時、各店舗について LLM が:
- 店舗名 + 公式 URL + (Markdown があれば) 会社概要
から `operator_name` を抽出する。エニタイムフィットネスは各店舗の
運営会社が異なる FC なので、**地域ごとに違う株式会社名** が返る。

### Step 4: メガジー抽出

```bash
sqlite3 var/pizza.sqlite <<'SQL'
SELECT operator_name, store_count, ROUND(avg_confidence, 2) AS conf
FROM mega_franchisees
WHERE store_count >= 5        -- 新宿近辺なので閾値を緩めに
ORDER BY store_count DESC
LIMIT 10;
SQL
```

**期待される出力 (実運営会社名は変動):**
```
operator_name          | store_count | conf
株式会社ファストフィット  | 12         | 0.88
株式会社スポーツ・ワン   | 9          | 0.82
株式会社フィットネス24    | 6          | 0.79
...
```

### Step 5: 可視化

```bash
uv run streamlit run cmd/box-ui/app.py
# => http://localhost:8501 を開く
```

UI で:
1. サイドバーで「エニタイムフィットネス」ブランドを選択
2. 「Mega Franchisees」タブで閾値を `5` に設定
3. 地図タブで 72 店舗の分布を確認
4. CSV ダウンロードでレポート納品

## 広域版: 東京都全体

```bash
./bin/pizza bake --query "エニタイムフィットネス" --area "東京都" --cell-km 2.0 --with-judge
```

東京都全域は ~400 セル (cell_km=2.0 で)、想定抽出数 ~150-250 店舗。
LLM call は 3-5 分、コスト目安 Claude Opus で ~$1-2。

## コストと所要時間の目安

| シナリオ | セル数 | 店舗数 | LLM calls | 所要時間 | コスト (Claude Opus) |
|---|---|---|---|---|---|
| 新宿 1km | 25 | 72 | 72 | 5s + 60s judge | ~$0.50 |
| 東京都 2km | 400 | 200-300 | 200-300 | 30s + 5min judge | ~$1.50 |
| 都心 6 区 1km | 150 | 500-700 | 500-700 | 1min + 10min judge | ~$3.00 |

`LLM_MODEL=claude-haiku-4-5-20251001` を使えば 1/10 のコストで同精度に近い結果。

## トラブルシュート

### 判定結果の operator_name が空

- Markdown 未取得 (Kitchen 未起動) の場合、LLM が会社概要にアクセスできない
- 解決: Firecrawl を立ち上げ `FIRECRAWL_MODE=saas` + API キーで実運用、
  または `ENABLE_BROWSER_FALLBACK=1` で low-confidence 時に browser-use Agent
  が公式サイトを訪問する

### 同じ FC チェーンで operator_name がばらつく

- LLM の出力ゆらぎ。表記揺れ (「株式会社A」vs「(株)A」) を正規化する後処理を
  入れる予定 (Phase 4: `internal/scoring/normalize.go`)

### `mega_franchisees` view が空

- 判定がまだ走っていない (`--with-judge` なしで bake した) か、mock モード
- `sqlite3 var/pizza.sqlite "SELECT COUNT(*), llm_provider FROM judgements GROUP BY llm_provider"` で確認

## 関連ドキュメント

- [phase3-plan.md](../phase3-plan.md) — Phase 3 の成功条件と作業順
- [phase1-audit.md](../phase1-audit.md) — Phase 1 完了後の初心回帰
- [architecture.md](../architecture.md) — モジュール間のデータフロー詳細
