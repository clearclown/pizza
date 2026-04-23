# Phase 9: 自走 `pizza scan` + 抽出バグ fix + 日本語 OSS 統合 (2026-04-23)

> 「データ品質を高めるクレンジングを工夫して。OSS 使用して良い」
> 「テストカバレッジを増やして徹底的にバグ潰し」
> 「自律的に命令に従うように — 例: 東京都のエニタイム運営会社リストアップを自走」

## 🎯 達成した 3 本柱

### 1. 自走 `pizza scan` コマンド
1 コマンドで **全 4 ステップ** を自動実行:

```bash
./bin/pizza scan --brand "エニタイムフィットネス" --areas "東京都" --cell-km 2.0
```

→ 内部で migrate (+registry seed) → bake (Places 全店舗) → research (per-store
operator 抽出) → audit (Top-down × Bottom-up 突合) を順次実行、最後にサマリ CSV。

ユーザー指示「東京都内のエニタイム店舗運営会社リストアップ」が **1 コマンド** で完遂。

### 2. 抽出バグ 2 件 fix (audit-context-building agent 解析)

#### バグ 1: Unicode dash でセブン-イレブンが途切れ
- **原因**: `_COMPANY_BODY_CHARS` が ASCII `-` のみ、U+2010 HYPHEN / U+2013 EN DASH 未対応
- **修正**: `r"...ー・\s&\-‐-―−"` に Unicode dash 範囲追加
- **結果**: `株式会社セブン‐イレブン・ジャパン` が正しく抽出

#### バグ 2: ファミマで 3 社連結誤抽出
- **原因**: suffix regex body が greedy で `株式会社` 自体を吸収、`株式会社A株式会社B株式会社` を 1 つと誤認
- **修正**: `_COMPANY_BODY_ATOM` で `(?!株式会社|㈱|\(株\))[...]` の lookahead atom を定義、body に「株式会社」が出現しないよう制御
- **結果** (実 research で確認):
  ```
  Before: 株式会社ファミマ・サポート株式会社クリアーウォーター津南株式会社
  After:  株式会社クリアーウォーター津南 (+ 個別社名を分離)
  ```

#### 追加: NFKC 正規化を find_company_names_in_snippet に導入
全角マイナス `－` / 全角英数 `ＡＢＣ` などを自動吸収。

### 3. 日本語クレンジング OSS 統合 (`neologdn` + `rapidfuzz`)

agent 調査結果から **推奨 A (neologdn + rapidfuzz)** を採用。

#### `normalize.py` 強化
- `normalize_operator_name`: `neologdn.normalize` を優先使用 (mecab-neologd 準拠、繰り返し記号・波ダッシュ等を広くカバー) → fallback で NFKC
- `_strip_kabushiki` 新規: 前後の `株式会社`/`有限会社` を剥がした **core 部分** を取り出し
- `operators_match` 4 段階化:
  1. 完全一致
  2. **core 完全一致** (`株式会社川勝商事` ⇔ `川勝商事株式会社` が同一判定)
  3. substring (≥3 文字)
  4. `rapidfuzz.token_set_ratio` ≥ 88 (空白違いの語順違い吸収) + `fuzz.ratio` on core ≥ 88

#### 新テスト
`tests/test_normalize_oss.py` (11 ケース):
- neologdn の全角/半角/繰り返し吸収
- rapidfuzz の部分類似度
- core ストリップによる prefix/suffix ゆれ吸収
- 完全に異なる会社の reject

## 📊 テスト数の推移

| Phase | Python tests | Go pkgs |
|---|---|---|
| Phase 7 終了時 | 206 | 9 ok |
| Phase 8 終了時 | 225 | 9 ok |
| **Phase 9 終了時** | **243 passed + 6 live skipped** | 9 ok |

Phase 9 で **+18 tests**:
- evidence バグ回帰 +7 (Unicode dash / 連結防止 / HTML ラベル strip)
- normalize OSS +11 (neologdn / rapidfuzz)

## 📈 Python カバレッジ (Phase 9 終了時)

| モジュール | カバレッジ |
|---|---|
| panel.py / providers/* | **100%** |
| chain_discovery.py | 99% |
| claude_critic.py | 96% |
| per_store.py | 96% |
| places_client.py | 96% |
| research_pipeline.py | 95% |
| match.py | 94% |
| critic.py | 94% |
| evidence.py | 91% |
| per_store.py + evidence.py 系 (抽出コア) | ≥91% |
| normalize.py | 87% |
| houjin_bangou.py | 84% |
| agent.py | 78% |
| server.py | 47% (gRPC、integration test 別) |
| research_cli.py / audit_cli.py | 0-10% (CLI 入口、e2e でカバー) |
| **Total** | **83%** |

## 🔧 変更ファイル

### 新規
- `cmd/pizza/main.go` (cmdScan) ※ 変更
- `services/delivery/pizza_delivery/audit_cli.py` (Phase 8 の一部)
- `services/delivery/tests/test_normalize_oss.py` (新規 11 テスト)
- `docs/phase9-autonomous-scan.md` (本書)

### 変更
- `services/delivery/pizza_delivery/evidence.py` (Unicode dash + non-greedy atom + NFKC)
- `services/delivery/pizza_delivery/normalize.py` (neologdn + rapidfuzz + core strip)
- `services/delivery/tests/test_evidence.py` (+7 回帰テスト)
- `pyproject.toml` (neologdn, rapidfuzz 追加)

### 依存追加
- `neologdn==0.5.6` (Apache-2.0)
- `rapidfuzz==3.14.5` (MIT)

## 🎬 実走行結果 (Anytime 新宿 scan)

```
🧭 pizza scan: brand="エニタイムフィットネス" areas="新宿" → var/output/scan/...
━━ [1/4] migrate --with-registry ━━
✅ migrated (schema + views)
🌱 seeded 0 registry rows  (既存と一致)

━━ [2/4] bake (各 area) ━━
   Cells: 9  Stores: 62  (Places で新宿エニタイム全件)

━━ [3/4] research (per-store operator 抽出) ━━
   [chain] found 1 operator groups, 10 / 10 with operator
   Operator                       Stores  Type       Conf
   株式会社Fast Fitness Japan        10   franchisor  0.65  (本部として正しく分類)

━━ [4/4] audit (Top-down × Bottom-up 突合) ━━
   bottom_up_total=62  franchisees=5  unknown_stores=62  missing=0

✅ scan done → var/output/scan/エニタイムフィットネス-20260423-121126.csv
```

## 残課題 / 次 phase 候補

| Task | 難度 |
|---|---|
| #79 Panel + Houjin の E2E 統合テスト | 中 |
| #54 Streamlit Review タブ (UI で人間確認) | 中 |
| **Phase 10**: `normalize-japanese-addresses` 採用 (住所を pref/city/town に分割) | 中 |
| **Phase 10**: `splink` で同名会社 disambiguation (法人番号ベース) | 大 |
| **Phase 11**: `pizza audit` 実 API で全国 run、coverage% 実測 | 中 (API 課金) |
| Registry 自動拡充 loop (unknown_stores → Web search → 追加) | 大 |

## 再現コマンド

```bash
# 1 コマンドで「東京都のエニタイム店舗運営会社リストアップ」
set -a; source .env; set +a
./bin/pizza scan --brand "エニタイムフィットネス" --areas "東京都" --cell-km 2.0

# より広く
./bin/pizza scan --brand "エニタイムフィットネス" \
    --areas "東京都,大阪府,愛知県,北海道" --cell-km 2.0

# 結果確認
sqlite3 var/pizza.sqlite \
    "SELECT size_class, operator_name, store_count FROM all_franchisees ORDER BY store_count DESC;"
cat var/output/scan/エニタイムフィットネス-*.csv
```
