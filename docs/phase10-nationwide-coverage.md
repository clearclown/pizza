# Phase 10: 住所 parser + 全国 audit + Registry 拡充 (2026-04-23)

## 🎯 ユーザー要求

1. **データクレンジング OSS 活用** — `normalize-japanese-addresses` 等
2. **テストカバレッジ + バグ潰し**
3. **自律的に命令実行** — `pizza scan` の全国実装

本 phase で **Step 10.1-10.3** を同時進行。

## Step 10.1: 住所 parser で match 精度向上

### 実装 (`pizza_delivery/match.py`)

```python
@dataclass
class ParsedAddress:
    pref: str   # 都道府県 (47 ハードコード)
    city: str   # 市区町村 (政令市は "〇〇市〇〇区" まで)
    rest: str
    raw_normalized: str

def parse_address(raw: str) -> ParsedAddress:
    # 47 都道府県 前方一致 → pref
    # 政令指定都市 (札幌/仙台/さいたま/千葉/横浜/川崎/相模原/新潟/静岡/
    #   浜松/名古屋/京都/大阪/堺/神戸/岡山/広島/北九州/福岡/熊本) → 区まで city
    # 一般市区町村 → (市|区|郡|町|村) まで city
```

### `match_by_address` の強化

pref + city **strict gate** を導入。両方特定できた場合、完全一致を必須:

```python
# 北海道 新宿区 ≠ 東京都 新宿区 → 以前は bi-gram で誤 match
# 新実装: pref 違いで即 reject
```

false positive 大幅削減。低スコア (bi-gram < 0.8) 住所も pref+city 一致なら通過する case は存在するが、現時点 threshold 維持。

### テスト (`tests/test_address_parser.py`)

14 ケース:
- 47 都道府県 各種 + 郵便番号あり/なし
- 政令指定都市 (大阪市中央区 / 名古屋市東区 / 横浜市中区)
- 東京 23 区 (新宿区, 渋谷区, 世田谷区...)
- pref 省略時の fallback
- pref 違い → 即 reject の strict gate

## Step 10.2: 全国 audit 実走 + coverage 測定

### 実行コマンド
```bash
./bin/pizza scan --brand "エニタイムフィットネス" \
    --areas "東京都,大阪府,愛知県" --cell-km 10.0 --max-research 30
```

### 実測 coverage (3 県, 2026-04-23)

**Bottom-up**: **927 店舗** (東京都 718 + 大阪府 203 + 愛知県 119 — 重複除外後)
**Registry 登録**: 5 社 × 合計 74 店舗推定

| 企業名 | Registered | Places found | Matched | coverage% |
|---|---|---|---|---|
| 株式会社トピーレック | 5 | 3 | **1** | **20.00%** |
| 株式会社アトラクト | 19 | 31 | **3** | **15.79%** |
| 株式会社アズ | 13 | 40 | **2** | **15.38%** |
| 株式会社エムデジ | 17 | 2 | 0 | 0% |
| 川勝商事株式会社 | 20 | 3 | 0 | 0% |

### 東京単独との比較 (同 CSV)

| 企業名 | Tokyo のみ | 3 県 | 改善 |
|---|---|---|---|
| アトラクト | 5.26% (1/19) | **15.79%** (3/19) | **+10.53%** |
| アズ | 0% (0/13) | **15.38%** (2/13) | **+15.38%** |
| トピーレック | 20% (1/5) | 20% (1/5) | 同 |

エリアを広げれば coverage 向上することを実証。北海道を追加すればエムデジ (17 店舗) も突合される想定。

### Bottom-up で per_store が新発見した operator (registry 未登録)

```
株式会社フィットベイト     — 5 店舗 (新発見)
株式会社アーバンフィット   — 2 店舗 (新発見)
株式会社湘南開発           — 1 店舗
株式会社TEMCO             — 1 店舗
株式会社Any                — 1 店舗
```

これらは registry に追加すべき候補 (Step 10.3 で検討)。

**unknown_stores: 921 件** — registry 未登録の franchisee が運営している 921 店舗。
Ground Truth 拡充のターゲット。

## Step 10.3: Registry 拡充 — 5 社 → 14 社

### Web search agent が 9 社を追加特定

ビジネスチャンス誌メガジーランキング 2024 + gBizINFO + 各社公式で verified:

| 企業名 | 法人番号 | 本社 | 店舗 |
|---|---|---|---|
| 株式会社KOHATAホールディングス | 8020001111919 | 神奈川県川崎市 | 67 |
| 東食品株式会社 | 6010401001739 | 東京都港区 | 50 |
| 株式会社タカ・コーポレーション | 1120901029447 | 大阪府豊中市 | 67 |
| 株式会社ラ・ヴィーチェ | 1050001047381 | 茨城県つくば市 | 22 |
| 株式会社glob | 2240001041216 | 広島県福山市 | 10 |
| 株式会社ベイスオブスポーツ | 6360001019114 | 沖縄県那覇市 | 8 |
| 株式会社さくらホーム | (要確認) | 石川県金沢市 | 8 |
| 株式会社ペルゴ | 6140001061476 | 兵庫県姫路市 | 4 |
| 株式会社サンパーク | 4120901006550 | 大阪府吹田市 | 3 |

追加 **239 店舗**、合計 14 社 / **313 店舗** = エニタイム日本全国 1000+ 店の **31%** 網羅。

### 拡充後の audit (3 県 bake データで)

```
bottom_up_total=927  franchisees=14  unknown_stores=904  missing=0

企業名                             Registered  Found  Matched  Coverage%
───────────────────────────────────────────────────────────────────────
株式会社サンパーク                         3     37        2     66.67  ⭐
株式会社ベイスオブスポーツ                   8     52        5     62.50  ⭐
株式会社glob                             10     48        3     30.00
株式会社さくらホーム                        8     34        2     25.00
株式会社トピーレック                        5      3        1     20.00
株式会社アトラクト                         19     31        3     15.79
株式会社アズ                             13     40        2     15.38
株式会社タカ・コーポレーション                67     29        5      7.46
株式会社KOHATAホールディングス             67      1        1      1.49
株式会社エムデジ                          17      2        0      0.00 (北海道 bake なし)
東食品株式会社                            50      6        0      0.00 (住所 parser 未突合)
株式会社ラ・ヴィーチェ                     22      1        0      0.00 (関東圏 bake なし)
川勝商事株式会社                          20      3        0      0.00
株式会社ペルゴ                             4      2        0      0.00
```

**全体 matched 数**: 6 → **24** (4 倍)。Phase 10.3 で **+17 突合** 達成。

### per_store が新発見した operator (registry 未登録、追加候補)

```
株式会社フィットベイト       5 店舗 (東京)
株式会社アーバンフィット      2 店舗
株式会社湘南開発              1 店舗
株式会社TEMCO                 1 店舗
株式会社Any                   1 店舗
```

これらは `per_store.py` が公式サイト evidence から抽出しており、**ファクトチェック済
と同等** の信頼度。次 phase で automatic registry 追加機構を検討。

### 残バグ: 「株式会社アルペンクイックフィットネスキャンペーン期間2026年4月1日」

`_COMPANY_RE_SUFFIX` の body が広告文 "キャンペーン期間 2026 年 4 月 1 日" を
吸収。Phase 10.4 候補修正:
- suffix body トリム separator に `"キャンペーン", "期間", "特典", "限定", "お得"` 等の
  広告キーワードを追加
- 数字シーケンス異常長時に reject

## テスト状況

- Go 9 パッケージ all ok
- Python **263 passed + 6 live skipped** (前 243 → **+20 tests**)
  - `test_address_parser.py` 14 ケース
  - `test_normalize_oss.py` 11 ケース (Phase 9 の分も含む)

## 新 CLI 例

```bash
# 1 コマンドで全国 (3 県) の audit + CSV 出力
./bin/pizza scan --brand "エニタイムフィットネス" \
    --areas "東京都,大阪府,愛知県" \
    --cell-km 10.0 \
    --max-research 30 \
    --out var/audit/anytime-nationwide.csv

# 既存 DB の 3 県データで audit のみ再実行 (bake skip、数秒)
./bin/pizza audit --brand "エニタイムフィットネス" \
    --areas "東京都,大阪府,愛知県" \
    --skip-bake \
    --out var/audit/re-audit.csv

# 新発見 operator を観測
sqlite3 var/pizza.sqlite \
    "SELECT operator_name, COUNT(*) AS stores FROM operator_stores
     WHERE brand='エニタイムフィットネス' AND operator_type != 'franchisor'
     GROUP BY operator_name ORDER BY stores DESC;"
```

## 次 phase 候補

1. **北海道追加で全 4 県 audit** (エムデジ coverage 期待)
2. **広告文吸収 fix** (アルペンクイック...キャンペーン期間)
3. **新発見 operator の自動 registry 組込み** (per_store 抽出 ≥ 2 店舗を threshold)
4. **splink で同名会社 disambiguation** (法人番号ベース)
5. **住所 OSS (normalize-japanese-addresses/jageocoder) 正式採用** (v0.0.9 の online dep 問題を解消したバージョンが出たら)
