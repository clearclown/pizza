# Phase 8: `pizza audit` — 1 ブランドの全加盟店 2 系統突合 (2026-04-23)

> 「そのフランチャイズ展開をしているブランドについて、全てを調べるツール」

## 🎯 達成

指定ブランド 1 つについて、Top-down (registry) と Bottom-up (Places API) を
3 段階決定論で突合し、**各加盟店会社の網羅率 (coverage%)** と **未突合店舗 /
未発見 operator** を CSV で出す。

## フロー

```
pizza audit --brand "エニタイムフィットネス" --areas "東京都,大阪府,愛知県,北海道" \
            --cell-km 2.0 --out var/audit/anytime.csv

  (A) Bottom-up                      (B) Top-down
  各 area で bake → stores           registry の known_franchisees 各社
  (Places Text Search +              × area_hint で search_by_operator
   strict brand filter)              → 候補 PlaceRaw
                │                             │
                └──────────── 突合 (merge_all) ──────────┐
                  1. place_id 完全一致
                  2. 住所 normalize + bi-gram Jaccard (≥0.7)
                  3. 緯度経度 Haversine (≤150m)
                                                ↓
                                     AuditReport → CSV × 3
```

## 新規ファイル

| ファイル | 役割 |
|---|---|
| `services/delivery/pizza_delivery/match.py` | 決定論 3 段階突合 (normalize_address / haversine / merge_all) |
| `services/delivery/pizza_delivery/audit.py` | BrandAuditor + run_audit + write_report_csvs |
| `services/delivery/pizza_delivery/audit_cli.py` | Python エントリ (`python -m pizza_delivery.audit_cli`) |
| `services/delivery/tests/test_match.py` | 14 ケース |
| `services/delivery/tests/test_audit.py` | 5 ケース |
| `docs/phase8-brand-audit.md` | 本書 |

## 変更
- `cmd/pizza/main.go` — `pizza audit` サブコマンド + `cmdAudit` + `runBakeForArea` helper

## CLI

```bash
# 0. 初回 seed (必要なら)
./bin/pizza migrate --with-registry

# 1. 複数エリアで bottom-up + top-down 突合 (実 API 課金発生)
./bin/pizza audit \
    --brand "エニタイムフィットネス" \
    --areas "東京都,大阪府,愛知県,北海道" \
    --cell-km 2.0 \
    --out var/audit/anytime.csv

# 2. 既存 stores で即座に dry-run (課金なし)
./bin/pizza audit --brand "エニタイムフィットネス" --areas "東京都" \
    --skip-bake --out /tmp/anytime.csv
```

### 主要フラグ
- `--brand` 必須 / `--areas` カンマ区切り / `--out` 出力 CSV
- `--skip-bake`: Bottom-up bake を skip、既存 stores のみで突合 (iteration 向け)
- `--addr-threshold` (default 0.7)、`--radius-m` (default 150)、`--cell-km` (default 2.0)

## 出力 (3 ファイル)

### メイン CSV
```csv
企業名,本部所在地,URL,登録推定店舗数,実発見数,coverage%,法人番号
株式会社アトラクト,愛知県名古屋市中区栄3-18-1...,https://www.attract-cc.com/fitness/index.php,19,1,5.26,3180001058546
...
```

### `*-unknown-stores.csv`
Bottom-up (stores) にあるが registry 突合に失敗した店舗全件 (place_id + 住所 +
lat/lng + official_url)。 **未登録 franchisee を発見するヒント** になる。

### `*-missing-operators.csv`
Registry にあるが Places `search_by_operator` で 0 件だった operator。
エリアを広げる必要があるか、登録 name に誤記があるか、実店舗が無いか。

## 初回動作確認 (`--skip-bake` 東京都)

```
✅ audit done in 2.0s  brand=エニタイムフィットネス  areas=['東京都']
   bottom_up_total=62  franchisees=5  unknown_stores=61  missing=0

企業名             Registered  Found  Matched  Coverage%
株式会社アトラクト           19     20        1      5.26
株式会社エムデジ             17      1        0      0.00
株式会社トピーレック           5      1        0      0.00
川勝商事株式会社             20      1        0      0.00
株式会社アズ                13      1        0      0.00
```

**解釈**:
- 新宿 62 店舗 × 東京都 area_hint では、名古屋本社の**アトラクト**が東京展開 1 店を突合
- 他 4 社は大阪/北海道中心展開で東京の bottom-up に該当店舗が少ない
- `unknown_stores=61` = 新宿の残 61 店は **registry 未登録 franchisee が運営** (Ground Truth 未網羅)
- **解決策 (2 方向)**:
  1. `--areas "東京都,大阪府,愛知県,北海道"` と広げて bake → matched 上昇
  2. unknown_stores から新しい franchisee 候補を Web search で拡充 → registry に追加

## 3 段階突合の意義

| 層 | 目的 | 威力 |
|---|---|---|
| (1) place_id 完全一致 | Google 側で同一店舗と認識されたもの | 最強、false positive なし |
| (2) 住所 normalize + bi-gram Jaccard | 〒 / 丁目 / 番地 の表記ゆれ吸収 | 中、`normalize_address` が要 |
| (3) 緯度経度 Haversine | 住所文字列が全く違う場合の救済 | 弱、同一ビル複数店に注意 (150m) |

## 回帰 / 追加テスト

- `test_match.py` 14 ケース (住所 normalize, place_id / address / proximity 単体 + merge_all)
- `test_audit.py` 5 ケース (coverage 計算, address fallback, missing brand, CSV 出力, dataclass)
- Go 9 パッケージ all ok (変更なし、Go 側は新 cmdAudit のみ追加)
- **Python 225 passed + 6 live skipped** (前回 206 → +19)

## 設計上のポイント

- **LLM 不使用**: 突合はすべて決定論。再現性と監査可能性を優先
- **冪等性**: `--skip-bake` で既存 stores に対して何度でも実行可能、CSV 上書き
- **段階適用**: merge_all が上から順に適用、前段でマッチしたものは後段から除外
- **副 CSV の活用**: unknown_stores は **registry 拡充の input**、missing_operators は
  **エリア指定見直しの input**

## 次 phase 候補

1. **unknown_stores から自動的に franchisee 候補を抽出** (既存 per_store extractor +
   /company/ fallback + ChainDiscovery で店舗公式ページを巡る)
2. **複数ブランドの横串 audit** (Phase 9): `pizza audit --brands A,B,C` で比較
3. **Streamlit UI tab_audit**: audit CSV を可視化 (coverage heatmap + 未突合 map)
4. **HOUJIN_BANGOU_APP_ID live 統合**: registry の corporate_number 未記入分を自動補完
