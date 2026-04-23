# Phase 7 Step 3: 多ブランド Ground Truth + マイクロサービス整合性監査 (2026-04-23)

> 「メガジー以外をも集めたい、全てをリストアップしたい」
> 「Google Maps API や他マイクロサービスが使用できているか確認」
> 「他フランチャイズ店や事業会社の情報整理ができるか」
> 「テストデータはインターネット上で検索」

## 🎯 達成したこと

### A. Ground Truth DB を **1 ブランド → 8 ブランド** に拡大

ファクトチェック agent が **2024 年版「ビジネスチャンス」誌メガFCランキング 202 社** + gBizINFO + 公式プレスから検証済みのデータを収集。以下を `franchisee_registry.yaml` に追加:

| ブランド | 本部 (法人番号) | 登録 franchisee | 備考 |
|---|---|---|---|
| エニタイムフィットネス | Fast Fitness Japan | 5 社 | 既存、全て法人番号付 |
| **ファミリーマート** | 2013301010706 | 4 社 | JR九州リテール 212店舗 等 |
| **ローソン** | 2010701019195 | 4 社 | セーブオン 338店舗 等 |
| **マクドナルド** | 5011101033783 | 3 社 | クォリティフーズ 183店舗 等 |
| **モスバーガー** | (要確認) | 1 社 | モスストアカンパニー 209店舗 |
| ガスト | すかいらーく | 0 社 | **全店直営**、FC なし |
| サイゼリヤ | サイゼリヤ | 0 社 | **全店直営** |
| すき家 | ゼンショー | 0 社 | **全店直営** (地域子会社) |

### B. `all_franchisees` view の大幅拡充 (小-中-大 全規模リスト)

```
size_class  operator_name                             store_count  brands
----------  ----------------------------------------  -----------  ----------------
mega        株式会社セーブオン                        338          ローソン
mega        JR九州リテール株式会社                    212          ファミリーマート
mega        株式会社モスストアカンパニー              209          モスバーガー
mega        クォリティフーズ株式会社                  183          マクドナルド
mega        株式会社豊昇                              71           マクドナルド
mega        株式会社新鮮組本部                        70           ローソン
mega        エイ・ケイ・フランチャイズシステム        48           ファミリーマート
mega        株式会社Fusion'z                          39           ローソン
mega        株式会社タイレル                          34           ファミリーマート
mega        ヒロフーズ株式会社                        33           マクドナルド
mega        川勝商事株式会社                          20           エニタイムフィットネス
medium      株式会社アトラクト                        19           エニタイムフィットネス
medium      株式会社エムデジ                          17           エニタイムフィットネス
medium      株式会社アズ                              13           エニタイムフィットネス
medium      株式会社キノシタ                          12           ファミリーマート
medium      株式会社トピーレック                      5            エニタイムフィットネス
small       株式会社アズナス                          1            ローソン
```

**17 社のメガ/中規模フランチャイジー** を法人番号 + 本社所在地 + ソース URL 付で記録。

---

## 📊 マイクロサービス整合性監査 (監査 agent 結果)

| モジュール | 状態 | 備考 |
|---|---|---|
| **M1 Seed** (Google Places API New) | ✅ | `internal/dough/places.go` + CLI cmdBake で統合済 |
| **M2 Kitchen** (Firecrawl REST) | ✅ | docker / saas 両対応、pipeline で optional |
| **M3 Delivery** (gRPC Judge) | ⚠️ | JudgeFranchiseType OK、**BatchJudge は実装のみ呼び出し元なし** |
| **M4 Box** (SQLite) | ✅ | stores / operator_stores / markdown_docs / judgements / store_evidence |
| **Oven** (Orchestrator) | ✅ | pipeline.go で M1 → M2 → M3 → M4 を順次実行 |
| **Streamlit UI** | ✅ (本 update で拡張) | **Stores / Mega / All Franchisees / Franchisors / Judgements の 5 タブ** |

### API 互換性確認
- **Places API FieldMask**: Go + Python 両クライアントで **完全一致** (順序違いのみ)
- **Firecrawl**: v1 で統一
- **gRPC**: JudgeFranchiseType 疎通 OK、BatchJudge 未使用だが実装あり

### 見つかった未統合
- BatchJudge: sequential で足りているので Phase 3.5 候補
- `operator_stores` テーブル: Streamlit 未接続だったので **本 update で修正** (tab_all, tab_hq 追加)
- Phase 4 判定列 (franchisor_name / franchisee_name / operation_type): Judgements タブに未表示 (Phase 8 候補)

## 💡 ファクトチェック時に判明した業界知見

1. **メガ FC が実在するのはマクドナルド / ローソン / ファミマ / エニタイム**
2. **セブン-イレブンは個人オーナーモデル** — 公開 franchisee list がほぼ無い
3. **ガスト / サイゼリヤ / すき家は全店直営** — FC 概念適用外
4. **最強一次情報源**: 月刊「ビジネスチャンス」誌 (bc01.net) の年次ランキング 202 社
5. **セーブオンは 2026-03-01 にローソン本体吸収** — Registry に `valid_to` を記録する設計が必要

### Ground Truth 運用推奨
- `observed_at` / `valid_from` / `valid_to` のタイムスタンプ必須 (M&A 変動対応)
- 法人番号 (13 桁) での識別が**必須** (同名会社多数存在)
- 2+ 独立ソースで裏取り、単一ソースに依存しない

## 🛠 新 Streamlit UI タブ構成

```
🏪 Stores             — Places で取得した全店舗、地図 + CSV export
⭐ Mega Franchisees   — 20 店舗以上のメガ (従来)
🏢 All Franchisees    — NEW: mega/medium/small 全規模 + size フィルタ
🏛 Franchisors (本部) — NEW: 本部を別枠表示 (集計から除外したが閲覧可能)
🛵 Judgements         — LLM 判定履歴
```

実行: `uv run streamlit run cmd/box-ui/app.py`

## 🔄 next phase 候補

1. **抽出品質の追加改善** (Phase 8 候補)
   - セブン-イレブン research で `株式会社セブン` で途切れる (正: セブン-イレブン・ジャパン)
   - ファミマで複数社名連結誤抽出 (例: `株式会社ファミマ・サポート株式会社クリアーウォーター津南株式会社`)
2. **法人番号 API live 統合** — `HOUJIN_BANGOU_APP_ID` 取得後、不明番号を自動補完
3. **Streamlit Judgements タブに Phase 4 列追加** — franchisor_name / franchisee_name / operation_type
4. **Ground Truth の日付管理** — セーブオンの 2026-03 吸収のように `valid_to` 実装

## 再現コマンド

```bash
# 1. DB 初期化 + Ground Truth seed (1,250 rows)
./bin/pizza migrate --with-registry

# 2. 全加盟店リスト
sqlite3 var/pizza.sqlite "SELECT size_class, operator_name, store_count, brands \
  FROM all_franchisees ORDER BY store_count DESC;"

# 3. 本部一覧
sqlite3 var/pizza.sqlite "SELECT * FROM franchisors;"

# 4. Streamlit 起動 (5 タブ)
uv run streamlit run cmd/box-ui/app.py
```

## テスト状況
- Go 9 パッケージ all ok
- Python **206 passed + 6 live skipped**
- Ground Truth: **17 社 × 1,250 行** seeded successfully
