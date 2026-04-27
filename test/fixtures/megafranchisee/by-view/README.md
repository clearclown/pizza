# 事業会社派生リスト (by-view)

`megajii-enriched.csv` / `fc-operators-all.csv` を起点に、**視点別** に切り出した
派生 CSV 群。すべて ORM (`pizza-registry.sqlite`) + pipeline (`pizza.sqlite`)
の SQL JOIN で生成、pizza コマンド (or sqlite3) で再現可能。

## ファイル

### `tokyo-entering-operators.csv` (32 社)
東京都内に pipeline で 1+ 店舗を観測できた事業会社一覧。本社所在地不問。
地方本社 (大和フーヅ 埼玉 / ありがとう 愛媛 等) も東京進出店があれば含まれる。

**生成**:
```sql
ATTACH 'var/pizza.sqlite' AS pipe;
SELECT oc.name, oc.corporate_number, oc.prefecture, oc.head_office,
       COUNT(DISTINCT fb.name) AS orm_brand_count,
       GROUP_CONCAT(DISTINCT fb.name) AS brands,
       (SELECT COUNT(DISTINCT s.place_id) FROM pipe.stores s
        JOIN pipe.operator_stores os ON os.place_id = s.place_id
        WHERE os.operator_name = oc.name AND s.address LIKE '%東京都%') AS tokyo_stores_observed
FROM operator_company oc
LEFT JOIN brand_operator_link bol ON bol.operator_id = oc.id
LEFT JOIN franchise_brand fb ON bol.brand_id = fb.id
WHERE EXISTS (...) GROUP BY oc.id ORDER BY tokyo_stores_observed DESC;
```

### `megajii-ranking.csv` (22 社)
ORM で **14 対象ブランド内の店舗数根拠あり 2+ 業態** かつ
target total_stores 20+ の operator を total_stores / brand_count 降順。
brand alias は CSV 集約時に canonical 化し、同一 operator × brand の複数
source は最大店舗数だけを採用する。コンビニ / 自動車用品 / その他外食など
14 対象外ブランドは `brands` と `total_stores` から除外する。
`operator_official_brand_link` のような 0 店舗 evidence は、業態 evidence として
保持するが、この厳密ランキングの `brand_count` には入れない。

### `by-brand/<brand>.csv` (14 ファイル)
各 brand の FC 運営会社 list。店舗数 (declared) 降順。ORM の brand_operator_link
由来 (source タグ: jfa / manual_megajii / pipeline 等)。

| brand | 社数 (link rows) |
|---|--:|
| TSUTAYA | 21 |
| シャトレーゼ | 18 |
| モスバーガー | 179 |
| 業務スーパー | 37 |
| エニタイムフィットネス | 28 |
| コメダ珈琲 | 33 |
| オフハウス | 12 |
| Itto個別指導学院 | 7 |
| ハードオフ | 14 |
| カーブス | 97 |
| アップガレージ | 4 |
| Kids Duo | 4 |
| Brand off | 5 |
| カルビ丼とスン豆腐専門店韓丼 | 10 |

### `unverified-63-focus.csv` (63 社)
人手 TSV の **未 verified** (corp 空) 63 社のフォーカスリスト。手動確認 / 再検索
候補。`reason_hint` 列で失敗要因を分類。

| reason_hint 分類 | 傾向 |
|---|---|
| 国税庁 exact miss | LLM が variant を生成しきれなかった or pref 不一致 |
| 公式 HP 無 | 手動確認が必要 (gBizINFO etc) |
| canonical 化後も国税庁 miss | 屋号のみ / 小規模 / 登記なし |

## 使い所
- 特定 brand / 特定 pref / 特定 focus のリストが即見られる
- 派生元 (`megajii-enriched.csv` / `fc-operators-all.csv`) に対する cross-check
- 手動レビュー候補を絞り込みやすい形で提供
