// Package box は PI-ZZA の SQLite 永続化層。
// Box 層の責務: operators / operator_brands / stores / nta_cache への読み書き。
// is_mega 判定は operator_totals VIEW が担当（Box 層はカラムに持たない）。
package box

// upsertOperatorBrands は stores テーブルを houjin_bangou で集約して
// operator_brands を更新するクエリ。
//
// INSERT OR REPLACE で冪等—再実行しても壊れない。
// verifier が houjin_bangou を確定した後に呼び出す。
const upsertOperatorBrands = `
INSERT OR REPLACE INTO operator_brands
    (houjin_bangou, brand_name, store_count, count_source, count_unit, confidence, updated_at)
SELECT
    houjin_bangou,
    brand_name,
    COUNT(*) AS store_count,
    'gmaps_cluster'  AS count_source,
    'franchisee'     AS count_unit,
    0.5              AS confidence,
    datetime('now')  AS updated_at
FROM stores
WHERE houjin_bangou = ?
GROUP BY houjin_bangou, brand_name
`

// megaQuery は operator_totals VIEW を使ってメガジー候補を取得するクエリ。
// 閾値 20 は VIEW 側で計算されるため、このクエリは変更不要。
const megaQuery = `
SELECT houjin_bangou, normalized_name, total_store_count, brand_count
FROM operator_totals
WHERE is_mega = 1
ORDER BY total_store_count DESC
`

// cleanNTACache は TTL 切れの nta_cache レコードを削除するクエリ。
// 起動時またはバッチで実行。
const cleanNTACache = `
DELETE FROM nta_cache WHERE expires_at < datetime('now')
`
