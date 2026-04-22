// Package scoring はメガジー判定とスコアリングロジックを提供する。
//
// Phase 0: スタブのみ。Phase 1〜3 で Green 化。
package scoring

// MegaFranchiseeDefaultThreshold はメガジー判定のデフォルト店舗数閾値。
// 環境変数 MEGA_FRANCHISEE_THRESHOLD で上書き可能。
const MegaFranchiseeDefaultThreshold = 20

// IsMegaFranchisee は storeCount が threshold 以上なら true を返す。
//
// threshold が 0 以下の場合は MegaFranchiseeDefaultThreshold を使う。
// Phase 0: 未実装（常に false を返す）。
func IsMegaFranchisee(storeCount, threshold int) bool {
	_ = storeCount
	_ = threshold
	return false
}
