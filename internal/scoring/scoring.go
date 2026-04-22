// Package scoring はメガジー判定とスコアリングロジックを提供する。
package scoring

// MegaFranchiseeDefaultThreshold はメガジー判定のデフォルト店舗数閾値。
// 環境変数 MEGA_FRANCHISEE_THRESHOLD で上書き可能。
const MegaFranchiseeDefaultThreshold = 20

// IsMegaFranchisee は storeCount が threshold 以上なら true を返す。
//
// threshold が 0 以下の場合は MegaFranchiseeDefaultThreshold (20) を使う。
func IsMegaFranchisee(storeCount, threshold int) bool {
	if threshold <= 0 {
		threshold = MegaFranchiseeDefaultThreshold
	}
	return storeCount >= threshold
}
