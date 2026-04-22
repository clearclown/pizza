package scoring

import "errors"

// ErrNotImplemented は Phase 0 スタブが呼ばれたときに返る。
var ErrNotImplemented = errors.New("scoring: not implemented")

// JudgementSample は正解データ 1 件を表す (開発工程.md §3.3)。
type JudgementSample struct {
	PlaceID          string
	TrueIsFranchise  bool
	TrueOperator     string
	PredIsFranchise  bool
	PredOperator     string
}

// ClassificationAccuracy は samples に対する分類一致率を返す [0.0, 1.0]。
// 一致条件: TrueIsFranchise == PredIsFranchise かつ
// (IsFranchise == false の場合) or (TrueOperator == PredOperator の場合)。
//
// 開発工程.md §3.3: 100 件の正解に対し 90% 超を要求。
// Phase 0: 未実装。
func ClassificationAccuracy(samples []JudgementSample) (float64, error) {
	_ = samples
	return 0, ErrNotImplemented
}

// RecallRate は実店舗数 (actualTotal) に対する抽出成功数 (extracted) の比率を返す。
//
// 開発工程.md §3.3: 95% 以上を要求。
// Phase 0: 未実装。
func RecallRate(extracted, actualTotal int) (float64, error) {
	_ = extracted
	_ = actualTotal
	return 0, ErrNotImplemented
}
