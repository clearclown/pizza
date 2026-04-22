package scoring

// JudgementSample は正解データ 1 件を表す (開発工程.md §3.3)。
type JudgementSample struct {
	PlaceID         string
	TrueIsFranchise bool
	TrueOperator    string
	PredIsFranchise bool
	PredOperator    string
}

// ClassificationAccuracy は samples に対する分類一致率を返す [0.0, 1.0]。
//
// 一致条件:
//   - TrueIsFranchise == PredIsFranchise かつ
//   - (IsFranchise == false) の場合は operator を無視
//     (直営店は運営会社比較しない)
//   - (IsFranchise == true) の場合は TrueOperator == PredOperator
//
// 開発工程.md §3.3: 100 件の正解に対し 90% 超を要求。
func ClassificationAccuracy(samples []JudgementSample) (float64, error) {
	if len(samples) == 0 {
		return 0, nil
	}
	correct := 0
	for _, s := range samples {
		if s.TrueIsFranchise != s.PredIsFranchise {
			continue
		}
		if !s.TrueIsFranchise {
			// 直営: 一致で OK (operator は比較しない)
			correct++
			continue
		}
		if s.TrueOperator == s.PredOperator {
			correct++
		}
	}
	return float64(correct) / float64(len(samples)), nil
}

// RecallRate は実店舗数 (actualTotal) に対する抽出成功数 (extracted) の比率を返す。
//
// actualTotal <= 0 の場合 1.0 (vacuously true) を返す。
// 開発工程.md §3.3: 95% 以上を要求。
func RecallRate(extracted, actualTotal int) (float64, error) {
	if actualTotal <= 0 {
		return 1.0, nil
	}
	return float64(extracted) / float64(actualTotal), nil
}
