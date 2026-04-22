// 🔴 Red phase test — 開発工程.md §3.3 Accuracy Benchmark 相当。
package scoring_test

import (
	"testing"

	"github.com/clearclown/pizza/internal/scoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassificationAccuracy_allMatch(t *testing.T) {
	t.Parallel()
	samples := []scoring.JudgementSample{
		{PlaceID: "p1", TrueIsFranchise: true, TrueOperator: "A", PredIsFranchise: true, PredOperator: "A"},
		{PlaceID: "p2", TrueIsFranchise: false, PredIsFranchise: false},
		{PlaceID: "p3", TrueIsFranchise: true, TrueOperator: "B", PredIsFranchise: true, PredOperator: "B"},
	}
	acc, err := scoring.ClassificationAccuracy(samples)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, acc, 0.001)
}

func TestClassificationAccuracy_ninetyPercentTarget(t *testing.T) {
	t.Parallel()
	// 10 件中 9 件正解 → 90% (§3.3 の最低ライン)
	samples := make([]scoring.JudgementSample, 10)
	for i := range samples {
		samples[i] = scoring.JudgementSample{
			PlaceID:          "p",
			TrueIsFranchise:  true,
			TrueOperator:     "X",
			PredIsFranchise:  true,
			PredOperator:     "X",
		}
	}
	// 1 件だけ間違う
	samples[0].PredOperator = "WRONG"

	acc, err := scoring.ClassificationAccuracy(samples)
	require.NoError(t, err)
	assert.InDelta(t, 0.9, acc, 0.001,
		"§3.3: Classification Accuracy 90% をこの fixture で表現")
}

func TestRecallRate_exact95Percent(t *testing.T) {
	t.Parallel()
	// 100 店舗中 95 件抽出 → 0.95 (§3.3 最低ライン)
	rate, err := scoring.RecallRate(95, 100)
	require.NoError(t, err)
	assert.InDelta(t, 0.95, rate, 0.0001)
}

func TestRecallRate_guardsAgainstZeroTotal(t *testing.T) {
	t.Parallel()
	// 実店舗 0 で 0 抽出 → 1.0 (vacuously true) か 0 — どちらでもよいが error にしない
	rate, err := scoring.RecallRate(0, 0)
	require.NoError(t, err)
	_ = rate
}
