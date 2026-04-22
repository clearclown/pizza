// golden_test.go — §3.3 Classification Accuracy のベンチマーク雛形。
//
// test/fixtures/judgement-golden.csv の正解データに対し、
// 予測結果の一致率を測定する。Phase 2 では mock 予測 (全部 is_franchise=true)
// に対するベースラインを記録するだけで、Phase 3 の本判定で数値を改善する。
package scoring_test

import (
	"encoding/csv"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/clearclown/pizza/internal/scoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadGolden(t *testing.T) []scoring.JudgementSample {
	t.Helper()
	f, err := os.Open("../../test/fixtures/judgement-golden.csv")
	require.NoError(t, err)
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	require.NoError(t, err)
	require.Greater(t, len(rows), 1, "golden CSV must have header + rows")

	header := rows[0]
	idx := map[string]int{}
	for i, h := range header {
		idx[h] = i
	}
	required := []string{"place_id", "brand", "true_is_franchise", "true_operator"}
	for _, col := range required {
		_, ok := idx[col]
		require.True(t, ok, "missing column %s in golden CSV", col)
	}

	samples := make([]scoring.JudgementSample, 0, len(rows)-1)
	for _, row := range rows[1:] {
		if strings.TrimSpace(row[idx["place_id"]]) == "" {
			continue
		}
		isFC, err := strconv.ParseBool(strings.TrimSpace(row[idx["true_is_franchise"]]))
		require.NoError(t, err, "row %v: true_is_franchise must be bool", row)
		samples = append(samples, scoring.JudgementSample{
			PlaceID:         row[idx["place_id"]],
			TrueIsFranchise: isFC,
			TrueOperator:    strings.TrimSpace(row[idx["true_operator"]]),
		})
	}
	return samples
}

func TestGolden_datasetLoadsAndHasMinimumSamples(t *testing.T) {
	t.Parallel()
	samples := loadGolden(t)
	assert.GreaterOrEqual(t, len(samples), 5, "golden dataset must have ≥5 samples")
}

func TestGolden_mockAllTrueBaseline(t *testing.T) {
	t.Parallel()
	// 全部 is_franchise=true を予測する「mock」。golden dataset が FC/直営 を含む
	// 限り、accuracy < 1.0 になる。Phase 3 の本判定で 0.9 超を目指す。
	samples := loadGolden(t)
	predicted := make([]scoring.JudgementSample, len(samples))
	for i, s := range samples {
		predicted[i] = scoring.JudgementSample{
			PlaceID:         s.PlaceID,
			TrueIsFranchise: s.TrueIsFranchise,
			TrueOperator:    s.TrueOperator,
			PredIsFranchise: true,
			PredOperator:    s.TrueOperator, // operator は正解を使う (is_franchise だけ評価)
		}
	}
	acc, err := scoring.ClassificationAccuracy(predicted)
	require.NoError(t, err)
	t.Logf("mock-all-true baseline accuracy on %d samples: %.1f%%", len(samples), acc*100)
	// mock 予測 + 現在の golden (FC/直営混在) なら 0.9 未満のはず
	// (Phase 3 で本判定を入れたら > 0.9 を目指す。ここではベースライン記録のみ)
	assert.Less(t, acc, 1.0, "mock-all-true must not hit 100% on mixed golden")
}

func TestGolden_perfectPredictionHits100Percent(t *testing.T) {
	t.Parallel()
	// sanity check: 正解と同じ予測なら acc = 1.0
	samples := loadGolden(t)
	predicted := make([]scoring.JudgementSample, len(samples))
	for i, s := range samples {
		predicted[i] = scoring.JudgementSample{
			PlaceID:         s.PlaceID,
			TrueIsFranchise: s.TrueIsFranchise,
			TrueOperator:    s.TrueOperator,
			PredIsFranchise: s.TrueIsFranchise,
			PredOperator:    s.TrueOperator,
		}
	}
	acc, err := scoring.ClassificationAccuracy(predicted)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, acc, 0.001, "perfect predictions must yield 100% accuracy")
}
