// E2E pipeline integration test.
//
// Phase 3 実装:
//   - In-process で Seed (実 Places API) + Box (tmpfile SQLite) + mock Judge
//     の Pipeline.Bake を回し、CSV 出力と DB 状態を検証する
//   - build tag=integration で通常 test から除外 (コスト & ネットワーク依存)
//   - GOOGLE_MAPS_API_KEY が未設定なら Skip
//
// 実行:
//   set -a; source .env; set +a
//   go test -tags=integration -v -timeout=5m ./test/e2e/...
//
//go:build integration

package e2e_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/box"
	"github.com/clearclown/pizza/internal/dough"
	"github.com/clearclown/pizza/internal/menu"
	"github.com/clearclown/pizza/internal/oven"
)

// mockJudge は Phase 3 E2E では固定判定を返す (コスト抑制)。
type mockJudge struct{}

func (mockJudge) JudgeFranchiseType(_ context.Context, req *pb.JudgeFranchiseTypeRequest) (*pb.JudgeFranchiseTypeResponse, error) {
	s := req.GetContext().GetStore()
	return &pb.JudgeFranchiseTypeResponse{Result: &pb.JudgeResult{
		PlaceId:            s.GetPlaceId(),
		IsFranchise:        true,
		OperatorName:       "(e2e-mock) 株式会社運営",
		StoreCountEstimate: 25,
		Confidence:         0.6,
		LlmProvider:        "mock",
		LlmModel:           "none",
	}}, nil
}

func TestE2E_pipelineProducesStoresAndCSV_withRealPlacesAPI(t *testing.T) {
	apiKey := os.Getenv("GOOGLE_MAPS_API_KEY")
	if apiKey == "" {
		t.Skip("GOOGLE_MAPS_API_KEY is not set; skipping live E2E")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// tmp dir に SQLite を用意
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "pizza.sqlite")
	store, err := box.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// Polygon は "新宿" で 1km グリッド
	polygon, err := menu.ResolvePolygon("新宿")
	require.NoError(t, err)

	// Pipeline を in-process で組み立て
	p := &oven.Pipeline{
		Seed: &dough.Searcher{
			Places:   &dough.PlacesClient{APIKey: apiKey, Language: "ja", Region: "JP"},
			Language: "ja",
			Region:   "JP",
		},
		Kitchen: nil,        // E2E では skip (Firecrawl 未起動)
		Judge:   mockJudge{}, // mock で判定まで通る動作確認
		Box:     store,
		Workers: 4,
	}

	report, err := p.Bake(ctx, &pb.SearchStoresInGridRequest{
		Brand:    "エニタイムフィットネス",
		Polygon:  polygon,
		CellKm:   1.0,
		Language: "ja",
	})
	require.NoError(t, err, "Bake should complete end-to-end")
	require.NotNil(t, report)

	t.Logf("E2E report: cells=%d stores=%d judgements=%d csv_bytes=%d elapsed=%.1fs",
		report.CellsGenerated, report.StoresFound, report.JudgementsMade,
		len(report.CSV), report.ElapsedSec)

	// Shinjuku なら 10 店舗以上は期待できる (2026 年時点)
	assert.Greater(t, report.CellsGenerated, 0)
	assert.GreaterOrEqual(t, report.StoresFound, 10,
		"expected ≥10 stores in 新宿 for エニタイムフィットネス")
	assert.Equal(t, report.StoresFound, report.JudgementsMade,
		"judge should be invoked for every store")
	assert.NotEmpty(t, report.CSV)
	assert.Less(t, report.ElapsedSec, 120.0, "should finish in < 2 min")

	// CSV の構造検証
	r := csv.NewReader(bytes.NewReader(report.CSV))
	header, err := r.Read()
	require.NoError(t, err)
	assert.Equal(t, []string{
		"place_id", "brand", "name", "address", "lat", "lng",
		"official_url", "phone", "grid_cell_id", "extracted_at",
	}, header)
	rows, err := r.ReadAll()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(rows), 10)

	// SQLite 直接確認: judgements & mega_franchisees view が効く
	n, err := store.CountStores(ctx, "エニタイムフィットネス")
	require.NoError(t, err)
	assert.Equal(t, report.StoresFound, n)

	mega, err := store.QueryMegaFranchisees(ctx, 5)
	require.NoError(t, err)
	require.NotEmpty(t, mega, "mock-operator 1 件以上ヒットするはず")
	assert.Equal(t, "(e2e-mock) 株式会社運営", mega[0].GetOperatorName())
	assert.Equal(t, int32(report.JudgementsMade), mega[0].GetStoreCount())
}

func TestE2E_polygonUnknown_returnsError(t *testing.T) {
	// menu.ResolvePolygon の契約テストを E2E レイヤでも確認
	_, err := menu.ResolvePolygon("nonexistent-area")
	require.Error(t, err)
	assert.ErrorContains(t, err, "unknown area")
}
