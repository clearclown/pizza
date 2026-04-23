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
	"database/sql"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

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

// Phase 11 integration: エニタイム新宿 bake でブランド混入 0 が維持されるか。
// Layer A (blocklist + similarity) を外した fuzzy 結果で
// "FIT PLACE24" "24GYM" 等が 1 件も混入しないことを保証する。
func TestE2E_AnytimeShinjuku_noBrandContamination(t *testing.T) {
	if os.Getenv("GOOGLE_MAPS_API_KEY") == "" {
		t.Skip("GOOGLE_MAPS_API_KEY not set; skipping live integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	polygon, err := menu.ResolvePolygon("新宿")
	require.NoError(t, err)

	// 実 Places API + StrictBrandMatch=true
	seed := &dough.Searcher{
		Places: &dough.PlacesClient{
			APIKey: os.Getenv("GOOGLE_MAPS_API_KEY"), Language: "ja", Region: "JP",
		},
		Language: "ja", Region: "JP", StrictBrandMatch: true,
	}

	// tmpfile DB
	tmpDB := filepath.Join(t.TempDir(), "test.sqlite")
	store, err := box.Open(tmpDB)
	require.NoError(t, err)
	defer store.Close()

	p := &oven.Pipeline{Seed: seed, Box: store, Workers: 4}
	report, err := p.Bake(ctx, &pb.SearchStoresInGridRequest{
		Brand: "エニタイムフィットネス", Polygon: polygon, CellKm: 1.0, Language: "ja",
	})
	require.NoError(t, err)
	require.True(t, report.StoresFound > 30,
		"新宿エニタイムは通常 40+ 店舗、実行時点で大きく下回るのは異常 (got %d)",
		report.StoresFound)

	// DB 内の brand=エニタイムフィットネス 全店舗に「エニタイム」or「Anytime」が
	// 含まれることを確認 (Layer A blocklist の効果、混入 0)
	// SQLite に直接 SELECT (box.Store のテスト拡張は別作業)
	db, err := sql.Open("sqlite", tmpDB)
	require.NoError(t, err)
	defer db.Close()
	rows, err := db.QueryContext(ctx,
		"SELECT name FROM stores WHERE brand=?", "エニタイムフィットネス")
	require.NoError(t, err)
	defer rows.Close()
	checked := 0
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		checked++
		hasAnytime := false
		for _, kw := range []string{"エニタイム", "Anytime", "anytime", "ANYTIME"} {
			if stringsContains(name, kw) {
				hasAnytime = true
				break
			}
		}
		assert.True(t, hasAnytime,
			"混入検出: %q は Anytime ブランドの店舗名を含まない", name)
	}
	assert.Greater(t, checked, 30, "少なくとも 30 店舗は検証対象")
}

// stringsContains は strings.Contains と同等 (import 衝突回避のため自前)。
func stringsContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
