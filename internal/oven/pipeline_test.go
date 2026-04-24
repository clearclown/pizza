// oven.Pipeline.Bake の in-process Green テスト。
// in-process SearchBackend / KitchenBackend / JudgeBackend / BoxStore の
// 各 fake を使ってパイプライン全体を検証する。
package oven_test

import (
	"context"
	"testing"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/box"
	"github.com/clearclown/pizza/internal/grid"
	"github.com/clearclown/pizza/internal/oven"
	"github.com/clearclown/pizza/internal/verifier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── fakes ──────────────────────────────────────────────────────────────

type inProcSeed struct {
	stores []*pb.Store
	called int
}

func (s *inProcSeed) SearchStoresInGrid(
	ctx context.Context,
	_ string,
	_ []grid.Cell,
	emit func(*pb.Store) error,
) error {
	s.called++
	for _, st := range s.stores {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := emit(st); err != nil {
			return err
		}
	}
	return nil
}

type fakeKitchen struct {
	byURL map[string]string // url -> markdown
	fails map[string]bool
}

func (f *fakeKitchen) Scrape(_ context.Context, url string) (*pb.MarkdownDoc, error) {
	if f.fails[url] {
		return nil, assert.AnError
	}
	md, ok := f.byURL[url]
	if !ok {
		md = "(no content)"
	}
	return &pb.MarkdownDoc{Url: url, Markdown: md, Title: "T", Metadata: map[string]string{}}, nil
}

type fakeJudge struct{}

func (fakeJudge) JudgeFranchiseType(_ context.Context, req *pb.JudgeFranchiseTypeRequest) (*pb.JudgeFranchiseTypeResponse, error) {
	st := req.GetContext().GetStore()
	return &pb.JudgeFranchiseTypeResponse{Result: &pb.JudgeResult{
		PlaceId:            st.GetPlaceId(),
		IsFranchise:        true,
		OperatorName:       "(mock) 株式会社ピザ運営",
		StoreCountEstimate: 25,
		Confidence:         0.42,
		LlmProvider:        "mock",
	}}, nil
}

// ─── tests ──────────────────────────────────────────────────────────────

func tokyoSquarePolygon() *pb.Polygon {
	return &pb.Polygon{
		Vertices: []*pb.LatLng{
			{Lat: 35.685, Lng: 139.690},
			{Lat: 35.685, Lng: 139.700},
			{Lat: 35.695, Lng: 139.700},
			{Lat: 35.695, Lng: 139.690},
		},
	}
}

func newTestPipeline(t *testing.T, kitchen oven.KitchenBackend, judge oven.JudgeBackend) (*oven.Pipeline, *box.Store) {
	t.Helper()
	b, err := box.Open("")
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	return &oven.Pipeline{
		Seed: &inProcSeed{stores: []*pb.Store{
			{PlaceId: "p1", Brand: "B", Name: "S1", OfficialUrl: "https://example.com/1", Location: &pb.LatLng{}},
			{PlaceId: "p2", Brand: "B", Name: "S2", OfficialUrl: "https://example.com/2", Location: &pb.LatLng{}},
			{PlaceId: "p3", Brand: "B", Name: "S3", Location: &pb.LatLng{}}, // URL なし
		}},
		Kitchen: kitchen,
		Judge:   judge,
		Box:     b,
	}, b
}

func TestPipeline_Bake_seedsStoresIntoBoxAndExportsCSV(t *testing.T) {
	t.Parallel()
	p, b := newTestPipeline(t, nil, nil)
	report, err := p.Bake(context.Background(), &pb.SearchStoresInGridRequest{
		Brand:   "B",
		Polygon: tokyoSquarePolygon(),
		CellKm:  1.0,
	})
	require.NoError(t, err)
	assert.Equal(t, 3, report.StoresFound)
	assert.Equal(t, 0, report.MarkdownsFetched)
	assert.Equal(t, 0, report.JudgementsMade)
	assert.NotEmpty(t, report.CSV, "CSV should be exported")

	n, err := b.CountStores(context.Background(), "B")
	require.NoError(t, err)
	assert.Equal(t, 3, n)
}

func TestPipeline_Bake_runsKitchenForStoresWithURL(t *testing.T) {
	t.Parallel()
	kitchen := &fakeKitchen{byURL: map[string]string{
		"https://example.com/1": "# store1",
		"https://example.com/2": "# store2",
	}}
	p, _ := newTestPipeline(t, kitchen, nil)
	report, err := p.Bake(context.Background(), &pb.SearchStoresInGridRequest{
		Brand:   "B",
		Polygon: tokyoSquarePolygon(),
		CellKm:  1.0,
	})
	require.NoError(t, err)
	assert.Equal(t, 3, report.StoresFound)
	assert.Equal(t, 2, report.MarkdownsFetched,
		"URL なしの p3 は kitchen 対象外")
}

func TestPipeline_Bake_invokesJudgeForEachStore(t *testing.T) {
	t.Parallel()
	p, _ := newTestPipeline(t, nil, fakeJudge{})
	report, err := p.Bake(context.Background(), &pb.SearchStoresInGridRequest{
		Brand:   "B",
		Polygon: tokyoSquarePolygon(),
		CellKm:  1.0,
	})
	require.NoError(t, err)
	assert.Equal(t, 3, report.JudgementsMade)
}

func TestPipeline_Bake_requiresBrand(t *testing.T) {
	t.Parallel()
	p, _ := newTestPipeline(t, nil, nil)
	_, err := p.Bake(context.Background(), &pb.SearchStoresInGridRequest{Polygon: tokyoSquarePolygon()})
	assert.ErrorContains(t, err, "brand")
}

func TestPipeline_Bake_requiresBackends(t *testing.T) {
	t.Parallel()
	_, err := (&oven.Pipeline{}).Bake(context.Background(), &pb.SearchStoresInGridRequest{Brand: "b"})
	assert.ErrorContains(t, err, "Seed")
}

// fakeVerifier は VerifierBackend の mock。常に IsVerified=true を返す。
type fakeVerifier struct{}

func (fakeVerifier) Verify(_ context.Context, name string) verifier.VerifyResult {
	return verifier.VerifyResult{
		InputName:       name,
		OfficialName:    name,
		CorporateNumber: "1234567890123",
		IsVerified:      true,
		IsActive:        true,
		NameSimilarity:  1.0,
		Source:          "houjin_csv",
		MatchLevel:      verifier.MatchExact,
	}
}

func TestBakeVerifierPhase(t *testing.T) {
	t.Parallel()
	b, err := box.Open("")
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })

	p := &oven.Pipeline{
		Seed: &inProcSeed{stores: []*pb.Store{
			{PlaceId: "v1", Brand: "B", Name: "S1", Location: &pb.LatLng{}},
			{PlaceId: "v2", Brand: "B", Name: "S2", Location: &pb.LatLng{}},
		}},
		Judge:    fakeJudge{},
		Verifier: fakeVerifier{},
		Box:      b,
	}

	report, err := p.Bake(context.Background(), &pb.SearchStoresInGridRequest{
		Brand:   "B",
		Polygon: tokyoSquarePolygon(),
		CellKm:  1.0,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, report.JudgementsMade)
	assert.Greater(t, report.VerificationsRun, 0, "verifier phase should have run at least once")
}
