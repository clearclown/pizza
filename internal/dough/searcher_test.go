package dough_test

import (
	"context"
	"errors"
	"testing"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/dough"
	"github.com/clearclown/pizza/internal/grid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePlaces は固定応答 or cell 別応答を返す mock PlacesSearcher。
type fakePlaces struct {
	byCenterLat map[float64][]dough.PlaceRaw
	calls       int
}

func (f *fakePlaces) SearchText(_ context.Context, req *dough.SearchTextRequest) (*dough.SearchTextResponse, error) {
	f.calls++
	var key float64
	if req.LocationBias != nil && req.LocationBias.Circle != nil {
		key = req.LocationBias.Circle.Center.Latitude
	}
	places := f.byCenterLat[key]
	return &dough.SearchTextResponse{Places: places}, nil
}

func TestSearcher_emitsStoresFromEachCell(t *testing.T) {
	t.Parallel()
	cells := []grid.Cell{
		{ID: "cell-00000", Center: &pb.LatLng{Lat: 35.68, Lng: 139.76}, RadiusM: 500},
		{ID: "cell-00001", Center: &pb.LatLng{Lat: 35.69, Lng: 139.77}, RadiusM: 500},
	}
	fp := &fakePlaces{byCenterLat: map[float64][]dough.PlaceRaw{
		35.68: {{ID: "A", DisplayName: dough.DisplayName{Text: "A店"}}},
		35.69: {{ID: "B", DisplayName: dough.DisplayName{Text: "B店"}}},
	}}
	s := &dough.Searcher{Places: fp, Language: "ja"}

	var got []*pb.Store
	err := s.SearchStoresInGrid(context.Background(), "ブランドX", cells, func(st *pb.Store) error {
		got = append(got, st)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "A", got[0].GetPlaceId())
	assert.Equal(t, "cell-00000", got[0].GetGridCellId())
	assert.Equal(t, "B", got[1].GetPlaceId())
	assert.Equal(t, "cell-00001", got[1].GetGridCellId())
	assert.Equal(t, 2, fp.calls)
}

func TestSearcher_dedupesDuplicatePlaceIDsAcrossCells(t *testing.T) {
	t.Parallel()
	cells := []grid.Cell{
		{ID: "c0", Center: &pb.LatLng{Lat: 1}, RadiusM: 100},
		{ID: "c1", Center: &pb.LatLng{Lat: 2}, RadiusM: 100},
	}
	// 両セルから X が返ってくる状況
	fp := &fakePlaces{byCenterLat: map[float64][]dough.PlaceRaw{
		1: {{ID: "X"}, {ID: "Y"}},
		2: {{ID: "X"}, {ID: "Z"}},
	}}
	s := &dough.Searcher{Places: fp}

	seen := make(map[string]int)
	err := s.SearchStoresInGrid(context.Background(), "b", cells, func(st *pb.Store) error {
		seen[st.GetPlaceId()]++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, seen["X"], "X should be emitted only once across cells")
	assert.Equal(t, 1, seen["Y"])
	assert.Equal(t, 1, seen["Z"])
}

func TestSearcher_stopsOnEmitError(t *testing.T) {
	t.Parallel()
	cells := []grid.Cell{
		{ID: "c0", Center: &pb.LatLng{}},
		{ID: "c1", Center: &pb.LatLng{}},
	}
	fp := &fakePlaces{byCenterLat: map[float64][]dough.PlaceRaw{
		0: {{ID: "A"}, {ID: "B"}},
	}}
	s := &dough.Searcher{Places: fp}

	stop := errors.New("client gone")
	err := s.SearchStoresInGrid(context.Background(), "b", cells, func(_ *pb.Store) error {
		return stop
	})
	assert.ErrorIs(t, err, stop)
}

func TestSearcher_respectsCancelledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &dough.Searcher{Places: &fakePlaces{}}
	err := s.SearchStoresInGrid(ctx, "b", []grid.Cell{{Center: &pb.LatLng{}}}, func(*pb.Store) error {
		return nil
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestSearcher_requiresPlacesBackend(t *testing.T) {
	t.Parallel()
	s := &dough.Searcher{}
	err := s.SearchStoresInGrid(context.Background(), "b", nil, func(*pb.Store) error {
		return nil
	})
	assert.ErrorContains(t, err, "Places is nil")
}
