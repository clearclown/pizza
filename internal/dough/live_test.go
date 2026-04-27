// Live API smoke test — 実 Google Places API を叩く。
// GOOGLE_MAPS_API_KEY が設定されている時のみ実行 (CI では skip)。
// 実行: go test -tags=live ./internal/dough/... -run TestLive

//go:build live

package dough_test

import (
	"context"
	"os"
	"testing"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/dough"
	"github.com/clearclown/pizza/internal/grid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLive_PlacesClient_searchTextReturnsRealStores(t *testing.T) {
	apiKey := os.Getenv("GOOGLE_MAPS_API_KEY")
	if apiKey == "" {
		t.Skip("GOOGLE_MAPS_API_KEY not set; skipping live test")
	}
	if os.Getenv("PIZZA_ENABLE_PAID_GOOGLE_APIS") != "1" {
		t.Skip("paid Google Places API disabled; set PIZZA_ENABLE_PAID_GOOGLE_APIS=1")
	}

	c := &dough.PlacesClient{APIKey: apiKey, Language: "ja", Region: "JP"}
	resp, err := c.SearchText(context.Background(), &dough.SearchTextRequest{
		TextQuery:      "エニタイムフィットネス 新宿",
		LanguageCode:   "ja",
		RegionCode:     "JP",
		MaxResultCount: 5,
	})
	require.NoError(t, err, "real Places API call")
	require.NotEmpty(t, resp.Places, "should return at least one store")

	t.Logf("✅ %d stores returned:", len(resp.Places))
	for _, p := range resp.Places {
		t.Logf("  - %s: %s", p.DisplayName.Text, p.FormattedAddress)
		assert.NotEmpty(t, p.ID)
		assert.NotEmpty(t, p.DisplayName.Text)
	}
}

func TestLive_Searcher_endToEndWithRealAPI(t *testing.T) {
	apiKey := os.Getenv("GOOGLE_MAPS_API_KEY")
	if apiKey == "" {
		t.Skip("GOOGLE_MAPS_API_KEY not set; skipping live test")
	}
	if os.Getenv("PIZZA_ENABLE_PAID_GOOGLE_APIS") != "1" {
		t.Skip("paid Google Places API disabled; set PIZZA_ENABLE_PAID_GOOGLE_APIS=1")
	}

	// 新宿駅周辺 (約 1km 四方)
	polygon := &pb.Polygon{
		Vertices: []*pb.LatLng{
			{Lat: 35.685, Lng: 139.690},
			{Lat: 35.685, Lng: 139.710},
			{Lat: 35.705, Lng: 139.710},
			{Lat: 35.705, Lng: 139.690},
		},
	}
	cells, err := grid.Split(polygon, 1.0)
	require.NoError(t, err)
	require.NotEmpty(t, cells)

	s := &dough.Searcher{
		Places:   &dough.PlacesClient{APIKey: apiKey, Language: "ja", Region: "JP"},
		Language: "ja",
		Region:   "JP",
	}

	var stores []*pb.Store
	err = s.SearchStoresInGrid(context.Background(), "エニタイムフィットネス", cells,
		func(st *pb.Store) error {
			stores = append(stores, st)
			return nil
		},
	)
	require.NoError(t, err)

	t.Logf("✅ End-to-end: %d unique stores extracted from %d cells",
		len(stores), len(cells))
	for _, s := range stores {
		t.Logf("  - %s: %s (%.4f,%.4f)",
			s.GetName(), s.GetAddress(),
			s.GetLocation().GetLat(), s.GetLocation().GetLng())
	}

	// 新宿なら少なくとも 1 件はあるはず
	assert.Greater(t, len(stores), 0, "at least one Anytime Fitness in 新宿")
}
