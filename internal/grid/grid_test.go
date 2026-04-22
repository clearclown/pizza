// 🔴 Red phase test — Phase 1 で Green 化する。
//
// 開発工程.md §3.1 Grid Test:
//   緯度経度計算において、指定範囲が 100% カバーされているか。
package grid_test

import (
	"testing"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/grid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tokyoRoughPolygon は東京都をざっくり囲む矩形。
func tokyoRoughPolygon() *pb.Polygon {
	return &pb.Polygon{
		Vertices: []*pb.LatLng{
			{Lat: 35.500, Lng: 139.500}, // SW
			{Lat: 35.500, Lng: 139.900}, // SE
			{Lat: 35.900, Lng: 139.900}, // NE
			{Lat: 35.900, Lng: 139.500}, // NW
		},
	}
}

func TestSplit_producesCellsForPolygon(t *testing.T) {
	t.Parallel()
	cells, err := grid.Split(tokyoRoughPolygon(), 1.0)
	require.NoError(t, err, "Split should succeed for a valid polygon")
	assert.NotEmpty(t, cells, "polygon must be covered by at least one cell")

	for _, c := range cells {
		assert.NotEmpty(t, c.ID, "cell id must be non-empty")
		assert.NotNil(t, c.Center, "cell center must be non-nil")
		assert.Greater(t, c.RadiusM, 0.0, "radius must be positive")
	}
}

func TestSplit_respectsCellSize(t *testing.T) {
	t.Parallel()
	largeCells, err := grid.Split(tokyoRoughPolygon(), 5.0)
	require.NoError(t, err)
	smallCells, err := grid.Split(tokyoRoughPolygon(), 1.0)
	require.NoError(t, err)

	assert.Less(t, len(largeCells), len(smallCells),
		"smaller cell_km should yield more cells")
}

func TestCoverage_fullyCoversPolygon(t *testing.T) {
	t.Parallel()
	polygon := tokyoRoughPolygon()
	cells, err := grid.Split(polygon, 1.0)
	require.NoError(t, err)

	coverage, err := grid.Coverage(polygon, cells)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, coverage, 0.01,
		"grid must cover at least 99%% of the input polygon (§3.1 Grid Test)")
}
