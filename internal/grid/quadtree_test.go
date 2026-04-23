package grid

import (
	"testing"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
)

func TestQuadCell_Subdivide_produces4Children(t *testing.T) {
	root := QuadCell{
		ID: "r", MinLat: 35.0, MinLng: 139.0, MaxLat: 36.0, MaxLng: 140.0,
		Depth: 0,
	}
	children := root.Subdivide()
	if len(children) != 4 {
		t.Fatalf("expected 4 children, got %d", len(children))
	}
	// 4 つ全て depth=1、id が root-XX 形式
	for _, c := range children {
		if c.Depth != 1 {
			t.Errorf("child depth=%d, want 1", c.Depth)
		}
		if len(c.ID) <= len("r-") {
			t.Errorf("child id too short: %q", c.ID)
		}
	}
	// 4 象限で重複なくカバーする
	totalArea := func(min, max float64) float64 { return max - min }
	rootLatSpan := totalArea(root.MinLat, root.MaxLat)
	rootLngSpan := totalArea(root.MinLng, root.MaxLng)
	sumLat, sumLng := 0.0, 0.0
	for _, c := range children {
		sumLat += totalArea(c.MinLat, c.MaxLat)
		sumLng += totalArea(c.MinLng, c.MaxLng)
	}
	// 合計 lat span は root の 2 倍 (2 段で積み上げ)
	if sumLat != 2*rootLatSpan {
		t.Errorf("sum lat span = %v, want %v", sumLat, 2*rootLatSpan)
	}
	if sumLng != 2*rootLngSpan {
		t.Errorf("sum lng span = %v, want %v", sumLng, 2*rootLngSpan)
	}
}

func TestQuadCell_SideMetersMax(t *testing.T) {
	// 東京近辺で 1km x 1km 程度の cell
	c := QuadCell{
		MinLat: 35.68, MaxLat: 35.69,   // 約 1.1 km
		MinLng: 139.70, MaxLng: 139.71, // 約 0.9 km (cos(35.68°) ≈ 0.81)
	}
	side := c.SideMetersMax()
	if side < 800 || side > 1400 {
		t.Errorf("side = %.0fm, expected 800-1400m range", side)
	}
}

func TestNewQuadCellFromPolygonBBox(t *testing.T) {
	poly := &pb.Polygon{Vertices: []*pb.LatLng{
		{Lat: 35.5, Lng: 139.5},
		{Lat: 35.9, Lng: 139.5},
		{Lat: 35.9, Lng: 140.0},
		{Lat: 35.5, Lng: 140.0},
	}}
	root := NewQuadCellFromPolygonBBox(poly)
	if root.MinLat != 35.5 || root.MaxLat != 35.9 {
		t.Errorf("root lat range incorrect: [%v, %v]", root.MinLat, root.MaxLat)
	}
	if root.MinLng != 139.5 || root.MaxLng != 140.0 {
		t.Errorf("root lng range incorrect: [%v, %v]", root.MinLng, root.MaxLng)
	}
	if root.Depth != 0 || root.ID != "root" {
		t.Errorf("root meta incorrect: depth=%d id=%s", root.Depth, root.ID)
	}
}

func TestQuadCell_Subdivide_recursive(t *testing.T) {
	// 2 段階分割で 16 cells になる
	root := QuadCell{MinLat: 0, MinLng: 0, MaxLat: 1, MaxLng: 1, ID: "r"}
	level1 := root.Subdivide()
	var level2 []QuadCell
	for _, c := range level1 {
		level2 = append(level2, c.Subdivide()...)
	}
	if len(level2) != 16 {
		t.Errorf("expected 16 grandchildren, got %d", len(level2))
	}
	// all at depth 2
	for _, c := range level2 {
		if c.Depth != 2 {
			t.Errorf("grandchild depth %d, want 2", c.Depth)
		}
	}
}
