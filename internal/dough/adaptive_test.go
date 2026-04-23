package dough

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
)

// countingFakePlaces: 呼ばれる度に設定した件数を返す mock。
// cell ごとに異なる件数を返せるよう、呼び出し回数でスイッチ。
type countingFakePlaces struct {
	results   [][]PlaceRaw // 呼び出し順に返すスライス
	callCount int
}

func (f *countingFakePlaces) SearchText(_ context.Context, _ *SearchTextRequest) (*SearchTextResponse, error) {
	if f.callCount >= len(f.results) {
		return &SearchTextResponse{Places: nil}, nil
	}
	r := f.results[f.callCount]
	f.callCount++
	return &SearchTextResponse{Places: r}, nil
}

func makePlaces(n int, baseLat, baseLng float64) []PlaceRaw {
	out := make([]PlaceRaw, n)
	for i := 0; i < n; i++ {
		out[i] = PlaceRaw{
			ID:          fmt.Sprintf("place-%d", i),
			DisplayName: DisplayName{Text: fmt.Sprintf("エニタイム店舗%d", i)},
			Location: struct {
				Latitude  float64 `json:"latitude"`
				Longitude float64 `json:"longitude"`
			}{Latitude: baseLat + 0.0001*float64(i), Longitude: baseLng + 0.0001*float64(i)},
		}
	}
	return out
}

func simpleSquarePolygon() *pb.Polygon {
	return &pb.Polygon{Vertices: []*pb.LatLng{
		{Lat: 35.0, Lng: 139.0},
		{Lat: 36.0, Lng: 139.0},
		{Lat: 36.0, Lng: 140.0},
		{Lat: 35.0, Lng: 140.0},
	}}
}

func TestSearchStoresAdaptive_noSplit_whenBelowCap(t *testing.T) {
	// 小さめ polygon (1km 範囲) で root radius が 50km 以下になるよう、
	// 飽和してない場合は 1 call で終了することを検証。
	smallPoly := &pb.Polygon{Vertices: []*pb.LatLng{
		{Lat: 35.680, Lng: 139.700},
		{Lat: 35.685, Lng: 139.700},
		{Lat: 35.685, Lng: 139.705},
		{Lat: 35.680, Lng: 139.705},
	}}
	fp := &countingFakePlaces{results: [][]PlaceRaw{makePlaces(10, 35.682, 139.702)}}
	s := &Searcher{Places: fp, Language: "ja", Region: "JP", StrictBrandMatch: false}
	count := 0
	err := s.SearchStoresAdaptive(context.Background(), "エニタイム", smallPoly,
		&AdaptiveSearchOptions{SaturationThreshold: 20, MaxDepth: 6, MinCellMeters: 100},
		func(_ *pb.Store) error {
			count++
			return nil
		})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if count != 10 {
		t.Errorf("emitted = %d, want 10", count)
	}
	if s.Metrics.APICalls != 1 {
		t.Errorf("API calls = %d, want 1 (no split needed)", s.Metrics.APICalls)
	}
	if s.Metrics.CellsHitCap != 0 {
		t.Errorf("CellsHitCap = %d, want 0", s.Metrics.CellsHitCap)
	}
}

func TestSearchStoresAdaptive_splits_whenSaturated(t *testing.T) {
	// small polygon (radius < 50km) で root で 20 件飽和 → 4 分割、
	// 子 4 cell で 3 件ずつ → 合計 5 call。
	smallPoly := &pb.Polygon{Vertices: []*pb.LatLng{
		{Lat: 35.680, Lng: 139.700},
		{Lat: 35.690, Lng: 139.700},
		{Lat: 35.690, Lng: 139.710},
		{Lat: 35.680, Lng: 139.710},
	}}
	fp := &countingFakePlaces{results: [][]PlaceRaw{
		makePlaces(20, 35.685, 139.705), // root で飽和
		makePlaces(3, 35.6825, 139.7025),
		makePlaces(3, 35.6825, 139.7075),
		makePlaces(3, 35.6875, 139.7025),
		makePlaces(3, 35.6875, 139.7075),
	}}
	s := &Searcher{Places: fp, Language: "ja", Region: "JP", StrictBrandMatch: false}
	count := 0
	err := s.SearchStoresAdaptive(context.Background(), "エニタイム", smallPoly,
		&AdaptiveSearchOptions{SaturationThreshold: 20, MaxDepth: 2, MinCellMeters: 100},
		func(_ *pb.Store) error {
			count++
			return nil
		})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if s.Metrics.APICalls != 5 {
		t.Errorf("API calls = %d, want 5", s.Metrics.APICalls)
	}
	if s.Metrics.CellsHitCap != 1 {
		t.Errorf("CellsHitCap = %d, want 1", s.Metrics.CellsHitCap)
	}
	if count != 20 {
		t.Errorf("emitted = %d, want 20", count)
	}
}

func TestSearchStoresAdaptive_skipsCoveredCells(t *testing.T) {
	// Phase 17.3: CoverMap で既知領域を skip するとき API call が減る
	smallPoly := &pb.Polygon{Vertices: []*pb.LatLng{
		{Lat: 35.680, Lng: 139.700},
		{Lat: 35.690, Lng: 139.700},
		{Lat: 35.690, Lng: 139.710},
		{Lat: 35.680, Lng: 139.710},
	}}
	// fake Places: root 飽和で subdivide、4 子があるが 1 子は covered で skip
	fp := &countingFakePlaces{results: [][]PlaceRaw{
		makePlaces(20, 35.685, 139.705),  // root: 飽和
		makePlaces(3, 35.6825, 139.7025), // 子1
		makePlaces(3, 35.6825, 139.7075), // 子2
		// 子3 は cover 内で skip、API call 無し (fake response はスキップされる前提で
		// ここでは使われない)
		makePlaces(3, 35.6875, 139.7075), // 子4
	}}
	s := &Searcher{Places: fp, Language: "ja", Region: "JP", StrictBrandMatch: false}
	// cover: 子1 (NE) cell の中心 (35.6875, 139.7075) を半径 100m で覆う。
	// root (center 35.685, 139.705) からは約 360m 離れるので root は cover 外。
	cover := []CoveredPoint{
		{Lat: 35.6875, Lng: 139.7075, RadiusM: 100},
	}
	err := s.SearchStoresAdaptive(context.Background(), "エニタイム", smallPoly,
		&AdaptiveSearchOptions{
			SaturationThreshold: 20, MaxDepth: 2, MinCellMeters: 100,
			Covered: cover,
		},
		func(_ *pb.Store) error { return nil })
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// root 1 + 子 3 (1 子は covered) = 4 calls
	if s.Metrics.APICalls != 4 {
		t.Errorf("API calls = %d, want 4 (cover で 1 子 skip)", s.Metrics.APICalls)
	}
	if s.Metrics.CellsSkippedByCover != 1 {
		t.Errorf("CellsSkippedByCover = %d, want 1", s.Metrics.CellsSkippedByCover)
	}
}

func TestSearchStoresAdaptive_stopsAtMaxDepth(t *testing.T) {
	// MaxDepth=1 で root 飽和 → 4 分割のみ、深掘りせず
	smallPoly := &pb.Polygon{Vertices: []*pb.LatLng{
		{Lat: 35.680, Lng: 139.700},
		{Lat: 35.690, Lng: 139.700},
		{Lat: 35.690, Lng: 139.710},
		{Lat: 35.680, Lng: 139.710},
	}}
	fp := &countingFakePlaces{results: [][]PlaceRaw{
		makePlaces(20, 35.685, 139.705),
		makePlaces(20, 35.6825, 139.7025),
		makePlaces(20, 35.6825, 139.7075),
		makePlaces(20, 35.6875, 139.7025),
		makePlaces(20, 35.6875, 139.7075),
	}}
	s := &Searcher{Places: fp, Language: "ja", Region: "JP", StrictBrandMatch: false}
	err := s.SearchStoresAdaptive(context.Background(), "エニタイム", smallPoly,
		&AdaptiveSearchOptions{SaturationThreshold: 20, MaxDepth: 1, MinCellMeters: 100},
		func(_ *pb.Store) error { return nil })
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// root 1 call + 子 4 call = 5 (MaxDepth=1 で子の飽和後の分割はしない)
	if s.Metrics.APICalls != 5 {
		t.Errorf("API calls = %d, want 5", s.Metrics.APICalls)
	}
}
