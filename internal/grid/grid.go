// Package grid は緯度経度ポリゴンを検索グリッドに分割する。
//
// 実装は Phase 1 で Green にする。Phase 0 時点では「コンパイルは通るが
// 機能しない」スタブを用意し、grid_test.go の Red を保証する。
package grid

import (
	"errors"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
)

// ErrNotImplemented は Phase 0 時点で未実装関数が呼ばれた際に返す。
// Phase 1 で実装完了と共に削除する。
var ErrNotImplemented = errors.New("grid: not implemented (Phase 1 target)")

// Cell は 1 メッシュセルを表す。id はデバッグ・重複排除用。
type Cell struct {
	ID     string
	Center *pb.LatLng
	// RadiusM は Google Maps nearbySearch の検索半径 (meter)。
	// cell_km * sqrt(2) / 2 * 1000 で計算される。
	RadiusM float64
}

// Split は polygon を cell_km のメッシュに切り分け、ポリゴン内側または
// 境界上にあるセルだけを返す。
//
// Phase 0: 未実装（常に ErrNotImplemented を返す）。
// Phase 1: 正方形メッシュ + point-in-polygon 判定で実装する。
func Split(polygon *pb.Polygon, cellKm float64) ([]Cell, error) {
	_ = polygon
	_ = cellKm
	return nil, ErrNotImplemented
}

// Coverage は Split 結果の合計カバレッジ比率（実装ポリゴン面積 / 入力ポリゴン面積）を返す。
// Phase 0: 未実装。Phase 1 で 1.0（100% カバー）を保証する。
func Coverage(polygon *pb.Polygon, cells []Cell) (float64, error) {
	_ = polygon
	_ = cells
	return 0, ErrNotImplemented
}
