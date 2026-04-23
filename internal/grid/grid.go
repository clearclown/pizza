// Package grid は緯度経度ポリゴンを検索グリッドに分割する。
//
// アルゴリズム:
//  1. polygon のバウンディングボックス (lat/lng の min/max) を計算
//  2. cell_km を緯度・経度の度数に変換し、min からグリッドを切る
//     - 1 度の緯度 ≈ 111.32 km
//     - 1 度の経度 ≈ 111.32 × cos(latitude) km
//  3. 各セルの中心がポリゴン内 or 4 頂点のいずれかが内側なら採用 (境界セル)
//  4. RadiusM = cell_km * √2 / 2 * 1000 でセル中心から対角まで覆う半径
//
// 開発工程.md §3.1: 指定範囲 100% カバーを保証する。
package grid

import (
	"errors"
	"fmt"
	"math"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
)

// ErrInvalidInput は polygon や cell_km が不正な場合に返る。
var ErrInvalidInput = errors.New("grid: invalid input (empty polygon or non-positive cell_km)")

// kmPerDegreeLat は緯度 1 度あたりの距離 (km)。WGS84 平均。
const kmPerDegreeLat = 111.32

// Cell は 1 メッシュセルを表す。
type Cell struct {
	ID      string
	Center  *pb.LatLng
	RadiusM float64 // Google Maps nearbySearch 用の検索半径 (meter)
	// セルの矩形境界 (BB) — coverage 計算用
	MinLat, MinLng, MaxLat, MaxLng float64
}

// Split は polygon を cell_km のメッシュに分割し、ポリゴンと重なるセルだけを返す。
func Split(polygon *pb.Polygon, cellKm float64) ([]Cell, error) {
	if polygon == nil || len(polygon.GetVertices()) < 3 || cellKm <= 0 {
		return nil, ErrInvalidInput
	}
	verts := polygon.GetVertices()

	// 1. Bounding Box
	minLat, minLng := verts[0].GetLat(), verts[0].GetLng()
	maxLat, maxLng := minLat, minLng
	for _, v := range verts[1:] {
		if v.GetLat() < minLat {
			minLat = v.GetLat()
		}
		if v.GetLat() > maxLat {
			maxLat = v.GetLat()
		}
		if v.GetLng() < minLng {
			minLng = v.GetLng()
		}
		if v.GetLng() > maxLng {
			maxLng = v.GetLng()
		}
	}

	// 2. cell_km を度に換算 (代表緯度で経度方向の距離を補正)
	centerLat := (minLat + maxLat) / 2
	latStep := cellKm / kmPerDegreeLat
	lngStep := cellKm / (kmPerDegreeLat * math.Cos(centerLat*math.Pi/180))
	if latStep <= 0 || lngStep <= 0 {
		return nil, ErrInvalidInput
	}

	// 3. グリッド生成 + polygon 重なり判定
	var cells []Cell
	idx := 0
	for lat := minLat; lat < maxLat; lat += latStep {
		for lng := minLng; lng < maxLng; lng += lngStep {
			cellMinLat, cellMaxLat := lat, math.Min(lat+latStep, maxLat)
			cellMinLng, cellMaxLng := lng, math.Min(lng+lngStep, maxLng)
			center := &pb.LatLng{
				Lat: (cellMinLat + cellMaxLat) / 2,
				Lng: (cellMinLng + cellMaxLng) / 2,
			}
			if cellOverlapsPolygon(center, cellMinLat, cellMinLng, cellMaxLat, cellMaxLng, verts) {
				cells = append(cells, Cell{
					ID:      fmt.Sprintf("cell-%05d", idx),
					Center:  center,
					RadiusM: cellKm * math.Sqrt2 / 2 * 1000,
					MinLat:  cellMinLat, MinLng: cellMinLng,
					MaxLat: cellMaxLat, MaxLng: cellMaxLng,
				})
				idx++
			}
		}
	}
	return cells, nil
}

// cellOverlapsPolygon はセルが polygon と重なるかを保守的に判定する。
// 中心 or 4 頂点のいずれかが内側にあれば true。
func cellOverlapsPolygon(center *pb.LatLng, minLat, minLng, maxLat, maxLng float64, verts []*pb.LatLng) bool {
	if pointInPolygon(center.GetLat(), center.GetLng(), verts) {
		return true
	}
	corners := [][2]float64{
		{minLat, minLng},
		{minLat, maxLng},
		{maxLat, minLng},
		{maxLat, maxLng},
	}
	for _, c := range corners {
		if pointInPolygon(c[0], c[1], verts) {
			return true
		}
	}
	return false
}

// PointInPolygon は Ray Casting アルゴリズムで点の内外判定を行う (exported)。
// 他パッケージからの polygon post-filter で使用する。
func PointInPolygon(lat, lng float64, verts []*pb.LatLng) bool {
	return pointInPolygon(lat, lng, verts)
}

// pointInPolygon は Ray Casting アルゴリズムで点の内外判定を行う。
func pointInPolygon(lat, lng float64, verts []*pb.LatLng) bool {
	n := len(verts)
	if n < 3 {
		return false
	}
	inside := false
	j := n - 1
	for i := 0; i < n; i++ {
		vi, vj := verts[i], verts[j]
		if ((vi.GetLat() > lat) != (vj.GetLat() > lat)) &&
			(lng < (vj.GetLng()-vi.GetLng())*(lat-vi.GetLat())/(vj.GetLat()-vi.GetLat())+vi.GetLng()) {
			inside = !inside
		}
		j = i
	}
	return inside
}

// Coverage は cells が polygon をどの程度カバーしているかの比率を返す。
// bounding box ベースの近似: セル BB の合計面積 / polygon BB の面積。
// 矩形ポリゴン + 同じ bbox から生成したセル列に対しては 1.0 を保証する。
func Coverage(polygon *pb.Polygon, cells []Cell) (float64, error) {
	if polygon == nil || len(polygon.GetVertices()) < 3 {
		return 0, ErrInvalidInput
	}
	verts := polygon.GetVertices()
	minLat, minLng := verts[0].GetLat(), verts[0].GetLng()
	maxLat, maxLng := minLat, minLng
	for _, v := range verts[1:] {
		if v.GetLat() < minLat {
			minLat = v.GetLat()
		}
		if v.GetLat() > maxLat {
			maxLat = v.GetLat()
		}
		if v.GetLng() < minLng {
			minLng = v.GetLng()
		}
		if v.GetLng() > maxLng {
			maxLng = v.GetLng()
		}
	}
	polyArea := (maxLat - minLat) * (maxLng - minLng)
	if polyArea <= 0 {
		return 0, nil
	}
	covered := 0.0
	for _, c := range cells {
		iMinLat := math.Max(c.MinLat, minLat)
		iMinLng := math.Max(c.MinLng, minLng)
		iMaxLat := math.Min(c.MaxLat, maxLat)
		iMaxLng := math.Min(c.MaxLng, maxLng)
		if iMaxLat > iMinLat && iMaxLng > iMinLng {
			covered += (iMaxLat - iMinLat) * (iMaxLng - iMinLng)
		}
	}
	return covered / polyArea, nil
}
