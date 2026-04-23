// Adaptive quad-tree cell split (Phase 13)。
//
// Google Places API (New) の Text Search は 1 リクエストあたり最大 20 件という
// 制約がある。このため cell に 20 件超の店舗があると取りこぼす。
// Adaptive split:
//  - 最初は粗い cell (例: 10km) で検索
//  - 20 件返ったら 4 分割して再検索 (quad-tree)
//  - depth 制限 (minCellM) に到達したら停止 (ただの密集地区として諦める)
//  - これで密度に応じた自動適応が可能 (都市は深く、郊外は浅く)

package grid

import (
	"fmt"
	"math"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
)

// latDegToM は緯度差 (度) を距離 (m) に変換する。
func latDegToM(latDeg float64) float64 {
	return math.Abs(latDeg) * kmPerDegreeLat * 1000.0
}

// lngDegToM は経度差 (度) を距離 (m) に変換する (緯度補正)。
func lngDegToM(lngDeg, atLat float64) float64 {
	return math.Abs(lngDeg) * kmPerDegreeLat * math.Cos(atLat*math.Pi/180.0) * 1000.0
}

// QuadCell は 再帰分割の 1 ノード。bbox 内での検索結果件数を記録する。
type QuadCell struct {
	ID       string
	MinLat   float64
	MinLng   float64
	MaxLat   float64
	MaxLng   float64
	Depth    int
	// 中心と radius を既存 Cell と互換で提供。
	Center   *pb.LatLng
	RadiusM  float64
}

// Subdivide は cell を 4 象限 (NE/NW/SE/SW) に分割して返す。
// depth は親 +1。
func (q *QuadCell) Subdivide() []QuadCell {
	midLat := (q.MinLat + q.MaxLat) / 2.0
	midLng := (q.MinLng + q.MaxLng) / 2.0
	children := make([]QuadCell, 4)
	specs := []struct {
		suffix                     string
		minLat, minLng, maxLat, maxLng float64
	}{
		{"NE", midLat, midLng, q.MaxLat, q.MaxLng},
		{"NW", midLat, q.MinLng, q.MaxLat, midLng},
		{"SE", q.MinLat, midLng, midLat, q.MaxLng},
		{"SW", q.MinLat, q.MinLng, midLat, midLng},
	}
	for i, s := range specs {
		c := QuadCell{
			ID:     fmt.Sprintf("%s-%s", q.ID, s.suffix),
			MinLat: s.minLat, MinLng: s.minLng,
			MaxLat: s.maxLat, MaxLng: s.maxLng,
			Depth: q.Depth + 1,
		}
		clat := (c.MinLat + c.MaxLat) / 2.0
		clng := (c.MinLng + c.MaxLng) / 2.0
		c.Center = &pb.LatLng{Lat: clat, Lng: clng}
		// radius: 矩形の対角線の 1/2 (sqrt で正しく計算)
		latDistM := latDegToM(c.MaxLat - c.MinLat)
		lngDistM := lngDegToM(c.MaxLng-c.MinLng, clat)
		c.RadiusM = 0.5 * math.Sqrt(latDistM*latDistM+lngDistM*lngDistM)
		children[i] = c
	}
	return children
}

// SideMetersMax は cell の長辺長 (m) を返す。停止条件 (minCellM) 判定に使用。
func (q *QuadCell) SideMetersMax() float64 {
	latM := latDegToM(q.MaxLat - q.MinLat)
	clat := (q.MinLat + q.MaxLat) / 2.0
	lngM := lngDegToM(q.MaxLng-q.MinLng, clat)
	if latM > lngM {
		return latM
	}
	return lngM
}

// NewQuadCellFromPolygonBBox は polygon の bbox から root QuadCell を作る。
func NewQuadCellFromPolygonBBox(poly *pb.Polygon) QuadCell {
	verts := poly.GetVertices()
	if len(verts) == 0 {
		return QuadCell{}
	}
	minLat, maxLat := verts[0].GetLat(), verts[0].GetLat()
	minLng, maxLng := verts[0].GetLng(), verts[0].GetLng()
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
	c := QuadCell{
		ID:     "root",
		MinLat: minLat, MinLng: minLng,
		MaxLat: maxLat, MaxLng: maxLng,
		Depth:  0,
	}
	clat := (minLat + maxLat) / 2.0
	clng := (minLng + maxLng) / 2.0
	c.Center = &pb.LatLng{Lat: clat, Lng: clng}
	latM := latDegToM(maxLat - minLat)
	lngM := lngDegToM(maxLng-minLng, clat)
	// radius は対角線の半分 (円で bbox をカバー)
	c.RadiusM = 0.5 * math.Sqrt(latM*latM+lngM*lngM)
	return c
}
