package menu

import (
	"fmt"
	"sort"
	"strings"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
)

// bbox は緯度経度のバウンディングボックス。
type bbox struct {
	MinLat, MaxLat, MinLng, MaxLng float64
	Aliases                        []string // 別名
}

// toPolygon は反時計回り (SW, SE, NE, NW) で 4 頂点ポリゴンに変換する。
func (b bbox) toPolygon() *pb.Polygon {
	return &pb.Polygon{
		Vertices: []*pb.LatLng{
			{Lat: b.MinLat, Lng: b.MinLng},
			{Lat: b.MinLat, Lng: b.MaxLng},
			{Lat: b.MaxLat, Lng: b.MaxLng},
			{Lat: b.MaxLat, Lng: b.MinLng},
		},
	}
}

// areas は初版対応 area。47 都道府県 + 主要ターゲット都市。
// bbox は外接矩形 (ゆる目) — Phase 1 では検索漏れを防ぐため広めに取る。
var areas = map[string]bbox{
	// ─── 47 都道府県 ───
	"北海道":  {MinLat: 41.35, MaxLat: 45.55, MinLng: 139.33, MaxLng: 148.90},
	"青森県":  {MinLat: 40.22, MaxLat: 41.56, MinLng: 139.50, MaxLng: 141.70},
	"岩手県":  {MinLat: 38.75, MaxLat: 40.45, MinLng: 140.65, MaxLng: 142.08},
	"宮城県":  {MinLat: 37.77, MaxLat: 39.00, MinLng: 140.26, MaxLng: 141.70},
	"秋田県":  {MinLat: 38.87, MaxLat: 40.52, MinLng: 139.69, MaxLng: 140.99},
	"山形県":  {MinLat: 37.73, MaxLat: 39.20, MinLng: 139.52, MaxLng: 140.64},
	"福島県":  {MinLat: 36.78, MaxLat: 37.98, MinLng: 139.16, MaxLng: 141.05},
	"茨城県":  {MinLat: 35.73, MaxLat: 36.95, MinLng: 139.70, MaxLng: 140.85},
	"栃木県":  {MinLat: 36.19, MaxLat: 37.16, MinLng: 139.32, MaxLng: 140.30},
	"群馬県":  {MinLat: 35.98, MaxLat: 37.10, MinLng: 138.40, MaxLng: 139.68},
	"埼玉県":  {MinLat: 35.74, MaxLat: 36.29, MinLng: 138.70, MaxLng: 139.90},
	"千葉県":  {MinLat: 34.90, MaxLat: 36.10, MinLng: 139.74, MaxLng: 140.87},
	"東京都":  {MinLat: 35.48, MaxLat: 35.90, MinLng: 138.90, MaxLng: 139.92},
	"神奈川県": {MinLat: 35.13, MaxLat: 35.67, MinLng: 138.91, MaxLng: 139.82},
	"新潟県":  {MinLat: 36.74, MaxLat: 38.56, MinLng: 137.59, MaxLng: 139.85},
	"富山県":  {MinLat: 36.27, MaxLat: 36.98, MinLng: 136.80, MaxLng: 137.77},
	"石川県":  {MinLat: 36.07, MaxLat: 37.86, MinLng: 136.26, MaxLng: 137.36},
	"福井県":  {MinLat: 35.34, MaxLat: 36.29, MinLng: 135.45, MaxLng: 136.84},
	"山梨県":  {MinLat: 35.17, MaxLat: 35.97, MinLng: 138.19, MaxLng: 139.16},
	"長野県":  {MinLat: 35.20, MaxLat: 37.03, MinLng: 137.32, MaxLng: 138.74},
	"岐阜県":  {MinLat: 35.12, MaxLat: 36.45, MinLng: 136.27, MaxLng: 137.65},
	"静岡県":  {MinLat: 34.57, MaxLat: 35.70, MinLng: 137.47, MaxLng: 139.17},
	"愛知県":  {MinLat: 34.58, MaxLat: 35.42, MinLng: 136.67, MaxLng: 137.83},
	"三重県":  {MinLat: 33.73, MaxLat: 35.26, MinLng: 135.85, MaxLng: 136.99},
	"滋賀県":  {MinLat: 34.80, MaxLat: 35.70, MinLng: 135.79, MaxLng: 136.45},
	"京都府":  {MinLat: 34.70, MaxLat: 35.78, MinLng: 134.85, MaxLng: 136.05},
	"大阪府":  {MinLat: 34.26, MaxLat: 35.05, MinLng: 135.12, MaxLng: 135.74},
	"兵庫県":  {MinLat: 34.15, MaxLat: 35.67, MinLng: 134.25, MaxLng: 135.47},
	"奈良県":  {MinLat: 33.86, MaxLat: 34.78, MinLng: 135.60, MaxLng: 136.22},
	"和歌山県": {MinLat: 33.43, MaxLat: 34.38, MinLng: 135.00, MaxLng: 136.02},
	"鳥取県":  {MinLat: 35.03, MaxLat: 35.61, MinLng: 133.15, MaxLng: 134.47},
	"島根県":  {MinLat: 34.30, MaxLat: 35.60, MinLng: 131.67, MaxLng: 133.38},
	"岡山県":  {MinLat: 34.35, MaxLat: 35.36, MinLng: 133.26, MaxLng: 134.43},
	"広島県":  {MinLat: 34.03, MaxLat: 35.10, MinLng: 132.05, MaxLng: 133.48},
	"山口県":  {MinLat: 33.71, MaxLat: 34.79, MinLng: 130.76, MaxLng: 132.44},
	"徳島県":  {MinLat: 33.53, MaxLat: 34.25, MinLng: 133.64, MaxLng: 134.80},
	"香川県":  {MinLat: 34.00, MaxLat: 34.65, MinLng: 133.46, MaxLng: 134.45},
	"愛媛県":  {MinLat: 32.90, MaxLat: 34.31, MinLng: 132.01, MaxLng: 133.68},
	"高知県":  {MinLat: 32.70, MaxLat: 33.88, MinLng: 132.48, MaxLng: 134.31},
	"福岡県":  {MinLat: 33.14, MaxLat: 34.22, MinLng: 130.06, MaxLng: 131.19},
	"佐賀県":  {MinLat: 32.95, MaxLat: 33.66, MinLng: 129.74, MaxLng: 130.55},
	"長崎県":  {MinLat: 32.55, MaxLat: 34.72, MinLng: 128.58, MaxLng: 130.46},
	"熊本県":  {MinLat: 32.11, MaxLat: 33.22, MinLng: 129.99, MaxLng: 131.17},
	"大分県":  {MinLat: 32.72, MaxLat: 33.74, MinLng: 130.80, MaxLng: 132.09},
	"宮崎県":  {MinLat: 31.36, MaxLat: 32.84, MinLng: 130.70, MaxLng: 131.88},
	"鹿児島県": {MinLat: 27.01, MaxLat: 32.17, MinLng: 128.38, MaxLng: 131.16},
	"沖縄県":  {MinLat: 24.04, MaxLat: 27.88, MinLng: 122.93, MaxLng: 131.33},

	// ─── 主要ターゲット都市 / エリア ───
	"東京": {Aliases: []string{"tokyo"}, MinLat: 35.52, MaxLat: 35.82, MinLng: 139.56, MaxLng: 139.92},
	"新宿":    {Aliases: []string{"shinjuku"}, MinLat: 35.676, MaxLat: 35.720, MinLng: 139.680, MaxLng: 139.725},
	"渋谷":    {Aliases: []string{"shibuya"}, MinLat: 35.640, MaxLat: 35.680, MinLng: 139.680, MaxLng: 139.725},
	"銀座":    {Aliases: []string{"ginza"}, MinLat: 35.660, MaxLat: 35.685, MinLng: 139.755, MaxLng: 139.785},
	"池袋":    {Aliases: []string{"ikebukuro"}, MinLat: 35.720, MaxLat: 35.748, MinLng: 139.695, MaxLng: 139.735},
	"横浜": {Aliases: []string{"yokohama"}, MinLat: 35.390, MaxLat: 35.510, MinLng: 139.570, MaxLng: 139.710},
	"大阪": {Aliases: []string{"osaka"}, MinLat: 34.610, MaxLat: 34.770, MinLng: 135.420, MaxLng: 135.590},
	"京都": {Aliases: []string{"kyoto"}, MinLat: 34.930, MaxLat: 35.090, MinLng: 135.660, MaxLng: 135.810},
	"名古屋": {Aliases: []string{"nagoya"}, MinLat: 35.090, MaxLat: 35.240, MinLng: 136.830, MaxLng: 137.020},
	"福岡市":  {Aliases: []string{"fukuoka"}, MinLat: 33.540, MaxLat: 33.670, MinLng: 130.320, MaxLng: 130.470},
	"札幌":    {Aliases: []string{"sapporo"}, MinLat: 42.990, MaxLat: 43.140, MinLng: 141.290, MaxLng: 141.480},
	"仙台":    {Aliases: []string{"sendai"}, MinLat: 38.200, MaxLat: 38.350, MinLng: 140.820, MaxLng: 141.000},
}

// ResolvePolygon は area 名 (日本語または英語小文字) から Polygon を生成する。
// 未知の area は ErrUnknownArea。
func ResolvePolygon(area string) (*pb.Polygon, error) {
	key := strings.TrimSpace(area)
	if key == "" {
		return nil, fmt.Errorf("menu: area is empty")
	}
	if b, ok := areas[key]; ok {
		return b.toPolygon(), nil
	}
	// alias (英語) も試す
	lower := strings.ToLower(key)
	for _, b := range areas {
		for _, a := range b.Aliases {
			if a == lower {
				return b.toPolygon(), nil
			}
		}
	}
	return nil, fmt.Errorf("menu: unknown area %q (use one of: %s)",
		area, strings.Join(KnownAreas(), ", "))
}

// KnownAreas はサポートしている area 名 (日本語) をソート済みで返す。
func KnownAreas() []string {
	out := make([]string, 0, len(areas))
	for k := range areas {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
