package dough

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/grid"
)

// PlacesSearcher は PlacesClient の抽象 (テスト mockable)。
type PlacesSearcher interface {
	SearchText(ctx context.Context, req *SearchTextRequest) (*SearchTextResponse, error)
}

// SearchMetrics は bake の過程で発生したイベント集計。
// API call 数、brand-filter で reject、polygon で reject など。
type SearchMetrics struct {
	APICalls             int // SearchText 呼び出し回数
	RawResultsTotal      int // API が返した delete 前の件数合計
	RejectedBrandFilter  int // Layer A brand-filter で弾かれた
	RejectedPolygon      int // polygon post-filter で弾かれた
	RejectedDuplicate    int // place_id 重複で弾かれた
	CellsHitCap          int // max_result_count に達した cell 数 (取りこぼし疑い)
	Emitted              int // 最終 emit された件数
}

// Searcher は M1 Seed のオーケストレータ。
// grid.Cell 列 × ブランド名 → Store 列 (place_id 重複排除済)。
type Searcher struct {
	Places     PlacesSearcher
	Language   string // "ja" など
	Region     string // "JP" など
	MaxPerCell int32  // 1 セル 1 呼び出しあたりの最大件数 (default 20)
	// StrictBrandMatch が true のとき、Places API が返した店舗の
	// displayName (or address) に brand 文字列が含まれないものを除外する。
	// Places Text Search は fuzzy match で別ブランドを返すことがあり、
	// BI ツール用途ではデフォルト true (厳密) が適切。
	StrictBrandMatch bool
	// RestrictToPolygon が非 nil のとき、Places から返った店舗の lat/lng が
	// この polygon 内にあるかを verify し、外れていれば emit しない。
	// Places の locationBias.circle は「優先」であって「絞り込み」ではないため、
	// 県外の店舗が混ざる問題を決定論で解消する。
	RestrictToPolygon *pb.Polygon
	// Metrics は 直前実行の統計。SearchStoresInGrid ごとにリセット。
	Metrics SearchMetrics
}

// SearchStoresInGrid は全セルを巡回して Store を emit する。
// emit コールバックで呼び出し側にストリーム (gRPC stream の Send 等)。
// 同一 place_id は一度だけ emit される。
func (s *Searcher) SearchStoresInGrid(
	ctx context.Context,
	brand string,
	cells []grid.Cell,
	emit func(*pb.Store) error,
) error {
	if s.Places == nil {
		return fmt.Errorf("dough: Searcher.Places is nil")
	}
	max := s.MaxPerCell
	if max <= 0 {
		max = 20
	}
	// Phase 12: metrics リセット
	s.Metrics = SearchMetrics{}

	var polygonVerts []*pb.LatLng
	if s.RestrictToPolygon != nil {
		polygonVerts = s.RestrictToPolygon.GetVertices()
	}

	seen := make(map[string]struct{})
	for _, cell := range cells {
		if err := ctx.Err(); err != nil {
			return err
		}
		req := &SearchTextRequest{
			TextQuery:      brand,
			LanguageCode:   s.Language,
			RegionCode:     s.Region,
			MaxResultCount: max,
			LocationBias: &LocationBias{
				Circle: &Circle{
					Center: Location{
						Latitude:  cell.Center.GetLat(),
						Longitude: cell.Center.GetLng(),
					},
					Radius: cell.RadiusM,
				},
			},
		}
		resp, err := s.Places.SearchText(ctx, req)
		s.Metrics.APICalls++
		if err != nil {
			return fmt.Errorf("dough: cell=%s search: %w", cell.ID, err)
		}
		s.Metrics.RawResultsTotal += len(resp.Places)
		// 20 件返ってきた cell は取りこぼし疑い (recursive split 候補)
		if int32(len(resp.Places)) >= max {
			s.Metrics.CellsHitCap++
		}

		for i := range resp.Places {
			p := &resp.Places[i]
			if p.ID == "" {
				continue
			}
			if _, dup := seen[p.ID]; dup {
				s.Metrics.RejectedDuplicate++
				continue
			}
			// Phase 5 強化: ブランドフィルタ
			if s.StrictBrandMatch && !matchesBrand(p, brand) {
				s.Metrics.RejectedBrandFilter++
				continue
			}
			// Phase 12: polygon post-filter (lat/lng が指定 polygon 内か verify)
			if len(polygonVerts) >= 3 {
				lat := p.Location.Latitude
				lng := p.Location.Longitude
				if !grid.PointInPolygon(lat, lng, polygonVerts) {
					s.Metrics.RejectedPolygon++
					continue
				}
			}
			seen[p.ID] = struct{}{}
			s.Metrics.Emitted++
			if err := emit(p.ToStore(brand, cell.ID)); err != nil {
				return err
			}
		}
	}
	return nil
}

// AdaptiveSearchOptions は SearchStoresAdaptive の制御パラメータ。
type AdaptiveSearchOptions struct {
	// MinCellMeters は cell の短辺がこの値未満になったら分割を停止。
	// 例: 100m = 1 cell に 20 件超あっても諦めて emit。
	MinCellMeters float64
	// MaxDepth は root から何段まで分割するか。安全網。
	MaxDepth int
	// SaturationThreshold: 何件返ったら 「飽和 = 取りこぼし疑い」とみなして分割するか。
	// API 上限は 20、ここは 20 で設定するのが標準。
	SaturationThreshold int32
}

func (o *AdaptiveSearchOptions) withDefaults() AdaptiveSearchOptions {
	res := *o
	if res.MinCellMeters <= 0 {
		res.MinCellMeters = 100.0
	}
	if res.MaxDepth <= 0 {
		res.MaxDepth = 6
	}
	if res.SaturationThreshold <= 0 {
		res.SaturationThreshold = 20
	}
	return res
}

// SearchStoresAdaptive は quad-tree 式 adaptive split で Places を網羅検索する。
// 20 件制約を突破する戦略: 1 cell で 20 件返ったら 4 分割して再検索、
// 一定以下になる or depth/minCell 到達で停止。
//
// 引数:
//   - polygon: 検索対象の多角形 (bbox 基準で root cell 生成)
//   - opts: 分割制御 (nil で default)
//
// 既存 SearchStoresInGrid と併用する想定。
func (s *Searcher) SearchStoresAdaptive(
	ctx context.Context,
	brand string,
	polygon *pb.Polygon,
	opts *AdaptiveSearchOptions,
	emit func(*pb.Store) error,
) error {
	if s.Places == nil {
		return fmt.Errorf("dough: Searcher.Places is nil")
	}
	if polygon == nil {
		return fmt.Errorf("dough: polygon is nil")
	}
	o := (&AdaptiveSearchOptions{}).withDefaults()
	if opts != nil {
		o = opts.withDefaults()
	}
	max := s.MaxPerCell
	if max <= 0 {
		max = 20
	}
	// metrics reset
	s.Metrics = SearchMetrics{}

	root := grid.NewQuadCellFromPolygonBBox(polygon)
	seen := make(map[string]struct{})
	verts := polygon.GetVertices()

	// Places API の circle.radius 上限 (50km)。これを超える cell は search 不可、
	// 必ず subdivide してから search する。
	const placesMaxRadiusM = 50000.0
	// iterative DFS (stack)
	stack := []grid.QuadCell{root}
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		// pop
		cell := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Phase 16 fix: cell が Places API circle radius 上限 (50km) 超なら
		// 必ず分割して search に進まない。initial root が bbox 全体の場合の対策。
		if cell.RadiusM > placesMaxRadiusM*0.95 && cell.Depth < o.MaxDepth {
			for _, child := range cell.Subdivide() {
				stack = append(stack, child)
			}
			continue
		}

		// cell の中心が polygon 完全外なら skip (矩形は含む場合)
		if !grid.PointInPolygon(cell.Center.GetLat(), cell.Center.GetLng(), verts) {
			// cell が polygon と重なるかの保守的チェック (矩形の 4 頂点)
			corners := [4][2]float64{
				{cell.MinLat, cell.MinLng},
				{cell.MinLat, cell.MaxLng},
				{cell.MaxLat, cell.MinLng},
				{cell.MaxLat, cell.MaxLng},
			}
			any := false
			for _, c := range corners {
				if grid.PointInPolygon(c[0], c[1], verts) {
					any = true
					break
				}
			}
			if !any {
				continue // 完全に外
			}
		}

		req := &SearchTextRequest{
			TextQuery:      brand,
			LanguageCode:   s.Language,
			RegionCode:     s.Region,
			MaxResultCount: max,
			LocationBias: &LocationBias{
				Circle: &Circle{
					Center: Location{
						Latitude: cell.Center.GetLat(), Longitude: cell.Center.GetLng(),
					},
					Radius: cell.RadiusM,
				},
			},
		}
		resp, err := s.Places.SearchText(ctx, req)
		s.Metrics.APICalls++
		if err != nil {
			return fmt.Errorf("dough: adaptive cell=%s: %w", cell.ID, err)
		}
		n := int32(len(resp.Places))
		s.Metrics.RawResultsTotal += int(n)

		// emit 処理 (ブランド + polygon フィルタ + dedup)
		for i := range resp.Places {
			p := &resp.Places[i]
			if p.ID == "" {
				continue
			}
			if _, dup := seen[p.ID]; dup {
				s.Metrics.RejectedDuplicate++
				continue
			}
			if s.StrictBrandMatch && !matchesBrand(p, brand) {
				s.Metrics.RejectedBrandFilter++
				continue
			}
			if !grid.PointInPolygon(p.Location.Latitude, p.Location.Longitude, verts) {
				s.Metrics.RejectedPolygon++
				continue
			}
			seen[p.ID] = struct{}{}
			s.Metrics.Emitted++
			if err := emit(p.ToStore(brand, cell.ID)); err != nil {
				return err
			}
		}

		// 分割判定: 飽和 + depth 余裕 + cell 大きさ十分
		if n >= o.SaturationThreshold &&
			cell.Depth < o.MaxDepth &&
			cell.SideMetersMax() > o.MinCellMeters {
			s.Metrics.CellsHitCap++
			for _, child := range cell.Subdivide() {
				stack = append(stack, child)
			}
		}
	}
	return nil
}

// brand 判定の閾値定数。KB に依存しすぎず、類似度数値を主判断に。
const (
	brandSimStrongAccept = 0.85 // これ以上なら KB を見ずに accept
	brandSimHardReject   = 0.20 // これ未満なら KB を見ずに reject
)

// matchesBrand は Places の 1 件が brand に該当するかを判定する。
//
// 「ナレッジベース (blocklist) に影響されすぎない」方針:
//   類似度スコアを主判断とし、KB は「中間帯の曖昧ケースを絞る補助情報」
//   にとどめる。KB が空でも、類似度だけで 98% のケースが決まるように。
//
// 評価順 (上から):
//   1. 直接 substring (生文字列) / 正規化後 substring            → accept (確実)
//   2. brandSimilarityScore >= 0.85                              → accept (高一致)
//   3. brandSimilarityScore < 0.20                               → reject (確実に別物)
//   4. prefix 5 文字以上が name に含まれる                         → accept
//   5. ここまで中間帯 — KB blocklist を参照 (情報として) → hit なら reject
//   6. それ以外 → accept (False negative より False positive を許容)
//
// brand が空文字の場合は必ずマッチ (filter 無効化)。
// LLM 推論は使わない (決定論的)。
func matchesBrand(p *PlaceRaw, brand string) bool {
	if brand == "" {
		return true
	}
	name := p.DisplayName.Text

	// 1. 完全 substring (生 & 正規化)
	if strings.Contains(name, brand) {
		return true
	}
	nname := normalizeForBrandMatch(name)
	nbrand := normalizeForBrandMatch(brand)
	if nname == "" || nbrand == "" {
		return false
	}
	if strings.Contains(nname, nbrand) {
		return true
	}

	// 2. 類似度スコア (主判定)
	score := brandSimilarityScore(brand, name)
	if score >= brandSimStrongAccept {
		return true
	}
	if score < brandSimHardReject {
		return false
	}

	// 3. prefix 5 文字以上
	if len([]rune(nbrand)) >= 5 {
		prefix := string([]rune(nbrand)[:5])
		if strings.Contains(nname, prefix) {
			return true
		}
	}

	// 4. 中間帯だけ KB を参照 (保険)
	if isKnownConflict(brand, name) {
		return false
	}

	// 中間帯で KB ヒットもない → 曖昧。recall 重視で accept
	return true
}

// normalizeForBrandMatch はブランド名の軽量正規化。
// 小文字化、空白/中黒/ASCII ハイフンを除去する。
// 注意: 長音記号 "ー" (U+30FC) は除去しない — "スターバックス" 等の
// 意味ある音を含むため。ASCII "-" のみを除去する
// (例: "セブン-イレブン" ⇔ "セブンイレブン" の吸収)。
func normalizeForBrandMatch(s string) string {
	s = strings.ToLower(s)
	replacer := strings.NewReplacer(
		" ", "",
		"　", "",
		"・", "",
		"-", "",
	)
	return replacer.Replace(s)
}
