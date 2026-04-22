package dough

import (
	"context"
	"fmt"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/grid"
)

// PlacesSearcher は PlacesClient の抽象 (テスト mockable)。
type PlacesSearcher interface {
	SearchText(ctx context.Context, req *SearchTextRequest) (*SearchTextResponse, error)
}

// Searcher は M1 Seed のオーケストレータ。
// grid.Cell 列 × ブランド名 → Store 列 (place_id 重複排除済)。
type Searcher struct {
	Places         PlacesSearcher
	Language       string // "ja" など
	Region         string // "JP" など
	MaxPerCell     int32  // 1 セル 1 呼び出しあたりの最大件数 (default 20)
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
		if err != nil {
			return fmt.Errorf("dough: cell=%s search: %w", cell.ID, err)
		}
		for i := range resp.Places {
			p := &resp.Places[i]
			if p.ID == "" {
				continue
			}
			if _, dup := seen[p.ID]; dup {
				continue
			}
			seen[p.ID] = struct{}{}
			if err := emit(p.ToStore(brand, cell.ID)); err != nil {
				return err
			}
		}
	}
	return nil
}
