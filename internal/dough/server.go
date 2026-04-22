package dough

import (
	"context"
	"fmt"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/grid"
)

// Server は pizza.v1.SeedServiceServer の実装。
type Server struct {
	pb.UnimplementedSeedServiceServer
	Searcher *Searcher
}

// NewServer は Places API キー + 言語/地域から Server を組み立てる。
func NewServer(apiKey, language, region string) *Server {
	return &Server{
		Searcher: &Searcher{
			Places:   &PlacesClient{APIKey: apiKey, Language: language, Region: region},
			Language: language,
			Region:   region,
		},
	}
}

// SearchStoresInGrid は gRPC streaming。polygon を grid.Split で分割し、
// 各セルで Places API を叩いて Store を stream で返す。
func (s *Server) SearchStoresInGrid(
	req *pb.SearchStoresInGridRequest,
	stream pb.SeedService_SearchStoresInGridServer,
) error {
	if s.Searcher == nil {
		return fmt.Errorf("dough: Server.Searcher is nil")
	}
	if req.GetBrand() == "" {
		return fmt.Errorf("dough: Brand is required")
	}
	cellKm := req.GetCellKm()
	if cellKm <= 0 {
		cellKm = 1.0
	}
	cells, err := grid.Split(req.GetPolygon(), cellKm)
	if err != nil {
		return fmt.Errorf("dough: split: %w", err)
	}
	if len(cells) == 0 {
		return nil
	}
	// 言語オーバーライド (req 優先)
	prevLang := s.Searcher.Language
	if l := req.GetLanguage(); l != "" {
		s.Searcher.Language = l
		defer func() { s.Searcher.Language = prevLang }()
	}

	return s.Searcher.SearchStoresInGrid(stream.Context(), req.GetBrand(), cells,
		func(st *pb.Store) error {
			return stream.Send(&pb.SearchStoresInGridResponse{Store: st})
		},
	)
}

// 型レベルでの契約チェック (compile 時)
var _ pb.SeedServiceServer = (*Server)(nil)
var _ context.Context = context.Background() // avoid unused import
