package dough_test

import (
	"context"
	"io"
	"net"
	"testing"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/dough"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// dialBufconnForDough は bufconn で dough.Server を立てて client を返す。
func dialBufconnForDough(t *testing.T, srv *dough.Server) pb.SeedServiceClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	g := grpc.NewServer()
	pb.RegisterSeedServiceServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewSeedServiceClient(conn)
}

// tokyoSquare は 1 km グリッドだとおよそ 4 セルになる小さなポリゴン。
func tokyoSquare() *pb.Polygon {
	return &pb.Polygon{
		Vertices: []*pb.LatLng{
			{Lat: 35.680, Lng: 139.760},
			{Lat: 35.680, Lng: 139.770},
			{Lat: 35.690, Lng: 139.770},
			{Lat: 35.690, Lng: 139.760},
		},
	}
}

func TestServer_SearchStoresInGrid_streamsResultsOverGRPC(t *testing.T) {
	t.Parallel()
	// 全セルで同じ place を返す stub (Searcher が dedupe する想定)
	fp := &fakePlaces{byCenterLat: map[float64][]dough.PlaceRaw{}}
	// 任意の lat key に対して同一 places を返すため、map より簡単な fallback 方式を使う:
	stub := &uniformPlaces{places: []dough.PlaceRaw{
		{ID: "SHIN-001", DisplayName: dough.DisplayName{Text: "テスト 新宿店"}},
		{ID: "SHIN-002", DisplayName: dough.DisplayName{Text: "テスト 神田店"}},
	}}
	_ = fp
	srv := &dough.Server{Searcher: &dough.Searcher{Places: stub}}
	cli := dialBufconnForDough(t, srv)

	stream, err := cli.SearchStoresInGrid(context.Background(), &pb.SearchStoresInGridRequest{
		Brand:    "テストブランド",
		Polygon:  tokyoSquare(),
		CellKm:   1.0,
		Language: "ja",
	})
	require.NoError(t, err)

	var got []*pb.Store
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, resp.GetStore())
	}
	// 2 件の unique place が全セル分通過しても dedupe で 2 件のまま
	require.Len(t, got, 2)
	ids := []string{got[0].GetPlaceId(), got[1].GetPlaceId()}
	assert.ElementsMatch(t, []string{"SHIN-001", "SHIN-002"}, ids)
	for _, s := range got {
		assert.Equal(t, "テストブランド", s.GetBrand())
		assert.NotEmpty(t, s.GetGridCellId(), "grid_cell_id should be propagated")
	}
	assert.Greater(t, stub.calls, 1, "multiple cells should have been queried")
}

func TestServer_SearchStoresInGrid_rejectsEmptyBrand(t *testing.T) {
	t.Parallel()
	srv := &dough.Server{Searcher: &dough.Searcher{Places: &uniformPlaces{}}}
	cli := dialBufconnForDough(t, srv)

	stream, err := cli.SearchStoresInGrid(context.Background(), &pb.SearchStoresInGridRequest{
		Brand:   "",
		Polygon: tokyoSquare(),
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Brand is required")
}

// uniformPlaces は全呼び出しで同じ places を返す PlacesSearcher。
type uniformPlaces struct {
	places []dough.PlaceRaw
	calls  int
}

func (u *uniformPlaces) SearchText(_ context.Context, _ *dough.SearchTextRequest) (*dough.SearchTextResponse, error) {
	u.calls++
	return &dough.SearchTextResponse{Places: u.places}, nil
}
