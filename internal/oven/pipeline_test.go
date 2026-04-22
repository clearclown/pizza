// 🔴 Red phase test — Phase 1+ で Green 化する。
//
// bufconn で in-memory gRPC サーバを立て、Pipeline が Seed → Delivery の順で
// 呼び出すかを検証する。docs/tdd-workflow.md 参照。
package oven_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/oven"
)

const bufSize = 1024 * 1024

// fakeSeed は Phase 0 Red テスト用の固定応答 Seed サーバ。
type fakeSeed struct {
	pb.UnimplementedSeedServiceServer
	stores []*pb.Store
}

func (s *fakeSeed) SearchStoresInGrid(
	req *pb.SearchStoresInGridRequest,
	stream pb.SeedService_SearchStoresInGridServer,
) error {
	_ = req
	for _, st := range s.stores {
		if err := stream.Send(&pb.SearchStoresInGridResponse{Store: st}); err != nil {
			return err
		}
	}
	return nil
}

// fakeDelivery は固定判定結果を返す Delivery サーバ。
type fakeDelivery struct {
	pb.UnimplementedDeliveryServiceServer
}

func (fakeDelivery) JudgeFranchiseType(
	_ context.Context,
	req *pb.JudgeFranchiseTypeRequest,
) (*pb.JudgeFranchiseTypeResponse, error) {
	return &pb.JudgeFranchiseTypeResponse{
		Result: &pb.JudgeResult{
			PlaceId:            req.GetContext().GetStore().GetPlaceId(),
			IsFranchise:        true,
			OperatorName:       "テスト運営会社",
			StoreCountEstimate: 25,
			Confidence:         0.9,
		},
	}, nil
}

func dialBuf(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestPipeline_Bake_executesSeedToDelivery(t *testing.T) {
	t.Parallel()

	// Seed server
	seedLis := bufconn.Listen(bufSize)
	seedSrv := grpc.NewServer()
	pb.RegisterSeedServiceServer(seedSrv, &fakeSeed{
		stores: []*pb.Store{
			{PlaceId: "p1", Brand: "B", Name: "Store1"},
			{PlaceId: "p2", Brand: "B", Name: "Store2"},
		},
	})
	go func() { _ = seedSrv.Serve(seedLis) }()
	t.Cleanup(seedSrv.Stop)

	// Delivery server
	delLis := bufconn.Listen(bufSize)
	delSrv := grpc.NewServer()
	pb.RegisterDeliveryServiceServer(delSrv, fakeDelivery{})
	go func() { _ = delSrv.Serve(delLis) }()
	t.Cleanup(delSrv.Stop)

	p := &oven.Pipeline{
		Seed:     pb.NewSeedServiceClient(dialBuf(t, seedLis)),
		Delivery: pb.NewDeliveryServiceClient(dialBuf(t, delLis)),
		// Kitchen / Box は Phase 1+ で mock 注入
	}

	err := p.Bake(context.Background(), &pb.SearchStoresInGridRequest{
		Brand: "エニタイムフィットネス",
	})
	// Phase 0: ErrNotImplemented を期待。Phase 1 以降でこの assertion を差し替える。
	require.ErrorIs(t, err, oven.ErrNotImplemented,
		"Phase 0 baseline: Bake must return ErrNotImplemented until Phase 1+ Green")
}
