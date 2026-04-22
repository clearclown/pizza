// Command dough-service は M1 Seed の gRPC サーバ本体。
//
// 環境変数:
//
//	GOOGLE_MAPS_API_KEY  必須
//	GOOGLE_MAPS_LANGUAGE optional (default "ja")
//	GOOGLE_MAPS_REGION   optional (default "JP")
//	DOUGH_LISTEN_ADDR    optional (default ":50051")
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/dough"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	apiKey := os.Getenv("GOOGLE_MAPS_API_KEY")
	if apiKey == "" {
		log.Fatal("GOOGLE_MAPS_API_KEY is required")
	}
	lang := envOr("GOOGLE_MAPS_LANGUAGE", "ja")
	region := envOr("GOOGLE_MAPS_REGION", "JP")
	addr := envOr("DOUGH_LISTEN_ADDR", ":50051")

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	srv := grpc.NewServer()
	pb.RegisterSeedServiceServer(srv, dough.NewServer(apiKey, lang, region))
	healthpb.RegisterHealthServer(srv, &healthServer{})
	reflection.Register(srv)

	fmt.Printf("🫓 dough-service listening on %s (lang=%s region=%s)\n", addr, lang, region)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// healthServer は grpc_health_v1 の最小実装 (常に SERVING)。
type healthServer struct {
	healthpb.UnimplementedHealthServer
}

func (h *healthServer) Check(_ context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (h *healthServer) Watch(_ *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	return stream.Send(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING})
}
