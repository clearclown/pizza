// Package oven は PI-ZZA のオーケストレータ。
//
// Seed (M1) → Kitchen (M2) → Delivery (M3) → Box (M4) のパイプラインを
// gRPC / REST で繋ぎ、並列制御と retry を担う。
//
// Phase 0: 型定義のみ。Phase 1〜3 で各ステージを Green 化。
package oven

import (
	"context"
	"errors"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
)

// ErrNotImplemented は Phase 0 スタブが呼ばれたときに返る。
var ErrNotImplemented = errors.New("oven: not implemented (Phase 1+ target)")

// Pipeline は PI-ZZA のメインオーケストレータ。
type Pipeline struct {
	Seed     pb.SeedServiceClient
	Delivery pb.DeliveryServiceClient
	// Kitchen は REST クライアント（Firecrawl は別プロセス）。
	// Phase 1 以降で interface 化する。
	Kitchen KitchenClient
	// Box は SQLite 永続化層。
	Box BoxStore
}

// KitchenClient は Firecrawl REST をラップする interface。
type KitchenClient interface {
	ConvertToMarkdown(ctx context.Context, url string) (*pb.MarkdownDoc, error)
}

// BoxStore は M4 ストレージの永続化 interface。
type BoxStore interface {
	UpsertStore(ctx context.Context, s *pb.Store) error
	UpsertJudgement(ctx context.Context, j *pb.JudgeResult) error
}

// Bake はパイプライン全体を実行する。
// query.brand + polygon から Seed → Kitchen → Delivery を順に流し、結果を Box に保存する。
//
// Phase 0: 未実装。
func (p *Pipeline) Bake(ctx context.Context, query *pb.SearchStoresInGridRequest) error {
	_ = ctx
	_ = query
	return ErrNotImplemented
}
