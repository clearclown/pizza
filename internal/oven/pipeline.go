// Package oven は PI-ZZA のオーケストレータ。
//
// Seed (M1) → Kitchen (M2) → Delivery (M3, optional) → Box (M4)
// のパイプラインを in-process またはネットワーク経由で組み合わせる。
package oven

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/grid"
	"github.com/clearclown/pizza/internal/verifier"
)

// ErrNotImplemented は未実装関数が呼ばれたときに返る。
var ErrNotImplemented = errors.New("oven: not implemented")

// SearchBackend は M1 Seed の抽象 — in-process Searcher でも gRPC client でも可。
// stores をコールバックで逐次 emit する。
type SearchBackend interface {
	SearchStoresInGrid(
		ctx context.Context,
		brand string,
		cells []grid.Cell,
		emit func(*pb.Store) error,
	) error
}

// KitchenBackend は M2 Firecrawl 抽象 (nil なら Scrape スキップ)。
type KitchenBackend interface {
	Scrape(ctx context.Context, url string) (*pb.MarkdownDoc, error)
}

// JudgeBackend は M3 Delivery の抽象 (nil なら Judge スキップ)。
type JudgeBackend interface {
	JudgeFranchiseType(ctx context.Context, req *pb.JudgeFranchiseTypeRequest) (*pb.JudgeFranchiseTypeResponse, error)
}

// VerifierBackend は Layer D — 国税庁法人番号 CSV バックエンドの抽象。
// nil なら Verify フェーズを skip。
type VerifierBackend interface {
	Verify(ctx context.Context, name string) verifier.VerifyResult
}

// BoxStore は M4 ストレージの永続化 interface。
type BoxStore interface {
	UpsertStore(ctx context.Context, s *pb.Store) error
	UpsertMarkdown(ctx context.Context, d *pb.MarkdownDoc) error
	UpsertJudgement(ctx context.Context, j *pb.JudgeResult) error
	UpsertVerification(ctx context.Context, operatorName, placeID string, vr verifier.VerifyResult) error
	ExportCSV(ctx context.Context, brand string) ([]byte, error)
}

// Pipeline は PI-ZZA のメインオーケストレータ。
type Pipeline struct {
	Seed     SearchBackend   // 必須
	Kitchen  KitchenBackend  // nil なら Markdown 取得を skip
	Judge    JudgeBackend    // nil なら FC 判定を skip
	Verifier VerifierBackend // nil なら Verify フェーズを skip (Layer D)
	Box      BoxStore        // 必須
	Workers  int             // Kitchen 並列度 (default 4)
}

// BakeReport は Bake の実行結果サマリ。
type BakeReport struct {
	Brand             string
	CellsGenerated    int
	StoresFound       int
	MarkdownsFetched  int
	JudgementsMade    int
	VerificationsRun  int
	CSV               []byte
	ElapsedSec        float64
}

// Bake はパイプライン全体を実行する。
func (p *Pipeline) Bake(ctx context.Context, req *pb.SearchStoresInGridRequest) (*BakeReport, error) {
	start := time.Now()
	if p.Seed == nil {
		return nil, fmt.Errorf("oven: Seed backend is nil")
	}
	if p.Box == nil {
		return nil, fmt.Errorf("oven: Box store is nil")
	}
	if req.GetBrand() == "" {
		return nil, fmt.Errorf("oven: brand is required")
	}
	cellKm := req.GetCellKm()
	if cellKm <= 0 {
		cellKm = 1.0
	}
	cells, err := grid.Split(req.GetPolygon(), cellKm)
	if err != nil {
		return nil, fmt.Errorf("oven: grid.Split: %w", err)
	}

	report := &BakeReport{
		Brand:          req.GetBrand(),
		CellsGenerated: len(cells),
	}

	// Seed → Box (逐次。Kitchen は次段で並列化)
	var stores []*pb.Store
	err = p.Seed.SearchStoresInGrid(ctx, req.GetBrand(), cells, func(st *pb.Store) error {
		if err := p.Box.UpsertStore(ctx, st); err != nil {
			return fmt.Errorf("upsert store: %w", err)
		}
		stores = append(stores, st)
		return nil
	})
	if err != nil {
		return report, fmt.Errorf("oven: seed: %w", err)
	}
	report.StoresFound = len(stores)

	// Kitchen → Markdown → Box (workers 並列)
	if p.Kitchen != nil {
		mdCount, err := p.runKitchenPhase(ctx, stores)
		report.MarkdownsFetched = mdCount
		if err != nil {
			return report, fmt.Errorf("oven: kitchen: %w", err)
		}
	}

	// Delivery → Judge → Box (逐次。Phase 3 で並列化検討)
	var judgeResults []*pb.JudgeResult
	if p.Judge != nil {
		for _, st := range stores {
			if err := ctx.Err(); err != nil {
				return report, err
			}
			jres, err := p.runJudgeOne(ctx, st)
			if err != nil {
				// 判定エラーは warn で継続 (Phase 1 挙動)
				continue
			}
			if err := p.Box.UpsertJudgement(ctx, jres); err != nil {
				return report, fmt.Errorf("upsert judgement: %w", err)
			}
			judgeResults = append(judgeResults, jres)
			report.JudgementsMade++
		}
	}

	// Verifier → operator_stores に法人番号を記録 (Layer D)
	if p.Verifier != nil {
		verified := p.runVerifierPhase(ctx, judgeResults)
		report.VerificationsRun = verified
	}

	// CSV export
	csv, err := p.Box.ExportCSV(ctx, req.GetBrand())
	if err != nil {
		return report, fmt.Errorf("oven: export csv: %w", err)
	}
	report.CSV = csv
	report.ElapsedSec = time.Since(start).Seconds()
	return report, nil
}

// runKitchenPhase は official_url のある stores に対して並列に Scrape する。
func (p *Pipeline) runKitchenPhase(ctx context.Context, stores []*pb.Store) (int, error) {
	workers := p.Workers
	if workers <= 0 {
		workers = 4
	}
	type job struct{ store *pb.Store }
	jobs := make(chan job)
	var wg sync.WaitGroup
	var counter int
	var mu sync.Mutex
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := ctx.Err(); err != nil {
					return
				}
				doc, err := p.Kitchen.Scrape(ctx, j.store.GetOfficialUrl())
				if err != nil {
					// scrape 失敗は warn 扱いで継続 (Phase 1)
					continue
				}
				if doc.Metadata == nil {
					doc.Metadata = map[string]string{}
				}
				doc.Metadata["place_id"] = j.store.GetPlaceId()
				if err := p.Box.UpsertMarkdown(ctx, doc); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				mu.Lock()
				counter++
				mu.Unlock()
			}
		}()
	}

	for _, st := range stores {
		if st.GetOfficialUrl() == "" {
			continue
		}
		jobs <- job{store: st}
	}
	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return counter, err
	default:
	}
	return counter, nil
}

func (p *Pipeline) runJudgeOne(ctx context.Context, st *pb.Store) (*pb.JudgeResult, error) {
	resp, err := p.Judge.JudgeFranchiseType(ctx, &pb.JudgeFranchiseTypeRequest{
		Context: &pb.StoreContext{Store: st},
	})
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.GetResult() == nil {
		return nil, fmt.Errorf("oven: Judge returned nil result")
	}
	// place_id を補完 (respect returned value; fall back to store)
	r := resp.GetResult()
	if r.GetPlaceId() == "" {
		r.PlaceId = st.GetPlaceId()
	}
	return r, nil
}

// runVerifierPhase は judgeResults の OperatorName を国税庁法人番号 CSV で検証し、
// operator_stores の Layer D カラムを更新する。エラーは warn で継続。
func (p *Pipeline) runVerifierPhase(ctx context.Context, judgeResults []*pb.JudgeResult) int {
	var count int
	for _, jres := range judgeResults {
		opName := jres.GetOperatorName()
		if opName == "" {
			continue
		}
		vr := p.Verifier.Verify(ctx, opName)
		if !vr.IsVerified {
			continue
		}
		if err := p.Box.UpsertVerification(ctx, opName, jres.GetPlaceId(), vr); err != nil {
			// 検証書き込みエラーは warn で継続
			continue
		}
		count++
	}
	return count
}
