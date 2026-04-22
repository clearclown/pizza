// Command pizza は PI-ZZA のメイン CLI。
//
// 使い方:
//
//	pizza bake --query "エニタイムフィットネス" --area "新宿"
//	pizza bake --query ブランド --area エリア --cell-km 1.0 --out var/output/result.csv
//
// 動作:
//  1. .env を自動読込 (GOOGLE_MAPS_API_KEY, FIRECRAWL_*, LLM_* 等)
//  2. area を bbox ポリゴンに解決 (internal/menu/areas)
//  3. in-process で M1 Seed (Places API) → M2 Kitchen (optional) → M4 Box
//  4. CSV を --out パスに書き出し
//  5. 要約を stdout
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/box"
	"github.com/clearclown/pizza/internal/dough"
	"github.com/clearclown/pizza/internal/menu"
	"github.com/clearclown/pizza/internal/oven"
	"github.com/clearclown/pizza/internal/toppings"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// .env 自動読込 (なくても OK)
	_ = godotenv.Load()

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "bake":
		if err := cmdBake(os.Args[2:]); err != nil {
			log.Fatalf("pizza bake: %v", err)
		}
	case "version":
		fmt.Println("pi-zza v0.1.0 (Phase 1 Green)")
	case "areas":
		fmt.Println("Known areas:")
		for _, a := range menu.KnownAreas() {
			fmt.Printf("  %s\n", a)
		}
	case "-h", "--help", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`🍕 PI-ZZA — Process Integration & Zonal Search Agent

Usage:
  pizza bake --query BRAND --area AREA [flags]
  pizza areas       # list available areas
  pizza version
  pizza help

Flags for 'bake':
  --query        ブランド名 (例: "エニタイムフィットネス")
  --area         エリア名  (例: "新宿", "東京都", "大阪府")
  --cell-km      メッシュセル幅 (km, default 1.0)
  --out          CSV 出力パス (default var/output/pizza-<brand>-<area>-<ts>.csv)
  --db           SQLite DB パス (default var/pizza.sqlite)
  --no-kitchen   Firecrawl 呼び出しを skip (default: .env があれば自動呼び出し)

Environment (.env から自動読込):
  GOOGLE_MAPS_API_KEY    必須
  FIRECRAWL_MODE         docker | saas (optional)
  FIRECRAWL_API_URL      docker 時
  FIRECRAWL_API_KEY      saas 時`)
}

func cmdBake(args []string) error {
	fs := flag.NewFlagSet("bake", flag.ExitOnError)
	query := fs.String("query", "", "brand / store name to search")
	area := fs.String("area", "", "area (e.g. 新宿, 東京都) to cover")
	cellKm := fs.Float64("cell-km", 1.0, "grid cell width in km")
	outPath := fs.String("out", "", "CSV output path (auto-named if empty)")
	dbPath := fs.String("db", "", "SQLite DB path (default var/pizza.sqlite)")
	noKitchen := fs.Bool("no-kitchen", false, "skip Firecrawl Markdown fetch")
	withJudge := fs.Bool("with-judge", false, "connect to delivery-service gRPC and run judgements")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *query == "" || *area == "" {
		return errors.New("--query and --area are required")
	}

	cfg, err := menu.FromEnv()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if cfg.GoogleMapsAPIKey == "" {
		return errors.New("GOOGLE_MAPS_API_KEY is not set (create .env from .env.example)")
	}
	if *dbPath == "" {
		*dbPath = cfg.DBPath
	}

	// Area → Polygon
	polygon, err := menu.ResolvePolygon(*area)
	if err != nil {
		return err
	}

	// M1 Seed backend (in-process)
	seed := &dough.Searcher{
		Places:   &dough.PlacesClient{APIKey: cfg.GoogleMapsAPIKey, Language: "ja", Region: "JP"},
		Language: "ja",
		Region:   "JP",
	}

	// M2 Kitchen backend (optional)
	var kitchen oven.KitchenBackend
	if !*noKitchen && cfg.FirecrawlMode != "" {
		kc, kerr := toppings.NewFromMode(cfg.FirecrawlMode, cfg.FirecrawlAPIURL, cfg.FirecrawlAPIKey)
		if kerr != nil {
			log.Printf("⚠️  kitchen disabled: %v", kerr)
		} else {
			kitchen = kc
		}
	}

	// M4 Box
	store, err := box.Open(*dbPath)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer store.Close()

	// M3 Delivery backend (optional, --with-judge で有効化)
	var judge oven.JudgeBackend
	if *withJudge {
		conn, jerr := grpc.NewClient(cfg.DeliveryServiceAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if jerr != nil {
			log.Printf("⚠️  judge disabled: %v", jerr)
		} else {
			defer conn.Close()
			judge = &grpcJudge{client: pb.NewDeliveryServiceClient(conn)}
		}
	}

	// Pipeline
	p := &oven.Pipeline{
		Seed:    seed,
		Kitchen: kitchen,
		Judge:   judge,
		Box:     store,
		Workers: cfg.MaxConcurrency,
	}

	fmt.Printf("🍕 Baking: brand=%q area=%q cell_km=%.1f\n", *query, *area, *cellKm)
	if kitchen != nil {
		fmt.Printf("   Kitchen enabled (%s)\n", cfg.FirecrawlMode)
	} else {
		fmt.Printf("   Kitchen disabled (skip Markdown fetch)\n")
	}
	if judge != nil {
		fmt.Printf("   Judge enabled (%s)\n", cfg.DeliveryServiceAddr)
	} else {
		fmt.Printf("   Judge disabled (no --with-judge)\n")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	report, err := p.Bake(ctx, &pb.SearchStoresInGridRequest{
		Brand:    *query,
		Polygon:  polygon,
		CellKm:   *cellKm,
		Language: "ja",
	})
	if err != nil {
		return fmt.Errorf("bake failed: %w", err)
	}

	// CSV 出力
	if *outPath == "" {
		ts := time.Now().Format("20060102-150405")
		*outPath = filepath.Join(cfg.OutputDir, fmt.Sprintf("pizza-%s-%s-%s.csv", *query, *area, ts))
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		return fmt.Errorf("mkdir out: %w", err)
	}
	if err := os.WriteFile(*outPath, report.CSV, 0o644); err != nil {
		return fmt.Errorf("write csv: %w", err)
	}

	fmt.Printf("\n✅ Done in %.1fs\n", report.ElapsedSec)
	fmt.Printf("   Cells:       %d\n", report.CellsGenerated)
	fmt.Printf("   Stores:      %d\n", report.StoresFound)
	fmt.Printf("   Markdowns:   %d\n", report.MarkdownsFetched)
	fmt.Printf("   Judgements:  %d\n", report.JudgementsMade)
	fmt.Printf("   CSV:         %s\n", *outPath)
	fmt.Printf("   DB:          %s\n", *dbPath)
	return nil
}

// grpcJudge は oven.JudgeBackend を gRPC client で満たす adapter。
type grpcJudge struct {
	client pb.DeliveryServiceClient
}

func (g *grpcJudge) JudgeFranchiseType(ctx context.Context, req *pb.JudgeFranchiseTypeRequest) (*pb.JudgeFranchiseTypeResponse, error) {
	return g.client.JudgeFranchiseType(ctx, req)
}
