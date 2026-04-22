// Command pizza は PI-ZZA のメイン CLI。
//
// 使い方:
//
//	pizza bake --query "エニタイムフィットネス" --area "東京都"
//
// Phase 0: 骨格のみ（bake サブコマンドは未実装。ErrNotImplemented を返す）。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/menu"
	"github.com/clearclown/pizza/internal/oven"
)

func main() {
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
		fmt.Println("pi-zza v0.0.0 (Phase 0 Foundation)")
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
  pizza version
  pizza help

Commands:
  bake      Bake a fresh PI-ZZA: search stores and judge franchise operators
  version   Print version
  help      Show this help`)
}

func cmdBake(args []string) error {
	fs := flag.NewFlagSet("bake", flag.ExitOnError)
	query := fs.String("query", "", "brand / store name to search")
	area := fs.String("area", "", "area (e.g. prefecture) to cover")
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

	_ = area // Phase 0: area → polygon 変換は Phase 1 で実装

	p := &oven.Pipeline{
		// Phase 0: 接続を張らない。Phase 1 で実装。
	}

	return p.Bake(context.Background(), &pb.SearchStoresInGridRequest{
		Brand:    *query,
		CellKm:   cfg.GridCellKM,
		Language: "ja",
	})
}
