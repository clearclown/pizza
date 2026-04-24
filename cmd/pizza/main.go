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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/box"
	"github.com/clearclown/pizza/internal/dough"
	"github.com/clearclown/pizza/internal/grid"
	"github.com/clearclown/pizza/internal/menu"
	"github.com/clearclown/pizza/internal/oven"
	"github.com/clearclown/pizza/internal/toppings"
	"github.com/clearclown/pizza/internal/verifier"

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
	case "research":
		if err := cmdResearch(os.Args[2:]); err != nil {
			log.Fatalf("pizza research: %v", err)
		}
	case "serve":
		if err := cmdServe(os.Args[2:]); err != nil {
			log.Fatalf("pizza serve: %v", err)
		}
	case "migrate":
		if err := cmdMigrate(os.Args[2:]); err != nil {
			log.Fatalf("pizza migrate: %v", err)
		}
	case "audit":
		if err := cmdAudit(os.Args[2:]); err != nil {
			log.Fatalf("pizza audit: %v", err)
		}
	case "scan":
		if err := cmdScan(os.Args[2:]); err != nil {
			log.Fatalf("pizza scan: %v", err)
		}
	case "bench":
		if err := cmdBench(os.Args[2:]); err != nil {
			log.Fatalf("pizza bench: %v", err)
		}
	case "registry-expand":
		if err := cmdRegistryExpand(os.Args[2:]); err != nil {
			log.Fatalf("pizza registry-expand: %v", err)
		}
	case "megafranchisee":
		if err := cmdMegaFranchisee(os.Args[2:]); err != nil {
			log.Fatalf("pizza megafranchisee: %v", err)
		}
	case "houjin-import":
		if err := cmdHoujinImport(os.Args[2:]); err != nil {
			log.Fatalf("pizza houjin-import: %v", err)
		}
	case "houjin-search":
		if err := cmdHoujinSearch(os.Args[2:]); err != nil {
			log.Fatalf("pizza houjin-search: %v", err)
		}
	case "jfa-sync":
		if err := cmdJFASync(os.Args[2:]); err != nil {
			log.Fatalf("pizza jfa-sync: %v", err)
		}
	case "jfa-export":
		if err := cmdJFAExport(os.Args[2:]); err != nil {
			log.Fatalf("pizza jfa-export: %v", err)
		}
	case "integrate":
		if err := cmdIntegrate(os.Args[2:]); err != nil {
			log.Fatalf("pizza integrate: %v", err)
		}
	case "evaluate":
		if err := cmdEvaluate(os.Args[2:]); err != nil {
			log.Fatalf("pizza evaluate: %v", err)
		}
	case "enrich":
		if err := cmdEnrich(os.Args[2:]); err != nil {
			log.Fatalf("pizza enrich: %v", err)
		}
	case "version":
		fmt.Println("pi-zza v0.1.0 (Phase 6)")
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

Subcommands:
  pizza bake      店舗 seed (Places) + Markdown (Firecrawl) + 判定 (gRPC) + Box
  pizza research  SQLite stores から operator 深掘り (Research Pipeline, Python)
  pizza serve     Delivery gRPC service (browser-use + LLM 判定) を起動
  pizza migrate   SQLite schema/view の最新化 (+ --with-registry で Ground Truth seed)
  pizza audit     ブランド全加盟店の Top-down × Bottom-up 突合 + CSV 出力
  pizza scan      1 コマンドで全自動 (migrate→bake→research→audit、サマリ CSV)
  pizza bench     複数ブランド逐次実走 + metrics JSON (速度/API call 数 計測)
  pizza registry-expand  未登録 operator を集計して YAML-ready 候補を出力
  pizza megafranchisee   brand 横断で事業会社別 total 店舗数を集計 (operator 主語 CSV+YAML)
  pizza houjin-import    国税庁 法人番号 CSV/zip を local SQLite に取込 (Layer D Ground Truth)
  pizza houjin-search    local 法人番号 index を operator 名で検索
  pizza jfa-sync         日本フランチャイズチェーン協会 会員企業一覧を scrape + ORM 登録
  pizza jfa-export       ORM 登録済のブランド×事業会社リストを CSV 出力
  pizza integrate        JFA / 国税庁 CSV / pipeline operator を ORM に統合 (総合 FC 事業会社リスト)
  pizza evaluate         truth (JFA) × pipeline の突合 metric を算出 (brand/operator/link recall)
  pizza enrich           Places Details (phone) + browser-use 逆引きで operator 一括特定
  pizza areas     利用可能エリア一覧
  pizza version
  pizza help

Flags for 'bake':
  --query           ブランド名 (例: "エニタイムフィットネス")
  --area            エリア名  (例: "新宿", "東京都")
  --cell-km         メッシュセル幅 (km, default 1.0)
  --out             CSV 出力パス (default var/output/pizza-<brand>-<area>-<ts>.csv)
  --db              SQLite DB パス (default var/pizza.sqlite)
  --no-kitchen      Firecrawl 呼び出しを skip
  --with-judge      delivery-service gRPC に判定を委譲する
  --judge-mode      judge 経路 (mock|live|panel)、delivery-service の DELIVERY_MODE と一致させる想定
  --verify-houjin   国税庁法人番号 CSV で operator 実在確認 (Layer D, オフライン動作)

Flags for 'research':
  --brand           対象ブランド (空で全ブランド)
  --db              SQLite DB path (default var/pizza.sqlite)
  --max-stores      処理上限店舗数 (0 で全件)
  --no-verify       CrossVerifier を skip
  --verify-houjin   国税庁法人番号 API で operator 実在確認 (HOUJIN_BANGOU_APP_ID 必須)
  --expand          Places API で同 operator の他店舗を広域検索 (芋づる式)
  --expand-area     --expand 時の area hint (例: "東京都")
  --concurrency     並行 fetch 数 (default 4)

Flags for 'scan':  (1 コマンドで全自動)
  --brand           対象ブランド (必須、例: "エニタイムフィットネス")
  --areas           カンマ区切り area (必須、例: "東京都,大阪府")
  --cell-km         bake メッシュ幅 (default 2.0)
  --max-research    research で処理する上限店舗数 (default 100)
  --out             サマリ CSV 出力パス (default var/scan/<brand>-<ts>.csv)
  --no-research     per-store 抽出を skip (bake + audit のみ)

Flags for 'audit':
  --brand           対象ブランド (registry 登録済み、例: "エニタイムフィットネス")
  --areas           カンマ区切り area (例: "東京都,大阪府,愛知県")
  --cell-km         Bottom-up bake のメッシュ幅 (default 2.0)
  --db              SQLite DB path (default var/pizza.sqlite)
  --out             メイン CSV 出力パス
  --skip-bake       Bottom-up bake を skip し、既存 stores のみで突合
  --addr-threshold  住所類似度しきい値 (default 0.7)
  --radius-m        緯度経度突合半径 (default 150.0)

Flags for 'serve':
  --mode            mock | live | panel  (default env DELIVERY_MODE or mock)
  --addr            listen addr (default env DELIVERY_LISTEN_ADDR or 0.0.0.0:50053)

Environment (.env から自動読込):
  GOOGLE_MAPS_API_KEY     必須 (Places API)
  FIRECRAWL_MODE          docker | saas (optional)
  FIRECRAWL_API_URL       docker 時
  FIRECRAWL_API_KEY       saas 時
  LLM_PROVIDER            anthropic | openai | gemini (live 時)
  GEMINI_API_KEY          panel 時 (Worker)
  ANTHROPIC_API_KEY       panel 時 (Critic)
  HOUJIN_BANGOU_APP_ID    research --verify-houjin 時

Example (full pipeline 1 コマンド):
  pizza serve --mode panel &                    # 別シェルで gRPC 起動
  pizza bake --query エニタイムフィットネス --area 新宿 --with-judge --judge-mode panel
  pizza research --brand エニタイムフィットネス --expand --verify-houjin`)
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
	judgeMode := fs.String("judge-mode", "", "delivery-service の mode ヒント (mock|live|panel), env DELIVERY_MODE にも反映")
	verifyHoujin := fs.Bool("verify-houjin", false, "国税庁法人番号 CSV で operator 実在確認 (Layer D, HOUJIN_BANGOU_APP_ID 不要)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	// --judge-mode が渡されたら env に伝搬 (delivery-service 側が読む)
	if *judgeMode != "" {
		_ = os.Setenv("DELIVERY_MODE", *judgeMode)
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
	// StrictBrandMatch=true: Places Text Search の fuzzy 結果 (別ブランド混入) を
	// displayName ベースで弾く。BI ツール用途では厳密な方が正しい。
	seed := &dough.Searcher{
		Places:           &dough.PlacesClient{APIKey: cfg.GoogleMapsAPIKey, Language: "ja", Region: "JP"},
		Language:         "ja",
		Region:           "JP",
		StrictBrandMatch: true,
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

	// Layer D: Verifier (国税庁法人番号 CSV — オフライン動作)
	var verifierBackend oven.VerifierBackend
	if *verifyHoujin {
		vc, verr := verifier.New()
		if verr != nil {
			log.Printf("⚠️  verifier disabled: %v", verr)
		} else {
			verifierBackend = vc
		}
	}

	// Pipeline
	p := &oven.Pipeline{
		Seed:     seed,
		Kitchen:  kitchen,
		Judge:    judge,
		Verifier: verifierBackend,
		Box:      store,
		Workers:  cfg.MaxConcurrency,
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
	if verifierBackend != nil {
		fmt.Printf("   Verifier enabled (Layer D: 国税庁法人番号 CSV)\n")
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
	fmt.Printf("   Cells:         %d\n", report.CellsGenerated)
	fmt.Printf("   Stores:        %d\n", report.StoresFound)
	fmt.Printf("   Markdowns:     %d\n", report.MarkdownsFetched)
	fmt.Printf("   Judgements:    %d\n", report.JudgementsMade)
	fmt.Printf("   Verifications: %d\n", report.VerificationsRun)
	fmt.Printf("   CSV:           %s\n", *outPath)
	fmt.Printf("   DB:            %s\n", *dbPath)
	return nil
}

// grpcJudge は oven.JudgeBackend を gRPC client で満たす adapter。
type grpcJudge struct {
	client pb.DeliveryServiceClient
}

func (g *grpcJudge) JudgeFranchiseType(ctx context.Context, req *pb.JudgeFranchiseTypeRequest) (*pb.JudgeFranchiseTypeResponse, error) {
	return g.client.JudgeFranchiseType(ctx, req)
}

// cmdRegistryExpand は operator_stores から未登録 operator を集計、
// registry 追加候補を YAML に書き出す (Phase 17.4)。
func cmdRegistryExpand(args []string) error {
	fs := flag.NewFlagSet("registry-expand", flag.ExitOnError)
	brand := fs.String("brand", "", "対象ブランド (必須)")
	dbPath := fs.String("db", "", "SQLite DB path (default var/pizza.sqlite)")
	minStores := fs.Int("min-stores", 2, "候補にする最低店舗数")
	outPath := fs.String("out", "", "候補 YAML 出力パス (default var/registry_candidates/<brand>.yaml)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *brand == "" {
		return errors.New("--brand は必須")
	}
	cfg, _ := menu.FromEnv()
	if *dbPath == "" && cfg != nil {
		*dbPath = cfg.DBPath
	}
	if *dbPath == "" {
		*dbPath = "./var/pizza.sqlite"
	}
	if !filepath.IsAbs(*dbPath) {
		if abs, err := filepath.Abs(*dbPath); err == nil {
			*dbPath = abs
		}
	}
	if *outPath == "" {
		*outPath = fmt.Sprintf("var/registry_candidates/%s.yaml", *brand)
	}
	if !filepath.IsAbs(*outPath) {
		if abs, err := filepath.Abs(*outPath); err == nil {
			*outPath = abs
		}
	}

	deliveryDir := "services/delivery"
	if _, err := os.Stat(deliveryDir); os.IsNotExist(err) {
		return fmt.Errorf("registry-expand: %s が見つかりません", deliveryDir)
	}
	fmt.Printf("🔍 registry-expand: brand=%q min_stores=%d → %s\n",
		*brand, *minStores, *outPath)
	script := fmt.Sprintf(
		"from pizza_delivery.registry_expander import aggregate_unknown_operators, export_candidates_to_yaml;"+
			"cands = aggregate_unknown_operators(db_path=%q, brand=%q, min_stores=%d);"+
			"export_candidates_to_yaml(cands, out_path=%q);"+
			"print(f'✅ wrote {len(cands)} candidates');"+
			"[print(f'  {c.name}  ({c.estimated_store_count} 店舗)') for c in cands]",
		*dbPath, *brand, *minStores, *outPath,
	)
	cmd := exec.Command("uv", "run", "python", "-c", script)
	cmd.Dir = deliveryDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	return cmd.Run()
}

// runDeliveryPython は services/delivery を uv project として Python を実行する。
// CWD はユーザーの working directory のまま (絶対パス変換が不要になる)。
func runDeliveryPython(scriptOrModule ...string) *exec.Cmd {
	args := append([]string{"run", "--project", "services/delivery"}, scriptOrModule...)
	cmd := exec.Command("uv", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	return cmd
}

// cmdHoujinImport は 国税庁 法人番号 CSV (or zip) を local SQLite に取込む。
// APP_ID 不要で完全オフライン。Layer D (operator 実在検証) の Ground Truth。
//
//	pizza houjin-import --csv 13_20260331.zip --encoding cp932
func cmdHoujinImport(args []string) error {
	fs := flag.NewFlagSet("houjin-import", flag.ExitOnError)
	csvPath := fs.String("csv", "", "国税庁 CSV または zip ファイル (必須)")
	encoding := fs.String("encoding", "utf-8", "CSV encoding: utf-8 | cp932")
	dbPath := fs.String("db", "", "local sqlite index (default var/houjin/registry.sqlite)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *csvPath == "" {
		return errors.New("--csv は必須 (国税庁 CSV/zip パス)")
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.houjin_csv", "import",
		"--csv", *csvPath, "--encoding", *encoding}
	if *dbPath != "" {
		pyArgs = append(pyArgs, "--db", *dbPath)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdHoujinSearch は local 法人番号 index を operator 名で検索する。
// オフライン動作のため houjin-import 済みの前提。
//
//	pizza houjin-search --name "株式会社モスストアカンパニー"
func cmdHoujinSearch(args []string) error {
	fs := flag.NewFlagSet("houjin-search", flag.ExitOnError)
	name := fs.String("name", "", "operator 名 (部分一致)")
	limit := fs.Int("limit", 10, "上限件数")
	dbPath := fs.String("db", "", "local sqlite index")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name は必須")
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.houjin_csv", "search",
		"--name", *name, "--limit", strconv.Itoa(*limit)}
	if *dbPath != "" {
		pyArgs = append(pyArgs, "--db", *dbPath)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdJFASync は JFA (日本フランチャイズチェーン協会) 会員企業一覧を
// Web scrape して ORM DB (var/pizza-registry.sqlite) に upsert する。
//
//	pizza jfa-sync
//	pizza jfa-sync --url https://www.jfa-fc.or.jp/particle/22.html
func cmdJFASync(args []string) error {
	fs := flag.NewFlagSet("jfa-sync", flag.ExitOnError)
	url := fs.String("url", "", "JFA 会員一覧 URL (default: 公式 particle/22.html)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.jfa_fetcher", "sync"}
	if *url != "" {
		pyArgs = append(pyArgs, "--url", *url)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdIntegrate は 3 ソース (JFA / 国税庁 CSV / pipeline operator_stores) を
// ORM に統合、または総合 CSV を出力。
//
//	pizza integrate run                           # 統合実行
//	pizza integrate export --out var/all.csv      # 全体 CSV
func cmdIntegrate(args []string) error {
	fs := flag.NewFlagSet("integrate", flag.ExitOnError)
	mode := fs.String("mode", "run", "run | export")
	out := fs.String("out", "var/fc-operators-unified.csv", "export 先 (mode=export 時)")
	source := fs.String("source", "", "export 時 source フィルタ (空で全件)")
	pipelineDB := fs.String("pipeline-db", "var/pizza.sqlite", "pipeline 側 SQLite")
	houjinDB := fs.String("houjin-db", "", "Houjin CSV index (空で default)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var pyArgs []string
	switch *mode {
	case "run":
		pyArgs = []string{"python", "-m", "pizza_delivery.integrate", "run",
			"--pipeline-db", *pipelineDB}
		if *houjinDB != "" {
			pyArgs = append(pyArgs, "--houjin-db", *houjinDB)
		}
	case "export":
		pyArgs = []string{"python", "-m", "pizza_delivery.integrate", "export",
			"--out", *out}
		if *source != "" {
			pyArgs = append(pyArgs, "--source", *source)
		}
	default:
		return fmt.Errorf("unknown mode %q (run|export)", *mode)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdEvaluate は JFA (truth) × pipeline の突合 metric を算出。
// supervised 学習的な改良ループで「recall の悪い点」を検出するための入口。
//
//	pizza evaluate --out var/eval.json
//	pizza evaluate --truth jfa --pipeline pipeline
func cmdEvaluate(args []string) error {
	fs := flag.NewFlagSet("evaluate", flag.ExitOnError)
	truth := fs.String("truth", "jfa", "truth source label")
	pipe := fs.String("pipeline", "pipeline", "pipeline source label")
	out := fs.String("out", "", "JSON レポート (空で stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.evaluator",
		"--truth", *truth, "--pipeline", *pipe}
	if *out != "" {
		pyArgs = append(pyArgs, "--out", *out)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdEnrich は Places Details (phone) + browser-use 逆引きで operator を
// 一括特定する。モス等「公式ページに FC 加盟店名が載らない」brand 用。
//
//	pizza enrich --brand モスバーガー --max-stores 50
func cmdEnrich(args []string) error {
	fs := flag.NewFlagSet("enrich", flag.ExitOnError)
	brand := fs.String("brand", "", "対象ブランド (空で全件)")
	dbPath := fs.String("db", "var/pizza.sqlite", "pipeline SQLite")
	maxStores := fs.Int("max-stores", 50, "処理上限 (暴走ガード)")
	detConc := fs.Int("details-concurrency", 4, "Places Details 並列数")
	lookConc := fs.Int("lookup-concurrency", 2, "browser-use 並列数 (低め推奨)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.enrich",
		"--db", *dbPath, "--max-stores", strconv.Itoa(*maxStores),
		"--details-concurrency", strconv.Itoa(*detConc),
		"--lookup-concurrency", strconv.Itoa(*lookConc)}
	if *brand != "" {
		pyArgs = append(pyArgs, "--brand", *brand)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdJFAExport は ORM 登録済のブランド×事業会社 link を CSV に出力。
//
//	pizza jfa-export --out var/jfa-members.csv
func cmdJFAExport(args []string) error {
	fs := flag.NewFlagSet("jfa-export", flag.ExitOnError)
	outPath := fs.String("out", "var/jfa-members.csv", "CSV 出力パス")
	source := fs.String("source", "", "source filter (空で全件、jfa のみなら 'jfa')")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.jfa_fetcher", "export",
		"--out", *outPath}
	if *source != "" {
		pyArgs = append(pyArgs, "--source", *source)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdMegaFranchisee は brand を跨いだ事業会社集計 (Phase 19)。
// 1 事業会社 = 1 行。運営している全ブランドの店舗数内訳 + total を出す。
//
//	pizza megafranchisee --min-total 5 --min-brands 1 \
//	    --out-csv var/megajii/all.csv --out-yaml var/megajii/all.yaml
func cmdMegaFranchisee(args []string) error {
	fs := flag.NewFlagSet("megafranchisee", flag.ExitOnError)
	dbPath := fs.String("db", "", "SQLite DB path (default var/pizza.sqlite)")
	minTotal := fs.Int("min-total", 2, "候補にする最低合計店舗数")
	minBrands := fs.Int("min-brands", 1, "複数ブランド運営だけ残したいなら >=2")
	outCSV := fs.String("out-csv", "var/megajii/operators.csv", "operator 主語 CSV")
	outYAML := fs.String("out-yaml", "", "operator 主語 YAML (省略可)")
	includeHQ := fs.Bool("include-franchisor", false, "本部・直営も集計に含める")
	sortBy := fs.String("sort-by", "total", "ソート基準: total(合計店舗数) | brands(業態数)")
	top := fs.Int("top", 30, "top N 社を表示 (0 で全件)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, _ := menu.FromEnv()
	if *dbPath == "" && cfg != nil {
		*dbPath = cfg.DBPath
	}
	if *dbPath == "" {
		*dbPath = "./var/pizza.sqlite"
	}
	if !filepath.IsAbs(*dbPath) {
		if abs, err := filepath.Abs(*dbPath); err == nil {
			*dbPath = abs
		}
	}
	if !filepath.IsAbs(*outCSV) {
		if abs, err := filepath.Abs(*outCSV); err == nil {
			*outCSV = abs
		}
	}
	if *outYAML != "" && !filepath.IsAbs(*outYAML) {
		if abs, err := filepath.Abs(*outYAML); err == nil {
			*outYAML = abs
		}
	}
	deliveryDir := "services/delivery"
	if _, err := os.Stat(deliveryDir); os.IsNotExist(err) {
		return fmt.Errorf("megafranchisee: %s が見つかりません", deliveryDir)
	}
	excludeHQ := "True"
	if *includeHQ {
		excludeHQ = "False"
	}
	sortKey := "-o.total_stores"
	if *sortBy == "brands" {
		sortKey = "(-o.brand_count, -o.total_stores)"
	}
	fmt.Printf("🏢 megafranchisee: min_total=%d min_brands=%d sort=%s → %s\n",
		*minTotal, *minBrands, *sortBy, *outCSV)
	yamlStmt := ""
	if *outYAML != "" {
		yamlStmt = fmt.Sprintf("export_cross_brand_to_yaml(ops, out_path=%q);", *outYAML)
	}
	topExpr := fmt.Sprintf("ops[:%d]", *top)
	if *top == 0 {
		topExpr = "ops"
	}
	script := fmt.Sprintf(
		"from pizza_delivery.registry_expander import aggregate_cross_brand_operators, export_cross_brand_to_csv, export_cross_brand_to_yaml;"+
			"ops = aggregate_cross_brand_operators(db_path=%q, min_total_stores=%d, min_brands=%d, exclude_franchisor=%s);"+
			"ops.sort(key=lambda o: %s);"+
			"export_cross_brand_to_csv(ops, out_path=%q);"+
			"%s"+
			"print(f'✅ {len(ops)} operators');"+
			"[print(f'  {o.total_stores:4d} 店  {o.brand_count} 業態  {o.name}  ({\", \".join(f\"{b}:{n}\" for b,n in sorted(o.brand_counts.items(), key=lambda kv:-kv[1]))})') for o in %s]",
		*dbPath, *minTotal, *minBrands, excludeHQ, sortKey, *outCSV, yamlStmt, topExpr,
	)
	cmd := exec.Command("uv", "run", "python", "-c", script)
	cmd.Dir = deliveryDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	return cmd.Run()
}

// cmdBench は複数ブランドを逐次 scan し、速度/API call 数を JSON で出す。
// 並列は行わない (ユーザー指示)。
//
//	pizza bench --brands "エニタイムフィットネス,モスバーガー,TSUTAYA" \
//	            --areas "東京都" --cell-km 3.0
func cmdBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	brandsCSV := fs.String("brands", "", "カンマ区切りブランド名リスト")
	areasCSV := fs.String("areas", "", "カンマ区切り area")
	cellKm := fs.Float64("cell-km", 3.0, "bake メッシュ幅")
	outDir := fs.String("out-dir", "var/bench", "サマリ CSV/JSON 出力ディレクトリ")
	adaptive := fs.Bool("adaptive", true, "Adaptive quad-tree split を使う")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *brandsCSV == "" || *areasCSV == "" {
		return errors.New("--brands と --areas は必須")
	}
	brands := splitAndTrim(*brandsCSV)
	cfg, err := menu.FromEnv()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if cfg.GoogleMapsAPIKey == "" {
		return errors.New("GOOGLE_MAPS_API_KEY が未設定 (.env 確認)")
	}
	absOutDir, _ := filepath.Abs(*outDir)
	if err := os.MkdirAll(absOutDir, 0o755); err != nil {
		return fmt.Errorf("mkdir out: %w", err)
	}

	// サマリ record
	type BenchResult struct {
		Brand     string `json:"brand"`
		Areas     string `json:"areas"`
		Stores    int32  `json:"stores_found"`
		ElapsedSec float32 `json:"elapsed_sec"`
		APICalls  int    `json:"api_calls"`
		RejBrand  int    `json:"rejected_brand_filter"`
		RejPoly   int    `json:"rejected_polygon"`
		RejDup    int    `json:"rejected_duplicate"`
		CellsCap  int    `json:"cells_hit_cap"`
	}
	var results []BenchResult

	fmt.Printf("🧪 pizza bench: brands=%v areas=%q cell_km=%.1f adaptive=%v\n",
		brands, *areasCSV, *cellKm, *adaptive)

	ctx := context.Background()

	for _, brand := range brands {
		fmt.Printf("\n━━━ brand=%q ━━━\n", brand)
		t0 := time.Now()
		var totalStores int32
		var mergedMetrics dough.SearchMetrics

		for _, area := range splitAndTrim(*areasCSV) {
			polygon, err := menu.ResolvePolygon(area)
			if err != nil {
				log.Printf("⚠️  resolve %s: %v", area, err)
				continue
			}
			seed := &dough.Searcher{
				Places: &dough.PlacesClient{
					APIKey: cfg.GoogleMapsAPIKey, Language: "ja", Region: "JP",
				},
				Language: "ja", Region: "JP",
				StrictBrandMatch:  true,
				RestrictToPolygon: polygon,
			}
			if *adaptive {
				count := int32(0)
				err = seed.SearchStoresAdaptive(
					ctx, brand, polygon,
					&dough.AdaptiveSearchOptions{
						MaxDepth: 4, MinCellMeters: *cellKm * 500.0,
					},
					func(_ *pb.Store) error { count++; return nil },
				)
				if err != nil {
					log.Printf("⚠️  adaptive %s/%s: %v", brand, area, err)
				}
				totalStores += count
			} else {
				cells, err := grid.Split(polygon, *cellKm)
				if err != nil {
					log.Printf("⚠️  grid %s: %v", area, err)
					continue
				}
				count := int32(0)
				err = seed.SearchStoresInGrid(ctx, brand, cells,
					func(_ *pb.Store) error { count++; return nil })
				if err != nil {
					log.Printf("⚠️  grid-search %s/%s: %v", brand, area, err)
				}
				totalStores += count
			}
			mergedMetrics.APICalls += seed.Metrics.APICalls
			mergedMetrics.RawResultsTotal += seed.Metrics.RawResultsTotal
			mergedMetrics.RejectedBrandFilter += seed.Metrics.RejectedBrandFilter
			mergedMetrics.RejectedPolygon += seed.Metrics.RejectedPolygon
			mergedMetrics.RejectedDuplicate += seed.Metrics.RejectedDuplicate
			mergedMetrics.CellsHitCap += seed.Metrics.CellsHitCap
			mergedMetrics.Emitted += seed.Metrics.Emitted
		}
		elapsed := float32(time.Since(t0).Seconds())

		res := BenchResult{
			Brand: brand, Areas: *areasCSV,
			Stores: totalStores, ElapsedSec: elapsed,
			APICalls: mergedMetrics.APICalls,
			RejBrand: mergedMetrics.RejectedBrandFilter,
			RejPoly:  mergedMetrics.RejectedPolygon,
			RejDup:   mergedMetrics.RejectedDuplicate,
			CellsCap: mergedMetrics.CellsHitCap,
		}
		results = append(results, res)

		fmt.Printf("  stores=%d  api_calls=%d  polygon_rej=%d  dup=%d  cells_cap=%d\n",
			res.Stores, res.APICalls, res.RejPoly, res.RejDup, res.CellsCap)
		fmt.Printf("  elapsed=%.1fs\n", res.ElapsedSec)
	}

	// 結果 JSON 出力
	jsonPath := filepath.Join(absOutDir, fmt.Sprintf(
		"bench-%s.json", time.Now().Format("20060102-150405")))
	buf, _ := json.MarshalIndent(results, "", "  ")
	if err := os.WriteFile(jsonPath, buf, 0o644); err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	fmt.Printf("\n✅ bench done → %s\n", jsonPath)
	// サマリ表示
	fmt.Printf("\n%-30s %-10s %-10s %-10s %-10s\n",
		"brand", "stores", "api_calls", "cells_cap", "elapsed")
	fmt.Println(strings.Repeat("─", 72))
	for _, r := range results {
		fmt.Printf("%-30s %-10d %-10d %-10d %-8.1fs\n",
			r.Brand, r.Stores, r.APICalls, r.CellsCap, r.ElapsedSec)
	}
	return nil
}

// cmdScan は「1 コマンドで全自動」を実現する高位ラッパ。
//
//	pizza scan --brand "エニタイムフィットネス" --areas "東京都"
//
// 内部で:
//  1. migrate (+ registry seed) — Ground Truth を最新化
//  2. 各 area で bake — Places 店舗 seed (bottom-up)
//  3. research — per-store で operator 抽出 (SQLite 永続化)
//  4. audit — Top-down × Bottom-up 突合 + CSV
//
// ユーザーは「東京都のエニタイムフィットネス運営会社を調べて」だけで
// 完成 CSV が手に入る。
func cmdScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	brand := fs.String("brand", "", "対象ブランド (必須)")
	areasCSV := fs.String("areas", "", "カンマ区切り area (必須)")
	cellKm := fs.Float64("cell-km", 2.0, "bake メッシュ幅")
	maxResearch := fs.Int("max-research", 100, "research で処理する上限店舗数")
	outPath := fs.String("out", "", "サマリ CSV 出力パス")
	noResearch := fs.Bool("no-research", false, "research を skip")
	// LLM 層の opt-in フラグ (Phase 20+: 既定は LLM 全 off、明示的に on にする)
	withKitchen := fs.Bool("with-kitchen", false, "Firecrawl Markdown fetch を有効化 (M2 Kitchen)")
	withJudge := fs.Bool("with-judge", false, "Expert Panel (gRPC) で judge を回す")
	judgeMode := fs.String("judge-mode", "panel", "judge 経路: mock | live | panel")
	withVerify := fs.Bool("with-verify", false, "CrossVerifier (Layer C: 別 LLM critic) を有効化")
	verifyHoujin := fs.Bool("verify-houjin", false, "国税庁法人番号 API で operator 実在確認 (Layer D)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *brand == "" {
		return errors.New("--brand は必須")
	}
	if *areasCSV == "" {
		return errors.New("--areas は必須 (例: '東京都,大阪府')")
	}

	cfg, err := menu.FromEnv()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if cfg.GoogleMapsAPIKey == "" {
		return errors.New("GOOGLE_MAPS_API_KEY が未設定 (.env 確認)")
	}
	if *outPath == "" {
		ts := time.Now().Format("20060102-150405")
		*outPath = filepath.Join(cfg.OutputDir, "scan",
			fmt.Sprintf("%s-%s.csv", *brand, ts))
	}
	if !filepath.IsAbs(*outPath) {
		if abs, err2 := filepath.Abs(*outPath); err2 == nil {
			*outPath = abs
		}
	}

	fmt.Printf("🧭 pizza scan: brand=%q areas=%q → %s\n", *brand, *areasCSV, *outPath)

	// Step 1: migrate (+ registry seed)
	fmt.Println("━━ [1/4] migrate --with-registry ━━")
	if err := cmdMigrate([]string{"--with-registry"}); err != nil {
		return fmt.Errorf("scan/migrate: %w", err)
	}

	// Step 2: bake (各 area)
	fmt.Println("━━ [2/4] bake (各 area) ━━")
	areas := splitAndTrim(*areasCSV)
	for _, area := range areas {
		bakeArgs := []string{
			"--query", *brand,
			"--area", area,
			"--cell-km", strconv.FormatFloat(*cellKm, 'f', -1, 64),
		}
		if !*withKitchen {
			bakeArgs = append(bakeArgs, "--no-kitchen")
		}
		if *withJudge {
			bakeArgs = append(bakeArgs, "--with-judge", "--judge-mode", *judgeMode)
		}
		if err := cmdBake(bakeArgs); err != nil {
			log.Printf("⚠️  bake failed for area=%q: %v", area, err)
		}
	}

	// Step 3: research (per-store operator 抽出)
	if !*noResearch {
		fmt.Println("━━ [3/4] research (per-store operator 抽出) ━━")
		researchArgs := []string{
			"--brand", *brand,
			"--max-stores", strconv.Itoa(*maxResearch),
		}
		if !*withVerify {
			researchArgs = append(researchArgs, "--no-verify")
		}
		if *verifyHoujin {
			researchArgs = append(researchArgs, "--verify-houjin")
		}
		if err := cmdResearch(researchArgs); err != nil {
			log.Printf("⚠️  research skipped: %v", err)
		}
	} else {
		fmt.Println("━━ [3/4] research skipped (--no-research) ━━")
	}

	// Step 4: audit
	fmt.Println("━━ [4/4] audit (Top-down × Bottom-up 突合) ━━")
	auditArgs := []string{
		"--brand", *brand,
		"--areas", *areasCSV,
		"--skip-bake", // Step 2 で既に済
		"--out", *outPath,
	}
	if err := cmdAudit(auditArgs); err != nil {
		return fmt.Errorf("scan/audit: %w", err)
	}

	fmt.Printf("\n✅ scan done → %s\n", *outPath)
	return nil
}

// cmdAudit は 1 ブランドについて Top-down × Bottom-up の突合監査を実行する。
//
//  1. --areas で指定された各 area について bake (Places Text Search) を走らせ
//     stores テーブルに注入 (Bottom-up)
//  2. Python audit_cli を spawn して registry x stores の突合を実施
//  3. 結果 CSV を --out に出力 (+ unknown-stores / missing-operators の副 CSV)
func cmdAudit(args []string) error {
	fs := flag.NewFlagSet("audit", flag.ExitOnError)
	brand := fs.String("brand", "", "対象ブランド (registry 登録済み)")
	areasCSV := fs.String("areas", "", "カンマ区切り area (例: '東京都,大阪府')")
	cellKm := fs.Float64("cell-km", 2.0, "Bottom-up bake メッシュ幅")
	dbPath := fs.String("db", "", "SQLite DB path")
	outPath := fs.String("out", "", "メイン CSV 出力パス")
	skipBake := fs.Bool("skip-bake", false, "Bottom-up bake を skip")
	addrThreshold := fs.Float64("addr-threshold", 0.7, "住所類似度しきい値")
	radiusM := fs.Float64("radius-m", 150.0, "緯度経度突合半径 (m)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *brand == "" {
		return errors.New("--brand は必須")
	}
	if *outPath == "" {
		return errors.New("--out は必須")
	}

	cfg, err := menu.FromEnv()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if *dbPath == "" {
		*dbPath = cfg.DBPath
	}
	if !filepath.IsAbs(*dbPath) {
		if abs, err2 := filepath.Abs(*dbPath); err2 == nil {
			*dbPath = abs
		}
	}
	if !filepath.IsAbs(*outPath) {
		if abs, err2 := filepath.Abs(*outPath); err2 == nil {
			*outPath = abs
		}
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		return fmt.Errorf("mkdir out: %w", err)
	}

	areas := splitAndTrim(*areasCSV)

	// Phase 1: Bottom-up bake (各 area で Places Text Search)
	if !*skipBake {
		if cfg.GoogleMapsAPIKey == "" {
			return errors.New("GOOGLE_MAPS_API_KEY が未設定 (.env 確認)")
		}
		for _, area := range areas {
			fmt.Printf("🍞 [audit] bake brand=%q area=%q cell_km=%.1f\n", *brand, area, *cellKm)
			if err := runBakeForArea(context.Background(), cfg, *brand, area, *cellKm, *dbPath); err != nil {
				log.Printf("⚠️  [audit] bake skipped area=%q: %v", area, err)
			}
		}
	} else {
		fmt.Println("⏭  [audit] --skip-bake: 既存 stores のみで突合")
	}

	// Phase 2: Python audit_cli spawn
	deliveryDir := "services/delivery"
	if _, err := os.Stat(deliveryDir); os.IsNotExist(err) {
		return fmt.Errorf("audit: %s が見つかりません (repo root で実行)", deliveryDir)
	}
	pyArgs := []string{
		"run", "python", "-m", "pizza_delivery.audit_cli",
		"--db", *dbPath,
		"--brand", *brand,
		"--areas", *areasCSV,
		"--out", *outPath,
		"--addr-threshold", strconv.FormatFloat(*addrThreshold, 'f', -1, 64),
		"--radius-m", strconv.FormatFloat(*radiusM, 'f', -1, 64),
	}
	fmt.Printf("🔍 [audit] python pizza_delivery.audit_cli --brand %q\n", *brand)
	cmd := exec.Command("uv", pyArgs...)
	cmd.Dir = deliveryDir
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	return cmd.Run()
}

func splitAndTrim(csv string) []string {
	var out []string
	for _, t := range splitCSV(csv) {
		t = trimSpace(t)
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func splitCSV(s string) []string {
	res := []string{}
	cur := []byte{}
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			res = append(res, string(cur))
			cur = cur[:0]
		} else {
			cur = append(cur, s[i])
		}
	}
	res = append(res, string(cur))
	return res
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n') {
		end--
	}
	return s[start:end]
}

// runBakeForArea は 1 area について M1 Seed を走らせ stores に注入する。
// cmdBake の中核ロジックを簡略再利用。
func runBakeForArea(
	ctx context.Context,
	cfg *menu.Config,
	brand, area string,
	cellKm float64,
	dbPath string,
) error {
	polygon, err := menu.ResolvePolygon(area)
	if err != nil {
		return err
	}
	seed := &dough.Searcher{
		Places:           &dough.PlacesClient{APIKey: cfg.GoogleMapsAPIKey, Language: "ja", Region: "JP"},
		Language:         "ja",
		Region:           "JP",
		StrictBrandMatch: true,
	}
	store, err := box.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer store.Close()

	p := &oven.Pipeline{
		Seed:    seed,
		Box:     store,
		Workers: cfg.MaxConcurrency,
	}
	rctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	report, err := p.Bake(rctx, &pb.SearchStoresInGridRequest{
		Brand:    brand,
		Polygon:  polygon,
		CellKm:   cellKm,
		Language: "ja",
	})
	if err != nil {
		return err
	}
	fmt.Printf("   [audit] %s: cells=%d stores=%d in %.1fs\n",
		area, report.CellsGenerated, report.StoresFound, report.ElapsedSec)
	return nil
}

// cmdResearch は Python 側の Research Pipeline CLI を spawn する。
// pizza と research を 1 binary から叩けるようにして "コマンドが繋がっていない"
// 問題を解消する。
func cmdResearch(args []string) error {
	fs := flag.NewFlagSet("research", flag.ExitOnError)
	brand := fs.String("brand", "", "対象ブランド (空で全件)")
	dbPath := fs.String("db", "", "SQLite DB path (default var/pizza.sqlite)")
	maxStores := fs.Int("max-stores", 0, "処理上限店舗数 (0 で全件)")
	noVerify := fs.Bool("no-verify", false, "CrossVerifier をスキップ")
	verifyHoujin := fs.Bool("verify-houjin", false, "国税庁法人番号 API で operator 実在確認")
	expand := fs.Bool("expand", false, "Places API 広域芋づる式で operator の他店舗を発見")
	expandArea := fs.String("expand-area", "", "--expand 時の area hint")
	concurrency := fs.Int("concurrency", 4, "並行 fetch 数")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, _ := menu.FromEnv()
	if *dbPath == "" && cfg != nil {
		*dbPath = cfg.DBPath
	}
	if *dbPath == "" {
		*dbPath = "./var/pizza.sqlite"
	}
	// Python は services/delivery/ で spawn されるので、相対パスは repo root
	// 基準で絶対化しておかないと child が見つけられない。
	if !filepath.IsAbs(*dbPath) {
		if abs, err := filepath.Abs(*dbPath); err == nil {
			*dbPath = abs
		}
	}

	pyArgs := []string{"run", "python", "-m", "pizza_delivery.research", "--db", *dbPath}
	if *brand != "" {
		pyArgs = append(pyArgs, "--brand", *brand)
	}
	if *maxStores > 0 {
		pyArgs = append(pyArgs, "--max-stores", strconv.Itoa(*maxStores))
	}
	if *noVerify {
		pyArgs = append(pyArgs, "--no-verify")
	}
	if *verifyHoujin {
		pyArgs = append(pyArgs, "--verify-houjin")
	}
	if *expand {
		pyArgs = append(pyArgs, "--expand-via-places")
	}
	if *expandArea != "" {
		pyArgs = append(pyArgs, "--expand-area", *expandArea)
	}
	pyArgs = append(pyArgs, "--concurrency", strconv.Itoa(*concurrency))

	deliveryDir := "services/delivery"
	if _, err := os.Stat(deliveryDir); os.IsNotExist(err) {
		return fmt.Errorf("research: %s が見つかりません (repo root で実行してください)", deliveryDir)
	}
	fmt.Printf("🔬 pizza research → uv %s\n", pyArgs)
	cmd := exec.Command("uv", pyArgs...)
	cmd.Dir = deliveryDir
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	return cmd.Run()
}

// cmdMigrate は SQLite の schema/view を最新化する。
// --with-registry フラグで franchisee_registry.yaml を operator_stores に seed。
func cmdMigrate(args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	dbPath := fs.String("db", "", "SQLite DB path (default var/pizza.sqlite)")
	withRegistry := fs.Bool("with-registry", false, "franchisee_registry.yaml を operator_stores に seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, _ := menu.FromEnv()
	if *dbPath == "" && cfg != nil {
		*dbPath = cfg.DBPath
	}
	if *dbPath == "" {
		*dbPath = "./var/pizza.sqlite"
	}
	if !filepath.IsAbs(*dbPath) {
		if abs, err := filepath.Abs(*dbPath); err == nil {
			*dbPath = abs
		}
	}
	store, err := box.Open(*dbPath)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	store.Close()
	fmt.Printf("✅ migrated %s (schema + views)\n", *dbPath)

	if *withRegistry {
		// Python 側の registry seeder を spawn
		deliveryDir := "services/delivery"
		if _, err := os.Stat(deliveryDir); os.IsNotExist(err) {
			return fmt.Errorf("migrate: %s が見つかりません (repo root で実行)", deliveryDir)
		}
		fmt.Printf("🌱 seeding franchisee_registry.yaml → operator_stores\n")
		script := fmt.Sprintf(
			"from pizza_delivery.franchisee_registry import load_registry, seed_registry_to_sqlite;"+
				"print('seeded', seed_registry_to_sqlite(%q, load_registry()))",
			*dbPath,
		)
		cmd := exec.Command("uv", "run", "python", "-c", script)
		cmd.Dir = deliveryDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Env = os.Environ()
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("registry seed failed: %w", err)
		}
	}
	return nil
}

// cmdServe は delivery-service gRPC サーバを spawn する。
// mock / live / panel の mode 指定可能。
func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	mode := fs.String("mode", "", "mock | live | panel (default env DELIVERY_MODE or mock)")
	addr := fs.String("addr", "", "listen addr (default env DELIVERY_LISTEN_ADDR or 0.0.0.0:50053)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	env := os.Environ()
	if *mode != "" {
		env = append(env, "DELIVERY_MODE="+*mode)
	}
	if *addr != "" {
		env = append(env, "DELIVERY_LISTEN_ADDR="+*addr)
	}
	deliveryDir := "services/delivery"
	if _, err := os.Stat(deliveryDir); os.IsNotExist(err) {
		return fmt.Errorf("serve: %s が見つかりません (repo root で実行してください)", deliveryDir)
	}
	effectiveMode := *mode
	if effectiveMode == "" {
		effectiveMode = os.Getenv("DELIVERY_MODE")
		if effectiveMode == "" {
			effectiveMode = "mock"
		}
	}
	fmt.Printf("🛵 pizza serve → delivery-service (mode=%s)\n", effectiveMode)
	cmd := exec.Command("uv", "run", "python", "-m", "pizza_delivery")
	cmd.Dir = deliveryDir
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = env
	return cmd.Run()
}
