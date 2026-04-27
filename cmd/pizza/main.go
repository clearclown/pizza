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
	case "jfa-disclosure-sync":
		if err := cmdJFADisclosureSync(os.Args[2:]); err != nil {
			log.Fatalf("pizza jfa-disclosure-sync: %v", err)
		}
	case "anytime-official-sync":
		if err := cmdAnytimeOfficialSync(os.Args[2:]); err != nil {
			log.Fatalf("pizza anytime-official-sync: %v", err)
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
	case "brand-profile":
		if err := cmdBrandProfile(os.Args[2:]); err != nil {
			log.Fatalf("pizza brand-profile: %v", err)
		}
	case "fc-directory":
		if err := cmdFCDirectory(os.Args[2:]); err != nil {
			log.Fatalf("pizza fc-directory: %v", err)
		}
	case "coverage-export":
		if err := cmdCoverageExport(os.Args[2:]); err != nil {
			log.Fatalf("pizza coverage-export: %v", err)
		}
	case "review-houjin-hydrate":
		if err := cmdReviewHoujinHydrate(os.Args[2:]); err != nil {
			log.Fatalf("pizza review-houjin-hydrate: %v", err)
		}
	case "cleanse":
		if err := cmdCleanse(os.Args[2:]); err != nil {
			log.Fatalf("pizza cleanse: %v", err)
		}
	case "purge":
		if err := cmdPurge(os.Args[2:]); err != nil {
			log.Fatalf("pizza purge: %v", err)
		}
	case "address-reverse":
		if err := cmdAddressReverse(os.Args[2:]); err != nil {
			log.Fatalf("pizza address-reverse: %v", err)
		}
	case "deep-research":
		if err := cmdDeepResearch(os.Args[2:]); err != nil {
			log.Fatalf("pizza deep-research: %v", err)
		}
	case "recruitment-research":
		if err := cmdRecruitmentResearch(os.Args[2:]); err != nil {
			log.Fatalf("pizza recruitment-research: %v", err)
		}
	case "official-recruitment-crawl":
		if err := cmdOfficialRecruitmentCrawl(os.Args[2:]); err != nil {
			log.Fatalf("pizza official-recruitment-crawl: %v", err)
		}
	case "official-franchisee-sources":
		if err := cmdOfficialFranchiseeSources(os.Args[2:]); err != nil {
			log.Fatalf("pizza official-franchisee-sources: %v", err)
		}
	case "edinet-sync":
		if err := cmdEdinetSync(os.Args[2:]); err != nil {
			log.Fatalf("pizza edinet-sync: %v", err)
		}
	case "operator-spider":
		if err := cmdOperatorSpider(os.Args[2:]); err != nil {
			log.Fatalf("pizza operator-spider: %v", err)
		}
	case "operator-brand-discovery":
		if err := cmdOperatorBrandDiscovery(os.Args[2:]); err != nil {
			log.Fatalf("pizza operator-brand-discovery: %v", err)
		}
	case "extended-fc-brand-export":
		if err := cmdExtendedFCBrandExport(os.Args[2:]); err != nil {
			log.Fatalf("pizza extended-fc-brand-export: %v", err)
		}
	case "brand-fill-rate-export":
		if err := cmdBrandFillRateExport(os.Args[2:]); err != nil {
			log.Fatalf("pizza brand-fill-rate-export: %v", err)
		}
	case "osm-fetch-all":
		if err := cmdOSMFetchAll(os.Args[2:]); err != nil {
			log.Fatalf("pizza osm-fetch-all: %v", err)
		}
	case "import-megajii-csv":
		if err := cmdImportMegajiiCSV(os.Args[2:]); err != nil {
			log.Fatalf("pizza import-megajii-csv: %v", err)
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
  pizza jfa-disclosure-sync  JFA 情報開示書面 PDF から本部店舗数を ORM 登録
  pizza anytime-official-sync  エニタイム公式店舗一覧 → stores 補完 + false positive 監査
  pizza jfa-export       ORM 登録済のブランド×事業会社リストを CSV 出力
  pizza integrate        JFA / 国税庁 CSV / pipeline operator を ORM に統合 (総合 FC 事業会社リスト)
  pizza evaluate         truth (JFA) × pipeline の突合 metric を算出 (brand/operator/link recall)
  pizza enrich           Places Details (phone) + browser-use 逆引きで operator 一括特定
  pizza brand-profile    複数ブランドを 2 階層並列で prof iling (JFA + gBiz + 公式 HP + cross-brand)
  pizza fc-directory     都道府県別 FC 運営事業会社ディレクトリ (ORM + 国税庁 CSV 融合)
  pizza coverage-export  14 brand × 47 都道府県の店舗/operator coverage CSV を出力
  pizza review-houjin-hydrate  review対象メガジーを国税庁CSV SQLiteで高速hydrate
  pizza cleanse          operator_stores の dirty 名を LLM canonicalize + 国税庁検証で cleanse
  pizza purge            operator_stores から 国税庁未登録 garbage operator を削除 (ハルシネ防止)
  pizza address-reverse  店舗住所 → 国税庁 CSV 同住所 株式会社 逆引きで operator 候補抽出
  pizza deep-research    operator 不明店舗を Gemini research + Claude 監視 + 国税庁 3 段検証
  pizza recruitment-research  求人・採用ページ search + 本文 gate + 国税庁 verify
  pizza official-recruitment-crawl  公式求人 jobfind ページ → 募集者 + 店舗 match + 国税庁 verify
  pizza official-franchisee-sources  公式FC/運営会社/本部PR本文 → operator link + 国税庁 verify
  pizza edinet-sync      EDINET 有価証券報告書 → 関係会社・FC 契約先 → 国税庁 verify → ORM 登録
  pizza operator-spider  ORM 登録済 operator 公式 HP → 店舗一覧 scrape → 住所 match → operator 確定
  pizza operator-brand-discovery  operator 公式HPの事業/ブランドlink → FC brand link を追加収集
  pizza extended-fc-brand-export  追加FCブランド seed + 既存非14ブランド evidence → 監査/FC専用CSV出力
  pizza brand-fill-rate-export  brand別の本部公表店舗数に対する FC operator 充填率 CSV 出力
  pizza osm-fetch-all    OSM Overpass 全国 fetch + operator:ja tag capture
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

	// Phase 27 bugfix: 北海道等巨大 area で 10分 timeout exceeded
	// → 30 分に延伸 (他 area の速度に影響なし)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
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
	noSubstring := fs.Bool("no-substring", false, "遅い部分一致 LIKE を使わない")
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
	if *noSubstring {
		pyArgs = append(pyArgs, "--no-substring")
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

// cmdJFADisclosureSync は JFA 情報開示書面 index/PDF を ORM DB に upsert する。
//
//	pizza jfa-disclosure-sync
//	pizza jfa-disclosure-sync --fetch-pdfs --brands モスバーガーチェーン,カーブス
func cmdJFADisclosureSync(args []string) error {
	fs := flag.NewFlagSet("jfa-disclosure-sync", flag.ExitOnError)
	url := fs.String("url", "", "JFA 情報開示書面 URL (default: 公式 particle/3614.html)")
	brands := fs.String("brands", "", "対象ブランド名のカンマ区切り filter")
	fetchPDFs := fs.Bool("fetch-pdfs", false, "PDF 本文から店舗数も抽出")
	maxPDFs := fs.Int("max-pdfs", 0, "処理する PDF 数の上限 (0 なら制限なし)")
	rateLimitSec := fs.Float64("rate-limit-sec", 0.5, "PDF fetch 間隔 seconds")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.jfa_disclosure", "sync"}
	if *url != "" {
		pyArgs = append(pyArgs, "--url", *url)
	}
	if *brands != "" {
		pyArgs = append(pyArgs, "--brands", *brands)
	}
	if *fetchPDFs {
		pyArgs = append(pyArgs, "--fetch-pdfs")
	}
	if *maxPDFs > 0 {
		pyArgs = append(pyArgs, "--max-pdfs", strconv.Itoa(*maxPDFs))
	}
	pyArgs = append(pyArgs, "--rate-limit-sec", fmt.Sprintf("%.3f", *rateLimitSec))
	return runDeliveryPython(pyArgs...).Run()
}

// cmdAnytimeOfficialSync は Google API を使わず、エニタイム公式の
// 都道府県別店舗一覧から pipeline stores を補完する。
func cmdAnytimeOfficialSync(args []string) error {
	fs := flag.NewFlagSet("anytime-official-sync", flag.ExitOnError)
	dbPath := fs.String("db", "var/pizza.sqlite", "pipeline SQLite")
	out := fs.String("out", "var/phase28/anytime/anytime-official-stores.csv", "公式店舗 CSV")
	falseOut := fs.String("false-positive-out", "var/phase28/anytime/anytime-false-positive-stores.csv", "非エニタイム混入候補 CSV")
	nonOfficialOut := fs.String("non-official-out", "var/phase28/anytime/anytime-non-official-stores.csv", "公式一覧外の旧店舗候補 CSV")
	apply := fs.Bool("apply", false, "stores に upsert する")
	purgeFalse := fs.Bool("purge-false-positives", false, "非エニタイム混入候補を stores/operator_stores から削除する")
	purgeNonOfficial := fs.Bool("purge-non-official", false, "公式一覧外のエニタイム旧店舗行を stores/operator_stores から削除する")
	sleepSec := fs.Float64("sleep-sec", 0.2, "公式ページ取得間隔 秒")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.anytime_official",
		"--db", *dbPath,
		"--out", *out,
		"--false-positive-out", *falseOut,
		"--non-official-out", *nonOfficialOut,
		"--sleep-sec", fmt.Sprintf("%.3f", *sleepSec),
	}
	if *apply {
		pyArgs = append(pyArgs, "--apply")
	}
	if *purgeFalse {
		pyArgs = append(pyArgs, "--purge-false-positives")
	}
	if *purgeNonOfficial {
		pyArgs = append(pyArgs, "--purge-non-official")
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
	skipHoujin := fs.Bool("skip-houjin", false, "run 時に国税庁 hydrate を skip")
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
		if *skipHoujin {
			pyArgs = append(pyArgs, "--skip-houjin")
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
	lookConc := fs.Int("lookup-concurrency", 2, "lookup 並列数 (低め推奨)")
	lookupMode := fs.String("lookup-mode", "scrapling",
		"phone 逆引き経路: scrapling | browser_use (default: scrapling)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.enrich",
		"--db", *dbPath, "--max-stores", strconv.Itoa(*maxStores),
		"--details-concurrency", strconv.Itoa(*detConc),
		"--lookup-concurrency", strconv.Itoa(*lookConc),
		"--lookup-mode", *lookupMode}
	if *brand != "" {
		pyArgs = append(pyArgs, "--brand", *brand)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdBrandProfile は複数ブランドを 2 階層並列 (brand × intra) で profiling。
// 10 項目 (企業名 / ブランド名 / FC 店舗数 / 代表者 / 住所 / 売上 ×2 /
// HP / 加盟店ブランド / FC 募集 LP) を JFA + gBiz + 公式 HP + cross-brand から
// 決定論 で集約する。
//
//	pizza brand-profile --brands "カーブス,モスバーガー,TSUTAYA" \
//	  --brand-concurrency 4 --intra-concurrency 3 \
//	  --out var/brand-profiles.csv --out-json var/brand-profiles.json
func cmdBrandProfile(args []string) error {
	fs := flag.NewFlagSet("brand-profile", flag.ExitOnError)
	brands := fs.String("brands", "", "カンマ区切り ブランド一覧 (必須)")
	brandConc := fs.Int("brand-concurrency", 4, "ブランド間並列度 (default 4)")
	intraConc := fs.Int("intra-concurrency", 3, "ブランド内 source 並列度 (default 3)")
	pipelineDB := fs.String("pipeline-db", "var/pizza.sqlite", "pipeline SQLite (cross-brand 集計用)")
	out := fs.String("out", "var/brand-profiles.csv", "CSV 出力パス")
	outJSON := fs.String("out-json", "", "JSON 出力 (debug 情報付き、空で skip)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *brands == "" {
		return fmt.Errorf("--brands is required")
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.brand_profiler",
		"--brands", *brands,
		"--brand-concurrency", strconv.Itoa(*brandConc),
		"--intra-concurrency", strconv.Itoa(*intraConc),
		"--pipeline-db", *pipelineDB,
		"--out", *out}
	if *outJSON != "" {
		pyArgs = append(pyArgs, "--out-json", *outJSON)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdFCDirectory は ORM (FranchiseBrand × OperatorCompany × link) を走査し、
// 国税庁法人番号 CSV (577 万件) で prefecture を補完した後、
// 都道府県別の FC 運営事業会社ディレクトリを CSV/JSON 出力する。
//
//	pizza fc-directory --prefecture 東京都 --out var/tokyo-fc-operators.csv
func cmdFCDirectory(args []string) error {
	fs := flag.NewFlagSet("fc-directory", flag.ExitOnError)
	prefecture := fs.String("prefecture", "",
		"本社所在地の都道府県 filter (例: 東京都)、空で全国")
	storesInPref := fs.String("stores-in-prefecture", "",
		"Phase 26: 当該都道府県に店舗を持つ operator (本社所在地不問)")
	pipelineDB := fs.String("pipeline-db", "var/pizza.sqlite",
		"stores-in-prefecture 用の pipeline SQLite")
	brands := fs.String("brands", "", "カンマ区切りブランド filter (空で全ブランド)")
	out := fs.String("out", "var/fc-directory.csv", "CSV 出力パス")
	outJSON := fs.String("out-json", "", "JSON 出力 (debug 用、空で skip)")
	componentOut := fs.String("component-out", "",
		"qualified operator の brand 別 component CSV 出力")
	minTotal := fs.Int("min-total", 0,
		"operator 合計 estimated_store_count の下限 (0 で無効)")
	minBrands := fs.Int("min-brands", 0,
		"operator brand_count の下限 (0 で無効)")
	excludeZero := fs.Bool("exclude-zero-stores", false,
		"estimated_store_count=0 の operator を除外")
	includeFranchisor := fs.Bool("include-franchisor", false,
		"本部・直営 link も directory に含める")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.fc_directory",
		"--out", *out, "--pipeline-db", *pipelineDB}
	if *prefecture != "" {
		pyArgs = append(pyArgs, "--prefecture", *prefecture)
	}
	if *storesInPref != "" {
		pyArgs = append(pyArgs, "--stores-in-prefecture", *storesInPref)
	}
	if *brands != "" {
		pyArgs = append(pyArgs, "--brands", *brands)
	}
	if *outJSON != "" {
		pyArgs = append(pyArgs, "--out-json", *outJSON)
	}
	if *componentOut != "" {
		pyArgs = append(pyArgs, "--component-out", *componentOut)
	}
	if *minTotal > 0 {
		pyArgs = append(pyArgs, "--min-total", strconv.Itoa(*minTotal))
	}
	if *minBrands > 0 {
		pyArgs = append(pyArgs, "--min-brands", strconv.Itoa(*minBrands))
	}
	if *excludeZero {
		pyArgs = append(pyArgs, "--exclude-zero-stores")
	}
	if *includeFranchisor {
		pyArgs = append(pyArgs, "--include-franchisor")
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdCoverageExport は 14 brand × 47 都道府県の店舗/operator coverage CSV を出力する。
//
//	pizza coverage-export --out-dir var/phase28/nationwide-coverage
func cmdCoverageExport(args []string) error {
	fs := flag.NewFlagSet("coverage-export", flag.ExitOnError)
	dbPath := fs.String("db", "var/pizza.sqlite", "pipeline SQLite")
	brands := fs.String("brands", "", "カンマ区切り brand list (空なら14ブランド)")
	outDir := fs.String("out-dir", "var/phase28/nationwide-coverage", "CSV 出力先 directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.coverage_exports",
		"--db", *dbPath,
		"--out-dir", *outDir}
	if *brands != "" {
		pyArgs = append(pyArgs, "--brands", *brands)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdReviewHoujinHydrate は review 対象メガジーを国税庁 CSV SQLite の
// normalized_name 完全一致 + 本社住所で高速照合する。
//
//	pizza review-houjin-hydrate --apply
func cmdReviewHoujinHydrate(args []string) error {
	fs := flag.NewFlagSet("review-houjin-hydrate", flag.ExitOnError)
	reviewCSV := fs.String("review-csv", "var/phase28/nationwide-coverage/mega-franchisee-review-min20.csv", "review 対象 CSV")
	houjinDB := fs.String("houjin-db", "var/houjin/registry.sqlite", "国税庁 CSV SQLite")
	ormDB := fs.String("orm-db", "var/pizza-registry.sqlite", "registry ORM SQLite")
	out := fs.String("out", "var/phase28/nationwide-coverage/mega-franchisee-review-houjin-matches.csv", "照合結果 CSV")
	apply := fs.Bool("apply", false, "accepted match を ORM に反映する")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.megafranchisee_review_hydrate",
		"--review-csv", *reviewCSV,
		"--houjin-db", *houjinDB,
		"--orm-db", *ormDB,
		"--out", *out,
	}
	if *apply {
		pyArgs = append(pyArgs, "--apply")
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdOSMFetchAll は Overpass API から全国店舗を補完し、OSM operator tag が
// ある場合は operator_stores に provenance 付きで保存する。
func cmdOSMFetchAll(args []string) error {
	fs := flag.NewFlagSet("osm-fetch-all", flag.ExitOnError)
	brands := fs.String("brands", "", "カンマ区切り brand list (空なら14ブランド)")
	dbPath := fs.String("db", "var/pizza.sqlite", "pipeline SQLite")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.commands.osm_fetch_all", "--db", *dbPath}
	if *brands != "" {
		pyArgs = append(pyArgs, "--brands", *brands)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdCleanse は operator_stores の dirty 名 (regex garbage) を
// LLM canonicalize + 国税庁 CSV 検証で綺麗にする。
// LLM は **変換のみ** (free-form 生成禁止)、検証は 国税庁 577 万件が担当。
//
//	pizza cleanse --brand モスバーガー --db var/pizza.sqlite --dry-run
//	pizza cleanse --db var/pizza.sqlite  # 全 brand、実 apply
func cmdCleanse(args []string) error {
	fs := flag.NewFlagSet("cleanse", flag.ExitOnError)
	brand := fs.String("brand", "", "対象ブランド (空で全件)")
	dbPath := fs.String("db", "var/pizza.sqlite", "pipeline SQLite")
	dryRun := fs.Bool("dry-run", false, "提案のみで DB update しない")
	concurrency := fs.Int("concurrency", 3, "LLM 並列呼出数")
	out := fs.String("out", "", "提案 JSON 出力パス")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.cleanse",
		"--db", *dbPath,
		"--concurrency", strconv.Itoa(*concurrency)}
	if *brand != "" {
		pyArgs = append(pyArgs, "--brand", *brand)
	}
	if *dryRun {
		pyArgs = append(pyArgs, "--dry-run")
	}
	if *out != "" {
		pyArgs = append(pyArgs, "--out", *out)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdPurge は operator_stores から 国税庁法人番号 CSV に存在しない
// operator 名を削除する。enrich/research で紛れ込んだ garbage
// (例: 「NTTタウンページ株式会社」 iタウンページ 403 page footer、
// 「モスバーガーを展開する株式会社」 文字列断片) を除去。
//
//	pizza purge --brand モスバーガー --dry-run
//	pizza purge  # 全件、実 DELETE
func cmdPurge(args []string) error {
	fs := flag.NewFlagSet("purge", flag.ExitOnError)
	brand := fs.String("brand", "", "対象ブランド (空で全件)")
	dbPath := fs.String("db", "var/pizza.sqlite", "pipeline SQLite")
	dryRun := fs.Bool("dry-run", false, "削除候補列挙のみ")
	logPath := fs.String("log", "var/phase26/purge-log.csv", "provenance CSV")
	crossThreshold := fs.Int("cross-brand-threshold", 0,
		"corp 空 かつ brand_count >= N の operator を汚染扱いで削除 (0 で無効)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.purge",
		"--db", *dbPath, "--log", *logPath}
	if *brand != "" {
		pyArgs = append(pyArgs, "--brand", *brand)
	}
	if *dryRun {
		pyArgs = append(pyArgs, "--dry-run")
	}
	if *crossThreshold > 0 {
		pyArgs = append(pyArgs, "--cross-brand-threshold", strconv.Itoa(*crossThreshold))
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdAddressReverse は operator 不明の店舗の 住所 → 国税庁 CSV
// 同住所 株式会社検索で operator 候補を抽出。Mos の 161 不明店等の補完。
//
//	pizza address-reverse --brand モスバーガー --dry-run
//	pizza address-reverse --brand モスバーガー  # apply
func cmdAddressReverse(args []string) error {
	fs := flag.NewFlagSet("address-reverse", flag.ExitOnError)
	brand := fs.String("brand", "", "対象ブランド (必須)")
	dbPath := fs.String("db", "var/pizza.sqlite", "pipeline SQLite")
	maxStores := fs.Int("max-stores", 0, "処理上限 (0 で全件)")
	dryRun := fs.Bool("dry-run", false, "提案のみ")
	allowMulti := fs.Bool("allow-multi", false,
		"複数候補でも DB 更新 (non-single、要 LLM critic 併用推奨)")
	out := fs.String("out", "", "matches CSV 出力")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *brand == "" {
		return fmt.Errorf("--brand is required")
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.address_reverse",
		"--brand", *brand, "--db", *dbPath}
	if *maxStores > 0 {
		pyArgs = append(pyArgs, "--max-stores", strconv.Itoa(*maxStores))
	}
	if *dryRun {
		pyArgs = append(pyArgs, "--dry-run")
	}
	if *allowMulti {
		pyArgs = append(pyArgs, "--allow-multi")
	}
	if *out != "" {
		pyArgs = append(pyArgs, "--out", *out)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdDeepResearch は operator 不明店舗を Gemini で web 検索 + Claude critic で
// evidence URL 検証 + 国税庁 CSV で法人存在検証 の 3 段でハルシネ防止しつつ特定。
// L1-L4 (per_store/Scrapling/address-reverse/phone-reverse) 全て失敗した
// 店舗の最終手段。cost 配慮で max-stores 制限必須。
//
//	pizza deep-research --brand モスバーガー --max-stores 20 --dry-run
//	pizza deep-research --brand モスバーガー --max-stores 50  # apply
func cmdDeepResearch(args []string) error {
	fs := flag.NewFlagSet("deep-research", flag.ExitOnError)
	brand := fs.String("brand", "", "対象ブランド (必須)")
	dbPath := fs.String("db", "var/pizza.sqlite", "pipeline SQLite")
	maxStores := fs.Int("max-stores", 20,
		"Gemini 呼出上限 (cost 配慮、default 20)")
	dryRun := fs.Bool("dry-run", false, "提案のみ")
	concurrency := fs.Int("concurrency", 2, "Gemini 並列数")
	out := fs.String("out", "", "proposal JSON 出力")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *brand == "" {
		return fmt.Errorf("--brand is required")
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.deep_research",
		"--brand", *brand, "--db", *dbPath,
		"--max-stores", strconv.Itoa(*maxStores),
		"--concurrency", strconv.Itoa(*concurrency)}
	if *dryRun {
		pyArgs = append(pyArgs, "--dry-run")
	}
	if *out != "" {
		pyArgs = append(pyArgs, "--out", *out)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdRecruitmentResearch は求人・採用ページを search し、本文 gate と国税庁 CSV
// exact verify を通ったものだけ operator_stores に反映する。
//
//	pizza recruitment-research --brands "モスバーガー,業務スーパー" --max-stores 20
//	pizza recruitment-research --max-stores 0 --dry-run --out var/recruitment.json
func cmdRecruitmentResearch(args []string) error {
	fs := flag.NewFlagSet("recruitment-research", flag.ExitOnError)
	brand := fs.String("brand", "", "対象ブランド")
	brands := fs.String("brands", "", "カンマ区切りブランド (空なら14ブランド)")
	dbPath := fs.String("db", "var/pizza.sqlite", "pipeline SQLite")
	maxStores := fs.Int("max-stores", 20,
		"各ブランドの Gemini 呼出上限 (0 で全件)")
	offset := fs.Int("offset", 0, "各ブランドの未特定店舗リストの先頭から skip する件数")
	dryRun := fs.Bool("dry-run", false, "提案のみ")
	concurrency := fs.Int("concurrency", 2, "Gemini 並列数")
	brandConcurrency := fs.Int("brand-concurrency", 3, "ブランド横断並列数")
	llmPageCritic := fs.Bool("llm-page-critic", false,
		"Scrapling 取得済み HTML snippet を LLM で本文限定判定する")
	fetcher := fs.String("fetcher", "static",
		"求人 evidence URL 取得方式: static | dynamic | camofox | auto")
	out := fs.String("out", "", "proposal JSON 出力")
	exportSidecarsFrom := fs.String("export-sidecars-from", "",
		"既存 proposal JSON から取捨選択用 sidecar CSV を再生成")
	applyFrom := fs.String("apply-from", "",
		"既存 proposal JSON の accepted row だけを DB に反映")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *applyFrom != "" {
		return runDeliveryPython("python", "-m", "pizza_delivery.recruitment_research",
			"--db", *dbPath, "--apply-from", *applyFrom).Run()
	}
	if *exportSidecarsFrom != "" {
		return runDeliveryPython("python", "-m", "pizza_delivery.recruitment_research",
			"--export-sidecars-from", *exportSidecarsFrom).Run()
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.recruitment_research",
		"--db", *dbPath,
		"--max-stores", strconv.Itoa(*maxStores),
		"--offset", strconv.Itoa(*offset),
		"--concurrency", strconv.Itoa(*concurrency),
		"--brand-concurrency", strconv.Itoa(*brandConcurrency),
		"--fetcher", *fetcher}
	if *brand != "" {
		pyArgs = append(pyArgs, "--brand", *brand)
	}
	if *brands != "" {
		pyArgs = append(pyArgs, "--brands", *brands)
	}
	if *dryRun {
		pyArgs = append(pyArgs, "--dry-run")
	}
	if *llmPageCritic {
		pyArgs = append(pyArgs, "--llm-page-critic")
	}
	if *out != "" {
		pyArgs = append(pyArgs, "--out", *out)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdOfficialRecruitmentCrawl は公式 jobfind/Recop 系採用ページを直接巡回し、
// detail 本文の「募集者」表示と店舗住所 match、国税庁 exact verify を通したもの
// だけ operator_stores に追加する。検索 snippet や LLM 知識は使わない。
//
//	pizza official-recruitment-crawl --sources "モスバーガー=https://mos-recruit.net/jobfind-pc/area/All" --dry-run
func cmdOfficialRecruitmentCrawl(args []string) error {
	fs := flag.NewFlagSet("official-recruitment-crawl", flag.ExitOnError)
	dbPath := fs.String("db", "var/pizza.sqlite", "pipeline SQLite")
	sources := fs.String("sources", "",
		"'brand=url,brand2=url2'。空なら実装済み公式求人 sources")
	maxPages := fs.Int("max-pages", 999, "各 source の list page 上限")
	maxDetails := fs.Int("max-details", 0, "detail page 上限 (0 で全件)")
	concurrency := fs.Int("concurrency", 16, "detail fetch 並列数")
	timeout := fs.Float64("timeout", 8.0, "fetch timeout 秒")
	fetcher := fs.String("fetcher", "static",
		"list/detail page 取得方式: static | dynamic | camofox | auto")
	requestDelay := fs.Float64("request-delay", 0.0,
		"detail fetch ごとの sleep 秒 (429 回避用)")
	maxEmptyStreak := fs.Int("max-empty-streak", 30,
		"operator 抽出なし detail が連続したら停止 (0 で無効)")
	dryRun := fs.Bool("dry-run", false, "DB update なし")
	out := fs.String("out", "", "detail 判定 CSV 出力")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.official_recruitment_crawl",
		"--db", *dbPath,
		"--max-pages", strconv.Itoa(*maxPages),
		"--max-details", strconv.Itoa(*maxDetails),
		"--concurrency", strconv.Itoa(*concurrency),
		"--timeout", fmt.Sprintf("%.3f", *timeout),
		"--fetcher", *fetcher,
		"--request-delay", fmt.Sprintf("%.3f", *requestDelay),
		"--max-empty-streak", strconv.Itoa(*maxEmptyStreak)}
	if *sources != "" {
		pyArgs = append(pyArgs, "--sources", *sources)
	}
	if *dryRun {
		pyArgs = append(pyArgs, "--dry-run")
	}
	if *out != "" {
		pyArgs = append(pyArgs, "--out", *out)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdOfficialFranchiseeSources は公式FC募集ページ・公式運営会社ページ・本部PR本文
// から operator evidence を直接抽出し、国税庁CSVで照合して ORM に登録する。
// 検索 snippet や LLM 知識は使わない。
//
//	pizza official-franchisee-sources --brands "Brand off,カルビ丼とスン豆腐専門店韓丼" --clean-registry
func cmdOfficialFranchiseeSources(args []string) error {
	fs := flag.NewFlagSet("official-franchisee-sources", flag.ExitOnError)
	brands := fs.String("brands", "", "カンマ区切り brand filter")
	ormDB := fs.String("orm-db", "var/pizza-registry.sqlite", "registry ORM SQLite")
	houjinDB := fs.String("houjin-db", "var/houjin/registry.sqlite", "国税庁 CSV SQLite")
	out := fs.String("out", "var/phase28/nationwide-coverage/official-franchisee-sources.csv", "evidence CSV 出力")
	timeout := fs.Float64("timeout", 15.0, "fetch timeout 秒")
	cleanRegistry := fs.Bool("clean-registry", false,
		"対象 brand の pipeline structural garbage/重複corp空 link を削除")
	dryRun := fs.Bool("dry-run", false, "DB update なし")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.official_franchisee_sources",
		"--orm-db", *ormDB,
		"--houjin-db", *houjinDB,
		"--out", *out,
		"--timeout", fmt.Sprintf("%.3f", *timeout)}
	if *brands != "" {
		pyArgs = append(pyArgs, "--brands", *brands)
	}
	if *cleanRegistry {
		pyArgs = append(pyArgs, "--clean-registry")
	}
	if *dryRun {
		pyArgs = append(pyArgs, "--dry-run")
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdEdinetSync は EDINET 有価証券報告書 から FC 本部の関係会社 / 重要契約先 を
// 抽出して ORM に登録する。モスフードサービス (E03384) 等の listed FC 本部に有効。
// EDINET_API_KEY env var 必要 (https://disclosure2dl.edinet-fsa.go.jp/ で無料登録)。
//
//	pizza edinet-sync --edinet-code E03384 --brand モスバーガー \
//	    --date-from 2024-01-01 --out var/phase27/mos-edinet.csv
func cmdEdinetSync(args []string) error {
	fs := flag.NewFlagSet("edinet-sync", flag.ExitOnError)
	edinetCode := fs.String("edinet-code", "", "EDINET コード (例: E03384)")
	brand := fs.String("brand", "", "紐付ける brand 名 (必須)")
	dateFrom := fs.String("date-from", "",
		"検索開始日 YYYY-MM-DD (default 過去 400日)")
	dateTo := fs.String("date-to", "",
		"検索終了日 YYYY-MM-DD (default 今日)")
	dryRun := fs.Bool("dry-run", false, "ORM に write しない")
	out := fs.String("out", "", "抽出 companies CSV 出力パス")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *edinetCode == "" || *brand == "" {
		return fmt.Errorf("--edinet-code と --brand は必須")
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.sources.edinet",
		"--edinet-code", *edinetCode, "--brand", *brand}
	if *dateFrom != "" {
		pyArgs = append(pyArgs, "--date-from", *dateFrom)
	}
	if *dateTo != "" {
		pyArgs = append(pyArgs, "--date-to", *dateTo)
	}
	if *dryRun {
		pyArgs = append(pyArgs, "--dry-run")
	}
	if *out != "" {
		pyArgs = append(pyArgs, "--out", *out)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdOperatorSpider は ORM 登録済の operator の公式 HP から店舗一覧を Scrapling で
// 取得し、住所 prefix match で pipeline DB の Places 店舗と紐付ける。
// ハルシネ 0 (operator 名は ORM、place_id は Places、match は決定論)。
//
//	pizza operator-spider --brand モスバーガー --dry-run
func cmdOperatorSpider(args []string) error {
	fs := flag.NewFlagSet("operator-spider", flag.ExitOnError)
	brand := fs.String("brand", "", "対象ブランド (必須)")
	dbPath := fs.String("db", "var/pizza.sqlite", "pipeline SQLite")
	dryRun := fs.Bool("dry-run", false, "DB update なし")
	concurrency := fs.Int("concurrency", 2, "Scrapling 並列数")
	maxFollowLinks := fs.Int("max-follow-links", 5, "店舗一覧 link の追従上限")
	out := fs.String("out", "", "matches CSV 出力")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *brand == "" {
		return fmt.Errorf("--brand は必須")
	}
	pyArgs := []string{"python", "-m",
		"pizza_delivery.commands.operator_spider_cmd",
		"--brand", *brand, "--db", *dbPath,
		"--concurrency", strconv.Itoa(*concurrency),
		"--max-follow-links", strconv.Itoa(*maxFollowLinks)}
	if *dryRun {
		pyArgs = append(pyArgs, "--dry-run")
	}
	if *out != "" {
		pyArgs = append(pyArgs, "--out", *out)
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdOperatorBrandDiscovery は ORM 登録済 operator の公式 HP から
// 事業/ブランド一覧 link を巡回し、既知 FC brand との link を ORM に追加する。
// 店舗数はこの経路では増やさず estimated_store_count=0 の evidence link に留める。
//
//	pizza operator-brand-discovery --min-total 20 --dry-run
//	pizza operator-brand-discovery --fetcher camofox --concurrency 6
func cmdOperatorBrandDiscovery(args []string) error {
	fs := flag.NewFlagSet("operator-brand-discovery", flag.ExitOnError)
	ormDB := fs.String("orm-db", "var/pizza-registry.sqlite", "registry ORM SQLite")
	out := fs.String("out", "var/phase28/nationwide-coverage/operator-brand-discovery.csv", "discovery CSV 出力")
	minTotal := fs.Int("min-total", 20, "対象 operator の最低 estimated total stores")
	limit := fs.Int("limit", 0, "処理上限 operator 数 (0 で全件)")
	concurrency := fs.Int("concurrency", 8, "公式サイト fetch 並列数")
	timeout := fs.Float64("timeout", 8.0, "fetch timeout 秒")
	fetcher := fs.String("fetcher", "static", "取得方式: static | dynamic | camofox | auto")
	maxFollowLinks := fs.Int("max-follow-links", 4, "事業/ブランド link の追従上限")
	brands := fs.String("brands", "", "カンマ区切り brand filter (空なら既知FC全体)")
	allowExternalLinks := fs.Bool("allow-external-links", false,
		"operator公式上の外部 href brand anchor も自動反映する")
	dryRun := fs.Bool("dry-run", false, "DB update なし")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.operator_brand_discovery",
		"--orm-db", *ormDB,
		"--out", *out,
		"--min-total", strconv.Itoa(*minTotal),
		"--limit", strconv.Itoa(*limit),
		"--concurrency", strconv.Itoa(*concurrency),
		"--timeout", fmt.Sprintf("%.3f", *timeout),
		"--fetcher", *fetcher,
		"--max-follow-links", strconv.Itoa(*maxFollowLinks)}
	if *brands != "" {
		pyArgs = append(pyArgs, "--brands", *brands)
	}
	if *allowExternalLinks {
		pyArgs = append(pyArgs, "--allow-external-links")
	}
	if *dryRun {
		pyArgs = append(pyArgs, "--dry-run")
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdExtendedFCBrandExport はユーザー提供の追加 FC ブランド seed と既存 all-brand
// evidence にある非14ブランドを統合し、監査表と FC 運営会社専用表を生成する。
//
//	pizza extended-fc-brand-export
//	pizza extended-fc-brand-export --seed test/fixtures/megafranchisee/fc-brand-seeds-2026-04-27.tsv
func cmdExtendedFCBrandExport(args []string) error {
	fs := flag.NewFlagSet("extended-fc-brand-export", flag.ExitOnError)
	seed := fs.String("seed", "test/fixtures/megafranchisee/fc-brand-seeds-2026-04-27.tsv", "追加 FC brand seed TSV")
	fcLinks := fs.String("fc-links", "test/fixtures/megafranchisee/fc-links.csv", "all-brand brand/operator link CSV")
	ormDB := fs.String("orm-db", "var/pizza-registry.sqlite", "registry ORM SQLite")
	houjinDB := fs.String("houjin-db", "var/houjin/registry.sqlite", "国税庁 CSV SQLite")
	out := fs.String("out", "test/fixtures/megafranchisee/extended-brand-links.csv", "監査用 full link CSV")
	summaryOut := fs.String("summary-out", "test/fixtures/megafranchisee/extended-brand-summary.csv", "追加 brand 取得状況 summary CSV")
	byBrandDir := fs.String("by-brand-dir", "test/fixtures/megafranchisee/by-view/extended-by-brand", "監査用 brand 別出力 dir")
	fcOut := fs.String("fc-out", "test/fixtures/megafranchisee/extended-fc-operator-links.csv", "FC 運営会社専用 CSV")
	fcByBrandDir := fs.String("fc-by-brand-dir", "test/fixtures/megafranchisee/by-view/extended-fc-by-brand", "FC 運営会社 brand 別出力 dir")
	allFCOut := fs.String("all-fc-out", "test/fixtures/megafranchisee/all-fc-operator-links.csv", "全ブランド FC 運営会社専用 CSV")
	allFCByBrandDir := fs.String("all-fc-by-brand-dir", "test/fixtures/megafranchisee/by-view/all-fc-by-brand", "全ブランド FC 運営会社 brand 別出力 dir")
	allFCMin2ByBrandDir := fs.String("all-fc-min2-by-brand-dir", "test/fixtures/megafranchisee/by-view/all-fc-by-brand-min2", "全ブランド FC 運営会社 2件以上 brand 別出力 dir")
	allFCBrandIndexOut := fs.String("all-fc-brand-index-out", "test/fixtures/megafranchisee/by-view/all-fc-brand-index.csv", "全ブランド FC brand 別件数 index CSV")
	allFCSingletonsOut := fs.String("all-fc-singletons-out", "test/fixtures/megafranchisee/by-view/all-fc-singleton-brands.csv", "全ブランド FC 1件 brand 監査 CSV")
	allFCCandidatesOut := fs.String("all-fc-candidates-out", "test/fixtures/megafranchisee/all-fc-operator-candidates.csv", "全ブランド FC 運営会社候補 CSV (franchisee + unknown)")
	minOperatorRows := fs.Int("min-operator-rows", 2, "min2 brand 別出力に含める最小 operator 行数")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.extended_fc_brand_export",
		"--seed", *seed,
		"--fc-links", *fcLinks,
		"--orm-db", *ormDB,
		"--houjin-db", *houjinDB,
		"--out", *out,
		"--summary-out", *summaryOut,
		"--by-brand-dir", *byBrandDir,
		"--fc-out", *fcOut,
		"--fc-by-brand-dir", *fcByBrandDir,
		"--all-fc-out", *allFCOut,
		"--all-fc-by-brand-dir", *allFCByBrandDir,
		"--all-fc-min2-by-brand-dir", *allFCMin2ByBrandDir,
		"--all-fc-brand-index-out", *allFCBrandIndexOut,
		"--all-fc-singletons-out", *allFCSingletonsOut,
		"--all-fc-candidates-out", *allFCCandidatesOut,
		"--min-operator-rows", strconv.Itoa(*minOperatorRows),
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdBrandFillRateExport は fc-links.csv を brand 単位で集計し、本部公表店舗数に
// 対する FC 運営会社 evidence の充填率を監査 CSV として出力する。
//
//	pizza brand-fill-rate-export
func cmdBrandFillRateExport(args []string) error {
	fs := flag.NewFlagSet("brand-fill-rate-export", flag.ExitOnError)
	fcLinks := fs.String("fc-links", "test/fixtures/megafranchisee/fc-links.csv", "all-brand brand/operator link CSV")
	out := fs.String("out", "test/fixtures/megafranchisee/brand-fill-rate.csv", "brand 充填率 CSV 出力")
	officialSourceOut := fs.String("official-source-out", "test/fixtures/megafranchisee/official-source-audit.csv", "公式 source 経路 audit CSV 出力")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pyArgs := []string{"python", "-m", "pizza_delivery.brand_fill_rate_export",
		"--fc-links", *fcLinks,
		"--out", *out,
		"--official-source-out", *officialSourceOut,
	}
	return runDeliveryPython(pyArgs...).Run()
}

// cmdImportMegajiiCSV は人手集計 TSV (メガジー + 本部) を LLM+国税庁 verify で
// ORM に取込む。
//
//	pizza import-megajii-csv --csv var/external/megajii-manual.tsv --dry-run
//	pizza import-megajii-csv --csv var/external/megajii-manual.tsv \
//	    --out var/phase27/mega-proposals.json
func cmdImportMegajiiCSV(args []string) error {
	fs := flag.NewFlagSet("import-megajii-csv", flag.ExitOnError)
	csvPath := fs.String("csv", "", "入力 TSV")
	sourceTag := fs.String("source-tag", "manual_megajii_2026_04_24",
		"ORM レコードの source タグ")
	dryRun := fs.Bool("dry-run", false, "提案のみで DB 更新しない")
	out := fs.String("out", "", "提案 JSON 出力 (optional)")
	concurrency := fs.Int("concurrency", 3, "LLM 並列数")
	saveDB := fs.String("save-db", "",
		"TSV parse 結果を SQLite に保存 (例: var/external/megajii.sqlite)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *csvPath == "" {
		return fmt.Errorf("--csv は必須")
	}
	pyArgs := []string{"python", "-m",
		"pizza_delivery.commands.import_megajii_csv",
		"--csv", *csvPath,
		"--source-tag", *sourceTag,
		"--concurrency", strconv.Itoa(*concurrency),
	}
	if *dryRun {
		pyArgs = append(pyArgs, "--dry-run")
	}
	if *out != "" {
		pyArgs = append(pyArgs, "--out", *out)
	}
	if *saveDB != "" {
		pyArgs = append(pyArgs, "--save-db", *saveDB)
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
	brandsCSV := fs.String("brands", "", "カンマ区切りブランド filter (空で全ブランド)")
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
	brandFilterExpr := "None"
	if *brandsCSV != "" {
		parts := strings.Split(*brandsCSV, ",")
		quoted := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			quoted = append(quoted, strconv.Quote(p))
		}
		if len(quoted) > 0 {
			brandFilterExpr = "{" + strings.Join(quoted, ",") + "}"
		}
	}
	topExpr := fmt.Sprintf("ops[:%d]", *top)
	if *top == 0 {
		topExpr = "ops"
	}
	script := fmt.Sprintf(
		"from pizza_delivery.registry_expander import aggregate_cross_brand_operators, export_cross_brand_to_csv, export_cross_brand_to_yaml;"+
			"ops = aggregate_cross_brand_operators(db_path=%q, min_total_stores=%d, min_brands=%d, exclude_franchisor=%s, brands_filter=%s);"+
			"ops.sort(key=lambda o: %s);"+
			"export_cross_brand_to_csv(ops, out_path=%q);"+
			"%s"+
			"print(f'✅ {len(ops)} operators');"+
			"[print(f'  {o.total_stores:4d} 店  {o.brand_count} 業態  {o.name}  ({\", \".join(f\"{b}:{n}\" for b,n in sorted(o.brand_counts.items(), key=lambda kv:-kv[1]))})') for o in %s]",
		*dbPath, *minTotal, *minBrands, excludeHQ, brandFilterExpr, sortKey, *outCSV, yamlStmt, topExpr,
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
//
// _SPARSE_PREFS は人口密度の低い県 (低 FC 店舗密度)。
// 巨大 area + sparse cells で Places API call を無駄にしない様、大きな cell を使う。
//
// 基準 (人口密度 人/km² 2020年国勢調査):
//
//	北海道 (67), 岩手 (78), 秋田 (75), 高知 (95), 島根 (98), 山形 (111),
//	青森 (117), 鹿児島 (160), 福島 (122), 沖縄 (622、しかし面積広く分布疎)
var _SPARSE_PREFS = map[string]bool{
	"北海道": true, "岩手県": true, "秋田県": true,
	"青森県": true, "山形県": true, "福島県": true,
	"新潟県": true, "長野県": true,
	"鳥取県": true, "島根県": true, "高知県": true,
	"鹿児島県": true, "沖縄県": true,
}

// _DENSE_PREFS は大都市圏 (FC 密集、小 cell で精度優先)。
var _DENSE_PREFS = map[string]bool{
	"東京都": true, "大阪府": true, "神奈川県": true,
	"愛知県": true, "埼玉県": true, "千葉県": true,
	"兵庫県": true, "福岡県": true,
}

// cellKmForArea は area 名に応じて cell-km を調整。
// sparse (低密度県) は defaultKm × 2.5、dense (大都市圏) は defaultKm × 0.6。
// その他は default。
//
// 例: default 5km のとき 北海道 = 12.5km、東京都 = 3km。
func cellKmForArea(area string, defaultKm float64) float64 {
	if _SPARSE_PREFS[area] {
		return defaultKm * 2.5
	}
	if _DENSE_PREFS[area] {
		return defaultKm * 0.6
	}
	return defaultKm
}

func cmdBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	brandsCSV := fs.String("brands", "", "カンマ区切りブランド名リスト")
	areasCSV := fs.String("areas", "", "カンマ区切り area")
	cellKm := fs.Float64("cell-km", 3.0, "bake メッシュ幅")
	outDir := fs.String("out-dir", "var/bench", "サマリ CSV/JSON 出力ディレクトリ")
	adaptive := fs.Bool("adaptive", true, "Adaptive quad-tree split を使う")
	// Phase 27: full-scan mode (per brand に `pizza scan` を呼び出す)
	fullScan := fs.Bool("full-scan", false,
		"各 brand に pizza scan (research+audit 含む) を呼ぶ (default: bake-only bench)")
	withJudge := fs.Bool("with-judge", false,
		"--full-scan 時に Panel judge を有効化 (DELIVERY_MODE=panel 要)")
	judgeMode := fs.String("judge-mode", "",
		"--with-judge 時の mode (mock|live|panel)")
	withKitchen := fs.Bool("with-kitchen", false,
		"--full-scan 時に Firecrawl Kitchen を有効化")
	withVerify := fs.Bool("with-verify", false,
		"--full-scan 時に CrossVerifier (別 LLM critic) を有効化")
	useScrapling := fs.Bool("use-scrapling", false,
		"--full-scan 時に research を Scrapling で実行 (SPA 対応)")
	maxResearch := fs.Int("max-research", 150,
		"--full-scan 時の research 最大店舗数")
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
		Brand      string  `json:"brand"`
		Areas      string  `json:"areas"`
		Stores     int32   `json:"stores_found"`
		ElapsedSec float32 `json:"elapsed_sec"`
		APICalls   int     `json:"api_calls"`
		RejBrand   int     `json:"rejected_brand_filter"`
		RejPoly    int     `json:"rejected_polygon"`
		RejDup     int     `json:"rejected_duplicate"`
		CellsCap   int     `json:"cells_hit_cap"`
	}
	var results []BenchResult

	fmt.Printf("🧪 pizza bench: brands=%v areas=%q cell_km=%.1f adaptive=%v full_scan=%v\n",
		brands, *areasCSV, *cellKm, *adaptive, *fullScan)

	// Phase 27: full-scan mode — per brand に `pizza scan` を subprocess で呼ぶ
	if *fullScan {
		for _, brand := range brands {
			fmt.Printf("\n━━━ brand=%q (full-scan) ━━━\n", brand)
			t0 := time.Now()
			scanArgs := []string{
				"scan",
				"--brand", brand,
				"--areas", *areasCSV,
				"--cell-km", fmt.Sprintf("%.1f", *cellKm),
				"--max-research", strconv.Itoa(*maxResearch),
				"--out", filepath.Join(absOutDir, fmt.Sprintf("scan-%s.csv", brand)),
			}
			if *withJudge {
				scanArgs = append(scanArgs, "--with-judge")
			}
			if *judgeMode != "" {
				scanArgs = append(scanArgs, "--judge-mode", *judgeMode)
			}
			if *withKitchen {
				scanArgs = append(scanArgs, "--with-kitchen")
			}
			if *withVerify {
				scanArgs = append(scanArgs, "--with-verify")
			}
			_ = useScrapling // future: wire into --use-scrapling for research phase
			scanCmd := exec.Command("./bin/pizza", scanArgs...)
			scanCmd.Stdout = os.Stdout
			scanCmd.Stderr = os.Stderr
			scanCmd.Env = os.Environ()
			if err := scanCmd.Run(); err != nil {
				fmt.Printf("  ⚠️  scan %s failed: %v (続行)\n", brand, err)
			}
			fmt.Printf("  %s scan elapsed %.1fs\n", brand, time.Since(t0).Seconds())
		}
		fmt.Printf("\n✅ full-scan bench done → %s\n", absOutDir)
		return nil
	}

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
	// Phase 27 bugfix: area (都道府県) ごとに cell-km を自動調整
	//   - 低密度 (北海道/岩手等) → cell-km × 2.5 (cell 数 6x 削減)
	//   - 大都市 (東京/大阪等)    → cell-km × 0.6 (精度優先)
	//   - その他                   → default
	// → 北海道での context deadline exceeded 対策
	fmt.Println("━━ [2/4] bake (各 area、per-pref cell-km) ━━")
	areas := splitAndTrim(*areasCSV)
	for _, area := range areas {
		areaCellKm := cellKmForArea(area, *cellKm)
		if areaCellKm != *cellKm {
			fmt.Printf("   [cell-km auto] area=%s → %.1fkm (default %.1f)\n",
				area, areaCellKm, *cellKm)
		}
		bakeArgs := []string{
			"--query", *brand,
			"--area", area,
			"--cell-km", strconv.FormatFloat(areaCellKm, 'f', -1, 64),
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
	// Phase 27 bugfix: 北海道等巨大 area 対応で 30 分に延伸
	rctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
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
	useScrapling := fs.Bool("use-scrapling", false,
		"per_store fetch に Scrapling を使う (SPA 対応、JS rendering、~5s/store)")
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
	if *useScrapling {
		pyArgs = append(pyArgs, "--use-scrapling")
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
