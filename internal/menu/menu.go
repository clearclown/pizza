// Package menu は PI-ZZA のランタイム設定 (.env / CLI フラグ) を束ねる。
package menu

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config は PI-ZZA 実行時の全設定を保持する。
type Config struct {
	GoogleMapsAPIKey string
	// Phase 27: 複数 GCP project の Places API key を ',' 区切りで env 指定。
	// PIZZA_API_KEYS="k1,k2,k3" で round-robin → 実質 throughput 3x。
	// GoogleMapsAPIKey と重複 OK (自動 dedup)。
	PlacesAPIKeys []string

	FirecrawlMode   string // "docker" | "saas"
	FirecrawlAPIURL string
	FirecrawlAPIKey string

	LLMProvider string // "anthropic" | "openai" | "gemini"
	LLMModel    string

	SeedServiceAddr     string
	DeliveryServiceAddr string

	DBPath    string
	OutputDir string

	LogLevel                string
	MaxConcurrency          int
	GridCellKM              float64
	MegaFranchiseeThreshold int
}

// FromEnv は環境変数から Config を構築する。
// 未設定項目はデフォルト値を採用する。
func FromEnv() (*Config, error) {
	// Phase 27: PIZZA_API_KEYS="k1,k2,..." で key pool 指定 (GoogleMapsAPIKey も混ぜる)
	pool := []string{}
	primary := os.Getenv("GOOGLE_MAPS_API_KEY")
	if primary != "" {
		pool = append(pool, primary)
	}
	if raw := os.Getenv("PIZZA_API_KEYS"); raw != "" {
		for _, k := range strings.Split(raw, ",") {
			k = strings.TrimSpace(k)
			if k == "" || contains(pool, k) {
				continue
			}
			pool = append(pool, k)
		}
	}
	cfg := &Config{
		GoogleMapsAPIKey:        primary,
		PlacesAPIKeys:           pool,
		FirecrawlMode:           envDefault("FIRECRAWL_MODE", "docker"),
		FirecrawlAPIURL:         envDefault("FIRECRAWL_API_URL", "http://localhost:3002"),
		FirecrawlAPIKey:         os.Getenv("FIRECRAWL_API_KEY"),
		LLMProvider:             envDefault("LLM_PROVIDER", "anthropic"),
		LLMModel:                os.Getenv("LLM_MODEL"),
		SeedServiceAddr:         envDefault("SEED_SERVICE_ADDR", "localhost:50051"),
		DeliveryServiceAddr:     envDefault("DELIVERY_SERVICE_ADDR", "localhost:50053"),
		DBPath:                  envDefault("PIZZA_DB_PATH", "./var/pizza.sqlite"),
		OutputDir:               envDefault("PIZZA_OUTPUT_DIR", "./var/output"),
		LogLevel:                envDefault("LOG_LEVEL", "info"),
		MaxConcurrency:          envInt("MAX_CONCURRENCY", 8),
		GridCellKM:              envFloat("GRID_CELL_KM", 1.0),
		MegaFranchiseeThreshold: envInt("MEGA_FRANCHISEE_THRESHOLD", 20),
	}

	switch cfg.FirecrawlMode {
	case "docker", "saas":
	default:
		return nil, fmt.Errorf("FIRECRAWL_MODE must be docker|saas, got %q", cfg.FirecrawlMode)
	}
	switch cfg.LLMProvider {
	case "anthropic", "openai", "gemini":
	default:
		return nil, fmt.Errorf("LLM_PROVIDER must be anthropic|openai|gemini, got %q", cfg.LLMProvider)
	}
	return cfg, nil
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func contains(slice []string, target string) bool {
	for _, v := range slice {
		if v == target {
			return true
		}
	}
	return false
}
