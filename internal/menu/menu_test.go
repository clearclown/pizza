package menu

import (
	"testing"
)

func TestFromEnv_defaults(t *testing.T) {
	// 全 env を空にしてデフォルト値を確認
	for _, k := range []string{
		"GOOGLE_MAPS_API_KEY", "FIRECRAWL_MODE", "FIRECRAWL_API_URL",
		"FIRECRAWL_API_KEY", "LLM_PROVIDER", "LLM_MODEL",
		"SEED_SERVICE_ADDR", "DELIVERY_SERVICE_ADDR",
		"PIZZA_DB_PATH", "PIZZA_OUTPUT_DIR",
		"LOG_LEVEL", "MAX_CONCURRENCY", "GRID_CELL_KM", "MEGA_FRANCHISEE_THRESHOLD",
	} {
		t.Setenv(k, "")
	}
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() error = %v", err)
	}
	if cfg.FirecrawlMode != "docker" {
		t.Errorf("default FirecrawlMode: got %q, want docker", cfg.FirecrawlMode)
	}
	if cfg.FirecrawlAPIURL != "http://localhost:3002" {
		t.Errorf("default FirecrawlAPIURL: got %q", cfg.FirecrawlAPIURL)
	}
	if cfg.LLMProvider != "anthropic" {
		t.Errorf("default LLMProvider: got %q", cfg.LLMProvider)
	}
	if cfg.SeedServiceAddr != "localhost:50051" {
		t.Errorf("default SeedServiceAddr: got %q", cfg.SeedServiceAddr)
	}
	if cfg.DBPath != "./var/pizza.sqlite" {
		t.Errorf("default DBPath: got %q", cfg.DBPath)
	}
	if cfg.MaxConcurrency != 8 {
		t.Errorf("default MaxConcurrency: got %d, want 8", cfg.MaxConcurrency)
	}
	if cfg.GridCellKM != 1.0 {
		t.Errorf("default GridCellKM: got %v, want 1.0", cfg.GridCellKM)
	}
	if cfg.MegaFranchiseeThreshold != 20 {
		t.Errorf("default MegaFranchiseeThreshold: got %d, want 20", cfg.MegaFranchiseeThreshold)
	}
}

func TestFromEnv_envOverrides(t *testing.T) {
	t.Setenv("FIRECRAWL_MODE", "saas")
	t.Setenv("LLM_PROVIDER", "gemini")
	t.Setenv("MAX_CONCURRENCY", "16")
	t.Setenv("GRID_CELL_KM", "0.5")
	t.Setenv("MEGA_FRANCHISEE_THRESHOLD", "10")
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() error = %v", err)
	}
	if cfg.FirecrawlMode != "saas" {
		t.Errorf("FirecrawlMode: got %q", cfg.FirecrawlMode)
	}
	if cfg.LLMProvider != "gemini" {
		t.Errorf("LLMProvider: got %q", cfg.LLMProvider)
	}
	if cfg.MaxConcurrency != 16 {
		t.Errorf("MaxConcurrency: got %d", cfg.MaxConcurrency)
	}
	if cfg.GridCellKM != 0.5 {
		t.Errorf("GridCellKM: got %v", cfg.GridCellKM)
	}
	if cfg.MegaFranchiseeThreshold != 10 {
		t.Errorf("MegaFranchiseeThreshold: got %d", cfg.MegaFranchiseeThreshold)
	}
}

func TestFromEnv_invalidFirecrawlMode(t *testing.T) {
	t.Setenv("FIRECRAWL_MODE", "invalid")
	if _, err := FromEnv(); err == nil {
		t.Error("expected error for invalid FIRECRAWL_MODE")
	}
}

func TestFromEnv_invalidLLMProvider(t *testing.T) {
	t.Setenv("FIRECRAWL_MODE", "docker")
	t.Setenv("LLM_PROVIDER", "cohere")
	if _, err := FromEnv(); err == nil {
		t.Error("expected error for invalid LLM_PROVIDER")
	}
}

func TestEnvInt_invalidFallsBackToDefault(t *testing.T) {
	t.Setenv("TEST_INT_KEY", "not-a-number")
	got := envInt("TEST_INT_KEY", 42)
	if got != 42 {
		t.Errorf("envInt fallback: got %d, want 42", got)
	}
}

func TestEnvFloat_invalidFallsBackToDefault(t *testing.T) {
	t.Setenv("TEST_FLOAT_KEY", "xyz")
	got := envFloat("TEST_FLOAT_KEY", 3.14)
	if got != 3.14 {
		t.Errorf("envFloat fallback: got %v, want 3.14", got)
	}
}

func TestEnvDefault_emptyReturnsDefault(t *testing.T) {
	t.Setenv("TEST_STR_KEY", "")
	if got := envDefault("TEST_STR_KEY", "fallback"); got != "fallback" {
		t.Errorf("envDefault: got %q, want fallback", got)
	}
}
