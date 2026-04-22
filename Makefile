# 🍕 PI-ZZA Makefile — 全言語共通の開発 UX 起点
#
# 使い方:
#   make help        # 全ターゲット一覧
#   make bootstrap   # 依存一括インストール
#   make proto       # gRPC コード生成
#   make test        # Go + Python テスト
#   make up          # docker compose でセルフホスト起動
#   make down        # 停止

SHELL := /bin/bash

# Compose 実行ツールの自動検出 (docker compose → podman compose → podman-compose)
COMPOSE := $(shell \
	if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then \
		echo "docker compose"; \
	elif command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then \
		echo "podman compose"; \
	elif command -v podman-compose >/dev/null 2>&1; then \
		echo "podman-compose"; \
	else \
		echo "docker compose"; \
	fi)

COMPOSE_FILE := deploy/compose.yaml
COMPOSE_FIRECRAWL := deploy/compose.firecrawl.yaml

.DEFAULT_GOAL := help

.PHONY: help
help: ## 利用可能なターゲット一覧
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: bootstrap
bootstrap: ## 依存一括インストール (Go / Python / Proto)
	@./scripts/bootstrap.sh

.PHONY: proto
proto: ## gRPC コード生成 (gen/go, gen/python)
	@buf generate api

.PHONY: proto-lint
proto-lint: ## buf lint + breaking 検査
	@buf lint api
	@buf breaking api --against '.git#branch=main,subdir=api' 2>/dev/null || echo "(no main branch yet — skipping breaking check)"

.PHONY: test
test: test-go test-py ## 全言語ユニットテスト

.PHONY: test-go
test-go: ## Go ユニットテスト
	@go test -race -cover ./...

.PHONY: test-py
test-py: ## Python pytest
	@cd services/delivery && uv run pytest -v

.PHONY: test-e2e
test-e2e: ## E2E (docker compose 起動 → integration テスト)
	@./scripts/e2e.sh

.PHONY: up
up: ## セルフホスト起動 (app + ui profile)
	@$(COMPOSE) -f $(COMPOSE_FILE) --profile app --profile ui up -d
	@echo "✅ PI-ZZA services started with: $(COMPOSE)"
	@echo "   Firecrawl は別途 'make up-firecrawl' または SaaS モード ('FIRECRAWL_MODE=saas') を利用してください"

.PHONY: up-all
up-all: ## PI-ZZA + Firecrawl セルフホスト全部起動
	@$(COMPOSE) -f $(COMPOSE_FILE) -f $(COMPOSE_FIRECRAWL) \
	  --profile app --profile ui --profile firecrawl up -d
	@echo "✅ Full stack started with: $(COMPOSE)"

.PHONY: up-firecrawl
up-firecrawl: ## Firecrawl スタックだけ起動 (M2 Kitchen)
	@$(COMPOSE) -f $(COMPOSE_FIRECRAWL) --profile firecrawl up -d
	@echo "✅ Firecrawl self-host started → http://localhost:3002"

.PHONY: down
down: ## 停止 (ボリュームは維持)
	@$(COMPOSE) -f $(COMPOSE_FILE) -f $(COMPOSE_FIRECRAWL) down

.PHONY: down-volumes
down-volumes: ## 完全クリーンアップ (データ含めて削除)
	@$(COMPOSE) -f $(COMPOSE_FILE) -f $(COMPOSE_FIRECRAWL) down -v

.PHONY: logs
logs: ## ログ追尾 (例: make logs SVC=pizza-dough)
	@$(COMPOSE) -f $(COMPOSE_FILE) -f $(COMPOSE_FIRECRAWL) logs -f $(SVC)

.PHONY: lint
lint: ## 全言語 lint
	@golangci-lint run
	@uv run ruff check
	@buf lint api

.PHONY: fmt
fmt: ## 全言語フォーマッタ
	@gofmt -w .
	@uv run ruff format
	@buf format -w api

.PHONY: build
build: ## Go バイナリビルド (bin/pizza)
	@mkdir -p bin
	@go build -trimpath -ldflags="-s -w" -o bin/pizza ./cmd/pizza
	@go build -trimpath -ldflags="-s -w" -o bin/dough-service ./cmd/dough-service
	@echo "✅ Built: bin/pizza, bin/dough-service"

.PHONY: clean
clean: ## ビルド成果物を削除 (gen/ は残す)
	@rm -rf bin/ dist/ coverage.out *.prof

.PHONY: ci-check
ci-check: proto-lint test-go test-py ## CI と同じ一連のチェック
	@echo "✅ CI-equivalent check passed"

.PHONY: compose-detect
compose-detect: ## どの compose 実装が使われるか表示
	@echo "Using: $(COMPOSE)"
