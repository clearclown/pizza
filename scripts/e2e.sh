#!/usr/bin/env bash
# 🍕 e2e.sh — docker compose で起動 → E2E テスト → クリーンアップ

set -euo pipefail

COMPOSE_FILE="deploy/compose.yaml"

detect_compose() {
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    echo "docker compose"
  elif command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
    echo "podman compose"
  elif command -v podman-compose >/dev/null 2>&1; then
    echo "podman-compose"
  else
    echo "docker compose"  # fallback
  fi
}

COMPOSE=$(detect_compose)
echo "🧭 Using: $COMPOSE"

cleanup() {
  echo "🧹 Cleaning up..."
  $COMPOSE -f "$COMPOSE_FILE" down -v || true
}
trap cleanup EXIT

echo "🚀 Starting core services..."
$COMPOSE -f "$COMPOSE_FILE" up -d firecrawl playwright-service redis

echo "⏳ Waiting for Firecrawl to be ready..."
for i in {1..30}; do
  if curl -fsSL http://localhost:3002/v0/health/liveness >/dev/null 2>&1; then
    echo "✅ Firecrawl up"
    break
  fi
  sleep 2
done

echo "🧪 Running E2E tests..."
go test -tags=integration -race ./test/e2e/... || {
  echo "❌ E2E tests failed"
  exit 1
}

echo "✅ E2E tests passed"
