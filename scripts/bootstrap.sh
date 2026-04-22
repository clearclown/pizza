#!/usr/bin/env bash
# 🍕 PI-ZZA bootstrap — 依存一括インストール
# 想定: macOS (brew) / Linux (apt/yum)

set -euo pipefail

BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${BLUE}[bootstrap]${NC} $*"; }
ok()   { echo -e "${GREEN}[ok]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }

need() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 && { ok "$cmd found: $(command -v "$cmd")"; return 0; }
  warn "$cmd not found — please install it"
  return 1
}

log "🍕 Checking toolchain..."
ERR=0
need go          || ERR=1
need python3     || ERR=1
need uv          || ERR=1
need buf         || ERR=1
need protoc      || true   # optional — buf で代替
need grpcurl     || warn "grpcurl missing (used for local gRPC testing)"
need golangci-lint || warn "golangci-lint missing (used for Go lint)"

# コンテナエンジン
if command -v docker >/dev/null 2>&1; then ok "docker found"
elif command -v podman >/dev/null 2>&1; then ok "podman found"
else warn "no container engine found (docker / podman). make up is unavailable"; fi

if [[ "$ERR" -ne 0 ]]; then
  warn "Some required tools are missing. Install via:"
  echo "  macOS:  brew install go uv bufbuild/buf/buf golangci-lint grpcurl"
  echo "  Linux:  see docs/bootstrap-linux.md (TODO)"
  exit 1
fi

log "📦 Installing Go modules..."
go mod download
ok "go mod download complete"

log "🐍 Syncing Python workspace (uv)..."
uv sync --all-packages
ok "uv sync complete"

log "🔌 Generating gRPC code..."
buf generate api
ok "buf generate complete → gen/"

log "✅ Bootstrap successful. Next:"
echo "    cp .env.example .env    # edit API keys"
echo "    make test               # run unit tests"
echo "    make up-core            # start Firecrawl stack only"
