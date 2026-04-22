# Firecrawl セルフホスト — 現時点の状況

## 発見: `ghcr.io/firecrawl/firecrawl:latest` (2026-04 時点) の schema migration

Phase 3 で `deploy/compose.firecrawl.yaml` により本家公式 image を起動したところ、
以下のエラーで crash → supervisor がすべてのサブプロセスを shutdown する無限ループに陥った:

```
error: relation "nuq.queue_scrape" does not exist
  at /app/node_modules/.pnpm/pg-pool@3.10.1_...
Error: nuq-prefetch-worker failed with exit code 1
```

原因: Firecrawl の新 queue 実装 "NuQ" が PostgreSQL スキーマ `nuq` と
テーブル `queue_scrape` を期待するが、image 起動時に migration が走らない。
本家 compose にも初期化ステップが明記されていないため、**公式 image は
現状セルフホストに対してインストール手順が不完全**。

## 推奨代替案

### 代替 A: Firecrawl Cloud (SaaS) を使う ← 推奨

最小の手間:
```bash
# .env に
FIRECRAWL_MODE=saas
FIRECRAWL_API_KEY=fc-xxxxx   # https://www.firecrawl.dev で発行
```

`pizza bake --with-judge` はそのまま動く。月数千件の crawl なら無料枠内で収まることが多い。

### 代替 B: Firecrawl 公式リポジトリを clone して build

```bash
git clone https://github.com/firecrawl/firecrawl
cd firecrawl
docker compose up -d
```

本家の compose に同梱された migration スクリプトが走る。`ghcr.io` の
pre-built image と異なり、ローカル build 時に全環境が揃う。PI-ZZA から
は `FIRECRAWL_API_URL=http://localhost:3002` で接続するだけ。

### 代替 C: trieve/firecrawl (community fork)

`docker pull docker.io/trieve/firecrawl:latest` で動作報告あり。
ただしライセンス (AGPL 継承) と互換性保証に注意。

## M2 Kitchen を skip する運用 (Phase 3 までの暫定)

`pizza bake --no-kitchen` (default) で Markdown 取得を抜いても、
M1 Seed による店舗抽出 + M3 Delivery による判定 (mock/live) は動作する。
Phase 3 の live accuracy 計測も、golden CSV の `notes` 列を `markdown`
代わりに使う形で実施可能 (test_live_accuracy.py がその仕様)。

## Phase 4 以降の計画

- 公式 Firecrawl 本体の clone → 本家 compose による起動手順を `scripts/firecrawl-selfhost.sh` に入れる
- `compose.firecrawl.yaml` は SaaS ユーザ向けに `firecrawl-api` を省略し、
  Redis/Postgres/RabbitMQ だけ残してローカル開発を支援する構成に整理
- または MCP 対応の `mcp/firecrawl` image (Docker Hub) への移行を検討
