# 🚢 deploy/ — セルフホスト構成

PI-ZZA のセルフホスト配備資材。**docker / podman 両対応**。

## 🐳 Docker で起動

```bash
# プロファイル all: 全サービス起動
docker compose -f deploy/compose.yaml --profile app --profile ui up -d

# プロファイル app のみ（UI なし、CLI 専用運用）
docker compose -f deploy/compose.yaml --profile app up -d

# Firecrawl だけ起動（Go からは直接 REST で叩く場合）
docker compose -f deploy/compose.yaml up -d firecrawl
```

## 🦭 Podman で起動

```bash
# Podman 4.x+ (native compose)
podman compose -f deploy/compose.yaml --profile app up -d

# もしくは podman-compose (pip install podman-compose)
podman-compose -f deploy/compose.yaml --profile app up -d
```

### Podman 固有の注意点

- **rootless 運用**: 1024 以下のポートは bind できない。50051/50053/3002/8501 はすべて 1024 以上なのでそのまま動く
- **ネットワーク**: podman のデフォルト CNI は `podman0`。compose が `pizza-net` を作成するので追加設定不要
- **ボリューム**: `podman volume ls` で `pi-zza_pizza-data` が確認できる
- **healthcheck**: podman 4.4+ で compose の healthcheck 対応

## サービス一覧

| Service | ポート | 言語 | Profile |
|---|---|---|---|
| `firecrawl` | 3002 | TS | (default) |
| `playwright-service` | - | TS | (default) |
| `redis` | - | - | (default) |
| `dough-service` | 50051 | Go | app |
| `delivery-service` | 50053 | Python | app |
| `pizza-orchestrator` | - | Go | app |
| `box-ui` | 8501 | Python | ui |

## 環境変数

プロジェクトルートに `.env` を置けば compose が自動的に読み込みます:

```bash
cp .env.example .env
# APIキー等を編集
docker compose -f deploy/compose.yaml --profile app up -d
```

## ログ確認

```bash
docker compose -f deploy/compose.yaml logs -f firecrawl
docker compose -f deploy/compose.yaml logs -f dough-service
```

## 停止とクリーンアップ

```bash
docker compose -f deploy/compose.yaml down              # コンテナ停止
docker compose -f deploy/compose.yaml down -v           # ボリュームも削除（データ全消去）
```

## grpc 疎通確認

```bash
grpcurl -plaintext localhost:50051 list            # SeedService が見える
grpcurl -plaintext localhost:50053 list            # DeliveryService が見える
curl http://localhost:3002/v0/health/liveness      # Firecrawl liveness
```

## Phase 0 注意

Phase 0 時点で起動するのは `firecrawl` / `playwright-service` / `redis` のみ（app プロファイル未起動）。`dough-service` / `delivery-service` は Phase 1+ でロジック実装後に起動します。

## k8s

Kubernetes 化は `deploy/k8s/` で別途対応予定（Phase 4 以降）。
