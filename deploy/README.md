# 🚢 deploy/ — セルフホスト構成

PI-ZZA のセルフホスト配備資材。**docker / podman 両対応**。

## 構成

- `compose.yaml` — **PI-ZZA 本体の gRPC サービス群** (dough, delivery, orchestrator, box-ui)
- `compose.firecrawl.yaml` — **Firecrawl スタック** (api, playwright, redis, rabbitmq, postgres)
  - AGPL-3.0 隔離のため独立ファイル。REST 越境のみで本体と通信する
- `Dockerfile.*` — 各サービスの Docker イメージ定義

---

## 🚀 Makefile 経由 (推奨)

```bash
make up             # PI-ZZA 本体のみ起動 (Firecrawl は SaaS または別途起動)
make up-firecrawl   # Firecrawl スタックだけ起動
make up-all         # PI-ZZA + Firecrawl 全部起動
make down           # 両方停止
make logs SVC=pizza-dough   # ログ追尾
```

---

## 🐳 Docker compose 直接実行

```bash
# A. SaaS 前提 (Firecrawl Cloud を使う。FIRECRAWL_MODE=saas)
docker compose -f deploy/compose.yaml --profile app --profile ui up -d

# B. 完全セルフホスト (Firecrawl も含める)
docker compose \
  -f deploy/compose.yaml \
  -f deploy/compose.firecrawl.yaml \
  --profile app --profile ui --profile firecrawl up -d

# C. Firecrawl だけ先に起動し動作確認
docker compose -f deploy/compose.firecrawl.yaml --profile firecrawl up -d
curl http://localhost:3002/v2/health/liveness
```

## 🦭 Podman compose

コマンドの `docker compose` を `podman compose` または `podman-compose` に置換すれば同じ。

### Podman 固有の注意点

- **rootless 運用**: 1024 以下のポートは bind 不可。PI-ZZA が使う 50051/50053/3002/8501 はすべて 1024 以上
- **ネットワーク**: デフォルト CNI で `pizza-net` が自動作成される
- **ボリューム**: `podman volume ls` で `pi-zza_*` が確認できる
- **macOS**: 初回は `podman machine start` が必要

---

## 📋 サービス一覧

### compose.yaml (PI-ZZA 本体)

| Service | ポート | 言語 | Profile |
|---|---|---|---|
| `dough-service` | 50051 | Go | `app` |
| `delivery-service` | 50053 | Python | `app` |
| `pizza-orchestrator` | - | Go | `app` |
| `box-ui` | 8501 | Python/Streamlit | `ui` |

### compose.firecrawl.yaml (Firecrawl スタック)

| Service | ポート | 役割 | Profile |
|---|---|---|---|
| `firecrawl-api` | 3002 | REST API | `firecrawl` |
| `firecrawl-playwright` | - | ヘッドレスブラウザ | `firecrawl` |
| `firecrawl-redis` | - | ジョブキュー | `firecrawl` |
| `firecrawl-rabbitmq` | - | メッセージキュー | `firecrawl` |
| `firecrawl-postgres` | - | メタデータ保存 | `firecrawl` |

---

## 🔑 環境変数

`cp .env.example .env` でテンプレからコピーし、API キーを埋めてください。
`docker compose` は自動で `.env` を読み込みます。

重要な切替:

| 変数 | 意味 |
|---|---|
| `FIRECRAWL_MODE` | `docker` (セルフホスト) or `saas` (Cloud) |
| `FIRECRAWL_API_URL` | docker 時は `http://firecrawl-api:3002` |
| `FIRECRAWL_API_KEY` | saas 時のみ |
| `LLM_PROVIDER` | `anthropic` / `openai` / `gemini` |

---

## 🔬 疎通確認

```bash
grpcurl -plaintext localhost:50051 list        # SeedService が見える
grpcurl -plaintext localhost:50053 list        # DeliveryService が見える
curl http://localhost:3002/v2/health/liveness  # Firecrawl liveness
curl http://localhost:8501                     # Streamlit UI
```

---

## 🧹 停止 & クリーンアップ

```bash
make down           # 両 compose のコンテナ停止 (ボリューム維持)
make down-volumes   # ボリュームも削除 (データ全消去)
```

---

## 📌 Phase 0 現状

- ✅ `compose.yaml` / `compose.firecrawl.yaml` は `docker compose config` で syntax 検証済
- ✅ Firecrawl image `ghcr.io/firecrawl/firecrawl:latest` を pull 可能確認済
- 🚧 PI-ZZA 本体 (dough/delivery/orchestrator/box-ui) は Phase 1+ でロジック実装後に起動
- ✅ `make up-firecrawl` だけは Phase 0 でも動作可能 (Firecrawl REST テスト用)

## k8s

Kubernetes 化は `deploy/k8s/` で Phase 4 以降に対応予定。
