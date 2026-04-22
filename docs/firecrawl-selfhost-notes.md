# Firecrawl セルフホスト — 自力解決完了 ✅

## 解決ステータス

Phase 4 で `ghcr.io/firecrawl/firecrawl:latest` の起動時 schema エラーを
**本家に頼らず自力で解決** した。reverse engineering + init SQL 提供方式。

```
-- Before Phase 4 ----------
ERROR: relation "nuq.queue_scrape" does not exist    → crash loop
-- After Phase 4 (init SQL 提供後) ----------
✅ nuq スキーマ + 4 テーブル + enum 自動作成、起動可能
```

## 採用した解決方式

### 1. Image 内 source からの schema reverse engineering

```bash
docker run --rm --entrypoint sh ghcr.io/firecrawl/firecrawl:latest -c '
  grep -rE "CREATE TABLE|nuq\." /app/dist/src/services/worker/nuq.js | head
'
```

発見した要素:
- **Schema**: `nuq`
- **Enum**: `nuq.job_status` ('queued', 'active', 'completed', 'failed')
- **Tables**:
  - `nuq.queue_scrape` — メインスクレイプキュー
  - `nuq.queue_scrape_backlog` — バックログキュー (options.backlog=true)
  - `nuq.queue_crawl_finished` — クロール完了通知キュー
  - `nuq.group_crawl` — クロール group (NuQJobGroup)
- **Columns** (from INSERT + UPDATE SET):
  - `id, status, created_at, priority, data, finished_at, listen_channel_id,
    returnvalue, failedreason, lock, locked_at, owner_id, group_id`
  - backlog 専用: `+ times_out_at`
  - group 専用: `id, status, created_at, owner_id, ttl, expires_at`

### 2. PostgreSQL init SQL を自作してマウント

`deploy/firecrawl/init/01-nuq-schema.sql` に schema 定義を配置。
`compose.firecrawl.yaml` で `/docker-entrypoint-initdb.d:ro` にマウント:

```yaml
firecrawl-postgres:
  image: postgres:16-alpine
  volumes:
    - firecrawl-postgres:/var/lib/postgresql/data
    - ./firecrawl/init:/docker-entrypoint-initdb.d:ro   # ← 追加
```

PostgreSQL image は初回起動時に `/docker-entrypoint-initdb.d/*.sql` を
自動実行するため、fresh な volume で上げるたびに schema が揃う。

### 3. 既存 volume がある場合

init SQL は **初回のみ** 走る。既に `pi-zza_firecrawl-postgres` volume が
存在している場合は `docker volume rm pi-zza_firecrawl-postgres` してから
`make up-firecrawl` で clean state にする。

## 検証

```bash
$ docker exec pizza-firecrawl-postgres psql -U postgres -c '\dt nuq.*'
                List of relations
 Schema |         Name         | Type  |  Owner
--------+----------------------+-------+----------
 nuq    | group_crawl          | table | postgres
 nuq    | queue_crawl_finished | table | postgres
 nuq    | queue_scrape         | table | postgres
 nuq    | queue_scrape_backlog | table | postgres
(4 rows)
```

```bash
$ docker exec pizza-firecrawl-postgres psql -U postgres -c '\d nuq.queue_scrape'
                                    Table "nuq.queue_scrape"
      Column       |           Type           |            Default
-------------------+--------------------------+--------------------------
 id                | uuid                     | gen_random_uuid()
 status            | nuq.job_status           | 'queued'::nuq.job_status
 created_at        | timestamp with time zone | now()
 priority          | integer                  | 0
 data              | jsonb                    |
 finished_at       | timestamp with time zone |
 listen_channel_id | text                     |
 returnvalue       | jsonb                    |
 failedreason      | text                     |
 lock              | text                     |
 locked_at         | timestamp with time zone |
 owner_id          | uuid                     |
 group_id          | uuid                     |
```

Firecrawl API logs からの確認:
- ❌ 以前: `error: relation "nuq.queue_scrape" does not exist` → 繰返 crash
- ✅ 現在: 上記エラー消滅、schema レベルは健全

## 残存課題 (schema 外の別問題)

schema が解決した後、**OOM (exit code 137)** で API が終了するパターンが
観測された。原因:

```
[notice] alarm_handler: {set,{system_memory_high_watermark,[]}}
```

RabbitMQ が memory_high_watermark を越えたため Producer を blocking し、
その結果 API / workers が連鎖的に死亡。これは **podman VM の 2GB 制限**
が Firecrawl の 5 コンテナ (redis + rabbitmq + postgres + playwright + api)
を抱えきれないことが原因で、**schema 問題ではなくインフラ容量問題**。

### 対処案

1. **Podman VM メモリ増量** (推奨):
   ```bash
   podman machine stop
   podman machine set --memory 4096  # 4GB
   podman machine start
   ```

2. **Firecrawl を一部停止して運用**:
   - api だけ起動すれば軽量。nuq-prefetch-worker と crawl 系は無効化可
   - 本 PI-ZZA では Phase 4 で **Firecrawl をスキップし browser-use direct** で
     evidence 収集する設計に pivot したため、Firecrawl live 稼働は
     optional (SaaS でも OK)

3. **trieve/firecrawl community fork** (最後の選択肢):
   - schema 含め最小構成。同じ AGPL 境界で互換

## ファイル

- `deploy/firecrawl/init/01-nuq-schema.sql` — 自作 init SQL
- `deploy/compose.firecrawl.yaml` — init volume mount を追加
- `docs/firecrawl-selfhost-notes.md` — 本ドキュメント
