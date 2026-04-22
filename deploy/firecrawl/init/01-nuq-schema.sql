-- 🍕 PI-ZZA Firecrawl self-host init schema
--
-- ghcr.io/firecrawl/firecrawl:latest が起動時に期待する nuq スキーマを
-- 事前に作成する。本家 image には migration が同梱されていないため、
-- ソースコード (dist/src/services/worker/nuq.js) から reverse engineering した。
--
-- 対象テーブル:
--   nuq.queue_scrape         — メインのスクレイプジョブキュー
--   nuq.queue_scrape_backlog — バックログキュー (options.backlog=true)
--   nuq.queue_crawl_finished — クロール完了通知キュー
--   nuq.group_crawl          — クロールジョブの group (NuQJobGroup)
--
-- enum:
--   nuq.job_status ('queued','active','completed','failed')

CREATE SCHEMA IF NOT EXISTS nuq;

-- ─── Enum: job_status ───────────────────────────────────────────────
DO $$ BEGIN
    CREATE TYPE nuq.job_status AS ENUM ('queued', 'active', 'completed', 'failed');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- ─── Helper function for UUID generation ────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ─── Main queue: queue_scrape ────────────────────────────────────────
-- jobReturning columns (from nuq.js):
--   id, status, created_at, priority, data, finished_at,
--   listen_channel_id, returnvalue, failedreason, lock, owner_id, group_id
CREATE TABLE IF NOT EXISTS nuq.queue_scrape (
    id                 UUID             PRIMARY KEY DEFAULT gen_random_uuid(),
    status             nuq.job_status   NOT NULL DEFAULT 'queued',
    created_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),
    priority           INTEGER          NOT NULL DEFAULT 0,
    data               JSONB,
    finished_at        TIMESTAMPTZ,
    listen_channel_id  TEXT,
    returnvalue        JSONB,
    failedreason       TEXT,
    lock               TEXT,
    locked_at          TIMESTAMPTZ,
    owner_id           UUID,
    group_id           UUID
);
CREATE INDEX IF NOT EXISTS idx_queue_scrape_status     ON nuq.queue_scrape(status);
CREATE INDEX IF NOT EXISTS idx_queue_scrape_created_at ON nuq.queue_scrape(created_at);
CREATE INDEX IF NOT EXISTS idx_queue_scrape_owner_id   ON nuq.queue_scrape(owner_id);
CREATE INDEX IF NOT EXISTS idx_queue_scrape_group_id   ON nuq.queue_scrape(group_id);

-- ─── Backlog: queue_scrape_backlog ───────────────────────────────────
-- jobBacklogReturning: queue_scrape の列 + times_out_at
CREATE TABLE IF NOT EXISTS nuq.queue_scrape_backlog (
    id                 UUID             PRIMARY KEY DEFAULT gen_random_uuid(),
    status             nuq.job_status   NOT NULL DEFAULT 'queued',
    created_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),
    priority           INTEGER          NOT NULL DEFAULT 0,
    data               JSONB,
    finished_at        TIMESTAMPTZ,
    listen_channel_id  TEXT,
    returnvalue        JSONB,
    failedreason       TEXT,
    lock               TEXT,
    locked_at          TIMESTAMPTZ,
    owner_id           UUID,
    group_id           UUID,
    times_out_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_queue_scrape_backlog_status     ON nuq.queue_scrape_backlog(status);
CREATE INDEX IF NOT EXISTS idx_queue_scrape_backlog_times_out  ON nuq.queue_scrape_backlog(times_out_at);

-- ─── Secondary: queue_crawl_finished ─────────────────────────────────
CREATE TABLE IF NOT EXISTS nuq.queue_crawl_finished (
    id                 UUID             PRIMARY KEY DEFAULT gen_random_uuid(),
    status             nuq.job_status   NOT NULL DEFAULT 'queued',
    created_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),
    priority           INTEGER          NOT NULL DEFAULT 0,
    data               JSONB,
    finished_at        TIMESTAMPTZ,
    listen_channel_id  TEXT,
    returnvalue        JSONB,
    failedreason       TEXT,
    lock               TEXT,
    locked_at          TIMESTAMPTZ,
    owner_id           UUID,
    group_id           UUID
);
CREATE INDEX IF NOT EXISTS idx_queue_crawl_finished_status ON nuq.queue_crawl_finished(status);

-- ─── Group: group_crawl (NuQJobGroup) ────────────────────────────────
-- groupReturning columns (from nuq.js):
--   id, status, created_at, owner_id, ttl, expires_at
CREATE TABLE IF NOT EXISTS nuq.group_crawl (
    id          UUID             PRIMARY KEY DEFAULT gen_random_uuid(),
    status      nuq.job_status   NOT NULL DEFAULT 'queued',
    created_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    owner_id    UUID,
    ttl         INTEGER          NOT NULL DEFAULT 86400000,
    expires_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_group_crawl_owner_id   ON nuq.group_crawl(owner_id);
CREATE INDEX IF NOT EXISTS idx_group_crawl_expires_at ON nuq.group_crawl(expires_at);

-- ─── Permissions (USE_DB_AUTHENTICATION=false で postgres user が使う) ─
GRANT ALL ON SCHEMA nuq TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA nuq TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA nuq TO postgres;
