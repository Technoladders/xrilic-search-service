-- ─────────────────────────────────────────────────────────────────────────────
-- migration_001_search_sync.sql
-- Run once in Supabase SQL editor
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Ensure updated_at column exists and auto-updates on hr_talent_pool
--    (safe to run even if already exists)

ALTER TABLE hr_talent_pool
  ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

-- Trigger to auto-stamp updated_at on every UPDATE
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_talent_pool_updated_at ON hr_talent_pool;
CREATE TRIGGER trg_talent_pool_updated_at
  BEFORE UPDATE ON hr_talent_pool
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 2. Index on updated_at for fast incremental sync queries
CREATE INDEX IF NOT EXISTS idx_htp_updated_at
  ON hr_talent_pool (updated_at DESC);

-- Combined index: org + updated_at (what the poller uses)
CREATE INDEX IF NOT EXISTS idx_htp_org_updated
  ON hr_talent_pool (organization_id, updated_at DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- SUPABASE WEBHOOK SETUP (do this in Supabase Dashboard, not SQL)
-- ─────────────────────────────────────────────────────────────────────────────
-- Go to: Supabase Dashboard → Database → Webhooks → Create new webhook
--
-- Name:          talent-pool-search-sync
-- Table:         hr_talent_pool
-- Events:        INSERT, UPDATE, DELETE
-- URL:           https://sync.xrilic.ai/webhook/talent-pool
--                (or http://147.79.66.219:8009/webhook/talent-pool for direct)
-- HTTP Method:   POST
-- Headers:
--   x-webhook-secret: <value of SEARCH_WEBHOOK_SECRET>
--
-- This gives real-time sync: new/updated resumes appear in search within ~2 seconds
-- The poller (every 60s) acts as fallback if webhook misses any events
-- ─────────────────────────────────────────────────────────────────────────────