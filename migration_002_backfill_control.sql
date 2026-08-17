-- ─────────────────────────────────────────────────────────────────────────
-- migration_002_backfill_control.sql
--
-- Run ONCE in the Supabase SQL editor. Matches this repo's existing
-- migration_001_search_sync.sql convention (there is no migration runner
-- here -- schema changes for this service are applied manually).
--
-- Adds ONE new table backing the portal_a backfill's server-side job
-- control (start/pause/stop, persisted cursor, single-claimant heartbeat).
-- Does NOT touch mc_process_control / mc_process_runs (a different,
-- unrelated pipeline -- see sync_service/master_candidates/ingest.py),
-- and does NOT touch portal_a_backfill_queue / enqueue_new_portal_a_row --
-- both are explicitly out of scope for this migration and are left
-- completely untouched.
-- ─────────────────────────────────────────────────────────────────────────


-- ─────────────────────────────────────────────────────────────────────────
-- STEP 0 -- PRE-FLIGHT CHECKS (run these SELECTs first; read the results
-- before proceeding, and adjust backfill/state.py / backfill/match.py if
-- any of them reveal something different from what this migration assumes)
-- ─────────────────────────────────────────────────────────────────────────

-- 0a. Exact allowed values for master_candidates_backfill_progress's CHECK
--     constraints -- writes outside these sets hard-fail at the Postgres
--     layer. backfill/state.py currently writes status in
--     ('running','completed','failed') and source='portal_a', matching
--     exactly what the original Edge Function used.
select conname, pg_get_constraintdef(oid)
from pg_constraint
where conname in ('backfill_progress_source_check', 'backfill_progress_status_check');

-- 0b. Exact allowed values for candidate_source_links / master_candidates /
--     candidate_merge_queue's relevant CHECK constraints.
select conrelid::regclass as table_name, conname, pg_get_constraintdef(oid)
from pg_constraint
where conname in (
  'candidate_source_links_source_check',
  'candidate_source_links_match_method_check',
  'master_candidates_primary_source_check',
  'master_candidates_seniority_check',
  'candidate_merge_queue_status_check',
  'candidate_merge_queue_confidence_check'
);

-- 0c. Is master_candidates.has_contact a plain column or generated/trigger-
--     maintained? backfill/match.py's build_insert_row() deliberately
--     omits it -- confirm that omission is correct (i.e. it has a default
--     or is populated by a trigger, not a NOT NULL column with no default).
select column_name, is_generated, generation_expression, is_nullable, column_default
from information_schema.columns
where table_schema = 'public' and table_name = 'master_candidates' and column_name = 'has_contact';

-- 0d. master_candidates_backfill_progress's full column list (the write
--     shape in backfill/state.py matches the original Edge Function's own
--     insert/update calls; confirm no NOT NULL column without a default is
--     missing from that shape).
select column_name, data_type, is_nullable, column_default
from information_schema.columns
where table_schema = 'public' and table_name = 'master_candidates_backfill_progress'
order by ordinal_position;

-- 0e. Confirm the hr_employees.role_id -> hr_roles.id FK exists (enables
--     the PostgREST embed select=role_id,hr_roles(name) used by
--     backfill/api.py's require_global_superadmin).
select conname, pg_get_constraintdef(oid)
from pg_constraint
where conrelid = 'public.hr_employees'::regclass and contype = 'f' and confrelid = 'public.hr_roles'::regclass;


-- ─────────────────────────────────────────────────────────────────────────
-- STEP 1 -- the new control table
-- ─────────────────────────────────────────────────────────────────────────
create table if not exists public.master_candidates_backfill_control (
  process_name        text primary key default 'portal_a',
  desired_state        text not null default 'stopped'
                          check (desired_state in ('running', 'paused', 'stopped')),
  last_run_status       text
                          check (last_run_status in ('completed', 'stopped_by_user', 'failed')),
  last_error            text,
  cursor_updated_at     timestamptz,
  cursor_id             text,
  last_batch_number     integer not null default 0,
  batch_size            integer not null default 500,
  concurrency           integer not null default 20,
  worker_instance_id    text,
  worker_heartbeat_at   timestamptz,
  session_started_at    timestamptz,
  updated_at            timestamptz not null default now()
);

comment on table public.master_candidates_backfill_control is
  'Singleton-per-source control row for the Python-side portal_a backfill '
  '(xrilic-search-service/sync_service/master_candidates/backfill/). '
  'Unrelated to mc_process_control (a different pipeline, ingest.py).';

-- Reuse the existing generic trigger function rather than defining a new one.
drop trigger if exists set_updated_at on public.master_candidates_backfill_control;
create trigger set_updated_at
  before update on public.master_candidates_backfill_control
  for each row execute function public.set_updated_at();

alter table public.master_candidates_backfill_control enable row level security;

drop policy if exists backfill_control_select_superadmin on public.master_candidates_backfill_control;
create policy backfill_control_select_superadmin
  on public.master_candidates_backfill_control
  for select
  to authenticated
  using (public.is_global_superadmin());

-- No write policy: all writes come from the Python service using the
-- service-role key, which bypasses RLS by design. Real enforcement for who
-- can trigger a write happens in backfill/api.py's require_global_superadmin,
-- not here.


-- ─────────────────────────────────────────────────────────────────────────
-- STEP 2 -- seed the initial row (idempotent)
-- ─────────────────────────────────────────────────────────────────────────
insert into public.master_candidates_backfill_control (process_name, desired_state)
values ('portal_a', 'stopped')
on conflict (process_name) do nothing;


-- ─────────────────────────────────────────────────────────────────────────
-- OUT OF SCOPE -- explicitly NOT part of this migration, left untouched:
--   - portal_a_backfill_queue (table) and enqueue_new_portal_a_row (trigger
--     on naukri_candidates). The new backfill design has zero runtime
--     dependency on either, but retiring them is a separate, deliberate,
--     human-confirmed follow-up after the new implementation has proven
--     itself in production -- not drafted here, not scheduled here.
--   - mc_process_control / mc_process_runs and everything in ingest.py /
--     state.py / admin_api.py (a different, currently-disabled pipeline).
-- ─────────────────────────────────────────────────────────────────────────
