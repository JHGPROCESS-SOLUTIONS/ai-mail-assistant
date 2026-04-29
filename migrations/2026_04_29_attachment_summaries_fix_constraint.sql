-- =============================================================================
-- OfficeFlow: Attachment Summaries — fix on_conflict
-- Date: 2026-04-29 (hotfix)
-- Purpose: Replace the partial UNIQUE INDEX with a proper UNIQUE CONSTRAINT.
--          PostgREST's on_conflict parameter requires a constraint, not a
--          partial index. Without this, every insert into attachment_summaries
--          fails with "42P10: no unique or exclusion constraint matching
--          the ON CONFLICT specification".
-- =============================================================================

-- 1. Drop the partial unique index from the original migration.
drop index if exists public.attachment_summaries_unique_per_attachment;

-- 2. Add a proper unique constraint that PostgREST recognises.
--    Postgres treats NULL values as distinct by default, so multiple rows
--    with gmail_attachment_id=NULL are allowed (legacy / inline parts).
alter table public.attachment_summaries
  add constraint attachment_summaries_unique_per_attachment
  unique (gmail_message_id, gmail_attachment_id);
