-- =============================================================================
-- OfficeFlow: Attachment AI Summaries
-- Date: 2026-04-29
-- Purpose: Store AI-generated summaries + structured key data for attachments
--          (PDF, DOCX) on incoming Gmail messages. Surfaced under each draft
--          so the user can review the attachment without opening it.
-- =============================================================================

create table if not exists public.attachment_summaries (
  id                    uuid primary key default gen_random_uuid(),
  user_id               uuid not null references auth.users(id) on delete cascade,
  email_id              uuid references public.emails(id) on delete cascade,
  gmail_message_id      text not null,
  gmail_attachment_id   text,
  filename              text not null,
  mime_type             text,
  size_bytes            bigint,
  summary               text,
  key_data              jsonb,
  status                text not null default 'success'
                        check (status in ('success','failed','unsupported','too_large')),
  error_message         text,
  created_at            timestamptz not null default now()
);

-- Frontend lists "summaries for these drafts": fetch by email_id batch
create index if not exists attachment_summaries_email_idx
  on public.attachment_summaries (email_id);

-- Per-user history view
create index if not exists attachment_summaries_user_created_idx
  on public.attachment_summaries (user_id, created_at desc);

-- Idempotency: never summarize the same attachment twice
create unique index if not exists attachment_summaries_unique_per_attachment
  on public.attachment_summaries (gmail_message_id, gmail_attachment_id)
  where gmail_attachment_id is not null;

comment on table public.attachment_summaries is
  'AI-generated summaries + structured key data for attachments (PDF, DOCX) on incoming '
  'Gmail messages. Created during process_inbox_for_user, displayed on the dashboard '
  'beneath the matching draft.';
