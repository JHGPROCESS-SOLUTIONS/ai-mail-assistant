-- =============================================================================
-- OfficeFlow: Scheduled Sends
-- Date: 2026-04-29
-- Purpose: Allow users to queue a Gmail draft for sending at a chosen future
--          time. Background loop dispatches due rows. Status transitions:
--            pending -> sent | cancelled | failed
-- =============================================================================

create table if not exists public.scheduled_sends (
  id              uuid primary key default gen_random_uuid(),
  user_id         uuid not null references auth.users(id) on delete cascade,
  mailbox_id      uuid not null references public.mailboxes(id) on delete cascade,
  gmail_draft_id  text not null,
  gmail_thread_id text,
  subject         text,
  to_email        text,
  scheduled_at    timestamptz not null,
  status          text not null default 'pending'
                  check (status in ('pending','sent','cancelled','failed')),
  sent_at         timestamptz,
  error_message   text,
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

-- Performance: the dispatcher polls "pending and due now"
create index if not exists scheduled_sends_due_idx
  on public.scheduled_sends (scheduled_at)
  where status = 'pending';

-- Frontend lists "my upcoming scheduled sends"
create index if not exists scheduled_sends_user_status_idx
  on public.scheduled_sends (user_id, status, scheduled_at);

-- Keep updated_at fresh on every row update
create or replace function public.touch_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists scheduled_sends_touch_updated_at on public.scheduled_sends;
create trigger scheduled_sends_touch_updated_at
  before update on public.scheduled_sends
  for each row execute function public.touch_updated_at();

comment on table public.scheduled_sends is
  'Queue of Gmail drafts the user has scheduled to send at a future time. '
  'A background worker dispatches rows where status=pending AND scheduled_at <= now().';
