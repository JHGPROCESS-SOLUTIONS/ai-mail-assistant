-- =============================================================================
-- OfficeFlow: Snoozed Emails
-- Date: 2026-05-01
-- Purpose: Allow users to temporarily remove a mail from Inbox and have it
--          automatically returned at a chosen wake time. Preserves original
--          OfficeFlow status label so the mail re-appears in the correct tab.
--          Status transitions: pending -> woken | cancelled | failed
-- =============================================================================

create table if not exists public.snoozed_emails (
  id                uuid primary key default gen_random_uuid(),
  user_id           uuid not null references public.users(id) on delete cascade,
  mailbox_id        uuid not null references public.mailboxes(id) on delete cascade,
  email_id          uuid not null references public.emails(id) on delete cascade,
  gmail_message_id  text not null,
  gmail_thread_id   text,
  subject           text,
  -- Original OfficeFlow status label that was on the mail at snooze time.
  -- Restored on wake so the mail re-enters the correct tab.
  original_label    text,
  -- Gmail label_id for original_label, captured at snooze time so the
  -- wake worker doesn't have to re-resolve it.
  original_label_id text,
  wake_at           timestamptz not null,
  status            text not null default 'pending'
                    check (status in ('pending','woken','cancelled','failed')),
  woken_at          timestamptz,
  error_message     text,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);

-- Performance: the wake worker polls "pending and due now"
create index if not exists snoozed_emails_due_idx
  on public.snoozed_emails (wake_at)
  where status = 'pending';

-- Frontend: "show my currently snoozed mails, soonest first"
create index if not exists snoozed_emails_user_status_idx
  on public.snoozed_emails (user_id, status, wake_at);

-- Prevent the same mail from being snoozed twice in parallel.
-- A mail can only have ONE pending snooze; finished rows (woken/cancelled/failed)
-- stay in the table for history and don't block new snoozes.
create unique index if not exists snoozed_emails_one_pending_per_email_idx
  on public.snoozed_emails (email_id)
  where status = 'pending';

-- Reuse the touch_updated_at trigger function defined by the
-- 2026_04_29_scheduled_sends migration. Create only if missing so this
-- migration is idempotent on databases that already have it.
do $$
begin
  if not exists (
    select 1 from pg_proc where proname = 'touch_updated_at'
  ) then
    create function public.touch_updated_at()
    returns trigger language plpgsql as $fn$
    begin
      new.updated_at = now();
      return new;
    end;
    $fn$;
  end if;
end $$;

drop trigger if exists snoozed_emails_touch_updated_at on public.snoozed_emails;
create trigger snoozed_emails_touch_updated_at
  before update on public.snoozed_emails
  for each row execute function public.touch_updated_at();

-- =============================================================================
-- RLS — only the owning user can see / mutate their own snooze rows.
-- The backend's service-role key bypasses RLS automatically.
-- =============================================================================
alter table public.snoozed_emails enable row level security;

drop policy if exists snoozed_emails_select_own on public.snoozed_emails;
create policy snoozed_emails_select_own
  on public.snoozed_emails
  for select
  using (auth.uid() = user_id);

drop policy if exists snoozed_emails_insert_own on public.snoozed_emails;
create policy snoozed_emails_insert_own
  on public.snoozed_emails
  for insert
  with check (auth.uid() = user_id);

drop policy if exists snoozed_emails_update_own on public.snoozed_emails;
create policy snoozed_emails_update_own
  on public.snoozed_emails
  for update
  using (auth.uid() = user_id)
  with check (auth.uid() = user_id);

drop policy if exists snoozed_emails_delete_own on public.snoozed_emails;
create policy snoozed_emails_delete_own
  on public.snoozed_emails
  for delete
  using (auth.uid() = user_id);

comment on table public.snoozed_emails is
  'Mails the user has temporarily removed from Inbox via Snooze. A background '
  'worker dispatches rows where status=pending AND wake_at <= now(), restoring '
  'the original status label and INBOX label.';
