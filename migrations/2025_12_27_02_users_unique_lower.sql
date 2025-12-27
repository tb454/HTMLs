-- 2025_12_27_02_users_unique_lower.sql
-- Purpose: ensure ON CONFLICT ((lower(email))) / ((lower(username))) are valid

create extension if not exists pgcrypto;

-- Email lower unique (nullable-safe)
create unique index if not exists uq_users_email_lower_mig
  on public.users ((lower(email)))
  where email is not null;

-- Username lower unique (nullable-safe)
create unique index if not exists uq_users_username_lower_mig
  on public.users ((lower(username)))
  where username is not null;