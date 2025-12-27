-- 2025_12_26_01_schema_migrations.sql
-- Purpose: ensure schema_migrations table exists (idempotent)
create table if not exists public.schema_migrations (
  version   text primary key,
  applied_at timestamptz not null default now(),
  checksum  text not null
);