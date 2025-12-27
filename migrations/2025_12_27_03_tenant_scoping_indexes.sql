-- 2025_12_27_03_tenant_scoping_indexes.sql
-- Purpose: strengthen tenant scoping with indexes + optional FK constraints (NULL-safe)

-- Tenants table must exist
create extension if not exists pgcrypto;

create table if not exists public.tenants (
  id         uuid primary key default gen_random_uuid(),
  slug       text unique not null,
  name       text not null,
  region     text,
  created_at timestamptz not null default now()
);

-- Helpful indexes for tenant-scoped reads
create index if not exists idx_contracts_tenant_created_at
  on public.contracts (tenant_id, created_at desc)
  where tenant_id is not null;

create index if not exists idx_bols_tenant_pickup_time
  on public.bols (tenant_id, pickup_time desc)
  where tenant_id is not null;

create index if not exists idx_inventory_items_tenant_updated_at
  on public.inventory_items (tenant_id, updated_at desc)
  where tenant_id is not null;

create index if not exists idx_inventory_movements_tenant_created_at
  on public.inventory_movements (tenant_id, created_at desc)
  where tenant_id is not null;

create index if not exists idx_buyer_positions_tenant_purchased_at
  on public.buyer_positions (tenant_id, purchased_at desc)
  where tenant_id is not null;

create index if not exists idx_receipts_tenant_created_at
  on public.receipts (tenant_id, created_at desc)
  where tenant_id is not null;

-- Optional FK constraints (safe with NULL tenant_id)
do .\migrations\2025_12_27_02_users_unique_lower.sql
begin
  if not exists (select 1 from pg_constraint where conname='fk_contracts_tenant') then
    alter table public.contracts
      add constraint fk_contracts_tenant
      foreign key (tenant_id) references public.tenants(id) not valid;
  end if;

  if not exists (select 1 from pg_constraint where conname='fk_bols_tenant') then
    alter table public.bols
      add constraint fk_bols_tenant
      foreign key (tenant_id) references public.tenants(id) not valid;
  end if;

  if not exists (select 1 from pg_constraint where conname='fk_inventory_items_tenant') then
    alter table public.inventory_items
      add constraint fk_inventory_items_tenant
      foreign key (tenant_id) references public.tenants(id) not valid;
  end if;

  if not exists (select 1 from pg_constraint where conname='fk_inventory_movements_tenant') then
    alter table public.inventory_movements
      add constraint fk_inventory_movements_tenant
      foreign key (tenant_id) references public.tenants(id) not valid;
  end if;

  if not exists (select 1 from pg_constraint where conname='fk_buyer_positions_tenant') then
    alter table public.buyer_positions
      add constraint fk_buyer_positions_tenant
      foreign key (tenant_id) references public.tenants(id) not valid;
  end if;

  if not exists (select 1 from pg_constraint where conname='fk_receipts_tenant') then
    alter table public.receipts
      add constraint fk_receipts_tenant
      foreign key (tenant_id) references public.tenants(id) not valid;
  end if;
end .\migrations\2025_12_27_02_users_unique_lower.sql;