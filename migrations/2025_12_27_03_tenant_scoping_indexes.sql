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
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_contracts_tenant') THEN
    ALTER TABLE public.contracts
      ADD CONSTRAINT fk_contracts_tenant
      FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) NOT VALID;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_bols_tenant') THEN
    ALTER TABLE public.bols
      ADD CONSTRAINT fk_bols_tenant
      FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) NOT VALID;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_inventory_items_tenant') THEN
    ALTER TABLE public.inventory_items
      ADD CONSTRAINT fk_inventory_items_tenant
      FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) NOT VALID;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_inventory_movements_tenant') THEN
    ALTER TABLE public.inventory_movements
      ADD CONSTRAINT fk_inventory_movements_tenant
      FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) NOT VALID;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_buyer_positions_tenant') THEN
    ALTER TABLE public.buyer_positions
      ADD CONSTRAINT fk_buyer_positions_tenant
      FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) NOT VALID;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_receipts_tenant') THEN
    ALTER TABLE public.receipts
      ADD CONSTRAINT fk_receipts_tenant
      FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) NOT VALID;
  END IF;
END
$$;
