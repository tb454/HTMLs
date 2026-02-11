# bridge_seed_sandbox_data.py
import os
import random
import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

# Import your already-deployed app pieces
from bridge_buyer_backend import database, utcnow, _ensure_org_exists

# ----------------- CONFIG -----------------
DAYS_BACK           = int(os.getenv("SBX_DAYS_BACK", "120"))   # how many days of history
CONTRACTS_PER_DAY   = int(os.getenv("SBX_CONTRACTS_PER_DAY", "25"))
PCT_WITH_BOL        = float(os.getenv("SBX_PCT_WITH_BOL", "0.70"))  # 70% of contracts get a BOL
PCT_VENDOR_QUOTES   = float(os.getenv("SBX_PCT_VENDOR_QUOTES", "0.30"))
SEED_INDICES        = os.getenv("SBX_SEED_INDICES", "1") in ("1","true","yes")
REGION              = os.getenv("SBX_REGION", "blended")
CURRENCY            = "USD"

# Keep it obviously sandbox-only
if (os.getenv("ENV","").lower() == "production"):
    raise SystemExit("Refusing to seed: ENV=production")

# Sellers & buyers as plain strings (matches your columns)
SELLERS = [
    "Winski Brothers", "Lewis Salvage", "C&Y Global", "Newco", "Nucor",
    "TriState Metals", "Farnsworth Metals", "Crawford Iron", "Atlas Yard A", "Atlas Yard B"
]
BUYERS = [
    "Mill East", "Mill West", "Foundry Alpha", "Foundry Beta", "Processor One",
    "Processor Two", "OEM Midwest", "OEM Southwest", "Broker House", "Rolling Works"
]
MATERIALS = [
    "Shred Steel", "HMS #1", "P&S 5ft", "Al 6061", "Al 6063",
    "Bare Bright", "#1 Copper", "#2 Copper", "Insulated Wire", "SS 304"
]

def _rand_price_ton(material: str) -> float:
    # crude material bases (USD/ton)
    base = {
        "Shred Steel": 240, "HMS #1": 260, "P&S 5ft": 280,
        "Al 6061": 1900, "Al 6063": 1800,
        "Bare Bright": 8500, "#1 Copper": 8200, "#2 Copper": 7600,
        "Insulated Wire": 2200, "SS 304": 1250
    }.get(material, 500)
    return round(base * random.uniform(0.95, 1.05), 2)

def _rand_tons(material: str) -> float:
    return round(random.uniform(18, 45), 2) if "Steel" in material or "HMS" in material or "P&S" in material else round(random.uniform(8, 24), 2)

def _choice(seq): return seq[random.randint(0, len(seq)-1)]
def _now_utc(): return datetime.now(timezone.utc)

async def seed_orgs():
    # Ensure orgs exist for sellers & buyers (idempotent)
    vals = []
    seen = set()
    for name in SELLERS + BUYERS:
        key = name.strip()
        if key in seen: continue
        seen.add(key)
        slug = key.lower().replace(" ", "-")
        await _ensure_org_exists(key)  # uses your helper (INSERT INTO orgs(org, display_name) ON CONFLICT DO NOTHING)

async def seed_contracts_and_bols():
    """
    Writes to:
      contracts(id, buyer, seller, material, weight_tons, price_per_ton, status, currency, created_at)
      bols(bol_id, contract_id, buyer, seller, material, weight_tons, price_per_unit, total_value, pickup_time, status)
    """
    print("Seeding contracts + bols ...")
    end = _now_utc().date()
    start = end - timedelta(days=DAYS_BACK)

    total_contracts = 0
    day = start
    while day <= end:
        rows_contracts = []
        rows_bols = []

        for _ in range(CONTRACTS_PER_DAY):
            seller   = _choice(SELLERS)
            buyer    = _choice(BUYERS)
            material = _choice(MATERIALS)
            tons     = _rand_tons(material)
            ppt      = _rand_price_ton(material)
            status   = random.choices(["Open","Signed","Dispatched","Fulfilled"], weights=[1,3,3,2], k=1)[0]

            cid = str(uuid4())
            created_at = datetime(day.year, day.month, day.day, random.randint(0,23), random.randint(0,59), tzinfo=timezone.utc)

            rows_contracts.append({
                "id": cid,
                "buyer": buyer,
                "seller": seller,
                "material": material,
                "weight_tons": tons,
                "price_per_ton": ppt,
                "status": status,
                "currency": CURRENCY,
                "created_at": created_at
            })

            if random.random() < PCT_WITH_BOL:
                bol_id = str(uuid4())
                pickup_time = created_at + timedelta(hours=random.randint(2, 48))
                bol_status = random.choices(["Scheduled","In Transit","Delivered"], weights=[2,2,5], k=1)[0]
                rows_bols.append({
                    "bol_id": bol_id,
                    "contract_id": cid,
                    "buyer": buyer,
                    "seller": seller,
                    "material": material,
                    "weight_tons": tons,
                    "price_per_unit": ppt,
                    "total_value": round(ppt * tons, 2),
                    "pickup_time": pickup_time,
                    "status": bol_status
                })

        if rows_contracts:
            await database.execute_many("""
                INSERT INTO contracts
                  (id,buyer,seller,material,weight_tons,price_per_ton,status,currency,created_at)
                VALUES
                  (:id,:buyer,:seller,:material,:weight_tons,:price_per_ton,:status,:currency,:created_at)
                ON CONFLICT (id) DO NOTHING
            """, rows_contracts)

        if rows_bols:
            await database.execute_many("""
                INSERT INTO bols
                  (bol_id,contract_id,buyer,seller,material,weight_tons,price_per_unit,total_value,pickup_time,status)
                VALUES
                  (:bol_id,:contract_id,:buyer,:seller,:material,:weight_tons,:price_per_unit,:total_value,:pickup_time,:status)
                ON CONFLICT (bol_id) DO NOTHING
            """, rows_bols)

        total_contracts += len(rows_contracts)
        if total_contracts and total_contracts % 1000 == 0:
            print(f"  … {total_contracts} contracts seeded")

        day += timedelta(days=1)

    print(f"Contracts seeded: {total_contracts}")

async def seed_vendor_quotes_and_indices():
    """
    Writes to:
      vendor_quotes(vendor, category, material, price_per_lb, unit_raw, sheet_date, source_file, inserted_at)
      indices_daily(as_of_date, region, material, avg_price, volume_tons, currency)
    Then you can call:
      POST /reference_prices/upsert_from_vendor
      POST /indices/snapshot_blended
    """
    print("Seeding vendor quotes + indices snapshots …")
    nowd = _now_utc().date()

    # Vendor quotes (light sampling)
    vrows = []
    vendors = ["C&Y Global, Inc. / Pro Metal Recycling", "Lewis Salvage", "Metro Scrap", "Atlas Vendor A", "Atlas Vendor B"]
    for mat in MATERIALS:
        if random.random() > PCT_VENDOR_QUOTES:
            continue
        for v in vendors:
            # Convert ton price → lb for vendor feed
            ton = _rand_price_ton(mat)
            per_lb = round(ton / 2000.0, 4)
            vrows.append({
                "vendor": v,
                "category": "Auto" if "Steel" in mat or "HMS" in mat or "P&S" in mat else "Nonferrous",
                "material": mat,
                "price_per_lb": per_lb,
                "unit_raw": "LB",
                "sheet_date": nowd,
                "source_file": "seed",
                "inserted_at": _now_utc()
            })
    if vrows:
        await database.execute_many("""
            INSERT INTO vendor_quotes
              (vendor,category,material,price_per_lb,unit_raw,sheet_date,source_file,inserted_at)
            VALUES
              (:vendor,:category,:material,:price_per_lb,:unit_raw,:sheet_date,:source_file,:inserted_at)
        """, vrows)

    if SEED_INDICES:
        # Simple daily snapshot backfill (avg_price) for a couple of materials
        id_rows = []
        for d in range(DAYS_BACK):
            asof = (nowd - timedelta(days=d))
            for mat in ("Shred Steel", "Bare Bright", "#1 Copper"):
                id_rows.append({
                    "as_of_date": asof,
                    "region": REGION,
                    "material": mat,
                    "avg_price": _rand_price_ton(mat),
                    "volume_tons": random.randint(0, 200),
                    "currency": CURRENCY
                })
        # store as $/ton (avg_price)
        await database.execute_many("""
            INSERT INTO indices_daily
              (as_of_date, region, material, avg_price, volume_tons, currency)
            VALUES
              (:as_of_date,:region,:material,:avg_price,:volume_tons,:currency)
            ON CONFLICT (as_of_date, region, material) DO UPDATE
              SET avg_price=EXCLUDED.avg_price, volume_tons=EXCLUDED.volume_tons, currency=EXCLUDED.currency
        """, id_rows)

async def main():
    print("== BRidge Sandbox Seeder ==")
    if not database.is_connected:
        await database.connect()

    print("Seeding orgs …")
    await seed_orgs()

    print("Seeding contracts & bols …")
    await seed_contracts_and_bols()

    print("Seeding vendor quotes & indices …")
    await seed_vendor_quotes_and_indices()

    print("✅ Sandbox seed complete.")
    await database.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
