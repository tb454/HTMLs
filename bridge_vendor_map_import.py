# bridge_vendor_map_import.py
import os
import sys
import asyncio
import csv
from databases import Database

DDL = """
CREATE TABLE IF NOT EXISTS vendor_material_map (
  id BIGSERIAL PRIMARY KEY,
  vendor TEXT NOT NULL,
  material_vendor TEXT NOT NULL,
  material_canonical TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_vmm_vendor_vendor_mat
  ON vendor_material_map(vendor, material_vendor);
"""

async def _ensure_schema(db: Database):
    # split multi-statement DDL for asyncpg
    for stmt in DDL.split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        if not sql.endswith(";"):
            sql += ";"
        await db.execute(sql)

async def import_map(db: Database, path: str) -> int:
    if not os.path.exists(path):
        print(f"ERROR: mapping CSV not found: {path}")
        return 0

    print(f"Mapping import: {os.path.basename(path)}")
    count = 0

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        async with db.transaction():
            for row in rdr:
                vendor   = (row.get("vendor") or "").strip()
                mat_v    = (row.get("material_vendor") or "").strip()
                mat_c    = (row.get("material_canonical") or "").strip()
                if not (vendor and mat_v and mat_c):
                    continue
                await db.execute(
                    """
                    INSERT INTO vendor_material_map(vendor, material_vendor, material_canonical)
                    VALUES (:v, :mv, :mc)
                    ON CONFLICT (vendor, material_vendor) DO UPDATE
                    SET material_canonical = EXCLUDED.material_canonical
                    """,
                    {"v": vendor, "mv": mat_v, "mc": mat_c},
                )
                count += 1

    print(f"Mapping: upserted {count} rows into vendor_material_map")
    return count

async def main():
    db_url = os.getenv("DATABASE_URL") or os.getenv("ASYNC_DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL/ASYNC_DATABASE_URL is not set.")
        sys.exit(1)

    # default path: ./vendor_material_map.csv in your HTMLs dir
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), "vendor_material_map.csv")

    db = Database(db_url)
    await db.connect()
    print(f"Connected to {db_url}")
    await _ensure_schema(db)

    try:
        await import_map(db, path)
    finally:
        await db.disconnect()
        print("Disconnected.")

if __name__ == "__main__":
    asyncio.run(main())
