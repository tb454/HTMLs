import os
import glob
import csv
import datetime as dt
from decimal import Decimal, InvalidOperation

import psycopg2


# ---- CONFIG: auto-pick newest multi-vendor CSV from _failed ----
FAILED_DIR = r"C:\Users\tbyer\BRidge-html\HTMLs\Vendor Quotes\_failed"
candidates = glob.glob(os.path.join(FAILED_DIR, "*prices for vendor quotes*Sheet1*.csv"))
if not candidates:
    # fallback: any "prices for vendor quotes" csv
    candidates = glob.glob(os.path.join(FAILED_DIR, "*prices for vendor quotes*.csv"))
if not candidates:
    raise RuntimeError(f"No matching 'prices for vendor quotes' CSV found in {FAILED_DIR}")

CSV_PATH = max(candidates, key=os.path.getmtime)
print("Using:", CSV_PATH)

DEFAULT_CATEGORY = "vendor_sheet"


def to_price_per_lb(price, uom: str) -> float:
    u = (uom or "").strip().upper()

    if u in ("LB", "LBS", "$/LB", "USD/LB"):
        return float(price)

    # short ton / net ton (assume 2000 lb)
    if u in ("NT", "TON", "T", "ST", "SHORT TON", "$/NT", "USD/NT", "$/TON", "USD/TON"):
        return float(price) / 2000.0

    # gross ton (2240 lb)
    if u in ("GT", "GROSS TON", "$/GT", "USD/GT"):
        return float(price) / 2240.0

    # guess if blank
    if u == "":
        p = float(price)
        return p if p < 10 else (p / 2000.0)

    raise ValueError(f"Unknown uom='{uom}' for price={price}")


def parse_sheet_date(row: dict) -> dt.date:
    for key in ("sheet_date", "date"):
        v = (row.get(key) or "").strip()
        if v:
            return dt.date.fromisoformat(v)

    m = (row.get("month") or "").strip()
    if m:
        y, mm = m.split("-")
        return dt.date(int(y), int(mm), 1)

    # fallback: today (only if your CSV truly has no date/month)
    return dt.date.today()


def clean_str(x) -> str:
    return (x or "").strip()


def parse_price(x) -> float:
    s = clean_str(x).replace(",", "")
    if s == "":
        raise ValueError("Empty price")
    try:
        return float(Decimal(s))
    except (InvalidOperation, ValueError):
        raise ValueError(f"Bad price value: {x}")


def main():
    db = os.environ["DATABASE_URL"]

    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError("CSV has no header row")
        rows_in = list(reader)

    if not rows_in:
        raise RuntimeError("No rows found in CSV")

    inserts = []
    vendors_seen = set()
    dates_seen = set()

    for r in rows_in:
        vendor = clean_str(r.get("vendor") or r.get("Vendor") or r.get("VENDOR"))
        material = clean_str(
            r.get("material")
            or r.get("material_vendor")
            or r.get("Material")
            or r.get("sku")
            or r.get("grade")
        )

        if not vendor or not material:
            continue

        price_raw = parse_price(
            r.get("price")
            or r.get("Price")
            or r.get("price_per_lb")
            or r.get("value")
            or r.get("rate")
        )
        uom = clean_str(r.get("uom") or r.get("UOM") or r.get("unit") or r.get("Unit") or "")
        sheet_date = parse_sheet_date(r)
        category = clean_str(r.get("category") or r.get("Category")) or DEFAULT_CATEGORY

        price_per_lb = to_price_per_lb(price_raw, uom)
        source_file = os.path.basename(CSV_PATH)

        vendors_seen.add(vendor)
        dates_seen.add(sheet_date)
        inserts.append((vendor, category, material, price_per_lb, uom, sheet_date, source_file))

    print(f"Parsed rows: {len(inserts)} | Vendors: {len(vendors_seen)} | Dates: {len(dates_seen)}")

    if not inserts:
        raise RuntimeError("No valid rows parsed. Check column names: vendor/material/price/uom/date.")

    conn = psycopg2.connect(db)
    cur = conn.cursor()

    # Ensure orgs exist (FK requirement on vendor_quotes.vendor)
    for v in sorted(vendors_seen):
        cur.execute(
            """
            INSERT INTO public.orgs (org, display_name)
            VALUES (%s, %s)
            ON CONFLICT (org) DO NOTHING
            """,
            (v, v),
        )

    # Optional: remove prior rows from this same source file (keeps reruns clean)
    cur.execute(
        """
        DELETE FROM public.vendor_quotes
        WHERE source_file = %s
        """,
        (os.path.basename(CSV_PATH),),
    )

    cur.executemany(
        """
        INSERT INTO public.vendor_quotes
          (vendor, category, material, price_per_lb, unit_raw, sheet_date, source_file)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s)
        """,
        inserts,
    )

    # Ensure vendor_material_map rows exist for later canonical overrides
    cur.execute(
        """
        INSERT INTO public.vendor_material_map (vendor, material_vendor, material_canonical)
        SELECT DISTINCT q.vendor, q.material, q.material
        FROM public.vendor_quotes q
        WHERE q.source_file = %s
        ON CONFLICT (vendor, material_vendor) DO NOTHING
        """,
        (os.path.basename(CSV_PATH),),
    )

    conn.commit()
    cur.close()
    conn.close()
    print("Ingest complete.")


if __name__ == "__main__":
    main()
