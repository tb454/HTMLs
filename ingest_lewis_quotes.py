import os
import glob
import datetime as dt
import csv
import psycopg2

FAILED_DIR = r"C:\Users\tbyer\BRidge-html\HTMLs\Vendor Quotes\_failed"
candidates = glob.glob(os.path.join(FAILED_DIR, "*Lewis Salvage Vendor Quotes*.csv"))
if not candidates:
    raise RuntimeError(f"No matching Lewis CSV found in {FAILED_DIR}")

CSV_PATH = max(candidates, key=os.path.getmtime)
print("Using:", CSV_PATH)

VENDOR_CANON = "Lewis Salvage"

def to_price_per_lb(price: float, uom: str) -> float:
    u = (uom or "").strip().upper()
    if u in ("LB", "LBS"):
        return float(price)
    # Assume NT = 2000 lb (US “net ton”)
    if u in ("NT", "TON", "T", "ST", "SHORT TON"):
        return float(price) / 2000.0
    # If you ever see GT (2240 lb), add it here
    if u in ("GT", "GROSS TON"):
        return float(price) / 2240.0
    raise ValueError(f"Unknown uom: {uom}")

def month_to_date(month_str: str) -> dt.date:
    # month like "2025-10" -> use first day of that month
    y, m = month_str.split("-")
    return dt.date(int(y), int(m), 1)

def main():
    db = os.environ["DATABASE_URL"]
    rows = []

    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            vendor = (r.get("vendor") or "").strip()
            material = (r.get("material") or "").strip()
            price = float(r.get("price"))
            uom = (r.get("uom") or "").strip()
            month = (r.get("month") or "").strip()
            delivery = (r.get("delivery") or "").strip()

            sheet_date = month_to_date(month)
            price_per_lb = to_price_per_lb(price, uom)

            # category: simple heuristic (you can change later)
            mat_lower = material.lower()
            if any(k in mat_lower for k in ["shred", "sheet", "clip", "bushel", "hms", "p&s", "rebar", "turning"]):
                category = "ferrous"
            else:
                category = "nonferrous"

            rows.append((VENDOR_CANON, category, material, price_per_lb, uom, sheet_date, os.path.basename(CSV_PATH), delivery))

    print(f"Parsed {len(rows)} rows.")

    conn = psycopg2.connect(db)
    cur = conn.cursor()

    # Ensure org exists (FK)
    cur.execute("""
        INSERT INTO public.orgs (org, display_name)
        VALUES (%s, %s)
        ON CONFLICT (org) DO NOTHING
    """, (VENDOR_CANON, VENDOR_CANON))

    # Replace existing Lewis rows for that month(s)
    months = sorted(set(r[5] for r in rows))
    cur.execute("""
        DELETE FROM public.vendor_quotes
        WHERE vendor = %s AND sheet_date = ANY(%s)
    """, (VENDOR_CANON, months))

    # Insert
    cur.executemany("""
        INSERT INTO public.vendor_quotes
          (vendor, category, material, price_per_lb, unit_raw, sheet_date, source_file)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s)
    """, [(v, cat, mat, pplb, uom, sdate, src) for (v, cat, mat, pplb, uom, sdate, src, _delivery) in rows])

    conn.commit()
    cur.close()
    conn.close()
    print("Lewis ingest complete.")

if __name__ == "__main__":
    main()
