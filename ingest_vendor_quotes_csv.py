import os
import csv
import glob
import datetime as dt
from decimal import Decimal, InvalidOperation

import psycopg2


# ---- CONFIG ----
CSV_PATH = r"C:\Users\tbyer\BRidge-html\HTMLs\Vendor Quotes\prices for vendor quotes(Sheet1).csv"

# If you want it to auto-pick the newest matching file in that folder instead:
# BASE_DIR = r"C:\Users\tbyer\BRidge-html\HTMLs\Vendor Quotes"
# candidates = glob.glob(os.path.join(BASE_DIR, "*prices for vendor quotes*.csv"))
# if not candidates:
#     raise RuntimeError(f"No matching vendor quotes CSV found in {BASE_DIR}")
# CSV_PATH = max(candidates, key=os.path.getmtime)

DEFAULT_CATEGORY = "vendor_sheet"
DEFAULT_UNIT_RAW = None  # will use CSV uom if present, else None


def to_price_per_lb(price, uom: str) -> float:
    """Convert price to $/lb if needed."""
    u = (uom or "").strip().upper()

    # already per pound
    if u in ("LB", "LBS", "$/LB", "USD/LB"):
        return float(price)

    # short ton / net ton (assume 2000 lb)
    if u in ("NT", "TON", "T", "ST", "SHORT TON", "$/NT", "USD/NT", "$/TON", "USD/TON"):
        return float(price) / 2000.0

    # gross ton (2240 lb)
    if u in ("GT", "GROSS TON", "$/GT", "USD/GT"):
        return float(price) / 2240.0

    # If uom is blank, assume $/lb ONLY if the price looks like a small decimal (e.g. 0.25)
    if u == "":
        p = float(price)
        if p < 10:
            return p
        # otherwise likely per ton
        return p / 2000.0

    raise ValueError(f"Unknown uom='{uom}' for price={price}")


def parse_sheet_date(row: dict) -> dt.date:
    """
    Accepts any of:
      - sheet_date: YYYY-MM-DD
      - date: YYYY-MM-DD
      - month: YYYY-MM   (interprets as first day of month)
    """
    for key in ("sheet_date", "date"):
        v = (row.get(key) or "").strip()
        if v:
            return dt.date.fromisoformat(v)

    m = (row.get("month") or "").strip()
    if m:
        y, mm = m.split("-")
        return dt.date(int(y), int(mm), 1)

    # fallback to today (not ideal, but prevents hard failure)
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

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(CSV_PATH)

    print("Using:", CSV_PATH)

    # Read CSV
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError("CSV has no header row")

        # Expected columns (flexible):
        # vendor, material, price, uom, sheet_date/date/month, category, delivery, location, notes
        rows_in = list(reader)

    if not rows_in:
        raise RuntimeError("No rows found in CSV")

    # Normalize rows to insert
    inserts = []
    vendors_seen = set()
    dates_seen = set()

    for r in rows_in:
        vendor = clean_str(r.get("vendor"))
        material = clean_str(r.get("material") or r.get("material_vendor") or r.get("sku") or r.get("grade"))
        if not vendor or not material:
            continue

        price_raw = parse_price(r.get("price") or r.get("price_per_lb") or r.get("value") or r.get("rate"))
        uom = clean_str(r.get("uom") or r.get("unit") or DEFAULT_UNIT_RAW)
        sheet_date = parse_sheet_date(r)
        category = clean_str(r.get("category")) or DEFAULT_CATEGORY
        source_file = os.path.basename(CSV_PATH)

        price_per_lb = to_price_per_lb(price_raw, uom)

        vendors_seen.add(vendor)
        dates_seen.add(sheet_date)

        inserts.append((vendor, category, material, price_per_lb, uom or "", sheet_date, source_file))

    print(f"Parsed rows: {len(inserts)} | Vendors: {len(vendors_seen)} | Dates: {len(dates_seen)}")

    if not inserts:
        raise RuntimeError("No valid rows parsed (check vendor/material/price columns).")

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

    # Insert vendor quotes (append-only by default).
    # If you want to replace existing rows for same vendor+date+source_file, uncomment the delete block below.
    #
    # cur.execute(
    #     """
    #     DELETE FROM public.vendor_quotes
    #     WHERE source_file = %s
    #     """,
    #     (os.path.basename(CSV_PATH),),
    # )

    cur.executemany(
        """
        INSERT INTO public.vendor_quotes
          (vendor, category, material, price_per_lb, unit_raw, sheet_date, source_file)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s)
        """,
        inserts,
    )

    conn.commit()

    # Ensure vendor_material_map rows exist so mapping/canonicalization can be applied later
    # NOTE: requires your unique constraint on (vendor, material_vendor)
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
