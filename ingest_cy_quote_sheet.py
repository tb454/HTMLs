import os
import glob
import datetime as dt
import psycopg2
import openpyxl

# Auto-pick newest C&Y quote sheet from archive
ARCHIVE_DIR = r"C:\Users\tbyer\BRidge-html\HTMLs\Vendor Quotes\archive"
candidates = glob.glob(os.path.join(ARCHIVE_DIR, "*Pro Metal Quotation Chicago*.xlsx"))
if not candidates:
    raise RuntimeError(f"No matching C&Y quote sheet found in {ARCHIVE_DIR}")
EXCEL_PATH = max(candidates, key=os.path.getmtime)
print("Using:", EXCEL_PATH)

VENDOR_CANON = "C&Y Global, Inc. / Pro Metal Recycling"
SHEET_NAME = "ATL PRICE SHEET"
UNIT_RAW = "LBS"

def norm(s):
    return (s or "").strip()

def is_number(x):
    return isinstance(x, (int, float)) and x == x  # NaN guard

def main():
    db = os.environ["DATABASE_URL"]
    wb = openpyxl.load_workbook(EXCEL_PATH, data_only=True)
    ws = wb[SHEET_NAME]

    rows = list(ws.iter_rows(values_only=True))

    # Find sheet_date from the "DATE:" row in the right table (col E/G)
    sheet_date = None
    for r in rows:
        if isinstance(r[4], str) and r[4].strip().upper() == "DATE:":
            v = r[6]
            if isinstance(v, dt.datetime):
                sheet_date = v.date()
            elif isinstance(v, dt.date):
                sheet_date = v
            break
    if not sheet_date:
        raise RuntimeError("Could not find sheet DATE: in the Excel.")

    def parse_triplet(code_idx, desc_idx, price_idx):
        out = []
        section = None
        for r in rows:
            code = r[code_idx]
            desc = r[desc_idx]
            price = r[price_idx]

            if isinstance(code, str) and "PRODUCT" in code.upper():
                section = code.strip()
                continue

            if section and is_number(price):
                code_s = norm(code) or ""
                desc_s = norm(desc) or ""
                # Create a line-item material string (this is the key fix)
                material = f"{code_s} {desc_s}".strip() if code_s else desc_s
                if not material:
                    continue
                out.append((section, material, float(price)))
        return out

    left = parse_triplet(0, 1, 2)    # A/B/C
    right = parse_triplet(4, 5, 6)   # E/F/G

    # Filter out the COMEX row etc. (no section)
    items = [(sec, mat, px) for (sec, mat, px) in (left + right) if sec]
    print(f"Parsed {len(items)} line-items for {sheet_date}.")

    conn = psycopg2.connect(db)
    cur = conn.cursor()

    # Ensure vendor exists in orgs (FK)
    cur.execute("""
        INSERT INTO public.orgs (org, display_name)
        VALUES (%s, %s)
        ON CONFLICT (org) DO NOTHING
    """, (VENDOR_CANON, VENDOR_CANON))

    # Delete any prior ingest for this vendor+date (so reruns are clean)
    cur.execute("""
        DELETE FROM public.vendor_quotes
        WHERE vendor = %s AND sheet_date = %s
    """, (VENDOR_CANON, sheet_date))

    # Insert line-items
    cur.executemany("""
        INSERT INTO public.vendor_quotes
          (vendor, category, material, price_per_lb, unit_raw, sheet_date, source_file)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s)
    """, [
        (VENDOR_CANON, sec, mat, px, UNIT_RAW, sheet_date, os.path.basename(EXCEL_PATH))
        for (sec, mat, px) in items
    ])

    conn.commit()
    cur.close()
    conn.close()
    print("Ingest complete.")

if __name__ == "__main__":
    main()
