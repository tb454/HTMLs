# bridge_vendor_ingest.py
import csv, datetime as dt, os, sys, asyncio, re
from decimal import Decimal, InvalidOperation
from databases import Database

USAGE = "Usage:\n  python bridge_vendor_ingest.py <file.csv|.xlsx|.xls> [--vendor 'Name'] [--date YYYY-MM-DD]"

def _parse_args(argv):
    path, vendor, sheet_date = None, "C&Y Global, Inc. / Pro Metal Recycling", None
    i = 0
    while i < len(argv):
        a = argv[i]
        if a.startswith("--vendor"):
            if a == "--vendor" and i + 1 < len(argv):
                vendor = argv[i+1]; i += 2; continue
            if a.startswith("--vendor="):
                vendor = a.split("=",1)[1]; i += 1; continue
        elif a.startswith("--date"):
            if a == "--date" and i + 1 < len(argv):
                sheet_date = argv[i+1]; i += 2; continue
            if a.startswith("--date="):
                sheet_date = a.split("=",1)[1]; i += 1; continue
        elif a.startswith("-"):
            print(USAGE); sys.exit(1)
        else:
            path = a; i += 1; continue
    if not path: print(USAGE); sys.exit(1)
    # parse date if provided
    if sheet_date:
        try:
            sheet_date = dt.date.fromisoformat(sheet_date)
        except ValueError:
            print("ERROR: --date must be YYYY-MM-DD"); sys.exit(1)
    else:
        # try from filename like 10-02-2025, 10_2_2025, etc.
        m = re.search(r"(\d{1,2})[-_](\d{1,2})[-_](\d{4})", os.path.basename(path))
        if m:
            sheet_date = dt.date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        else:
            sheet_date = None
    return path, vendor, sheet_date

def _to_decimal(cell: str):
    """
    Extract numeric part from a cell like '$0.840', ' $ 6/EACH ', '1,234.56'.
    Returns Decimal or None.
    """
    s = str(cell)
    s = s.replace(",", "")
    # capture a leading number (int/float) even if followed by text
    m = re.search(r"([-+]?\d+(?:\.\d+)?)", s.replace("$",""))
    if not m: return None
    try:
        return Decimal(m.group(1))
    except InvalidOperation:
        return None

def _guess_unit(text: str) -> str:
    t = (text or "").upper()
    if "EACH" in t: return "EACH"
    if "TON" in t or "/TON" in t or " PER TON" in t: return "TON"
    if "LB" in t or "LBS" in t or "POUND" in t: return "LBS"
    return "LBS"  # default

def _csv_rows(path):
    # robust reader (handles weird BOMs and stray delimiters)
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.excel
        reader = csv.reader(f, dialect)
        for row in reader:
            yield [("" if c is None else str(c)).strip() for c in row]

def _xlsx_rows(path):
    import pandas as pd
    df = pd.read_excel(path, header=None)
    for _, r in df.iterrows():
        # Return as list of strings, keep blanks as ""
        yield [("" if (pd.isna(x) or x is None) else str(x)).strip() for x in r.tolist()]

def _categorize(name_upper: str) -> str:
    u = name_upper
    if "SCRAP" in u: return "E-Scrap"
    if u.startswith("AL ") or " AL " in f" {u} ": return "Aluminum"
    if u.startswith("CU") or " COPPER" in u: return "Copper"
    if "BRASS" in u: return "Brass"
    if "SS " in u or "STAINLESS" in u: return "Stainless"
    if "LEAD" in u: return "Lead"
    return "Other"

async def _insert_rows(db: Database, rows, vendor: str, sheet_date, source_file: str):
    inserted = 0
    for r in rows:
        await db.execute("""
            INSERT INTO vendor_quotes(
              vendor, category, material, price_per_lb, unit_raw, sheet_date, source_file
            )
            VALUES (:vendor, :cat, :mat, :price_lb, :unit_raw, :d, :src)
        """, {
            "vendor": vendor,
            "cat": r["category"],
            "mat": r["material"],
            "price_lb": r["price_per_lb"],
            "unit_raw": r["unit_raw"],
            "d": sheet_date,
            "src": source_file,
        })
        inserted += 1
    return inserted

async def ingest_file(path: str, vendor: str, sheet_date):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL is not set."); sys.exit(1)
    db = Database(db_url)
    await db.connect()
    print(f"Connected to DB")

    ext = os.path.splitext(path)[1].lower()
    source_file = os.path.basename(path)

    # dedupe by source_file (fast path)
    exists = await db.fetch_one("SELECT 1 FROM vendor_quotes WHERE source_file=:s LIMIT 1", {"s": source_file})
    if exists:
        print(f"SKIP (already ingested): {source_file}")
        await db.disconnect()
        return

    # choose reader
    if ext == ".csv":
        row_iter = _csv_rows(path)
    elif ext in (".xlsx", ".xls"):
        row_iter = _xlsx_rows(path)
    else:
        print(f"ERROR: Unsupported extension: {ext}")
        await db.disconnect()
        sys.exit(1)

    parsed = []
    seen = 0
    for row in row_iter:
        if not row or not (row[0] or "").strip():
            continue
        name = (row[0] or "").strip()
        upper = name.upper()

        # skip obvious headers/junk
        if any(h in upper for h in [
            "NON-FERROUS","ALUMINUM PRODUCT","RADIATOR PRODUCT","COPPER PRODUCT",
            "BRASS PRODUCT","MISC PRDOUCT","LEAD PRODUCT","STAINLESS STEEL PRODUCT",
            "FERROUS METALS","E SCRAPS","PRICE","CATEGORY","MATERIAL"
        ]):
            continue

        seen += 1

        # prefer first $ cell, else any numeric cell
        raw_cell = None
        for c in row:
            if "$" in c:
                raw_cell = c; break
        if raw_cell is None:
            for c in row[1:]:
                if re.search(r"\d", c):
                    raw_cell = c; break
        if raw_cell is None:
            continue

        unit_raw = _guess_unit(raw_cell)
        val = _to_decimal(raw_cell)
        if val is None:
            continue

        # normalize to $/lb for storage (pricing engine filters by unit_raw anyway)
        price_per_lb = val
        if unit_raw == "TON":
            price_per_lb = (val / Decimal(2000)).quantize(Decimal("0.0001"))
        else:
            price_per_lb = val.quantize(Decimal("0.0001"))

        parsed.append({
            "material": name,
            "category": _categorize(upper),
            "unit_raw": unit_raw,
            "price_per_lb": price_per_lb,
        })

    if not parsed:
        print(f"NO ROWS parsed from {source_file}")
        await db.disconnect()
        return

    try:
        async with db.transaction():
            # secondary dedupe: avoid re-inserting same vendor/material/date/source
            # (no unique index needed; we just check quickly)
            keep = []
            for r in parsed:
                hit = await db.fetch_one("""
                    SELECT 1 FROM vendor_quotes
                    WHERE vendor=:v AND material=:m AND sheet_date IS NOT DISTINCT FROM :d
                      AND source_file=:s AND unit_raw=:u
                    LIMIT 1
                """, {"v": vendor, "m": r["material"], "d": sheet_date, "s": source_file, "u": r["unit_raw"]})
                if not hit:
                    keep.append(r)
            if not keep:
                print(f"Done. Rows scanned: {seen}, rows inserted: 0 (all duplicates)")
            else:
                n = await _insert_rows(db, keep, vendor, sheet_date, source_file)
                print(f"Done. Rows scanned: {seen}, rows inserted: {n}")
    finally:
        await db.disconnect()
        print("Disconnected from DB")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(USAGE); sys.exit(1)
    path, vendor, sheet_date = _parse_args(sys.argv[1:])
    if not os.path.exists(path):
        print(f"ERROR: file not found: {path}"); sys.exit(1)
    asyncio.run(ingest_file(path, vendor, sheet_date))
