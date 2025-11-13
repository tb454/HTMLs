# bridge_vendor_ingest_all.py
import os, sys, asyncio, re, datetime as dt, csv
from decimal import Decimal, InvalidOperation
from databases import Database

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VENDOR_DIR = os.path.join(BASE_DIR, "Vendor Quotes")

DEFAULT_VENDOR = "C&Y Global, Inc. / Pro Metal Recycling"

def _parse_date_from_filename(fname: str):
    m = re.search(r"(\d{1,2})[-_](\d{1,2})[-_](\d{4})", fname)
    if not m: return None
    mm, dd, yyyy = map(int, m.groups())
    try: return dt.date(yyyy, mm, dd)
    except ValueError: return None

def _to_decimal(cell: str):
    s = str(cell).replace(",", "")
    m = re.search(r"([-+]?\d+(?:\.\d+)?)", s.replace("$",""))
    if not m: return None
    try: return Decimal(m.group(1))
    except InvalidOperation: return None

def _guess_unit(text: str) -> str:
    t = (text or "").upper()
    if "EACH" in t: return "EACH"
    if "TON" in t or "/TON" in t or " PER TON" in t: return "TON"
    if "LB" in t or "LBS" in t or "POUND" in t: return "LBS"
    return "LBS"

def _categorize(u: str) -> str:
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
            ) VALUES (:v, :c, :m, :p, :u, :d, :s)
        """, {"v": vendor, "c": r["category"], "m": r["material"],
              "p": r["price_per_lb"], "u": r["unit_raw"], "d": sheet_date, "s": source_file})
        inserted += 1
    return inserted

async def ingest_one_file(db: Database, path: str, vendor: str):
    fname = os.path.basename(path)
    # fast dedupe by filename
    hit = await db.fetch_one("SELECT 1 FROM vendor_quotes WHERE source_file=:s LIMIT 1", {"s": fname})
    if hit:
        print(f"SKIP (already ingested): {fname}")
        return 0

    sheet_date = _parse_date_from_filename(fname)
    ext = os.path.splitext(fname)[1].lower()

    rows = []
    if ext == ".csv":
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096); f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample)
            except Exception:
                dialect = csv.excel
            reader = csv.reader(f, dialect)
            for row in reader:
                if not row or not (row[0] or "").strip(): continue
                name = (row[0] or "").strip()
                u = name.upper()
                if any(h in u for h in [
                    "NON-FERROUS","ALUMINUM PRODUCT","RADIATOR PRODUCT","COPPER PRODUCT",
                    "BRASS PRODUCT","MISC PRDOUCT","LEAD PRODUCT","STAINLESS STEEL PRODUCT",
                    "FERROUS METALS","E SCRAPS","PRICE","CATEGORY","MATERIAL"
                ]): continue
                # prefer first $ cell, else any numeric cell
                raw = None
                for c in row:
                    if "$" in c: raw = c; break
                if raw is None:
                    for c in row[1:]:
                        if re.search(r"\d", c): raw = c; break
                if raw is None: continue
                val = _to_decimal(raw)
                if val is None: continue
                unit_raw = _guess_unit(raw)
                if unit_raw == "TON":
                    price_lb = (val / Decimal(2000)).quantize(Decimal("0.0001"))
                else:
                    price_lb = val.quantize(Decimal("0.0001"))
                rows.append({"material": name, "category": _categorize(u), "unit_raw": unit_raw, "price_per_lb": price_lb})
    elif ext in (".xlsx", ".xls"):
        import pandas as pd
        df = pd.read_excel(path, header=None)
        for _, r in df.iterrows():
            first = "" if pd.isna(r.iloc[0]) else str(r.iloc[0]).strip()
            if not first or first.upper() in {"NAN","NONE"}: continue
            name = first; u = name.upper()
            if any(h in u for h in [
                "NON-FERROUS","ALUMINUM PRODUCT","RADIATOR PRODUCT","COPPER PRODUCT",
                "BRASS PRODUCT","MISC PRDOUCT","LEAD PRODUCT","STAINLESS STEEL PRODUCT",
                "FERROUS METALS","E SCRAPS","PRICE","CATEGORY","MATERIAL"
            ]): continue
            raw = None
            for c in r.tolist():
                s = "" if (pd.isna(c) or c is None) else str(c)
                if "$" in s: raw = s; break
            if raw is None:
                for c in r.tolist()[1:]:
                    s = "" if (pd.isna(c) or c is None) else str(c)
                    if re.search(r"\d", s): raw = s; break
            if raw is None: continue
            val = _to_decimal(raw)
            if val is None: continue
            unit_raw = _guess_unit(raw)
            if unit_raw == "TON":
                price_lb = (val / Decimal(2000)).quantize(Decimal("0.0001"))
            else:
                price_lb = val.quantize(Decimal("0.0001"))
            rows.append({"material": name, "category": _categorize(u), "unit_raw": unit_raw, "price_per_lb": price_lb})
    else:
        print(f"SKIP (unsupported): {fname}"); return 0

    if not rows:
        print(f"NO ROWS parsed: {fname}")
        return 0

    # secondary dedupe per row
    keep = []
    for r in rows:
        hit = await db.fetch_one("""
            SELECT 1 FROM vendor_quotes
            WHERE vendor=:v AND material=:m AND sheet_date IS NOT DISTINCT FROM :d
              AND source_file=:s AND unit_raw=:u
            LIMIT 1
        """, {"v": vendor, "m": r["material"], "d": sheet_date, "s": fname, "u": r["unit_raw"]})
        if not hit:
            keep.append(r)

    if not keep:
        print(f"OK {fname}: inserted 0 (duplicates)")
        return 0

    n = await _insert_rows(db, keep, vendor, sheet_date, fname)
    print(f"OK {fname}: inserted {n}")
    return n

async def main():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL is not set."); sys.exit(1)
    vendor = os.getenv("VENDOR_NAME", DEFAULT_VENDOR)

    if not os.path.isdir(VENDOR_DIR):
        print(f"ERROR: Vendor dir not found: {VENDOR_DIR}"); sys.exit(1)

    db = Database(db_url); await db.connect()
    print(f"Connected to {db_url}")
    try:
        total = 0
        for fname in sorted(os.listdir(VENDOR_DIR)):
            p = os.path.join(VENDOR_DIR, fname)
            if not os.path.isfile(p): continue
            if not any(fname.lower().endswith(ext) for ext in (".csv",".xlsx",".xls")): continue
            total += await ingest_one_file(db, p, vendor)
        print(f"Done. Total inserted rows: {total}")
    finally:
        await db.disconnect(); print("Disconnected.")

if __name__ == "__main__":
    asyncio.run(main())
