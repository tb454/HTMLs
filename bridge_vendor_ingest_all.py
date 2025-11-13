# bridge_vendor_ingest_all.py
import os, sys, asyncio, re, datetime as dt, csv, argparse
from decimal import Decimal, InvalidOperation
from typing import Iterable, List, Dict, Optional
from databases import Database

DDL = """
CREATE TABLE IF NOT EXISTS vendor_quotes(
  id            BIGSERIAL PRIMARY KEY,
  vendor        TEXT NOT NULL,
  category      TEXT NOT NULL,
  material      TEXT NOT NULL,
  price_per_lb  NUMERIC NOT NULL,
  unit_raw      TEXT,
  sheet_date    DATE,
  source_file   TEXT,
  inserted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_vq_mat_time ON vendor_quotes(material, inserted_at DESC);
"""

async def _ensure_schema(db: Database):
    await db.execute(DDL)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DIR = os.path.join(BASE_DIR, "Vendor Quotes")
DEFAULT_VENDOR = "C&Y Global, Inc. / Pro Metal Recycling"

def _parse_date_from_filename(fname: str) -> Optional[dt.date]:
    """Support both yyyy-mm-dd and mm-dd-yyyy in filenames (accepts underscores)."""
    f = fname.replace("_", "-")
    m1 = re.search(r"(\d{4})-(\d{2})-(\d{2})", f)        # yyyy-mm-dd
    if m1:
        yyyy, mm, dd = map(int, m1.groups())
        try: return dt.date(yyyy, mm, dd)
        except ValueError: pass
    m2 = re.search(r"(\d{1,2})-(\d{1,2})-(\d{4})", f)    # mm-dd-yyyy
    if m2:
        mm, dd, yyyy = map(int, m2.groups())
        try: return dt.date(yyyy, mm, dd)
        except ValueError: pass
    return None

def _to_decimal(cell: str) -> Optional[Decimal]:
    s = str(cell).replace(",", "")
    m = re.search(r"([-+]?\d+(?:\.\d+)?)", s.replace("$", ""))
    if not m: return None
    try: return Decimal(m.group(1))
    except InvalidOperation: return None

def _guess_unit(text: str) -> str:
    t = (text or "").upper()
    if "EACH" in t: return "EACH"
    if "PER TON" in t or "/TON" in t or re.search(r"\bTONS?\b", t): return "TON"
    if re.search(r"\b(POUNDS?|LBS?)\b", t) or "/LB" in t or "/LBS" in t: return "LBS"
    return "LBS"

def _categorize(u: str) -> str:
    if "SCRAP" in u or "E-SCRAP" in u or "E SCRAP" in u: return "E-Scrap"
    if u.startswith("AL ") or " AL " in f" {u} " or "ALUMINUM" in u: return "Aluminum"
    if u.startswith("CU") or " COPPER" in u: return "Copper"
    if "BRASS" in u: return "Brass"
    if "SS " in u or "STAINLESS" in u: return "Stainless"
    if "LEAD" in u: return "Lead"
    return "Other"

def _looks_like_header(s: str) -> bool:
    s = (s or "").strip().lower()
    return (
        s in {"material", "category", "price", "product"} or
        s.startswith("non-ferrous") or
        s.startswith("aluminum product") or
        s.startswith("radiator product") or
        s.startswith("copper product") or
        s.startswith("brass product") or
        s.startswith("stainless steel product") or
        s.startswith("ferrous") or
        s.startswith("e scraps")
    )

def _norm_material(s: str) -> str:
    return " ".join((s or "").split()).strip()

async def _insert_rows_batch(db: Database, rows: List[Dict], vendor: str, sheet_date, source_file: str) -> int:
    if not rows: return 0
    # Batch insert for speed
    q = """
    INSERT INTO vendor_quotes(vendor, category, material, price_per_lb, unit_raw, sheet_date, source_file)
    VALUES(:v, :c, :m, :p, :u, :d, :s)
    """
    vals = [
        {"v": vendor, "c": r["category"], "m": r["material"], "p": r["price_per_lb"],
         "u": r["unit_raw"], "d": sheet_date, "s": source_file}
        for r in rows
    ]
    # databases doesn't have executemany; run in a transaction for speed
    async with db.transaction():
        for v in vals:
            await db.execute(q, v)
    return len(rows)

def _iter_csv_rows(path: str) -> Iterable[list]:
    # Try multiple encodings & delimiters
    encodings = ["utf-8-sig", "utf-16", "latin-1"]
    delims = [None, ",", ";", "\t"]  # None = sniff
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                sample = f.read(4096); f.seek(0)
                dialect = None
                try:
                    dialect = csv.Sniffer().sniff(sample)
                except Exception:
                    pass
                for delim in delims:
                    try:
                        rdr = csv.reader(f, dialect if (dialect and delim is None) else csv.excel)
                        if delim is not None:
                            rdr = csv.reader(open(path, "r", encoding=enc, newline=""), delimiter=delim)
                        for row in rdr:
                            yield row
                        return
                    except Exception:
                        f.seek(0)
        except Exception:
            continue
    # if all fail, nothing yielded

async def ingest_one_file(db: Database, path: str, vendor: str, since: Optional[dt.date] = None, override_date: Optional[dt.date] = None) -> int:
    fname = os.path.basename(path)

    # Parse date from filename or override
    sheet_date = override_date or _parse_date_from_filename(fname)
    if not sheet_date:
        print(f"WARNING: could not parse date from filename, using NULL: {fname}")
    if since and sheet_date and sheet_date < since:
        print(f"SKIP (before --since): {fname} ({sheet_date})")
        return 0

    # Fast dedupe by vendor + filename + (optional) date
    hit = await db.fetch_one("""
        SELECT 1 FROM vendor_quotes
        WHERE source_file=:s AND vendor=:v AND sheet_date IS NOT DISTINCT FROM :d
        LIMIT 1
    """, {"s": fname, "v": vendor, "d": sheet_date})
    if hit:
        print(f"SKIP (already ingested for vendor/date): {fname}")
        return 0

    ext = os.path.splitext(fname)[1].lower()
    rows: List[Dict] = []

    if ext == ".csv":
        for row in _iter_csv_rows(path):
            if not row or not (row[0] or "").strip():
                continue
            name = _norm_material(row[0])
            if not name or _looks_like_header(name):
                continue
            u = name.upper()
            # Prefer first $ cell, else any numeric-ish cell
            raw = None
            for c in row:
                if "$" in str(c):
                    raw = str(c); break
            if raw is None:
                for c in row[1:]:
                    if re.search(r"\d", str(c or "")):
                        raw = str(c); break
            if raw is None:
                continue
            val = _to_decimal(raw)
            if val is None:
                continue
            unit_raw = _guess_unit(raw)
            # NOTE: We store raw price even for EACH; downstream math MUST filter unit_raw to LB/LBS/POUND
            if unit_raw == "TON":
                price_lb = (val / Decimal(2000)).quantize(Decimal("0.0001"))
            else:
                price_lb = val.quantize(Decimal("0.0001"))
            rows.append({
                "material": name,
                "category": _categorize(u),
                "unit_raw": unit_raw,
                "price_per_lb": price_lb
            })

    elif ext in (".xlsx", ".xls"):
        try:
            import pandas as pd
        except Exception:
            print(f"SKIP (pandas not available for Excel): {fname}")
            return 0
        df = pd.read_excel(path, header=None)
        for _, r in df.iterrows():
            first = r.iloc[0]
            first = "" if (pd.isna(first) or first is None) else str(first).strip()
            if not first or first.upper() in {"NAN","NONE"}:
                continue
            name = _norm_material(first)
            if not name or _looks_like_header(name):
                continue
            u = name.upper()
            raw = None
            for c in r.tolist():
                s = "" if (pd.isna(c) or c is None) else str(c)
                if "$" in s:
                    raw = s; break
            if raw is None:
                for c in r.tolist()[1:]:
                    s = "" if (pd.isna(c) or c is None) else str(c)
                    if re.search(r"\d", s):
                        raw = s; break
            if raw is None:
                continue
            val = _to_decimal(raw)
            if val is None:
                continue
            unit_raw = _guess_unit(raw)
            if unit_raw == "TON":
                price_lb = (val / Decimal(2000)).quantize(Decimal("0.0001"))
            else:
                price_lb = val.quantize(Decimal("0.0001"))
            rows.append({
                "material": name,
                "category": _categorize(u),
                "unit_raw": unit_raw,
                "price_per_lb": price_lb
            })

    else:
        print(f"SKIP (unsupported): {fname}")
        return 0

    if not rows:
        print(f"NO ROWS parsed: {fname}")
        return 0

    # Secondary dedupe per row (vendor+material+sheet_date+source+unit)
    keep: List[Dict] = []
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

    n = await _insert_rows_batch(db, keep, vendor, sheet_date, fname)
    print(f"OK {fname}: inserted {n}")
    return n

async def main():
    parser = argparse.ArgumentParser(description="Bulk vendor ingest")
    parser.add_argument("root", nargs="?", default=DEFAULT_DIR, help="Root folder (default: Vendor Quotes)")
    parser.add_argument("--vendor", default=None, help="Force vendor name (overrides subfolder detection)")
    parser.add_argument("--since", default=None, help="Only ingest files with date >= this (YYYY-MM-DD)")
    parser.add_argument("--date", default=None, help="Override sheet_date for all files (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Parse only; do not write to DB")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL") or os.getenv("ASYNC_DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL/ASYNC_DATABASE_URL is not set."); sys.exit(1)

    root = os.path.abspath(args.root)
    if not os.path.isdir(root):
        print(f"ERROR: Folder not found: {root}"); sys.exit(1)

    since = dt.date.fromisoformat(args.since) if args.since else None
    override_date = dt.date.fromisoformat(args.date) if args.date else None

    db = Database(db_url)
    await db.connect()
    print(f"Connected to {db_url}")
    await _ensure_schema(db)

    try:
        total = 0
        for dirpath, _, files in os.walk(root):
            for fname in sorted(files):
                if not any(fname.lower().endswith(ext) for ext in (".csv", ".xlsx", ".xls")):
                    continue
                path = os.path.join(dirpath, fname)
                # Vendor by subfolder: root/<vendor>/<files>
                if args.vendor:
                    vendor = args.vendor
                else:
                    rel = os.path.relpath(dirpath, root)
                    vendor = rel.split(os.sep)[0] if rel and rel != "." else DEFAULT_VENDOR
                if args.dry_run:
                    # still run parse path/date checks
                    _ = _parse_date_from_filename(fname)
                    print(f"DRY-RUN: {vendor} :: {os.path.relpath(path, root)}")
                    continue
                total += await ingest_one_file(db, path, vendor, since=since, override_date=override_date)
        print(f"Done. Total inserted rows: {total}")
    finally:
        await db.disconnect()
        print("Disconnected.")

if __name__ == "__main__":
    asyncio.run(main())
