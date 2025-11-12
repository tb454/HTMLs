# bridge_vendor_ingest_all.py
import os, sys, asyncio, csv, datetime, re, io
from decimal import Decimal, InvalidOperation

from databases import Database
import pandas as pd  # you already have this for Excel ingest

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VENDOR_DIR = os.path.join(BASE_DIR, "Vendor Quotes")

VENDOR_NAME = "C&Y Global, Inc. / Pro Metal Recycling"  # tweak if needed


def _parse_date_from_filename(fname: str):
    """
    Try to pull a date like 10-2-2025 / 10-02-2025 out of the filename.
    Fallback = None (DB will just store NULL for sheet_date).
    """
    m = re.search(r"(\d{1,2})[-_](\d{1,2})[-_](\d{4})", fname)
    if not m:
        return None
    month, day, year = map(int, m.groups())
    try:
        return datetime.date(year, month, day)
    except ValueError:
        return None


def _to_decimal(cell: str):
    """
    Strip $, commas, and text like '/EACH' or 'EACH' but *do not* decide units here.
    Just return the numeric part, or None.
    """
    s = str(cell).replace("$", "").replace(",", "").strip().upper()
    # leave 'EACH' / 'LB' detection to unit logic
    for junk in ["/EACH", "EACH", "/LB", "LB", "/LBS", "LBS"]:
        s = s.replace(junk, "").strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _guess_unit_from_text(text: str) -> str:
    t = text.upper()
    if "EACH" in t:
        return "EACH"
    if "TON" in t:
        return "TON"
    if "LB" in t or "POUND" in t:
        return "LBS"
    return "LBS"  # default


async def _ingest_rows(db: Database, rows, sheet_date, source_file: str):
    inserted = 0
    for r in rows:
        name = (r.get("material") or "").strip()
        if not name:
            continue
        price = r.get("price")
        if price is None:
            continue

        upper = name.upper()
        # category heuristic
        if "SCRAP" in upper:
            cat = "E-Scrap"
        elif upper.startswith("AL ") or " AL " in f" {upper} ":
            cat = "Aluminum"
        elif upper.startswith("CU") or " COPPER" in upper:
            cat = "Copper"
        elif "BRASS" in upper:
            cat = "Brass"
        elif "SS " in upper or "STAINLESS" in upper:
            cat = "Stainless"
        elif "LEAD" in upper:
            cat = "Lead"
        else:
            cat = "Other"

        unit_raw = r.get("unit_raw") or "LBS"

        await db.execute(
            """
            INSERT INTO vendor_quotes(
              vendor, category, material, price_per_lb, unit_raw, sheet_date, source_file
            )
            VALUES (:vendor, :cat, :mat, :price, :unit_raw, :d, :src)
            """,
            {
                "vendor": VENDOR_NAME,
                "cat": cat,
                "mat": name,
                "price": price,
                "unit_raw": unit_raw,
                "d": sheet_date,
                "src": source_file,
            },
        )
        inserted += 1
    return inserted


async def ingest_one_file(db: Database, path: str):
    fname = os.path.basename(path)
    # skip if we've already ingested this filename
    exists = await db.fetch_one(
        "SELECT 1 FROM vendor_quotes WHERE source_file = :src LIMIT 1",
        {"src": fname},
    )
    if exists:
        print(f"SKIP (already ingested): {fname}")
        return 0

    sheet_date = _parse_date_from_filename(fname)
    ext = os.path.splitext(fname)[1].lower()

    rows = []

    if ext in {".csv"}:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or not (row[0] or "").strip():
                    continue
                name = (row[0] or "").strip()
                upper = name.upper()

                # skip obvious header lines
                if any(h in upper for h in [
                    "NON-FERROUS", "ALUMINUM PRODUCT", "RADIATOR PRODUCT",
                    "COPPER PRODUCT", "BRASS PRODUCT", "MISC PRDOUCT",
                    "LEAD PRODUCT", "STAINLESS STEEL PRODUCT",
                    "FERROUS METALS", "E SCRAPS"
                ]):
                    continue

                # find first cell with a $
                raw_price_cell = None
                for cell in row:
                    if "$" in str(cell):
                        raw_price_cell = cell
                        break
                if not raw_price_cell:
                    continue

                price = _to_decimal(raw_price_cell)
                if price is None:
                    continue

                unit_raw = _guess_unit_from_text(str(raw_price_cell))
                rows.append({
                    "material": name,
                    "price": price,
                    "unit_raw": unit_raw,
                })

    elif ext in {".xlsx", ".xls"}:
        import pandas as pd
        import io as _io

        # pandas handles the messy merged cells reasonably
        df = pd.read_excel(path, header=None)
        for _, row in df.iterrows():
            first = str(row.iloc[0]).strip() if not pd.isna(row.iloc[0]) else ""
            if not first or first.upper() in {"NAN", "NONE"}:
                continue

            name = first
            upper = name.upper()
            if any(h in upper for h in [
                "NON-FERROUS", "ALUMINUM PRODUCT", "RADIATOR PRODUCT",
                "COPPER PRODUCT", "BRASS PRODUCT", "MISC PRDOUCT",
                "LEAD PRODUCT", "STAINLESS STEEL PRODUCT",
                "FERROUS METALS", "E SCRAPS"
            ]):
                continue

            raw_price_cell = None
            for cell in row:
                if isinstance(cell, str) and "$" in cell:
                    raw_price_cell = cell
                    break
                if isinstance(cell, (int, float)) and cell != 0:
                    raw_price_cell = str(cell)
                    break

            if not raw_price_cell:
                continue
            price = _to_decimal(str(raw_price_cell))
            if price is None:
                continue

            unit_raw = _guess_unit_from_text(str(raw_price_cell))
            rows.append({
                "material": name,
                "price": price,
                "unit_raw": unit_raw,
            })
    else:
        print(f"SKIP (unsupported extension): {fname}")
        return 0

    if not rows:
        print(f"NO ROWS parsed from {fname}")
        return 0

    inserted = await _ingest_rows(db, rows, sheet_date, fname)
    print(f"OK {fname}: inserted {inserted} rows")
    return inserted


async def main():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL is not set.")
        sys.exit(1)

    if not os.path.isdir(VENDOR_DIR):
        print(f"ERROR: Vendor dir not found: {VENDOR_DIR}")
        sys.exit(1)

    db = Database(db_url)
    await db.connect()
    print(f"Connected to {db_url}")
    try:
        total = 0
        for fname in os.listdir(VENDOR_DIR):
            path = os.path.join(VENDOR_DIR, fname)
            if not os.path.isfile(path):
                continue
            if not any(fname.lower().endswith(ext) for ext in (".csv", ".xlsx", ".xls")):
                continue
            total += await ingest_one_file(db, path)
        print(f"Done. Total inserted rows: {total}")
    finally:
        await db.disconnect()
        print("Disconnected.")


if __name__ == "__main__":
    asyncio.run(main())
