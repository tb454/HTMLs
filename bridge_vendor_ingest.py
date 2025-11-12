# bridge_vendor_ingest.py
import csv
import datetime
from decimal import Decimal, InvalidOperation
import os
import sys
import asyncio

from databases import Database


def _to_decimal(cell: str):
    """
    Extract a numeric value from a '$' cell.
    E.g. '$0.840', ' $ 6/EACH ' -> Decimal('0.840') or Decimal('6')
    Non-numeric stuff returns None instead of blowing up.
    """
    s = cell.replace("$", "").strip()
    # Strip common junk like '/EACH'
    for token in ["/EACH", "EACH", "/LB", "LB"]:
        s = s.replace(token, "").strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


async def ingest_vendor_csv(path: str):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL is not set in the environment.")
        return

    db = Database(db_url)
    await db.connect()
    print(f"Connected to DB: {db_url}")

    vendor = "C&Y Global, Inc. / Pro Metal Recycling"
    sheet_date = datetime.date(2024, 12, 3)

    inserted = 0
    seen = 0

    try:
        async with db.transaction():
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue

                    # Use first column as the material "name"
                    name = (row[0] or "").strip()
                    if not name:
                        continue

                    # Skip obvious headers
                    upper = name.upper()
                    if any(h in upper for h in [
                        "NON-FERROUS", "ALUMINUM PRODUCT", "RADIATOR PRODUCT",
                        "COPPER PRODUCT", "BRASS PRODUCT", "MISC PRDOUCT",
                        "LEAD PRODUCT", "STAINLESS STEEL PRODUCT",
                        "FERROUS METALS", "E SCRAPS"
                    ]):
                        continue

                    seen += 1

                    price = None
                    # Prefer a price on the "left side" of the row (your AL / CU side),
                    # but fall back to the last $ if necessary.
                    for cell in row:
                        if "$" in cell:
                            dec = _to_decimal(cell)
                            if dec is not None:
                                price = dec
                                # we *could* break here; for now we take the first valid
                                break

                    if price is None:
                        # No usable price on this row
                        continue

                    # Category heuristic
                    u = upper
                    if "SCRAP" in u:
                        cat = "E-Scrap"
                    elif u.startswith("AL ") or " AL " in f" {u} ":
                        cat = "Aluminum"
                    elif u.startswith("CU") or " COPPER" in u:
                        cat = "Copper"
                    elif "BRASS" in u:
                        cat = "Brass"
                    elif "SS " in u or "STAINLESS" in u:
                        cat = "Stainless"
                    elif "LEAD" in u:
                        cat = "Lead"
                    else:
                        cat = "Other"

                    await db.execute(
                        """
                        INSERT INTO vendor_quotes(
                          vendor, category, material, price_per_lb, unit_raw, sheet_date, source_file
                        )
                        VALUES (:vendor, :cat, :mat, :price, :unit_raw, :d, :src)
                        """,
                        {
                            "vendor": vendor,
                            "cat": cat,
                            "mat": name,
                            "price": price,
                            "unit_raw": "LBS",
                            "d": sheet_date,
                            "src": os.path.basename(path),
                        },
                    )
                    inserted += 1

        print(f"Done. Rows scanned: {seen}, rows inserted: {inserted}")
    finally:
        await db.disconnect()
        print("Disconnected from DB.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python bridge_vendor_ingest.py <csvfile>")
        sys.exit(1)

    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        print(f"ERROR: File not found: {csv_path}")
        sys.exit(1)

    asyncio.run(ingest_vendor_csv(csv_path))
