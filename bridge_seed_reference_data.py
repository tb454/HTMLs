# bridge_seed_reference_data.py
import os
import sys
import asyncio
import csv
import datetime as dt
from decimal import Decimal, InvalidOperation

from databases import Database

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

AL_PATH_DEFAULT = os.path.join(BASE_DIR, "Aluminum Futures Historical Data(in).csv")
CU_PATH_DEFAULT = os.path.join(BASE_DIR, "Copper Futures Historical Data(in).csv")
INV_PATH_DEFAULT = os.path.join(BASE_DIR, "qbo_out", "bridge_invoices.csv")

FUTURES_DDL = """
CREATE TABLE IF NOT EXISTS futures_prices(
  id          BIGSERIAL PRIMARY KEY,
  symbol      TEXT NOT NULL,
  as_of       DATE NOT NULL,
  open        NUMERIC,
  high        NUMERIC,
  low         NUMERIC,
  close       NUMERIC,
  volume      TEXT,
  change_pct  NUMERIC,
  source      TEXT NOT NULL DEFAULT 'seed_csv',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(symbol, as_of)
);
"""

INVOICES_DDL = """
CREATE TABLE IF NOT EXISTS legacy_invoices(
  id              BIGSERIAL PRIMARY KEY,
  customer        TEXT,
  invoice_id      BIGINT,
  invoice_number  BIGINT,
  invoice_date    DATE,
  service_date    DATE,
  product_service TEXT,
  qbo_item        TEXT,
  description     TEXT,
  ship_date       DATE,
  ship_via        TEXT,
  item            TEXT,
  item_original   TEXT,
  qty             NUMERIC,
  uom             TEXT,
  unit_price      NUMERIC,
  line_amount     NUMERIC,
  invoice_total   NUMERIC,
  invoice_balance NUMERIC,
  pdf_path        TEXT,
  UNIQUE(invoice_id, item_original, line_amount)
);
"""


def _parse_decimal(value) -> Decimal | None:
    if value is None:
        return None
    s = str(value).replace(",", "").strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _parse_change_pct(s) -> Decimal | None:
    if s is None:
        return None
    s = str(s).replace("%", "").strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _parse_us_date(s: str) -> dt.date:
    # handles "11/07/2025" and "7/7/2008"
    s = s.strip()
    return dt.datetime.strptime(s, "%m/%d/%Y").date()


def _parse_iso_date(s) -> dt.date | None:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    return dt.date.fromisoformat(s)


async def _ensure_schema(db: Database) -> None:
    # Each DDL string is a single statement → safe for asyncpg
    for ddl in (FUTURES_DDL, INVOICES_DDL):
        await db.execute(ddl)


async def ingest_futures_file(db: Database, path: str, symbol: str) -> int:
    if not os.path.exists(path):
        print(f"FUTURES: file not found, skipping: {path}")
        return 0

    print(f"FUTURES: ingesting {symbol} from {os.path.basename(path)}")
    count = 0

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        async with db.transaction():
            for row in reader:
                raw_date = row.get("Date")
                if not raw_date:
                    continue
                try:
                    as_of = _parse_us_date(raw_date)
                except Exception:
                    print(f"  WARN: bad date '{raw_date}' → skip row")
                    continue

                close_ = _parse_decimal(row.get("Price"))
                if close_ is None:
                    continue

                open_ = _parse_decimal(row.get("Open"))
                high_ = _parse_decimal(row.get("High"))
                low_ = _parse_decimal(row.get("Low"))
                vol_ = row.get("Vol.")
                chg_ = _parse_change_pct(row.get("Change %"))

                await db.execute(
                    """
                    INSERT INTO futures_prices(
                      symbol, as_of, open, high, low, close, volume, change_pct, source
                    )
                    VALUES (:symbol, :as_of, :open, :high, :low, :close, :volume, :change_pct, :source)
                    ON CONFLICT (symbol, as_of) DO UPDATE
                    SET open       = EXCLUDED.open,
                        high       = EXCLUDED.high,
                        low        = EXCLUDED.low,
                        close      = EXCLUDED.close,
                        volume     = EXCLUDED.volume,
                        change_pct = EXCLUDED.change_pct,
                        source     = EXCLUDED.source;
                    """,
                    {
                        "symbol": symbol,
                        "as_of": as_of,
                        "open": open_,
                        "high": high_,
                        "low": low_,
                        "close": close_,
                        "volume": vol_,
                        "change_pct": chg_,
                        "source": "seed_csv",
                    },
                )
                count += 1

    print(f"FUTURES: {symbol} → inserted/updated {count} rows")
    return count


async def ingest_invoices(db: Database, path: str) -> int:
    if not os.path.exists(path):
        print(f"INVOICES: file not found, skipping: {path}")
        return 0

    print(f"INVOICES: ingesting from {os.path.relpath(path, BASE_DIR)}")
    count = 0

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        async with db.transaction():
            for row in reader:
                invoice_id = row.get("invoice_id")
                invoice_number = row.get("invoice_number")
                inv_date = _parse_iso_date(row.get("invoice_date"))
                svc_date = _parse_iso_date(row.get("service_date"))
                ship_date = _parse_iso_date(row.get("ship_date"))

                qty = _parse_decimal(row.get("qty"))
                unit_price = _parse_decimal(row.get("unit_price"))
                line_amount = _parse_decimal(row.get("line_amount"))
                invoice_total = _parse_decimal(row.get("invoice_total"))
                invoice_balance = _parse_decimal(row.get("invoice_balance"))

                await db.execute(
                    """
                    INSERT INTO legacy_invoices(
                      customer, invoice_id, invoice_number,
                      invoice_date, service_date,
                      product_service, qbo_item, description,
                      ship_date, ship_via,
                      item, item_original,
                      qty, uom, unit_price, line_amount,
                      invoice_total, invoice_balance,
                      pdf_path
                    )
                    VALUES (
                      :customer, :invoice_id, :invoice_number,
                      :invoice_date, :service_date,
                      :product_service, :qbo_item, :description,
                      :ship_date, :ship_via,
                      :item, :item_original,
                      :qty, :uom, :unit_price, :line_amount,
                      :invoice_total, :invoice_balance,
                      :pdf_path
                    )
                    ON CONFLICT (invoice_id, item_original, line_amount) DO UPDATE
                    SET qty             = EXCLUDED.qty,
                        uom             = EXCLUDED.uom,
                        unit_price      = EXCLUDED.unit_price,
                        line_amount     = EXCLUDED.line_amount,
                        invoice_total   = EXCLUDED.invoice_total,
                        invoice_balance = EXCLUDED.invoice_balance,
                        pdf_path        = EXCLUDED.pdf_path;
                    """,
                    {
                        "customer": row.get("customer"),
                        "invoice_id": int(invoice_id) if invoice_id not in (None, "") else None,
                        "invoice_number": int(invoice_number) if invoice_number not in (None, "") else None,
                        "invoice_date": inv_date,
                        "service_date": svc_date,
                        "product_service": row.get("product_service"),
                        "qbo_item": row.get("qbo_item"),
                        "description": row.get("description"),
                        "ship_date": ship_date,
                        "ship_via": row.get("ship_via"),
                        "item": row.get("item"),
                        "item_original": row.get("item_original"),
                        "qty": qty,
                        "uom": row.get("uom"),
                        "unit_price": unit_price,
                        "line_amount": line_amount,
                        "invoice_total": invoice_total,
                        "invoice_balance": invoice_balance,
                        "pdf_path": row.get("pdf_path"),
                    },
                )
                count += 1

    print(f"INVOICES: inserted/updated {count} rows")
    return count


async def main():
    db_url = os.getenv("DATABASE_URL") or os.getenv("ASYNC_DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL/ASYNC_DATABASE_URL is not set.")
        sys.exit(1)

    db = Database(db_url)
    await db.connect()
    print(f"Connected to {db_url}")
    await _ensure_schema(db)

    try:
        # Aluminum → use symbol 'AL' (LME-style)
        await ingest_futures_file(db, AL_PATH_DEFAULT, "AL")

        # Copper → use symbol 'HG' (COMEX copper)
        await ingest_futures_file(db, CU_PATH_DEFAULT, "HG")

        # Legacy invoices (Winski + others)
        await ingest_invoices(db, INV_PATH_DEFAULT)
    finally:
        await db.disconnect()
        print("Disconnected.")


if __name__ == "__main__":
    asyncio.run(main())
