from fastapi import FastAPI, HTTPException, Request, Depends, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse, Response, StreamingResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel
from typing import List, Optional, Literal
from datetime import datetime, date
from sqlalchemy import create_engine, Table, MetaData, and_, select
import os
import databases
import uuid
import csv
import io
import zipfile
import tempfile
import pathlib
from decimal import Decimal
import json, hashlib, base64, hmac
from passlib.hash import bcrypt
from dotenv import load_dotenv
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
import re, time as _t
import requests

# ===== ADDED =====
from fastapi import APIRouter
from datetime import date as _date
# ===== /ADDED =====

# ===== middleware & observability deps =====
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
import structlog, time

# rate limiting
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# metrics & errors
from prometheus_fastapi_instrumentator import Instrumentator
import sentry_sdk

_PRICE_CACHE = {"copper_last": None, "ts": 0}
PRICE_TTL_SEC = 300  # 5 minutes

load_dotenv()

app = FastAPI(
    title="BRidge API",
    description="A secure, auditable contract and logistics platform for real-world commodity trading. Built for ICE, Nasdaq, and global counterparties.",
    version="1.0.0",
    contact={
        "name": "Atlas IP Holdings",
        "url": "https://scrapfutures.com",
        "email": "info@atlasipholdingsllc.com",
    },
    license_info={
        "name": "Proprietary — Atlas IP Holdings",
        "url": "https://scrapfutures.com/legal",
    },
)
instrumentator = Instrumentator()
instrumentator.instrument(app)

# === Prices endpoint (after app is defined) ===
@app.get(
    "/prices/copper_last",
    tags=["Prices"],
    summary="COMEX copper last trade (USD/lb)",
    description="Scrapes https://comexlive.org/copper/ for the 'last trade' price and returns base = last - 0.25",
    status_code=200
)
async def prices_copper_last():
    now = _t.time()
    if _PRICE_CACHE["copper_last"] and now - _PRICE_CACHE["ts"] < PRICE_TTL_SEC:
        last = _PRICE_CACHE["copper_last"]
        return {"last": last, "base_minus_025": round(last - 0.25, 4)}
    try:
        resp = requests.get("https://comexlive.org/copper/", timeout=6)
        resp.raise_for_status()
        html = resp.text
        m = (re.search(r"Last\s*Trade[^0-9]*([0-9]+\.[0-9]+)", html, re.I)
             or re.search(r"Last[^0-9]*([0-9]+\.[0-9]+)", html, re.I)
             or re.search(r'>(\d\.\d{2,4})<', html))
        if not m:
            raise ValueError("Unable to locate last trade price on page.")
        last = float(m.group(1))
        _PRICE_CACHE["copper_last"] = last
        _PRICE_CACHE["ts"] = now
        return {"last": last, "base_minus_025": round(last - 0.25, 4)}
    except Exception as e:
        last = 4.19
        return {"last": last, "base_minus_025": round(last - 0.25, 4), "note": f"fallback: {e.__class__.__name__}"}

# ===== Trusted hosts + session cookie =====
allowed = ["scrapfutures.com", "www.scrapfutures.com", "bridge-buyer.onrender.com"]

prod = os.getenv("ENV", "development").lower() == "production"
allow_local = os.getenv("ALLOW_LOCALHOST_IN_PROD", "") in ("1", "true", "yes")
if not prod or allow_local:
    allowed += ["localhost", "127.0.0.1", "testserver", "0.0.0.0"]

app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed)
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SESSION_SECRET", "3ZzQmVoYqNQk5t8R7yJ1xw0uHgBFh9dXea2MUpnCKlGTsvr4OjWPZ6LAiEbNYDf"))

# ===== Security headers =====
async def security_headers_mw(request, call_next):
    resp: Response = await call_next(request)
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    resp.headers["Permissions-Policy"] = "geolocation=()"
    resp.headers["Content-Security-Policy"] = (
        "default-src 'self' https://cdn.jsdelivr.net; "
        "img-src 'self' data:; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net"
    )
    return resp

app.middleware("http")(security_headers_mw)

# =====  request-id + structured logs =====
logger = structlog.get_logger()

@app.middleware("http")
async def request_id_logging(request: Request, call_next):
    rid = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    start = time.time()
    response = await call_next(request)
    elapsed = int((time.time() - start) * 1000)
    response.headers["X-Request-ID"] = rid
    logger.info("req", id=rid, path=str(request.url.path), method=request.method, status=response.status_code, ms=elapsed)
    return response

# =====  rate limiting =====
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@app.exception_handler(RateLimitExceeded)
async def ratelimit_handler(request, exc):
    return PlainTextResponse("Too Many Requests", status_code=429)

# =====  Prometheus metrics + optional Sentry =====
@app.on_event("startup")
async def _metrics_and_sentry():
    instrumentator.expose(app, include_in_schema=False)

dsn = os.getenv("SENTRY_DSN")
if dsn:
    sentry_sdk.init(dsn=dsn, traces_sample_rate=0.05)

# -------- Legal pages --------
@app.get("/terms", include_in_schema=True, tags=["Legal"], summary="Terms of Use", description="View the BRidge platform Terms of Use.", status_code=200)
async def terms_page():
    return FileResponse("static/legal/terms.html")

@app.get("/eula", include_in_schema=True, tags=["Legal"], summary="End User License Agreement (EULA)", description="View the BRidge platform EULA.", status_code=200)
async def eula_page():
    return FileResponse("static/legal/eula.html")

@app.get("/privacy", include_in_schema=True, tags=["Legal"], summary="Privacy Policy", description="View the BRidge platform Privacy Policy.", status_code=200)
async def privacy_page():
    return FileResponse("static/legal/privacy.html")

# -------- Static HTML --------
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", include_in_schema=False)
async def root():
    return FileResponse("static/bridge-login.html")

@app.get("/buyer", include_in_schema=False)
async def buyer_page():
    return FileResponse("static/bridge-buyer.html")

@app.get("/admin", include_in_schema=False)
async def admin_page():
    return FileResponse("static/bridge-admin-dashboard.html")

@app.get("/seller", include_in_schema=False)
async def seller_page():
    return FileResponse("static/seller.html")

# alias: support any old links that hit /yard
@app.get("/yard", include_in_schema=False)
async def yard_alias():
    return RedirectResponse("/seller", status_code=307)

@app.get("/yard/", include_in_schema=False)
async def yard_alias_slash():
    return RedirectResponse("/seller", status_code=307)

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

# -------- Health --------
@app.get("/healthz", tags=["Health"], summary="Health Check", description="Simple health check to confirm service uptime.", status_code=200)
async def healthz():
    return {"ok": True, "service": "bridge-buyer"}

# -------- Database setup --------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

database = databases.Database(DATABASE_URL)
metadata = MetaData()
engine = create_engine(DATABASE_URL)

USERS_TABLE_NAME = "users"
USERNAME_COL = "username"
PASSWORD_HASH_COL = "password_hash"  # fixed
ROLE_COL = "role"

users: Optional[Table] = None

@app.on_event("startup")
async def startup():
    global users
    await database.connect()
    metadata.reflect(bind=engine)
    if USERS_TABLE_NAME not in metadata.tables:
        raise RuntimeError(f"The '{USERS_TABLE_NAME}' table was not found in the database schema.")
    users = metadata.tables[USERS_TABLE_NAME]

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

# ======= AUTH ========
class LoginIn(BaseModel):
    username: str
    password: str

@app.post(
    "/login",
    tags=["Auth"],
    summary="Login",
    description="Authenticate via DB-side bcrypt using pgcrypto: crypt(:password, password_hash)"
)
async def login(body: LoginIn, request: Request):
    username = (body.username or "").strip()
    password = body.password or ""

    row = await database.fetch_one(
        """
        SELECT username, role
        FROM users
        WHERE username = :u
          AND password_hash = crypt(:p, password_hash)
          AND COALESCE(is_active, TRUE) = TRUE
        """,
        {"u": username, "p": password}
    )
    if not row:
        raise HTTPException(status_code=401, detail="Invalid credentials.")

    role = (row["role"] or "").lower()
    if role == "yard":  # back-compat
        role = "seller"

    request.session["username"] = row["username"]
    request.session["role"] = role

    return {"role": role, "redirect": f"/{role}"}

@app.post("/logout", tags=["Auth"], summary="Logout", description="Clears session cookie")
async def logout(request: Request):
    request.session.clear()
    return {"ok": True}
# ======= /AUTH ========

# ===== HMAC gating for inventory endpoints (missing helpers added) =====
INVENTORY_SECRET_ENV = "INVENTORY_WEBHOOK_SECRET"

def _require_hmac_in_this_env() -> bool:
    # Require HMAC only in production and only if a secret is set
    return os.getenv("ENV", "").lower() == "production" and bool(os.getenv(INVENTORY_SECRET_ENV))
# ======================================================================

# ===== FUTURES schema bootstrap =====
@app.on_event("startup")
async def _ensure_futures_schema():
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS futures_products (
          id UUID PRIMARY KEY,
          symbol_root TEXT UNIQUE NOT NULL,
          material TEXT NOT NULL,
          delivery_location TEXT NOT NULL,
          contract_size_tons NUMERIC NOT NULL,
          tick_size NUMERIC NOT NULL,
          currency TEXT NOT NULL DEFAULT 'USD',
          price_method TEXT NOT NULL DEFAULT 'VWAP_BASIS'
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS futures_listings (
          id UUID PRIMARY KEY,
          product_id UUID NOT NULL REFERENCES futures_products(id) ON DELETE CASCADE,
          contract_month CHAR(1) NOT NULL,
          contract_year INT NOT NULL,
          expiry_date DATE NOT NULL,
          first_notice_date DATE,
          last_trade_date DATE,
          status TEXT NOT NULL DEFAULT 'Draft'
        );
        """,
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_futures_series_idx ON futures_listings (product_id, contract_month, contract_year);",
        """
        CREATE TABLE IF NOT EXISTS futures_pricing_params (
          product_id UUID PRIMARY KEY REFERENCES futures_products(id) ON DELETE CASCADE,
          lookback_days INT NOT NULL DEFAULT 14,
          basis_adjustment NUMERIC NOT NULL DEFAULT 0,
          carry_per_month NUMERIC NOT NULL DEFAULT 0,
          manual_mark NUMERIC,
          external_source TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS futures_marks (
          id UUID PRIMARY KEY,
          listing_id UUID NOT NULL REFERENCES futures_listings(id) ON DELETE CASCADE,
          mark_date DATE NOT NULL DEFAULT CURRENT_DATE,
          mark_price NUMERIC NOT NULL,
          method TEXT NOT NULL
        );
        """,
        # Patched VWAP view (Signed/Fulfilled only)
        """
        CREATE OR REPLACE VIEW v_recent_vwap AS
        SELECT
          fp.id AS product_id,
          fp.material,
          fp.delivery_location,
          COALESCE(p.lookback_days, 14) AS lookback_days,
          CASE
            WHEN SUM(c.weight_tons) FILTER (
              WHERE c.status IN ('Signed','Fulfilled')
                AND c.created_at >= NOW() - (COALESCE(p.lookback_days,14) || ' days')::interval
            ) IS NULL
             OR SUM(c.weight_tons) FILTER (
              WHERE c.status IN ('Signed','Fulfilled')
                AND c.created_at >= NOW() - (COALESCE(p.lookback_days,14) || ' days')::interval
            ) = 0
            THEN NULL
            ELSE
              SUM(c.price_per_ton * c.weight_tons) FILTER (
                WHERE c.status IN ('Signed','Fulfilled')
                  AND c.created_at >= NOW() - (COALESCE(p.lookback_days,14) || ' days')::interval
              )
              / NULLIF(SUM(c.weight_tons) FILTER (
                  WHERE c.status IN ('Signed','Fulfilled')
                    AND c.created_at >= NOW() - (COALESCE(p.lookback_days,14) || ' days')::interval
                ), 0)
          END AS recent_vwap
        FROM futures_products fp
        LEFT JOIN futures_pricing_params p ON p.product_id = fp.id
        LEFT JOIN contracts c ON c.material = fp.material
        GROUP BY fp.id, fp.material, fp.delivery_location, COALESCE(p.lookback_days,14);
        """
    ]
    for stmt in ddl:
        try:
            await database.execute(stmt)
        except Exception as e:
            logger.warn("futures_schema_bootstrap_failed", err=str(e), sql=stmt[:80])

# ===== TRADING & CLEARING SCHEMA bootstrap =====
@app.on_event("startup")
async def _ensure_trading_schema():
    try:
        await database.execute("DROP MATERIALIZED VIEW IF EXISTS v_latest_settle")
        await database.execute("DROP TABLE IF EXISTS v_latest_settle")
    except Exception:
        pass

    ddl = [
        """
        CREATE TABLE IF NOT EXISTS accounts (
          id UUID PRIMARY KEY,
          name TEXT NOT NULL,
          type TEXT NOT NULL CHECK (type IN ('buyer','seller','broker'))
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS margin_accounts (
          account_id UUID PRIMARY KEY REFERENCES accounts(id),
          balance NUMERIC NOT NULL DEFAULT 0,
          initial_pct NUMERIC NOT NULL DEFAULT 0.10,
          maintenance_pct NUMERIC NOT NULL DEFAULT 0.07
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
          id UUID PRIMARY KEY,
          account_id UUID NOT NULL REFERENCES accounts(id),
          listing_id UUID NOT NULL REFERENCES futures_listings(id),
          side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
          price NUMERIC NOT NULL,
          qty NUMERIC NOT NULL,
          qty_open NUMERIC NOT NULL,
          status TEXT NOT NULL CHECK (status IN ('NEW','PARTIAL','FILLED','CANCELLED')),
          tif TEXT NOT NULL DEFAULT 'GTC',
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_orders_book ON orders(listing_id, side, price, created_at) WHERE status IN ('NEW','PARTIAL');",
        """
        CREATE TABLE IF NOT EXISTS trades (
          id UUID PRIMARY KEY,
          buy_order_id UUID NOT NULL REFERENCES orders(id),
          sell_order_id UUID NOT NULL REFERENCES orders(id),
          listing_id UUID NOT NULL REFERENCES futures_listings(id),
          price NUMERIC NOT NULL,
          qty NUMERIC NOT NULL,
          traded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_trades_listing_time ON trades(listing_id, traded_at DESC);",
        """
        CREATE TABLE IF NOT EXISTS positions (
          account_id UUID NOT NULL REFERENCES accounts(id),
          listing_id UUID NOT NULL REFERENCES futures_listings(id),
          net_qty NUMERIC NOT NULL DEFAULT 0,
          PRIMARY KEY (account_id, listing_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS margin_events (
          id UUID PRIMARY KEY,
          account_id UUID NOT NULL REFERENCES accounts(id),
          amount NUMERIC NOT NULL,
          reason TEXT NOT NULL,
          ref_id UUID,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE OR REPLACE VIEW v_latest_settle AS
        SELECT fl.id AS listing_id,
               (SELECT fm.mark_price
                  FROM futures_marks fm
                 WHERE fm.listing_id = fl.id
                 ORDER BY fm.mark_date DESC
                 LIMIT 1) AS settle_price
        FROM futures_listings fl;
        """
    ]
    for stmt in ddl:
        try:
            await database.execute(stmt)
        except Exception as e:
            logger.warn("trading_schema_bootstrap_failed", err=str(e), sql=stmt[:90])

# ===== TRADING HARDENING: risk limits, audit, trading status =====
@app.on_event("startup")
async def _ensure_trading_hardening():
    ddl = [
        """
        ALTER TABLE margin_accounts
        ADD COLUMN IF NOT EXISTS risk_limit_open_lots NUMERIC NOT NULL DEFAULT 50,
        ADD COLUMN IF NOT EXISTS is_blocked BOOLEAN NOT NULL DEFAULT FALSE;
        """,
        """
        CREATE TABLE IF NOT EXISTS orders_audit (
          id UUID PRIMARY KEY,
          order_id UUID NOT NULL,
          event TEXT NOT NULL,
          qty_open NUMERIC NOT NULL,
          reason TEXT,
          at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        ALTER TABLE futures_listings
        ADD COLUMN IF NOT EXISTS trading_status TEXT NOT NULL DEFAULT 'Trading',
        ADD CONSTRAINT chk_trading_status CHECK (trading_status IN ('Trading','Halted','Expired'));
        """
    ]
    for stmt in ddl:
        try:
            await database.execute(stmt)
        except Exception as e:
            logger.warn("trading_hardening_bootstrap_failed", err=str(e), sql=stmt[:120])

# ===== INVENTORY schema bootstrap (idempotent) =====
@app.on_event("startup")
async def _ensure_inventory_schema():
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS inventory_items (
          seller TEXT NOT NULL,
          sku TEXT NOT NULL,
          description TEXT,
          uom TEXT,
          location TEXT,
          qty_on_hand NUMERIC NOT NULL DEFAULT 0,
          qty_reserved NUMERIC NOT NULL DEFAULT 0,
          qty_committed NUMERIC NOT NULL DEFAULT 0,
          source TEXT,
          external_id TEXT,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (seller, sku)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS inventory_movements (
          seller TEXT NOT NULL,
          sku TEXT NOT NULL,
          movement_type TEXT NOT NULL,
          qty NUMERIC NOT NULL,
          ref_contract TEXT,
          meta JSONB,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS inventory_ingest_log (
          id BIGSERIAL PRIMARY KEY,
          source TEXT,
          seller TEXT,
          item_count INT,
          idem_key TEXT,
          sig_present BOOLEAN,
          sig_valid BOOLEAN,
          remote_addr TEXT,
          user_agent TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE OR REPLACE VIEW inventory_available AS
        SELECT
          seller,
          sku,
          description,
          COALESCE(uom, 'ton') AS uom,
          location,
          qty_on_hand,
          qty_reserved,
          (qty_on_hand - qty_reserved) AS qty_available,
          qty_committed,
          updated_at
        FROM inventory_items;
        """
    ]
    for stmt in ddl:
        try:
            await database.execute(stmt)
        except Exception as e:
            logger.warn("inventory_schema_bootstrap_failed", err=str(e), sql=stmt[:120])

class InventoryRowOut(BaseModel):
    seller: str
    sku: str
    description: Optional[str] = None
    uom: str
    location: Optional[str] = None
    qty_on_hand: float
    qty_reserved: float
    qty_available: float
    qty_committed: float
    updated_at: datetime

from typing import List  # already imported at top

@app.get(
    "/inventory",
    tags=["Inventory"],
    summary="List inventory (available view)",
    response_model=List[InventoryRowOut],   # <-- change _List -> List
    status_code=200
)

async def list_inventory(
    seller: str = Query(..., description="Seller name"),
    sku: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    q = "SELECT * FROM inventory_available WHERE seller = :seller"
    vals = {"seller": seller}
    if sku:
        q += " AND sku = :sku"
        vals["sku"] = sku
    q += " ORDER BY updated_at DESC LIMIT :limit OFFSET :offset"
    vals["limit"], vals["offset"] = limit, offset
    rows = await database.fetch_all(q, vals)
    return [InventoryRowOut(**dict(r)) for r in rows]

@app.get("/inventory/movements/list",
    tags=["Inventory"],
    summary="List inventory movements",
    description="Recent inventory movements. Filter by seller, sku, type, time range.",
    status_code=200
)
async def list_movements(
    seller: str = Query(...),
    sku: Optional[str] = Query(None),
    movement_type: Optional[str] = Query(None, description="upsert, adjust, reserve, unreserve, commit, ship, cancel, reconcile"),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    q = "SELECT * FROM inventory_movements WHERE seller=:seller"
    vals = {"seller": seller, "limit": limit, "offset": offset}
    if sku:
        q += " AND sku=:sku"; vals["sku"] = sku
    if movement_type:
        q += " AND movement_type=:mt"; vals["mt"] = movement_type
    if start:
        q += " AND created_at >= :start"; vals["start"] = start
    if end:
        q += " AND created_at <= :end"; vals["end"] = end
    q += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
    rows = await database.fetch_all(q, vals)
    return [dict(r) for r in rows]

# -------- CORS --------
ALLOWED_ORIGINS = [
    "https://scrapfutures.com",
    "https://www.scrapfutures.com",
    "https://bridge-buyer.onrender.com",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------- Models (+ OpenAPI examples) --------
class CarrierInfo(BaseModel):
    name: str
    driver: str
    truck_vin: str
    class Config:
        schema_extra = {"example": {"name":"ABC Trucking Co.","driver":"Jane Doe","truck_vin":"1FTSW21P34ED12345"}}

class Signature(BaseModel):
    base64: str
    timestamp: datetime
    class Config:
        schema_extra = {"example": {"base64":"data:image/png;base64,iVBORw0KGgoAAA...","timestamp":"2025-09-01T12:00:00Z"}}

class BOLRecord(BaseModel):
    bol_id: str
    contract_id: str
    buyer: str
    seller: str
    material: str
    weight_tons: float
    price_per_unit: float
    total_value: float
    carrier: CarrierInfo
    pickup_signature: Signature
    delivery_signature: Optional[Signature] = None
    pickup_time: datetime
    delivery_time: Optional[datetime] = None
    status: str

class LoginRequest(BaseModel):
    username: str
    password: str
    class Config:
        schema_extra = {"example": {"username":"admin","password":"securepass123"}}

class ContractIn(BaseModel):
    buyer: str
    seller: str
    material: str
    weight_tons: float
    price_per_ton: float
    class Config:
        schema_extra = {"example": {
            "buyer":"Lewis Salvage","seller":"Winski Brothers",
            "material":"Shred Steel","weight_tons":40.0,"price_per_ton":245.00
        }}

class PurchaseIn(BaseModel):
    op: Literal["purchase"] = "purchase"
    expected_status: Literal["Pending"] = "Pending"
    idempotency_key: Optional[str] = None

class ContractOut(ContractIn):
    id: uuid.UUID
    status: str
    created_at: datetime
    signed_at: Optional[datetime]
    signature: Optional[str]
    class Config:
        schema_extra = {"example": {
            "id":"b1c89b94-234a-4d55-b1fc-14bfb7fce7e9",
            "buyer":"Lewis Salvage","seller":"Winski Brothers",
            "material":"Shred Steel","weight_tons":40,"price_per_ton":245.00,
            "status":"Signed","created_at":"2025-09-01T10:00:00Z",
            "signed_at":"2025-09-01T10:15:00Z","signature":"abc123signature"
        }}

class BOLIn(BaseModel):
    contract_id: uuid.UUID
    buyer: str
    seller: str
    material: str
    weight_tons: float
    price_per_unit: float
    total_value: float
    carrier: CarrierInfo
    pickup_signature: Signature
    pickup_time: datetime
    class Config:
        schema_extra = {"example": {
            "contract_id":"1ec9e850-8b5a-45de-b631-f9fae4a1d4c9",
            "buyer":"Lewis Salvage","seller":"Winski Brothers",
            "material":"Shred Steel","weight_tons":40,"price_per_unit":245.00,
            "total_value":9800.00,
            "carrier":{"name":"ABC Trucking Co.","driver":"John Driver","truck_vin":"1FDUF5GY3KDA12345"},
            "pickup_signature":{"base64":"data:image/png;base64,iVBOR...","timestamp":"2025-09-01T12:00:00Z"},
            "pickup_time":"2025-09-01T12:15:00Z"
        }}

class BOLOut(BOLIn):
    bol_id: uuid.UUID
    status: str
    delivery_signature: Optional[Signature] = None
    delivery_time: Optional[datetime] = None
    class Config:
        schema_extra = {"example": {
            "bol_id":"9fd89221-4247-4f93-bf4b-df9473ed8e57",
            "contract_id":"b1c89b94-234a-4d55-b1fc-14bfb7fce7e9",
            "buyer":"Lewis Salvage","seller":"Winski Brothers",
            "material":"Shred Steel","weight_tons":40,
            "price_per_unit":245.0,"total_value":9800.0,
            "carrier":{"name":"ABC Trucking Co.","driver":"Jane Doe","truck_vin":"1FTSW21P34ED12345"},
            "pickup_signature":{"base64":"data:image/png;base64,iVBOR...","timestamp":"2025-09-01T12:00:00Z"},
            "pickup_time":"2025-09-01T12:15:00Z",
            "delivery_signature":None,"delivery_time":None,"status":"BOL Issued"
        }}

# ===== Idempotency cache for POST/Inventory/Purchase =====
_idem_cache = {}

# ===== Admin export helpers =====
def _normalize(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"), ensure_ascii=False)
    return v

def _rows_to_csv_bytes(rows):
    buf = io.StringIO(newline="")
    if not rows:
        writer = csv.writer(buf)
        writer.writerow(["(no rows)"])
    else:
        dict_rows = [dict(r) for r in rows]
        fields = sorted({k for r in dict_rows for k in r.keys()})
        writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for r in dict_rows:
            writer.writerow({k: _normalize(r.get(k)) for k in fields})
    return buf.getvalue().encode("utf-8")

def _is_admin_session(request: Request) -> bool:
    try:
        return request.session.get("role") == "admin"
    except Exception:
        return False

# ===== Webhook HMAC + replay protection =====
def verify_sig(raw: bytes, header_sig: str, secret_env: str) -> bool:
    secret = os.getenv(secret_env, "")
    if not (secret and header_sig):
        return False
    digest = hmac.new(secret.encode(), raw, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode()
    return hmac.compare_digest(expected, header_sig)

REPLAY_TTL = 300  # 5 minutes
REPLAY_CACHE: dict[str, float] = {}
def is_replay(sig: str | None) -> bool:
    if not sig:
        return True
    now = time.time()
    for k, t in list(REPLAY_CACHE.items()):
        if now - t > REPLAY_TTL:
            REPLAY_CACHE.pop(k, None)
    if sig in REPLAY_CACHE:
        return True
    REPLAY_CACHE[sig] = now
    return False

# -------- Documents: BOL PDF --------
@app.get(
    "/bol/{bol_id}/pdf",
    tags=["Documents"],
    summary="Download BOL as PDF",
    description="Generates and returns a downloadable PDF version of the specified BOL.",
    status_code=200
)
async def generate_bol_pdf(bol_id: str):
    row = await database.fetch_one("SELECT * FROM bols WHERE bol_id = :bol_id", {"bol_id": bol_id})
    if not row:
        raise HTTPException(status_code=404, detail="BOL not found")

    filename = f"bol_{bol_id}.pdf"
    filepath = f"/tmp/{filename}"
    c = canvas.Canvas(filepath, pagesize=LETTER)
    width, height = LETTER
    margin = 1 * inch
    y = height - margin

    c.setFont("Helvetica-Bold", 16)
    c.drawString(margin, y, "Bill of Lading")
    y -= 0.5 * inch

    c.setFont("Helvetica", 12)
    line_height = 18
    def draw(label, value):
        nonlocal y
        c.drawString(margin, y, f"{label}: {value}")
        y -= line_height

    draw("BOL ID", row["bol_id"])
    draw("Contract ID", row["contract_id"])
    draw("Status", row["status"])
    draw("Buyer", row["buyer"])
    draw("Seller", row["seller"])
    draw("Material", row["material"])
    draw("Weight (tons)", row["weight_tons"])
    draw("Price per ton", f"${row['price_per_unit']:.2f}")
    draw("Total Value", f"${row['total_value']:.2f}")
    draw("Pickup Time", row["pickup_time"].isoformat())
    draw("Delivery Time", row["delivery_time"].isoformat() if row["delivery_time"] else "—")
    draw("Carrier Name", row["carrier_name"])
    draw("Driver", row["carrier_driver"])
    draw("Truck VIN", row["carrier_truck_vin"])

    y -= line_height
    c.setFont("Helvetica-Oblique", 10)
    c.drawString(margin, y, f"Generated by BRidge on {datetime.utcnow().isoformat()}")

    try:
        d = dict(row)
        fingerprint = hashlib.sha256(
            json.dumps(d, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()[:12]
        y -= line_height
        c.setFont("Helvetica", 8)
        c.drawString(margin, y, f"Verify: https://bridge-buyer.onrender.com/bol/{row['bol_id']}  •  Hash: {fingerprint}")
    except Exception:
        pass

    c.save()
    return FileResponse(filepath, media_type="application/pdf", filename=filename)

# -------- Contracts (with Inventory linkage) --------
@app.post(
    "/contracts",
    response_model=ContractOut,
    tags=["Contracts"],
    summary="Create Contract",
    description="Creates a Pending contract and reserves inventory (qty_reserved += weight_tons).",
    status_code=201
)
async def create_contract(contract: ContractIn):
    qty = float(contract.weight_tons)
    seller = contract.seller.strip()
    sku = contract.material.strip()

    async with database.transaction():
        await database.execute("""
            INSERT INTO inventory_items (seller, sku, qty_on_hand, qty_reserved, qty_committed)
            VALUES (:seller, :sku, 0, 0, 0)
            ON CONFLICT (seller, sku) DO NOTHING
        """, {"seller": seller, "sku": sku})

        inv = await database.fetch_one("""
            SELECT qty_on_hand, qty_reserved FROM inventory_items
            WHERE seller=:seller AND sku=:sku
            FOR UPDATE
        """, {"seller": seller, "sku": sku})
        on_hand = float(inv["qty_on_hand"]) if inv else 0.0
        reserved = float(inv["qty_reserved"]) if inv else 0.0
        available = on_hand - reserved
        if available < qty:
            raise HTTPException(status_code=409, detail=f"Not enough inventory: available {available} ton(s) < requested {qty} ton(s).")

        await database.execute("""
            UPDATE inventory_items
            SET qty_reserved = qty_reserved + :q, updated_at = NOW()
            WHERE seller=:seller AND sku=:sku
        """, {"q": qty, "seller": seller, "sku": sku})
        await database.execute("""
            INSERT INTO inventory_movements (seller, sku, movement_type, qty, ref_contract, meta)
            VALUES (:seller, :sku, 'reserve', :q, NULL, :meta)
        """, {"seller": seller, "sku": sku, "q": qty, "meta": json.dumps({"reason": "contract_create"})})

        row = await database.fetch_one("""
            INSERT INTO contracts (id, buyer, seller, material, weight_tons, price_per_ton, status)
            VALUES (:id, :buyer, :seller, :material, :weight_tons, :price_per_ton, 'Pending')
            RETURNING *
        """, {"id": str(uuid.uuid4()), **contract.dict()})
        if not row:
            raise HTTPException(status_code=500, detail="Failed to create contract")
        return row

@app.get(
    "/contracts",
    response_model=List[ContractOut],
    tags=["Contracts"],
    summary="List Contracts",
    description="Retrieve contracts with optional filters: buyer, seller, material, status, created_at date range.",
    status_code=200
)
async def get_all_contracts(
    buyer: Optional[str] = Query(None),
    seller: Optional[str] = Query(None),
    material: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime]   = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    buyer    = buyer.strip()    if isinstance(buyer, str) else buyer
    seller   = seller.strip()   if isinstance(seller, str) else seller
    material = material.strip() if isinstance(material, str) else material
    status   = status.strip()   if isinstance(status, str) else status

    query = "SELECT * FROM contracts"
    conditions, values = [], {}

    if buyer:
        conditions.append("buyer ILIKE :buyer"); values["buyer"] = f"%{buyer}%"
    if seller:
        conditions.append("seller ILIKE :seller"); values["seller"] = f"%{seller}%"
    if material:
        conditions.append("material ILIKE :material"); values["material"] = f"%{material}%"
    if status:
        conditions.append("status ILIKE :status"); values["status"] = f"%{status}%"
    if start:
        conditions.append("created_at >= :start"); values["start"] = start
    if end:
        conditions.append("created_at <= :end"); values["end"] = end

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY created_at DESC NULLS LAST LIMIT :limit OFFSET :offset"
    values["limit"], values["offset"] = limit, offset

    return await database.fetch_all(query=query, values=values)

@app.get("/contracts/{contract_id}", response_model=ContractOut, tags=["Contracts"], summary="Get Contract by ID", status_code=200)
async def get_contract_by_id(contract_id: str):
    row = await database.fetch_one("SELECT * FROM contracts WHERE id = :id", {"id": contract_id})
    if not row:
        raise HTTPException(status_code=404, detail="Contract not found")
    return row

@app.put("/contracts/{contract_id}", response_model=ContractOut, tags=["Contracts"], summary="Update Contract", status_code=200)
async def update_contract(contract_id: str, update: ContractUpdate):
    row = await database.fetch_one("""
        UPDATE contracts
        SET status = :status,
            signature = :signature,
            signed_at = CASE WHEN :signature IS NOT NULL THEN NOW() ELSE signed_at END
        WHERE id = :id
        RETURNING *
    """, {"id": contract_id, "status": update.status, "signature": update.signature})
    if not row:
        raise HTTPException(status_code=404, detail="Contract not found")
    return row

@app.get("/contracts/export_csv", tags=["Contracts"], summary="Export Contracts as CSV", status_code=200)
async def export_contracts_csv():
    rows = await database.fetch_all("SELECT * FROM contracts ORDER BY created_at DESC")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID","Buyer","Seller","Material","Weight (tons)","Price/ton","Status","Created","Signed","Signature"])
    for r in rows:
        writer.writerow([
            r["id"], r["buyer"], r["seller"], r["material"],
            r["weight_tons"], r["price_per_ton"], r["status"],
            r["created_at"].isoformat(),
            r["signed_at"].isoformat() if r["signed_at"] else "",
            r["signature"] or ""
        ])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="contracts_export_{datetime.utcnow().isoformat()}.csv"'}
    )

@app.patch(
    "/contracts/{contract_id}/purchase",
    tags=["Contracts"],
    summary="Purchase (atomic)",
    description="Atomically change a Pending contract to Signed, move reserved→committed, and auto-create a Scheduled BOL.",
    status_code=200
)
async def purchase_contract(contract_id: str, body: PurchaseIn, request: Request):
    idem = body.idempotency_key or request.headers.get("Idempotency-Key")
    if idem and idem in _idem_cache:
        return _idem_cache[idem]

    async with database.transaction():
        row = await database.fetch_one("""
            UPDATE contracts
            SET status = 'Signed', signed_at = NOW()
            WHERE id = :id AND status = :expected
            RETURNING id, buyer, seller, material, weight_tons, price_per_ton
        """, {"id": contract_id, "expected": body.expected_status})
        if not row:
            raise HTTPException(status_code=409, detail="Contract not purchasable (already taken or not Pending).")

        qty = float(row["weight_tons"])
        seller = row["seller"].strip()
        sku = row["material"].strip()

        inv = await database.fetch_one("""
            SELECT qty_reserved FROM inventory_items
            WHERE seller=:seller AND sku=:sku
            FOR UPDATE
        """, {"seller": seller, "sku": sku})
        reserved = float(inv["qty_reserved"]) if inv else 0.0
        if reserved < qty:
            raise HTTPException(status_code=409, detail="Reserved inventory insufficient to commit.")

        await database.execute("""
            UPDATE inventory_items
            SET qty_reserved = qty_reserved - :q,
                qty_committed = qty_committed + :q,
                updated_at = NOW()
            WHERE seller=:seller AND sku=:sku
        """, {"q": qty, "seller": seller, "sku": sku})

        await database.execute("""
            INSERT INTO inventory_movements (seller, sku, movement_type, qty, ref_contract, meta)
            VALUES (:seller, :sku, 'commit', :q, :ref_contract, :meta)
        """, {"seller": seller, "sku": sku, "q": qty, "ref_contract": contract_id,
              "meta": json.dumps({"reason": "purchase"})})

        bol_id = str(uuid.uuid4())
        await database.fetch_one("""
            INSERT INTO bols (
                bol_id, contract_id, buyer, seller, material, weight_tons,
                price_per_unit, total_value,
                carrier_name, carrier_driver, carrier_truck_vin,
                pickup_signature_base64, pickup_signature_time,
                pickup_time, status
            )
            VALUES (
                :bol_id, :contract_id, :buyer, :seller, :material, :tons,
                :ppu, :total,
                :cname, :cdriver, :cvin,
                :ps_b64, :ps_time,
                :pickup_time, 'Scheduled'
            )
            RETURNING bol_id
        """, {
            "bol_id": bol_id,
            "contract_id": contract_id,
            "buyer": row["buyer"],
            "seller": row["seller"],
            "material": row["material"],
            "tons": qty,
            "ppu": float(row["price_per_ton"]),
            "total": qty * float(row["price_per_ton"]),
            "cname": "TBD", "cdriver": "TBD", "cvin": "TBD",
            "ps_b64": None, "ps_time": None,
            "pickup_time": datetime.utcnow()
        })

    resp = {"ok": True, "contract_id": contract_id, "new_status": "Signed", "bol_id": bol_id}
    if idem:
        _idem_cache[idem] = resp
    return resp

@app.post(
    "/contracts/{contract_id}/cancel",
    tags=["Contracts"],
    summary="Cancel Pending contract",
    description="Cancels a Pending contract and releases reserved inventory (unreserve).",
    status_code=200
)
async def cancel_contract(contract_id: str):
    async with database.transaction():
        row = await database.fetch_one("""
            UPDATE contracts
            SET status='Cancelled'
            WHERE id=:id AND status='Pending'
            RETURNING seller, material, weight_tons
        """, {"id": contract_id})
        if not row:
            raise HTTPException(status_code=409, detail="Only Pending contracts can be cancelled.")

        qty = float(row["weight_tons"])
        seller = row["seller"].strip()
        sku = row["material"].strip()

        _ = await database.fetch_one("""
            SELECT qty_reserved FROM inventory_items
            WHERE seller=:seller AND sku=:sku
            FOR UPDATE
        """, {"seller": seller, "sku": sku})

        await database.execute("""
            UPDATE inventory_items
            SET qty_reserved = GREATEST(0, qty_reserved - :q),
                updated_at = NOW()
            WHERE seller=:seller AND sku=:sku
        """, {"q": qty, "seller": seller, "sku": sku})

        await database.execute("""
            INSERT INTO inventory_movements (seller, sku, movement_type, qty, ref_contract, meta)
            VALUES (:seller, :sku, 'unreserve', :q, :ref_contract, :meta)
        """, {"seller": seller, "sku": sku, "q": qty, "ref_contract": contract_id,
              "meta": json.dumps({"reason": "cancel"})})

    return {"ok": True, "contract_id": contract_id, "status": "Cancelled"}

# -------- BOLs --------
@app.post("/bols", response_model=BOLOut, tags=["BOLs"], summary="Create BOL", status_code=201)
async def create_bol_pg(bol: BOLIn, request: Request):
    idem_key = request.headers.get("Idempotency-Key")
    if idem_key and idem_key in _idem_cache:
        return _idem_cache[idem_key]

    row = await database.fetch_one("""
        INSERT INTO bols (
            bol_id, contract_id, buyer, seller, material, weight_tons,
            price_per_unit, total_value,
            carrier_name, carrier_driver, carrier_truck_vin,
            pickup_signature_base64, pickup_signature_time,
            pickup_time, status
        )
        VALUES (
            :bol_id, :contract_id, :buyer, :seller, :material, :weight_tons,
            :price_per_unit, :total_value,
            :carrier_name, :carrier_driver, :carrier_truck_vin,
            :pickup_sig_b64, :pickup_sig_time,
            :pickup_time, 'Scheduled'
        )
        RETURNING *
    """, {
        "bol_id": str(uuid.uuid4()),
        "contract_id": str(bol.contract_id),
        "buyer": bol.buyer, "seller": bol.seller, "material": bol.material,
        "weight_tons": bol.weight_tons, "price_per_unit": bol.price_per_unit, "total_value": bol.total_value,
        "carrier_name": bol.carrier.name, "carrier_driver": bol.carrier.driver, "carrier_truck_vin": bol.carrier.truck_vin,
        "pickup_sig_b64": bol.pickup_signature.base64, "pickup_sig_time": bol.pickup_signature.timestamp,
        "pickup_time": bol.pickup_time
    })
    resp = {
        **bol.dict(),
        "bol_id": row["bol_id"],
        "status": row["status"],
        "delivery_signature": None,
        "delivery_time": None
    }
    if idem_key:
        _idem_cache[idem_key] = resp
    return resp

@app.get(
    "/bols",
    response_model=List[BOLOut],
    tags=["BOLs"],
    summary="List BOLs",
    description="Retrieve BOLs with optional filters (buyer, seller, material, status, contract_id, pickup date range).",
    status_code=200
)
async def get_all_bols_pg(
    buyer: Optional[str] = Query(None),
    seller: Optional[str] = Query(None),
    material: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    contract_id: Optional[str] = Query(None),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    q = "SELECT * FROM bols"
    cond, vals = [], {}
    if buyer:       cond.append("buyer ILIKE :buyer");           vals["buyer"] = f"%{buyer}%"
    if seller:      cond.append("seller ILIKE :seller");         vals["seller"] = f"%{seller}%"
    if material:    cond.append("material ILIKE :material");     vals["material"] = f"%{material}%"
    if status:      cond.append("status ILIKE :status");         vals["status"] = f"%{status}%"
    if contract_id: cond.append("contract_id = :contract_id");   vals["contract_id"] = contract_id
    if start:       cond.append("pickup_time >= :start");        vals["start"] = start
    if end:         cond.append("pickup_time <= :end");          vals["end"] = end
    if cond:
        q += " WHERE " + " AND ".join(cond)
    q += " ORDER BY pickup_time DESC NULLS LAST, bol_id DESC LIMIT :limit OFFSET :offset"
    vals["limit"], vals["offset"] = limit, offset

    rows = await database.fetch_all(q, vals)
    out = []
    for r in rows:
        d = dict(r)
        out.append({
            "bol_id": d["bol_id"],
            "contract_id": d["contract_id"],
            "buyer": d.get("buyer") or "",
            "seller": d.get("seller") or "",
            "material": d.get("material") or "",
            "weight_tons": float(d.get("weight_tons") or 0.0),
            "price_per_unit": float(d.get("price_per_unit") or 0.0),
            "total_value": float(d.get("total_value") or 0.0),
            "carrier": {
                "name": d.get("carrier_name") or "TBD",
                "driver": d.get("carrier_driver") or "TBD",
                "truck_vin": d.get("carrier_truck_vin") or "TBD",
            },
            "pickup_signature": {
                "base64": d.get("pickup_signature_base64") or "",
                "timestamp": d.get("pickup_signature_time") or d.get("pickup_time") or datetime.utcnow(),
            },
            "delivery_signature": (
                {"base64": d.get("delivery_signature_base64"), "timestamp": d.get("delivery_signature_time")}
                if d.get("delivery_signature_base64") is not None else None
            ),
            "pickup_time": d.get("pickup_time"),
            "delivery_time": d.get("delivery_time"),
            "status": d.get("status") or "",
        })
    return out

@app.post(
    "/inventory/bulk_upsert",
    tags=["Inventory"],
    summary="Bulk upsert inventory items",
    description="Partners push absolute qty_on_hand per SKU for a seller. Records delta in movements.",
    response_model=dict,
    status_code=200
)
async def inventory_bulk_upsert(body: dict, request: Request):
    # Minimal validation
    source = (body.get("source") or "").strip()
    seller = (body.get("seller") or "").strip()
    items  = body.get("items") or []
    if not (source and seller and isinstance(items, list)):
        raise HTTPException(400, "invalid payload")

    # HMAC only in production
    if _require_hmac_in_this_env():
        raw = await request.body()
        sig = request.headers.get("X-Signature", "")
        if not (sig and verify_sig(raw, sig, INVENTORY_SECRET_ENV)):
            raise HTTPException(401, "Bad signature")

    async with database.transaction():
        for it in items:
            sku = (it.get("sku") or "").strip()
            if not sku:
                continue
            desc = it.get("description")
            uom  = it.get("uom") or "ton"
            loc  = it.get("location")
            qty  = float(it.get("qty_on_hand") or 0.0)

            # ensure row
            await database.execute("""
                INSERT INTO inventory_items (seller, sku, description, uom, location, qty_on_hand, source, external_id)
                VALUES (:seller,:sku,:description,:uom,:location,0,:source,:external_id)
                ON CONFLICT (seller, sku) DO NOTHING
            """, {"seller": seller, "sku": sku, "description": desc, "uom": uom, "location": loc,
                  "source": source, "external_id": it.get("external_id")})

            # read old (for delta)
            prev = await database.fetch_one("""
                SELECT qty_on_hand FROM inventory_items
                WHERE seller=:seller AND sku=:sku
                FOR UPDATE
            """, {"seller": seller, "sku": sku})
            old = float(prev["qty_on_hand"]) if prev else 0.0
            delta = qty - old

            # update absolute qty_on_hand
            await database.execute("""
                UPDATE inventory_items
                   SET qty_on_hand = :new,
                       updated_at  = NOW(),
                       source      = :source
                 WHERE seller=:seller AND sku=:sku
            """, {"new": qty, "source": source, "seller": seller, "sku": sku})

            # movement delta (build JSON in Python)
            meta_json = json.dumps({"from": old, "to": qty})
            try:
                await database.execute("""
                    INSERT INTO inventory_movements (seller, sku, movement_type, qty, ref_contract, meta)
                    VALUES (:seller,:sku,'upsert',:qty,NULL, :meta::jsonb)
                """, {"seller": seller, "sku": sku, "qty": delta, "meta": meta_json})
            except Exception:
                await database.execute("""
                    INSERT INTO inventory_movements (seller, sku, movement_type, qty, ref_contract, meta)
                    VALUES (:seller,:sku,'upsert',:qty,NULL, :meta)
                """, {"seller": seller, "sku": sku, "qty": delta, "meta": meta_json})

    return {"ok": True, "count": len(items)}

# =============== FUTURES (Admin) ===============
futures_router = APIRouter(prefix="/admin/futures", tags=["Futures"])

class ProductIn(BaseModel):
    symbol_root: str
    material: str
    delivery_location: str
    contract_size_tons: float = 20.0
    tick_size: float = 0.5
    currency: str = "USD"
    price_method: Literal["VWAP_BASIS", "MANUAL", "EXTERNAL"] = "VWAP_BASIS"

class ProductOut(ProductIn):
    id: str

class PricingParamsIn(BaseModel):
    lookback_days: int = 14
    basis_adjustment: float = 0.0
    carry_per_month: float = 0.0
    manual_mark: Optional[float] = None
    external_source: Optional[str] = None

class ListingGenerateIn(BaseModel):
    product_id: str
    months_ahead: int = 12
    day_of_month: int = 15

class ListingOut(BaseModel):
    id: str
    product_id: str
    contract_month: str
    contract_year: int
    expiry_date: date
    status: str

class PublishMarkIn(BaseModel):
    listing_id: str
    mark_date: Optional[date] = None

MONTH_CODE = {1:"F",2:"G",3:"H",4:"J",5:"K",6:"M",7:"N",8:"Q",9:"U",10:"V",11:"X",12:"Z"}

async def _compute_mark_for_listing(listing_id: str, mark_date: Optional[_date] = None) -> tuple[float, str]:
    md = mark_date or _date.today()
    listing = await database.fetch_one("""
        SELECT fl.id AS listing_id,
               fl.expiry_date,
               fp.id  AS product_id,
               fp.price_method,
               COALESCE(p.lookback_days, 14)   AS lookback_days,
               COALESCE(p.basis_adjustment, 0) AS basis_adjustment,
               COALESCE(p.carry_per_month, 0)  AS carry_per_month,
               p.manual_mark
        FROM futures_listings fl
        JOIN futures_products fp        ON fp.id = fl.product_id
        LEFT JOIN futures_pricing_params p ON p.product_id = fp.id
        WHERE fl.id = :id AND fl.status IN ('Draft','Listed')
    """, {"id": listing_id})
    if not listing:
        raise HTTPException(404, "listing not found or not listable")

    vwap_row = await database.fetch_one(
        "SELECT recent_vwap FROM v_recent_vwap WHERE product_id=:pid",
        {"pid": listing["product_id"]}
    )
    recent_vwap = vwap_row["recent_vwap"] if vwap_row else None

    m_to_exp = max(
        0,
        (listing["expiry_date"].year - md.year) * 12 + (listing["expiry_date"].month - md.month)
    )

    method = listing["price_method"]
    if method == "MANUAL":
        manual = listing["manual_mark"]
        if manual is None:
            raise HTTPException(400, "manual_mark not set")
        return float(manual), "MANUAL"

    if method == "EXTERNAL":
        raise HTTPException(400, "external pricing not wired")

    if recent_vwap is None:
        raise HTTPException(400, "no recent VWAP in lookback window")
    mark = float(recent_vwap) + float(listing["basis_adjustment"]) + m_to_exp * float(listing["carry_per_month"])
    return float(mark), "VWAP_BASIS"

@futures_router.post("/products", response_model=ProductOut, summary="Create/Upsert a futures product")
async def create_product(p: ProductIn):
    row = await database.fetch_one("""
      INSERT INTO futures_products (id, symbol_root, material, delivery_location, contract_size_tons, tick_size, currency, price_method)
      VALUES (:id,:symbol_root,:material,:delivery_location,:contract_size_tons,:tick_size,:currency,:price_method)
      ON CONFLICT (symbol_root) DO UPDATE SET
        material=EXCLUDED.material,
        delivery_location=EXCLUDED.delivery_location,
        contract_size_tons=EXCLUDED.contract_size_tons,
        tick_size=EXCLUDED.tick_size,
        currency=EXCLUDED.currency,
        price_method=EXCLUDED.price_method
      RETURNING *
    """, {"id": str(uuid.uuid4()), **p.dict()})
    return row

@futures_router.post("/products/{symbol_root}/pricing", summary="Set pricing params")
async def set_pricing(symbol_root: str, body: PricingParamsIn):
    prod = await database.fetch_one("SELECT id FROM futures_products WHERE symbol_root=:s", {"s": symbol_root})
    if not prod:
        raise HTTPException(404, "product not found")
    await database.execute("""
      INSERT INTO futures_pricing_params (product_id, lookback_days, basis_adjustment, carry_per_month, manual_mark, external_source)
      VALUES (:pid,:lookback,:basis,:carry,:manual,:ext)
      ON CONFLICT (product_id) DO UPDATE SET
        lookback_days=EXCLUDED.lookback_days,
        basis_adjustment=EXCLUDED.basis_adjustment,
        carry_per_month=EXCLUDED.carry_per_month,
        manual_mark=EXCLUDED.manual_mark,
        external_source=EXCLUDED.external_source
    """, {"pid": prod["id"], "lookback": body.lookback_days, "basis": body.basis_adjustment,
          "carry": body.carry_per_month, "manual": body.manual_mark, "ext": body.external_source})
    return {"ok": True}

@futures_router.post("/series/generate", response_model=List[ListingOut], summary="Generate monthly listings")
async def generate_series(body: ListingGenerateIn):
    today = _date.today()
    out = []
    for k in range(body.months_ahead):
        y = today.year + (today.month + k - 1)//12
        m = ((today.month + k - 1)%12) + 1
        code = MONTH_CODE[m]
        exp = _date(y, m, min(body.day_of_month, 28))
        row = await database.fetch_one("""
          INSERT INTO futures_listings (id, product_id, contract_month, contract_year, expiry_date, status)
          VALUES (:id,:pid,:cm,:cy,:exp,'Draft')
          ON CONFLICT (product_id, contract_month, contract_year) DO NOTHING
          RETURNING *
        """, {"id": str(uuid.uuid4()), "pid": body.product_id, "cm": code, "cy": y, "exp": exp})
        if row:
            out.append(row)
    return out

@futures_router.post("/series/{listing_id}/list", summary="Publish Draft listing to Listed")
async def list_series(listing_id: str):
    await database.execute("UPDATE futures_listings SET status='Listed' WHERE id=:id", {"id": listing_id})
    return {"ok": True}

class TradingStatusIn(BaseModel):
    trading_status: Literal["Trading","Halted","Expired"]

@futures_router.post("/series/{listing_id}/trading_status", summary="Set listing trading status")
async def set_trading_status(listing_id: str, body: TradingStatusIn):
    await database.execute("UPDATE futures_listings SET trading_status=:st WHERE id=:id",
                           {"st": body.trading_status, "id": listing_id})
    return {"ok": True, "listing_id": listing_id, "trading_status": body.trading_status}

@futures_router.post("/series/{listing_id}/expire", summary="Expire listing (cash-settled)")
async def expire_listing(listing_id: str):
    await database.execute("""
      UPDATE futures_listings SET trading_status='Expired', status='Expired' WHERE id=:id
    """, {"id": listing_id})
    return {"ok": True, "listing_id": listing_id, "status": "Expired"}

@futures_router.post("/marks/publish", summary="Publish an official settlement/mark for a listing")
async def publish_mark(body: PublishMarkIn):
    mark_price, method = await _compute_mark_for_listing(body.listing_id, body.mark_date)
    await database.execute("""
      INSERT INTO futures_marks (id, listing_id, mark_date, mark_price, method)
      VALUES (:id,:lid,:dt,:px,:m)
    """, {"id": str(uuid.uuid4()), "lid": body.listing_id, "dt": (body.mark_date or _date.today()), "px": mark_price, "m": method})
    return {"listing_id": body.listing_id, "mark_date": str(body.mark_date or _date.today()), "mark_price": mark_price, "method": method}

@futures_router.get("/products", response_model=List[ProductOut], summary="List products")
async def list_products():
    rows = await database.fetch_all("SELECT * FROM futures_products ORDER BY symbol_root")
    return rows

@futures_router.get("/series", response_model=List[ListingOut], summary="List series")
async def list_series_admin(product_id: Optional[str] = Query(None), status: Optional[str] = Query(None)):
    q = "SELECT * FROM futures_listings WHERE 1=1"
    vals = {}
    if product_id:
        q += " AND product_id=:pid"; vals["pid"] = product_id
    if status:
        q += " AND status=:st"; vals["st"] = status
    q += " ORDER BY contract_year, array_position(ARRAY['F','G','H','J','K','M','N','Q','U','V','X','Z'], contract_month::text)"
    return await database.fetch_all(q, vals)

@futures_router.get("/marks", summary="List recent marks (join product + listing)", tags=["Futures"])
async def list_recent_marks(limit: int = Query(50, ge=1, le=500)):
    rows = await database.fetch_all("""
      SELECT
        fp.symbol_root,
        fp.material,
        fl.contract_month,
        fl.contract_year,
        fm.mark_date,
        fm.mark_price,
        fm.method
      FROM futures_marks fm
      JOIN futures_listings fl ON fl.id = fm.listing_id
      JOIN futures_products fp ON fp.id = fl.product_id
      ORDER BY fm.mark_date DESC, fl.contract_year DESC,
        array_position(ARRAY['F','G','H','J','K','M','N','Q','U','V','X','Z'], fl.contract_month::text) DESC
      LIMIT :lim
    """, {"lim": limit})
    return [dict(r) for r in rows]

app.include_router(futures_router)
# =============== /FUTURES (Admin) ===============

# ===================== TRADING (Order Book) =====================
trade_router = APIRouter(prefix="/trade", tags=["Trading"])

class OrderIn(BaseModel):
    account_id: str
    listing_id: str
    side: Literal["BUY","SELL"]
    price: Optional[float] = None
    qty: float
    order_type: Literal["LIMIT","MARKET"] = "LIMIT"
    tif: Optional[str] = "GTC"

class ModifyOrderIn(BaseModel):
    price: Optional[float] = None
    qty: Optional[float] = None

class CancelOut(BaseModel):
    id: str
    status: str

class BookLevel(BaseModel):
    price: float
    qty: float

class BookSnapshot(BaseModel):
    bids: List[BookLevel]
    asks: List[BookLevel]

async def _get_contract_size(listing_id: str) -> float:
    row = await database.fetch_one("""
      SELECT fp.contract_size_tons
      FROM futures_listings fl
      JOIN futures_products fp ON fp.id = fl.product_id
      WHERE fl.id = :id
    """, {"id": listing_id})
    if not row:
        raise HTTPException(404, "listing not found")
    return float(row["contract_size_tons"])

async def _ensure_margin_account(account_id: str):
    row = await database.fetch_one("SELECT account_id FROM margin_accounts WHERE account_id=:a", {"a": account_id})
    if not row:
        await database.execute("INSERT INTO margin_accounts (account_id, balance) VALUES (:a, 0)", {"a": account_id})

async def _get_margin_params(account_id: str) -> dict:
    r = await database.fetch_one("""
      SELECT balance, initial_pct, maintenance_pct, risk_limit_open_lots, is_blocked
      FROM margin_accounts WHERE account_id=:a
    """, {"a": account_id})
    if not r:
        raise HTTPException(400, "margin account missing")
    return dict(r)

async def _adjust_margin(account_id: str, amount: float, reason: str, ref_id: Optional[str] = None):
    await database.execute("UPDATE margin_accounts SET balance = balance + :amt WHERE account_id=:a",
                           {"amt": amount, "a": account_id})
    await database.execute("""
      INSERT INTO margin_events (id, account_id, amount, reason, ref_id)
      VALUES (:id,:a,:amt,:rsn,:ref)
    """, {"id": str(uuid.uuid4()), "a": account_id, "amt": amount, "rsn": reason, "ref": ref_id})

async def _update_position(account_id: str, listing_id: str, delta_qty: float):
    await database.execute("""
      INSERT INTO positions (account_id, listing_id, net_qty)
      VALUES (:a,:l,:dq)
      ON CONFLICT (account_id, listing_id) DO UPDATE
      SET net_qty = positions.net_qty + EXCLUDED.net_qty
    """, {"a": account_id, "l": listing_id, "dq": delta_qty})

async def _sum_open_lots(account_id: str) -> float:
    r = await database.fetch_one("""
      SELECT COALESCE(SUM(qty_open),0) AS open_lots
      FROM orders
      WHERE account_id=:a AND status IN ('NEW','PARTIAL')
    """, {"a": account_id})
    return float(r["open_lots"] or 0)

async def _audit_order(order_id: str, event: str, qty_open: float, reason: Optional[str] = None):
    await database.execute("""
      INSERT INTO orders_audit (id, order_id, event, qty_open, reason)
      VALUES (:id,:oid,:ev,:qo,:rsn)
    """, {"id": str(uuid.uuid4()), "oid": order_id, "ev": event, "qo": qty_open, "rsn": reason or ""})

async def _require_listing_trading(listing_id: str):
    r = await database.fetch_one("SELECT trading_status FROM futures_listings WHERE id=:id", {"id": listing_id})
    if not r:
        raise HTTPException(404, "listing not found")
    if r["trading_status"] != "Trading":
        raise HTTPException(423, f"listing not in Trading status (is {r['trading_status']})")

@trade_router.post("/orders", summary="Place order (limit or market) and match")
async def place_order(ord_in: OrderIn):
    await _ensure_margin_account(ord_in.account_id)
    contract_size = await _get_contract_size(ord_in.listing_id)

    order_id = str(uuid.uuid4())
    side = ord_in.side.upper()
    is_market = (ord_in.order_type == "MARKET")
    price = float(ord_in.price) if (ord_in.price is not None) else (0.0 if is_market and side == "SELL" else float("inf"))
    qty   = float(ord_in.qty)
    if qty <= 0:
        raise HTTPException(400, "qty must be positive")
    if not is_market and (ord_in.price is None or float(ord_in.price) <= 0):
        raise HTTPException(400, "price must be positive for LIMIT orders")

    async with database.transaction():
        await database.execute("""
          INSERT INTO orders (id, account_id, listing_id, side, price, qty, qty_open, status, tif)
          VALUES (:id,:a,:l,:s,:p,:q,:q,'NEW',:tif)
        """, {"id": order_id, "a": ord_in.account_id, "l": ord_in.listing_id,
              "s": side, "p": (0 if is_market else price), "q": qty, "tif": ord_in.tif or "GTC"})
        await _audit_order(order_id, "NEW", qty, "order accepted")

        await _require_listing_trading(ord_in.listing_id)

        params = await _get_margin_params(ord_in.account_id)
        if params.get("is_blocked"):
            raise HTTPException(402, "account blocked pending margin call")
        open_lots = await _sum_open_lots(ord_in.account_id)
        limit_lots = float(params.get("risk_limit_open_lots", 50))
        if open_lots + qty > limit_lots:
            raise HTTPException(429, f"risk limit exceeded: {open_lots}+{qty}>{limit_lots}")

        remaining = qty

        if side == "BUY":
            if is_market:
                opp = await database.fetch_all("""
                  SELECT * FROM orders
                   WHERE listing_id=:l AND side='SELL' AND status IN ('NEW','PARTIAL')
                   ORDER BY price ASC, created_at ASC
                   FOR UPDATE
                """, {"l": ord_in.listing_id})
            else:
                opp = await database.fetch_all("""
                  SELECT * FROM orders
                   WHERE listing_id=:l AND side='SELL' AND status IN ('NEW','PARTIAL') AND price <= :p
                   ORDER BY price ASC, created_at ASC
                   FOR UPDATE
                """, {"l": ord_in.listing_id, "p": price})
        else:
            if is_market:
                opp = await database.fetch_all("""
                  SELECT * FROM orders
                   WHERE listing_id=:l AND side='BUY' AND status IN ('NEW','PARTIAL')
                   ORDER BY price DESC, created_at ASC
                   FOR UPDATE
                """, {"l": ord_in.listing_id})
            else:
                opp = await database.fetch_all("""
                  SELECT * FROM orders
                   WHERE listing_id=:l AND side='BUY' AND status IN ('NEW','PARTIAL') AND price >= :p
                   ORDER BY price DESC, created_at ASC
                   FOR UPDATE
                """, {"l": ord_in.listing_id, "p": price})

        for row in opp:
            if remaining <= 0:
                break
            open_qty = float(row["qty_open"])
            if open_qty <= 0:
                continue

            trade_qty = min(remaining, open_qty)
            trade_px  = float(row["price"])

            buy_id  = order_id if side == "BUY" else row["id"]
            sell_id = row["id"]   if side == "BUY" else order_id

            notional = trade_px * contract_size * trade_qty

            part_a = await database.fetch_one("""
              SELECT o.account_id AS aid, m.balance, m.initial_pct
              FROM orders o JOIN margin_accounts m ON m.account_id = o.account_id
              WHERE o.id=:oid
            """, {"oid": order_id})
            part_b = await database.fetch_one("""
              SELECT o.account_id AS aid, m.balance, m.initial_pct
              FROM orders o JOIN margin_accounts m ON m.account_id = o.account_id
              WHERE o.id=:oid
            """, {"oid": row["id"]})
            if not part_a or not part_b:
                raise HTTPException(400, "margin account not found")

            need_a = float(part_a["initial_pct"]) * notional
            need_b = float(part_b["initial_pct"]) * notional
            if float(part_a["balance"]) < need_a:
                raise HTTPException(402, f"insufficient initial margin for account {part_a['aid']}")
            if float(part_b["balance"]) < need_b:
                raise HTTPException(402, f"insufficient initial margin for account {part_b['aid']}")

            await _adjust_margin(part_a["aid"], -need_a, "initial_margin", order_id)
            await _adjust_margin(part_b["aid"], -need_b, "initial_margin", row["id"])

            trade_id = str(uuid.uuid4())
            await database.execute("""
              INSERT INTO trades (id, buy_order_id, sell_order_id, listing_id, price, qty)
              VALUES (:id,:b,:s,:l,:px,:q)
            """, {"id": trade_id, "b": buy_id, "s": sell_id, "l": ord_in.listing_id, "px": trade_px, "q": trade_qty})

            new_open = open_qty - trade_qty
            new_status = "FILLED" if new_open <= 0 else "PARTIAL"
            await database.execute("UPDATE orders SET qty_open=:qo, status=:st WHERE id=:id",
                                   {"qo": new_open, "st": new_status, "id": row["id"]})
            await _audit_order(row["id"], new_status, new_open, "matched")

            remaining -= trade_qty
            my_status = "FILLED" if remaining <= 0 else "PARTIAL"
            await database.execute("UPDATE orders SET qty_open=:qo, status=:st WHERE id=:id",
                                   {"qo": remaining, "st": my_status, "id": order_id})
            await _audit_order(order_id, my_status, remaining, "matched")

        if is_market and remaining > 0:
            await database.execute("UPDATE orders SET qty_open=0, status='CANCELLED' WHERE id=:id", {"id": order_id})
            await _audit_order(order_id, "CANCELLED", 0, "market IOC remainder")

    final = await database.fetch_one("SELECT * FROM orders WHERE id=:id", {"id": order_id})
    return dict(final)

@trade_router.patch("/orders/{order_id}", summary="Modify order (price/qty) and rematch")
async def modify_order(order_id: str, body: ModifyOrderIn):
    if body.price is None and body.qty is None:
        raise HTTPException(400, "provide price and/or qty to modify")

    async with database.transaction():
        row = await database.fetch_one("SELECT * FROM orders WHERE id=:id FOR UPDATE", {"id": order_id})
        if not row:
            raise HTTPException(404, "order not found")
        if row["status"] in ("FILLED","CANCELLED"):
            raise HTTPException(409, f"order is {row['status']} and cannot be modified")

        account_id = row["account_id"]
        listing_id = row["listing_id"]
        side = row["side"]
        old_price = float(row["price"])
        old_qty   = float(row["qty"])
        old_open  = float(row["qty_open"])
        filled    = old_qty - old_open

        new_total = float(body.qty) if body.qty is not None else old_qty
        if new_total < filled:
            raise HTTPException(400, f"qty too low; already filled {filled}")

        new_price = float(body.price) if body.price is not None else old_price
        new_open  = new_total - filled

        delta_open = new_open - old_open
        if delta_open > 0:
            params = await _get_margin_params(account_id)
            if params.get("is_blocked"):
                raise HTTPException(402, "account blocked pending margin call")
            open_lots = await _sum_open_lots(account_id)
            limit_lots = float(params.get("risk_limit_open_lots", 50))
            if open_lots + delta_open > limit_lots:
                raise HTTPException(429, f"risk limit exceeded: {open_lots}+{delta_open}>{limit_lots}")

        new_status = "FILLED" if new_open <= 0 else ("PARTIAL" if filled > 0 else "NEW")
        await database.execute("""
          UPDATE orders SET price=:p, qty=:qt, qty_open=:qo, status=:st WHERE id=:id
        """, {"p": new_price, "qt": new_total, "qo": new_open, "st": new_status, "id": order_id})
        await _audit_order(order_id, "MODIFY", new_open, "amend price/qty")

        remaining = new_open
        if remaining > 0:
            if side == "BUY":
                opp = await database.fetch_all("""
                  SELECT * FROM orders
                   WHERE listing_id=:l AND side='SELL' AND status IN ('NEW','PARTIAL') AND price <= :p AND id <> :me
                   ORDER BY price ASC, created_at ASC
                   FOR UPDATE
                """, {"l": listing_id, "p": new_price, "me": order_id})
            else:
                opp = await database.fetch_all("""
                  SELECT * FROM orders
                   WHERE listing_id=:l AND side='BUY' AND status IN ('NEW','PARTIAL') AND price >= :p AND id <> :me
                   ORDER BY price DESC, created_at ASC
                   FOR UPDATE
                """, {"l": listing_id, "p": new_price, "me": order_id})

            for r in opp:
                if remaining <= 0:
                    break
                open_qty = float(r["qty_open"])
                if open_qty <= 0:
                    continue

                trade_qty = min(remaining, open_qty)
                trade_px  = float(r["price"])

                buy_id  = order_id if side == "BUY" else r["id"]
                sell_id = r["id"]   if side == "BUY" else order_id

                notional = trade_px * (await _get_contract_size(listing_id)) * trade_qty

                part_a = await database.fetch_one("""
                  SELECT o.account_id AS aid, m.balance, m.initial_pct
                  FROM orders o JOIN margin_accounts m ON m.account_id = o.account_id
                  WHERE o.id=:oid
                """, {"oid": order_id})
                part_b = await database.fetch_one("""
                  SELECT o.account_id AS aid, m.balance, m.initial_pct
                  FROM orders o JOIN margin_accounts m ON m.account_id = o.account_id
                  WHERE o.id=:oid
                """, {"oid": r["id"]})
                if not part_a or not part_b:
                    raise HTTPException(400, "margin account not found")

                need_a = float(part_a["initial_pct"]) * notional
                need_b = float(part_b["initial_pct"]) * notional
                if float(part_a["balance"]) < need_a:
                    raise HTTPException(402, f"insufficient initial margin for account {part_a['aid']}")
                if float(part_b["balance"]) < need_b:
                    raise HTTPException(402, f"insufficient initial margin for account {part_b['aid']}")

                await _adjust_margin(part_a["aid"], -need_a, "initial_margin", order_id)
                await _adjust_margin(part_b["aid"], -need_b, "initial_margin", r["id"])

                trade_id = str(uuid.uuid4())
                await database.execute("""
                  INSERT INTO trades (id, buy_order_id, sell_order_id, listing_id, price, qty)
                  VALUES (:id,:b,:s,:l,:px,:q)
                """, {"id": trade_id, "b": buy_id, "s": sell_id, "l": listing_id, "px": trade_px, "q": trade_qty})

                r_new_open = open_qty - trade_qty
                r_status = "FILLED" if r_new_open <= 0 else "PARTIAL"
                await database.execute("UPDATE orders SET qty_open=:qo, status=:st WHERE id=:id",
                                       {"qo": r_new_open, "st": r_status, "id": r["id"]})
                await _audit_order(r["id"], r_status, r_new_open, "matched")

                remaining -= trade_qty
                my_status = "FILLED" if remaining <= 0 else ("PARTIAL" if filled > 0 or new_total != remaining else "NEW")
                await database.execute("UPDATE orders SET qty_open=:qo, status=:st WHERE id=:id",
                                       {"qo": remaining, "st": my_status, "id": order_id})
                await _audit_order(order_id, my_status, remaining, "matched")

    final = await database.fetch_one("SELECT * FROM orders WHERE id=:id", {"id": order_id})
    return dict(final)

@trade_router.delete("/orders/{order_id}", response_model=CancelOut, summary="Cancel remaining qty")
async def cancel_order(order_id: str):
    async with database.transaction():
        row = await database.fetch_one("SELECT status, qty_open FROM orders WHERE id=:id FOR UPDATE", {"id": order_id})
        if not row:
            raise HTTPException(404, "order not found")
        if row["status"] in ("FILLED","CANCELLED") or float(row["qty_open"]) <= 0:
            return {"id": order_id, "status": row["status"]}
        await database.execute("UPDATE orders SET status='CANCELLED' WHERE id=:id", {"id": order_id})
        await _audit_order(order_id, "CANCELLED", float(row["qty_open"]), "manual cancel")
    return {"id": order_id, "status": "CANCELLED"}

@trade_router.get("/book", response_model=BookSnapshot, summary="Order book snapshot (top levels)")
async def get_book(listing_id: str, depth: int = Query(5, ge=1, le=50)):
    bids = await database.fetch_all("""
      SELECT price, SUM(qty_open) AS qty
        FROM orders
       WHERE listing_id=:l AND side='BUY' AND status IN ('NEW','PARTIAL')
       GROUP BY price ORDER BY price DESC LIMIT :d
    """, {"l": listing_id, "d": depth})
    asks = await database.fetch_all("""
      SELECT price, SUM(qty_open) AS qty
        FROM orders
       WHERE listing_id=:l AND side='SELL' AND status IN ('NEW','PARTIAL')
       GROUP BY price ORDER BY price ASC LIMIT :d
    """, {"l": listing_id, "d": depth})
    return {
        "bids": [{"price": float(r["price"]), "qty": float(r["qty"])} for r in bids],
        "asks": [{"price": float(r["price"]), "qty": float(r["qty"])} for r in asks],
    }

# ---- Exports (CSV) ----
@trade_router.get("/orders/export", summary="Export orders by day (CSV)")
async def export_orders_csv(day: str = Query(..., description="YYYY-MM-DD")):
    rows = await database.fetch_all("""
      SELECT * FROM orders
      WHERE DATE(created_at) = :d
      ORDER BY created_at, id
    """, {"d": day})
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["id","account_id","listing_id","side","price","qty","qty_open","status","tif","created_at"])
    for r in rows:
        w.writerow([r["id"], r["account_id"], r["listing_id"], r["side"], r["price"],
                    r["qty"], r["qty_open"], r["status"], r["tif"], r["created_at"].isoformat()])
    out.seek(0)
    return StreamingResponse(
        iter([out.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="orders_{day}.csv"'}
    )

@trade_router.get("/trades/export", summary="Export trades by day (CSV)")
async def export_trades_csv(day: str = Query(..., description="YYYY-MM-DD")):
    rows = await database.fetch_all("""
      SELECT t.*, fp.symbol_root, fl.contract_month, fl.contract_year
      FROM trades t
      JOIN futures_listings fl ON fl.id = t.listing_id
      JOIN futures_products fp ON fp.id = fl.product_id
      WHERE DATE(t.traded_at) = :d
      ORDER BY t.traded_at, t.id
    """, {"d": day})
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["id","buy_order_id","sell_order_id","listing_id","symbol","month","year","price","qty","traded_at"])
    for r in rows:
        w.writerow([r["id"], r["buy_order_id"], r["sell_order_id"], r["listing_id"],
                    r["symbol_root"], r["contract_month"], r["contract_year"],
                    r["price"], r["qty"], r["traded_at"].isoformat()])
    out.seek(0)
    return StreamingResponse(
        iter([out.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="trades_{day}.csv"'}
    )

app.include_router(trade_router)
# =================== /TRADING (Order Book) =====================

# ===================== CLEARING (Margin & Variation) =====================
clearing_router = APIRouter(prefix="/clearing", tags=["Clearing"])

class DepositIn(BaseModel):
    account_id: str
    amount: float

@clearing_router.get("/positions", summary="Positions by account")
async def get_positions(account_id: str):
    rows = await database.fetch_all("""
      SELECT p.listing_id, p.net_qty,
             fp.symbol_root, fl.contract_month, fl.contract_year
        FROM positions p
        JOIN futures_listings fl ON fl.id = p.listing_id
        JOIN futures_products fp ON fp.id = fl.product_id
       WHERE p.account_id=:a
    ORDER BY fp.symbol_root, fl.contract_year,
      array_position(ARRAY['F','G','H','J','K','M','N','Q','U','V','X','Z'], fl.contract_month::text)
    """, {"a": account_id})
    return [dict(r) for r in rows]

@clearing_router.get("/margin", summary="Margin account state")
async def get_margin(account_id: str):
    r = await database.fetch_one("SELECT * FROM margin_accounts WHERE account_id=:a", {"a": account_id})
    if not r:
        raise HTTPException(404, "margin account not found")
    return dict(r)

@clearing_router.post("/deposit", summary="Credit margin account (testing/funding)")
async def deposit(dep: DepositIn):
    if dep.amount == 0:
        return {"ok": True, "note": "no-op"}
    await _ensure_margin_account(dep.account_id)
    await _adjust_margin(dep.account_id, dep.amount, "deposit", None)
    return {"ok": True}

class VariationRunIn(BaseModel):
    mark_date: Optional[date] = None

@clearing_router.post("/variation_run", summary="Run daily variation margin across all accounts")
async def run_variation(body: VariationRunIn):
    dt = body.mark_date or _date.today()

    pos = await database.fetch_all("""
      SELECT p.account_id, p.listing_id, p.net_qty,
             COALESCE(
               (SELECT fm.mark_price FROM futures_marks fm
                 WHERE fm.listing_id = p.listing_id AND fm.mark_date <= :d
                 ORDER BY fm.mark_date DESC LIMIT 1),
               (SELECT settle_price FROM v_latest_settle v WHERE v.listing_id = p.listing_id)
             ) AS settle_price,
             fp.contract_size_tons,
             m.balance, m.maintenance_pct
        FROM positions p
        JOIN futures_listings fl ON fl.id = p.listing_id
        JOIN futures_products fp ON fp.id = fl.product_id
        JOIN margin_accounts m   ON m.account_id = p.account_id
    """, {"d": dt})

    prev_map = {}
    prev = await database.fetch_all("""
      SELECT DISTINCT ON (fm.listing_id) fm.listing_id, fm.mark_price
        FROM futures_marks fm
       WHERE fm.mark_date < :d
    ORDER BY fm.listing_id, fm.mark_date DESC
    """, {"d": dt})
    for r in prev:
        prev_map[str(r["listing_id"])] = float(r["mark_price"])

    results = []
    async with database.transaction():
        for r in pos:
            lst = str(r["listing_id"])
            px_t = float(r["settle_price"]) if r["settle_price"] is not None else None
            px_y = prev_map.get(lst)
            if px_t is None or px_y is None:
                continue

            qty  = float(r["net_qty"])
            size = float(r["contract_size_tons"])
            pnl  = (px_t - px_y) * qty * size

            if pnl != 0.0:
                await _adjust_margin(str(r["account_id"]), pnl, "variation_margin", None)

            req = abs(qty) * px_t * size * float(r["maintenance_pct"])
            cur = await database.fetch_one("SELECT balance FROM margin_accounts WHERE account_id=:a", {"a": r["account_id"]})
            cur_bal = float(cur["balance"]) if cur else 0.0

            if cur_bal < req:
                await _adjust_margin(str(r["account_id"]), 0.0, f"margin_call_required: need >= {req:.2f}", None)
                await database.execute("UPDATE margin_accounts SET is_blocked=TRUE WHERE account_id=:a", {"a": r["account_id"]})
            else:
                await database.execute("UPDATE margin_accounts SET is_blocked=FALSE WHERE account_id=:a", {"a": r["account_id"]})

            results.append({
                "account_id": str(r["account_id"]),
                "listing_id": lst,
                "qty": qty,
                "prev_settle": px_y,
                "settle": px_t,
                "variation_pnl": pnl,
                "maintenance_required": req,
                "balance_after": cur_bal
            })

    return {"mark_date": str(dt), "accounts_processed": len(results), "details": results}

app.include_router(clearing_router)
# =================== /CLEARING (Margin & Variation) =====================
