# --- ensure sibling modules are importable in CI/pytest ---
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.resolve()))
# ----------------------------------------------------------
from fastapi import FastAPI, HTTPException, Request, Depends, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse, Response, StreamingResponse, JSONResponse, PlainTextResponse
from fastapi.testclient import TestClient
from pydantic import BaseModel, EmailStr, Field 
from typing import List, Optional, Literal
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
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from fastapi import UploadFile, File, Form
from fastapi import APIRouter
from datetime import date as _date, date, timedelta
import asyncio
import inspect
from fastapi import Request
from typing import Iterable
from statistics import mean, stdev
from collections import defaultdict
from typing import Optional
from fastapi import HTTPException
import io, os, csv, zipfile, datetime
from typing import Dict, Any, List
import httpx
from fastapi import BackgroundTasks
from datetime import datetime
from zoneinfo import ZoneInfo
from babel.numbers import format_currency as _babel_fmt  # optional; will fallback if not installed
from typing import Literal
from fastapi import Header, HTTPException
import gzip
from decimal import InvalidOperation
from pricing_engine import compute_material_price
from price_sources import pull_comexlive_once, latest_price
from price_sources import pull_comex_home_once
from forecast_job import run_all as _forecast_run_all
from indices_builder import run_indices_builder
from price_sources import pull_comexlive_once, pull_lme_once, pull_comex_home_once, latest_price

# ===== middleware & observability deps =====
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
import structlog, time

# rate limiting (imports only here; init happens after ProxyHeaders)
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

# --- EXCHANGE ADJACENT ADD-ONS: imports (place near other FastAPI imports) ---
from fastapi import WebSocket, WebSocketDisconnect
from collections import defaultdict, deque
from typing import Tuple, Set

# metrics & errors
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter
METRICS_CONTRACTS_CREATED = Counter("bridge_contracts_created_total", "Contracts created")
METRICS_BOLS_CREATED      = Counter("bridge_bols_created_total", "BOLs created")
METRICS_INDICES_SNAPSHOTS = Counter("bridge_indices_snapshots_total", "Index snapshots generated")
import sentry_sdk

_PRICE_CACHE = {"copper_last": None, "ts": 0}
PRICE_TTL_SEC = 300  # 5 minutes

_KILL_SWITCH: Dict[str, bool] = defaultdict(bool)          # member_id -> True/False
_PRICE_BANDS: Dict[str, Tuple[float, float]] = {}          # symbol -> (lower, upper)
_LULD: Dict[str, Tuple[float, float]] = {}                 # symbol -> (down_pct, up_pct)
_ENTITLEMENTS: Dict[str, Set[str]] = defaultdict(set)      # username/member -> features set

# simple market-data feed state (used by WebSocket endpoint)
_md_subs: Set[WebSocket] = set()
_md_seq: int = 1

# instrument registry (example; persist later)
_INSTRUMENTS = {
    # symbol: lot_size (tons), tick_size ($/lb), description
    "CU-SHRED-1M": {"lot": 20.0, "tick": 0.0005, "desc": "Copper Shred 1-Month"},
    "AL-6061-1M": {"lot": 20.0, "tick": 0.0005, "desc": "Al 6061 1-Month"},
}

load_dotenv()

# ---- SaaS soft quotas (optional) ----
MAX_CONTRACTS_PER_DAY = int(os.getenv("MAX_CONTRACTS_PER_DAY", "0"))  # 0 = disabled

async def _check_contract_quota():
    if MAX_CONTRACTS_PER_DAY <= 0:
        return
    row = await database.fetch_one(
        "SELECT COUNT(*) AS c FROM contracts WHERE created_at >= NOW() - INTERVAL '1 day'"
    )
    if int(row["c"]) >= MAX_CONTRACTS_PER_DAY:
        raise HTTPException(429, "daily contract quota exceeded; contact sales")
# -------------------------------------

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

# Move docs under /api/v1 but keep existing endpoints 
app.docs_url = "/api/v1/docs"
app.redoc_url = None
app.openapi_url = "/api/v1/openapi.json"

# ===== Trusted hosts + session cookie =====
allowed = ["scrapfutures.com", "www.scrapfutures.com", "bridge.scrapfutures.com", "bridge-buyer.onrender.com"]

prod = os.getenv("ENV", "development").lower() == "production"
allow_local = os.getenv("ALLOW_LOCALHOST_IN_PROD", "") in ("1", "true", "yes")
if not prod or allow_local:
    allowed += ["localhost", "127.0.0.1", "testserver", "0.0.0.0"]

app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET", "dev-only-secret"),
    https_only=prod,
    same_site="lax",
    max_age=60*60*8,
)

# Make client IP visible behind proxies/CDN so SlowAPI uses the real address
if prod:
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed)

# =====  rate limiting =====
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

@app.exception_handler(RateLimitExceeded)
async def ratelimit_handler(request, exc):
    return PlainTextResponse("Too Many Requests", status_code=429)

SNAPSHOT_AUTH = os.getenv("SNAPSHOT_AUTH", "")

@app.post("/admin/run_snapshot_bg", tags=["Admin"], summary="Queue a snapshot upload (background)")
async def admin_run_snapshot_bg(background: BackgroundTasks, storage: str = "supabase", x_auth: str = Header(default="")):
    if SNAPSHOT_AUTH and x_auth != SNAPSHOT_AUTH:
        raise HTTPException(401, "bad auth")
    background.add_task(run_daily_snapshot, storage)
    return {"ok": True, "queued": True}

# === Prices endpoint ===
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

# ===== DB bootstrap for CI/staging =====
import sqlalchemy
from sqlalchemy import text as _sqltext

def _bootstrap_schema_if_needed(engine: sqlalchemy.engine.Engine) -> None:
    """Create minimal tables needed for app/tests when ENV is non-prod."""
    ddl = """
    CREATE EXTENSION IF NOT EXISTS "pgcrypto";
    CREATE TABLE IF NOT EXISTS public.users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL CHECK (role IN ('admin','buyer','seller')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)
        # seed an admin user if none exists (bcrypt is generated in Python)
        try:
            default_email = "admin@example.com"
            default_pass = bcrypt.hash("admin123!")  # only used in CI/staging
            conn.execute(
                _sqltext(
                    "INSERT INTO public.users (email, password_hash, role) "
                    "VALUES (:e, :p, 'admin') "
                    "ON CONFLICT (email) DO NOTHING"
                ),
                {"e": default_email, "p": default_pass},
            )
        except Exception:
            pass
# ===== /DB bootstrap =====

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

@app.middleware("http")
async def hsts_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains; preload"
    return response
# ===== /Security headers =====

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

@app.get("/indices-dashboard", include_in_schema=False)
async def indices_page():
    return FileResponse("static/indices.html")

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

# -------- Ensure public.users has id/email/username/is_active --------
@app.on_event("startup")
async def _ensure_users_columns():
    # Make sure gen_random_uuid() is available
    try:
        await database.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    except Exception:
        pass

    # Add columns if they don't exist
    try:
        await database.execute("ALTER TABLE public.users ADD COLUMN IF NOT EXISTS id UUID DEFAULT gen_random_uuid();")
    except Exception:
        pass

    try:
        await database.execute("ALTER TABLE public.users ADD COLUMN IF NOT EXISTS email TEXT;")
    except Exception:
        pass

    try:
        await database.execute("ALTER TABLE public.users ADD COLUMN IF NOT EXISTS username TEXT;")
    except Exception:
        pass

    try:
        await database.execute("ALTER TABLE public.users ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;")
    except Exception:
        pass

    # Uniqueness / PK-ish constraints 
    try:
        await database.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_users_id ON public.users(id);")
    except Exception:
        pass

    # Lowercased uniqueness on username/email when present
    try:
        await database.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_users_username_lower ON public.users ((lower(username))) WHERE username IS NOT NULL;")
    except Exception:
        pass

    try:
        await database.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_users_email_lower ON public.users ((lower(email))) WHERE email IS NOT NULL;")
    except Exception:
        pass
# ------------------------------------------------------------------------------

# -------- Health --------
@app.get("/healthz", tags=["Health"], summary="Health Check")
async def healthz():
    try:
        await database.execute("SELECT NOW()")
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "degraded", "error": str(e)})
    
@app.get("/__diag/users_count", tags=["Health"])
async def diag_users_count():
    try:
        row = await database.fetch_one("SELECT COUNT(*) AS c FROM public.users")
        return {"users": int(row["c"])}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# --- Pricing & Indices wiring (drop-in) --------------------------------------
import os, asyncio
from datetime import datetime, timedelta
import asyncpg
from fastapi import APIRouter, HTTPException
from sqlalchemy import create_engine
import databases  

# Your modules
from pricing_engine import compute_material_price
from price_sources import (
    pull_comexlive_once,
    pull_lme_once,
    pull_comex_home_once,  
    latest_price,
)
# Nightly index builder
from indices_builder import run_indices_builder as indices_generate_snapshot
# -----------------------------------------------------------------------------

# -------- Database setup (sync psycopg3 + async asyncpg) -----------
BASE_DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# Normalize legacy scheme
if BASE_DATABASE_URL.startswith("postgres://"):
    BASE_DATABASE_URL = BASE_DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Driver-specific DSNs
SYNC_DATABASE_URL = BASE_DATABASE_URL
if SYNC_DATABASE_URL.startswith("postgresql://") and "+psycopg" not in SYNC_DATABASE_URL and "+asyncpg" not in SYNC_DATABASE_URL:
    SYNC_DATABASE_URL = SYNC_DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

ASYNC_DATABASE_URL = BASE_DATABASE_URL
if ASYNC_DATABASE_URL.startswith("postgresql+psycopg://"):
    ASYNC_DATABASE_URL = ASYNC_DATABASE_URL.replace("postgresql+psycopg://", "postgresql://", 1)
if ASYNC_DATABASE_URL.startswith("postgresql+asyncpg://"):
    ASYNC_DATABASE_URL = ASYNC_DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://", 1)

# Instantiate clients
engine = create_engine(SYNC_DATABASE_URL, pool_pre_ping=True, future=True)
database = databases.Database(ASYNC_DATABASE_URL)

# Optional one-time sanity log 
try:
    _sync_tail  = SYNC_DATABASE_URL.split("@")[-1]
    _async_tail = ASYNC_DATABASE_URL.split("@")[-1]
    print(f"[DB] sync={_sync_tail}  async={_async_tail}")
except Exception:
    pass

@app.on_event("startup")
async def _startup_prices():
    # asyncpg pool uses the async DSN (no +psycopg)
    if not hasattr(app.state, "db_pool"):
        app.state.db_pool = await asyncpg.create_pool(ASYNC_DATABASE_URL, max_size=10)
# -------------------------------------------------------------------
# -------- Pricing & Indices Routers (drop-in) -------------------------
async def _fetch_base(symbol: str):
    return await latest_price(app.state.db_pool, symbol)

# Routers
router_prices = APIRouter(prefix="/reference_prices", tags=["Reference Prices"])
router_pricing = APIRouter(prefix="/pricing", tags=["Pricing"])
router_fc = APIRouter(prefix="/forecasts", tags=["Forecasts"])
router_idx = APIRouter(prefix="/indices", tags=["Indices"])

@router_prices.post("/pull_now_all", summary="Pull COMEX & LME reference prices (best-effort)")
async def pull_now_all():
    await pull_comex_home_once(app.state.db_pool)
    await pull_comexlive_once(app.state.db_pool)
    await pull_lme_once(app.state.db_pool)
    return {"ok": True}

@router_prices.get("/latest", summary="Get latest stored reference price")
async def get_latest(symbol: str):
    row = await latest_price(app.state.db_pool, symbol)
    if not row:
        raise HTTPException(status_code=404, detail="No price yet for symbol")
    return row

@router_prices.post("/pull_home", summary="Pull COMEX homepage snapshot (best-effort)")
async def pull_home():
    await pull_comex_home_once(app.state.db_pool)
    return {"ok": True}

@router_pricing.get("/quote", summary="Compute material price using internal formulas")
async def quote(category: str, material: str):
    price = await compute_material_price(_fetch_base, category, material)
    if price is None:
        raise HTTPException(status_code=404, detail="No price available for that category/material")
    return {
        "category": category,
        "material": material,
        "price_per_lb": round(float(price), 4),
        "notes": "Internal-only; bases use COMEX/LME with Cu rule (COMEX − $0.10).",
    }

@router_fc.post("/run", summary="Run nightly forecast for all symbols")
async def run_forecast_now():
    await _forecast_run_all()
    return {"ok": True}

@router_fc.get("/latest", summary="Get latest forecast curve for a symbol")
async def get_latest_forecasts(symbol: str, horizon_days: int = 30):
    q = """SELECT forecast_date, predicted_price, conf_low, conf_high, model_name
           FROM bridge_forecasts
           WHERE symbol=$1 AND horizon_days=$2
           ORDER BY forecast_date"""
    rows = await app.state.db_pool.fetch(q, symbol, horizon_days)
    if not rows:
        raise HTTPException(404, "No forecasts available")
    return [dict(r) for r in rows]

@router_idx.post("/run", summary="Build today's BRidge Index closes (UTC)")
async def run_indices_now():
    await run_indices_builder()
    return {"ok": True}

@router_idx.get("/latest", summary="Latest close for an index symbol")
async def latest_index(symbol: str):
    q = """SELECT dt, close_price, unit, currency, source_note
           FROM bridge_index_history
           WHERE symbol=$1
           ORDER BY dt DESC
           LIMIT 1"""
    row = await app.state.db_pool.fetchrow(q, symbol)
    if not row:
        raise HTTPException(404, "No index history for that symbol")
    return dict(row)

@router_idx.post("/backfill", summary="Backfill indices for a date range (inclusive)")
async def indices_backfill(start: date = Query(...), end: date = Query(...)):  
    day = start
    n = 0
    while day <= end:        
        await indices_generate_snapshot(snapshot_date=day)
        day += timedelta(days=1)
        n += 1
    return {"ok": True, "days_processed": n, "from": start.isoformat(), "to": end.isoformat()}

@router_idx.get("/history", summary="Historical closes for an index symbol")
async def history(symbol: str, start: date | None = None, end: date | None = None):
    q = """SELECT dt, close_price
           FROM bridge_index_history
           WHERE symbol=$1
             AND ($2::date IS NULL OR dt >= $2)
             AND ($3::date IS NULL OR dt <= $3)
           ORDER BY dt"""
    rows = await app.state.db_pool.fetch(q, symbol, start, end)
    return [dict(r) for r in rows]

@router_idx.get("/universe", summary="List available index symbols (DB or defaults)")
async def universe():   
    rows = await app.state.db_pool.fetch(
        "SELECT symbol, method, factor, base_symbol, enabled FROM bridge_index_definitions ORDER BY symbol"
    )
    if rows:
        return [dict(r) for r in rows]    
    from indices_builder import DEFAULT_INDEX_SET
    return [{"symbol": d["symbol"], "method": d["method"], "factor": d["factor"], "base_symbol": d["base_symbol"], "enabled": True} for d in DEFAULT_INDEX_SET]

app.include_router(router_idx)
app.include_router(router_fc)
app.include_router(router_prices)
app.include_router(router_pricing)

# Optional 3-minute refresher loop (best-effort)
async def _price_refresher():
    while True:
        try:
            await pull_comex_home_once(app.state.db_pool)
            await pull_comexlive_once(app.state.db_pool)
            await pull_lme_once(app.state.db_pool)
        except Exception:
            pass
        await asyncio.sleep(180)  # every 3 minutes

@app.on_event("startup")
async def _kickoff_refresher():
    asyncio.create_task(_price_refresher())

# Nightly index close snapshot at ~01:00 UTC
@app.on_event("startup")
async def _nightly_index_cron():
    async def _runner():
        while True:
            now = datetime.utcnow()
            target = now.replace(hour=1, minute=0, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            await asyncio.sleep((target - now).total_seconds())
            try:
                await indices_generate_snapshot()
            except Exception:                
                pass
    asyncio.create_task(_runner())

async def _daily_indices_job():
    while True:
        try:
            await run_indices_builder()
        except Exception:
            pass        
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        tomorrow = (now + timedelta(days=1)).date()
        next_run = datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc)
        await asyncio.sleep((next_run - now).total_seconds())

@app.on_event("startup")
async def _start_daily_indices():
    asyncio.create_task(_daily_indices_job())

# Optional bootstrap for CI/staging/local: create minimal schema if missing
def _bootstrap_schema_if_needed(sqlalchemy_engine):
    with sqlalchemy_engine.begin() as conn:
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS reference_prices (
            id bigserial PRIMARY KEY,
            symbol text NOT NULL,
            source text NOT NULL,
            price numeric(16,6) NOT NULL,
            ts_market timestamptz NULL,
            ts_server timestamptz NOT NULL DEFAULT now(),
            raw_snippet text
        );
        CREATE INDEX IF NOT EXISTS idx_refprices_symbol_ts ON reference_prices(symbol, ts_server DESC);

        CREATE TABLE IF NOT EXISTS bridge_index_history (
            id bigserial PRIMARY KEY,
            symbol text NOT NULL,
            dt date NOT NULL,
            close_price numeric(16,6) NOT NULL,
            unit text DEFAULT 'USD/lb',
            currency text DEFAULT 'USD',
            source_note text,
            created_at timestamptz DEFAULT now(),
            UNIQUE(symbol, dt)
        );

        CREATE TABLE IF NOT EXISTS bridge_index_definitions (
            id bigserial PRIMARY KEY,
            symbol text UNIQUE NOT NULL,
            method text NOT NULL,
            factor numeric(16,6) NOT NULL,
            base_symbol text NOT NULL,
            notes text,
            enabled boolean DEFAULT true
        );

        CREATE TABLE IF NOT EXISTS model_runs (
            id bigserial PRIMARY KEY,
            model_name text NOT NULL,
            symbol text NOT NULL,
            train_start date NOT NULL,
            train_end date NOT NULL,
            features_used jsonb,
            backtest_mae numeric(16,6),
            backtest_mape numeric(16,6),
            created_at timestamptz DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS bridge_forecasts (
            id bigserial PRIMARY KEY,
            symbol text NOT NULL,
            horizon_days int NOT NULL,
            forecast_date date NOT NULL,
            predicted_price numeric(16,6) NOT NULL,
            conf_low numeric(16,6),
            conf_high numeric(16,6),
            model_name text NOT NULL,
            run_id bigint,
            generated_at timestamptz DEFAULT now(),
            UNIQUE(symbol, horizon_days, forecast_date, model_name)
        );
        """)

@app.on_event("startup")
async def startup_bootstrap_and_connect():
    env = os.getenv("ENV", "").lower()
    init_flag = os.getenv("INIT_DB", "0").lower() in ("1", "true", "yes")
    if env in {"ci", "test", "staging"} or init_flag:
        try:
            _bootstrap_schema_if_needed(engine)
        except Exception as e:
            print(f"[bootstrap] non-fatal init error: {e}")
    try:
        await database.connect() 
    except Exception as e:
        print(f"[startup] database connect failed: {e}")
# ---------------------------------------------------------------------------


# ===== /Contracts CSV Import =====
REQUIRED_IMPORT_COLS = ["seller","buyer","material","price_per_ton","weight_tons","created_at"]

def _rows_from_csv_bytes(raw_bytes: bytes):
    # auto-handle gzip vs plain text
    try:
        text = io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(raw_bytes))).read()
    except OSError:
        text = raw_bytes.decode("utf-8", errors="ignore")
    rdr = csv.DictReader(io.StringIO(text))
    for r in rdr:
        try:
            seller   = (r["seller"] or "").strip()
            buyer    = (r["buyer"] or "").strip()
            material = (r["material"] or "").strip()

            # $/lb → $/ton if price_per_lb present
            if "price_per_lb" in r and str(r["price_per_lb"]).strip():
                price_per_ton = Decimal(str(r["price_per_lb"])) * Decimal("2000")
            else:
                price_per_ton = Decimal(str(r["price_per_ton"]))

            # lbs → tons if weight_lbs present
            if "weight_lbs" in r and str(r["weight_lbs"]).strip():
                weight_tons = Decimal(str(r["weight_lbs"])) / Decimal("2000")
            else:
                weight_tons = Decimal(str(r["weight_tons"]))

            ts = (r["created_at"] or "").strip().replace("Z", "+00:00")
            created_at = datetime.fromisoformat(ts)

            raw_key = f"{seller}|{buyer}|{material}|{price_per_ton}|{weight_tons}|{created_at.isoformat()}"
            dedupe_key = hashlib.sha256(raw_key.encode()).hexdigest()

            yield {
                "seller": seller, "buyer": buyer, "material": material,
                "price_per_ton": price_per_ton, "weight_tons": weight_tons,
                "created_at": created_at, "dedupe_key": dedupe_key
            }
        except (KeyError, InvalidOperation, ValueError):
            continue

@app.post("/import/contracts_csv", tags=["Data"], summary="Bulk import normalized contracts CSV (gzip or csv)")
async def import_contracts_csv(
    file: UploadFile = File(...),
    database_dep = Depends(lambda: database)
):
    raw = await file.read()
    rows = list(_rows_from_csv_bytes(raw))
    if not rows:
        return {"imported": 0}

    q = """
    INSERT INTO contracts (seller,buyer,material,price_per_ton,weight_tons,created_at,dedupe_key)
    VALUES (:seller,:buyer,:material,:price_per_ton,:weight_tons,:created_at,:dedupe_key)
    ON CONFLICT (dedupe_key) DO NOTHING
    """
    CHUNK = 3000
    total = 0
    for i in range(0, len(rows), CHUNK):
        batch = rows[i:i+CHUNK]
        await database_dep.execute_many(query=q, values=batch)
        total += len(batch)
    return {"imported": total}
# ===== /Contracts CSV Import =====

# ==== Quick user creation (admin-only bootstrap) =========================
ADMIN_SETUP_TOKEN = os.getenv("ADMIN_SETUP_TOKEN", "")

class NewUserIn(BaseModel):
    email: str
    password: str
    role: Literal["admin", "buyer", "seller"]

class NewUserOut(BaseModel):
    ok: bool
    email: str
    role: str
    created: bool  # False if it already existed or raced

@app.post(
    "/admin/create_user",
    tags=["Auth"],
    include_in_schema=False,
    summary="Create a user (bootstrap only)",
    response_model=NewUserOut,
)
async def create_user(
    payload: NewUserIn,
    x_setup_token: str = Header(default="", alias="X-Setup-Token"),
):
    if not ADMIN_SETUP_TOKEN or x_setup_token != ADMIN_SETUP_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid setup token")

    email = (payload.email or "").strip().lower()
    base_username = email.split("@", 1)[0][:64]
    username = base_username

    # idempotent: skip if exists
    existing = await database.fetch_one(
        "SELECT email, role FROM public.users WHERE email = :e",
        {"e": email},
    )
    if existing:
        return NewUserOut(ok=True, email=existing["email"], role=existing["role"], created=False)

    # best-effort: enable pgcrypto; ignore perms errors
    try:
        await database.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    except Exception:
        pass

    async def _try_insert_with_username(u: str) -> bool:
        try:
            # path 1: has username + is_active
            await database.execute("""
                INSERT INTO public.users (email, username, password_hash, role, is_active)
                VALUES (:email, :username, crypt(:pwd, gen_salt('bf')), :role, TRUE)
            """, {"email": email, "username": u, "pwd": payload.password, "role": payload.role})
            return True
        except Exception as e:
            # username unique conflict? try a different suffix; otherwise bubble up
            msg = str(e).lower()
            if "duplicate key" in msg or "unique constraint" in msg:
                return False
            raise

    # 1) Try richest shape; vary username if unique-conflict
    try:
        if await _try_insert_with_username(username):
            return NewUserOut(ok=True, email=email, role=payload.role, created=True)
        for i in range(1, 6):
            candidate = (base_username[:58] + f"-{i}") if len(base_username) > 58 else f"{base_username}-{i}"
            if await _try_insert_with_username(candidate):
                return NewUserOut(ok=True, email=email, role=payload.role, created=True)
        # 2) No is_active column but username exists
        try:
            await database.execute("""
                INSERT INTO public.users (email, username, password_hash, role)
                VALUES (:email, :username, crypt(:pwd, gen_salt('bf')), :role)
            """, {"email": email, "username": username, "pwd": payload.password, "role": payload.role})
            return NewUserOut(ok=True, email=email, role=payload.role, created=True)
        except Exception as e:
            if "duplicate key" in str(e).lower() or "unique constraint" in str(e).lower():
                return NewUserOut(ok=True, email=email, role=payload.role, created=False)
            raise
    except Exception:
        # 3) No username column at all
        try:
            await database.execute("""
                INSERT INTO public.users (email, password_hash, role)
                VALUES (:email, crypt(:pwd, gen_salt('bf')), :role)
            """, {"email": email, "pwd": payload.password, "role": payload.role})
            return NewUserOut(ok=True, email=email, role=payload.role, created=True)
        except Exception as e:
            if "duplicate key" in str(e).lower() or "unique constraint" in str(e).lower():
                return NewUserOut(ok=True, email=email, role=payload.role, created=False)
            raise

    return NewUserOut(ok=True, email=email, role=payload.role, created=True)

@app.on_event("shutdown")
async def shutdown_disconnect():
    try:
        await database.disconnect()
    except Exception:
        pass

# ======= AUTH ========
from fastapi import Request

class LoginIn(BaseModel):
    username: str   # can be email OR username
    password: str

class LoginOut(BaseModel):
    ok: bool
    role: str
    redirect: str | None = None

@app.post("/login", tags=["Auth"], response_model=LoginOut, summary="Login with email or username")
async def login(body: LoginIn, request: Request):
    ident = (body.username or "").strip().lower()
    pwd   = body.password or ""

    row = await database.fetch_one(
        """
        SELECT id, email, COALESCE(username, '') AS username, role
        FROM public.users
        WHERE (LOWER(email) = :ident OR LOWER(COALESCE(username, '')) = :ident)
          AND password_hash = crypt(:pwd, password_hash)
        LIMIT 1
        """,
        {"ident": ident, "pwd": pwd},
    )
    if not row:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    role = (row["role"] or "").lower()
    if role == "yard":  
        role = "seller"

    request.session["username"] = (row["username"] or row["email"])
    request.session["role"] = role

    return LoginOut(ok=True, role=role, redirect=f"/{role}")

ALLOW_PUBLIC_SELLER_SIGNUP = os.getenv("ALLOW_PUBLIC_SELLER_SIGNUP", "0").lower() in ("1","true","yes")

class SignupIn(BaseModel):
    email: str
    password: str
    role: Literal["buyer","seller"] = "buyer"

class SignupOut(BaseModel):
    ok: bool
    created: bool
    email: str
    role: str
    redirect: str | None = None

@app.post(
    "/signup",
    tags=["Auth"],
    summary="Create an account (public: buyers; sellers optional via env)",
    response_model=SignupOut,
)
@limiter.limit("10/minute")
async def public_signup(payload: SignupIn, request: Request):
    email = payload.email.strip().lower()
    pwd   = payload.password.strip()
  
    if len(pwd) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters.")

    req_role = (payload.role or "buyer").lower()
    if req_role not in ("buyer","seller"):
        req_role = "buyer"
    if req_role == "seller" and not ALLOW_PUBLIC_SELLER_SIGNUP:
        req_role = "buyer"

    # Idempotent on email
    existing = await database.fetch_one(
        "SELECT email, role FROM public.users WHERE lower(email) = :e",
        {"e": email},
    )
    if existing:
        return SignupOut(ok=True, created=False, email=existing["email"], role=existing["role"], redirect="/buyer")

    try:
        await database.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    except Exception:
        pass

    base_username = email.split("@",1)[0][:64]
    username = base_username

    async def _insert_with_username(u: str) -> bool:
        try:
            await database.execute(
                """
                INSERT INTO public.users (email, username, password_hash, role, is_active)
                VALUES (:email, :username, crypt(:pwd, gen_salt('bf')), :role, TRUE)
                """,
                {"email": email, "username": u, "pwd": pwd, "role": req_role},
            )
            return True
        except Exception as e:
            msg = str(e).lower()
            if "duplicate key" in msg or "unique constraint" in msg:
                return False
            raise  

    try:
        if await _insert_with_username(username):
            return SignupOut(ok=True, created=True, email=email, role=req_role, redirect=f"/{req_role}")

        for i in range(1, 6):
            cand = (base_username[:58] + f"-{i}") if len(base_username) > 58 else f"{base_username}-{i}"
            if await _insert_with_username(cand):
                return SignupOut(ok=True, created=True, email=email, role=req_role, redirect=f"/{req_role}")

        # username but no is_active
        try:
            await database.execute(
                """
                INSERT INTO public.users (email, username, password_hash, role)
                VALUES (:email, :username, crypt(:pwd, gen_salt('bf')), :role)
                """,
                {"email": email, "username": username, "pwd": pwd, "role": req_role},
            )
            return SignupOut(ok=True, created=True, email=email, role=req_role, redirect=f"/{req_role}")
        except Exception as e:
            if "duplicate key" in str(e).lower() or "unique constraint" in str(e).lower():
                return SignupOut(ok=True, created=False, email=email, role=req_role, redirect="/buyer")
            raise

    except Exception:       
        try:
            await database.execute(
                """
                INSERT INTO public.users (email, password_hash, role)
                VALUES (:email, crypt(:pwd, gen_salt('bf')), :role)
                """,
                {"email": email, "pwd": pwd, "role": req_role},
            )
            return SignupOut(ok=True, created=True, email=email, role=req_role, redirect=f"/{req_role}")
        except Exception as e:
            if "duplicate key" in str(e).lower() or "unique constraint" in str(e).lower():
                return SignupOut(ok=True, created=False, email=email, role=req_role, redirect="/buyer")
            raise

@app.post("/logout", tags=["Auth"], summary="Logout", description="Clears session cookie")
async def logout(request: Request):
    request.session.clear()
    return {"ok": True}
# ======= /AUTH ========

# --- ZIP export (all core data) ---
from fastapi.responses import StreamingResponse
import zipfile, io, csv
from sqlalchemy import text as _sqltext

@app.get("/admin/export_all", tags=["Admin"], summary="Download ZIP of all CSVs")
def admin_export_all():
    exports = {
        "contracts.csv": """
            SELECT id, buyer, seller, material, sku, weight_tons, price_per_ton,
                   total_value, status, contract_date, pickup_time, delivery_time, currency
            FROM contracts
            ORDER BY contract_date DESC NULLS LAST, id DESC
        """,
        "bols.csv": """
            SELECT bol_id, contract_id, buyer, seller, material, weight_tons, status,
                   pickup_time, delivery_time, total_value
            FROM bols
            ORDER BY pickup_time DESC NULLS LAST, bol_id DESC
        """,
        "inventory_items.csv": """
            SELECT seller, sku, description, uom, location, qty_on_hand, qty_reserved,
                   qty_committed, source, external_id, updated_at
            FROM inventory_items
            ORDER BY seller, sku
        """,
        "inventory_movements.csv": """
            SELECT seller, sku, movement_type, qty, ref_contract, created_at
            FROM inventory_movements
            ORDER BY created_at DESC
        """,
    }

    with engine.begin() as conn:
        mem = io.BytesIO()
        with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for fname, sql in exports.items():
                rows = conn.execute(_sqltext(sql)).fetchall()
                cols = rows[0].keys() if rows else []
                s = io.StringIO()
                w = csv.writer(s)
                if cols: w.writerow(cols)
                for r in rows:                    
                    if hasattr(r, "keys"):
                        w.writerow([r[c] for c in cols])
                    else:
                        w.writerow(list(r))
                zf.writestr(fname, s.getvalue())
        mem.seek(0)
        headers = {"Content-Disposition": 'attachment; filename="bridge_export_all.zip"'}
        return StreamingResponse(mem, media_type="application/zip", headers=headers)

# ===== HMAC gating for inventory endpoints =====
INVENTORY_SECRET_ENV = "INVENTORY_WEBHOOK_SECRET"

def _require_hmac_in_this_env() -> bool:    
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

# ---------- Inventory helpers (role check, unit conversion, upsert core) ----------
def _is_admin_or_seller(request: Request) -> bool:
    try:
        return (request.session.get("role") or "").lower() in {"admin", "seller"}
    except Exception:
        return False

def _to_tons(qty: float, uom: str | None) -> float:
    q = float(qty or 0.0)
    u = (uom or "ton").strip().lower()
    if u.startswith("lb"):  # lb, lbs, pounds
        return q / 2000.0
    return q  # assume tons by default

async def _manual_upsert_absolute_tx(
    *, seller: str, sku: str, qty_on_hand_tons: float,
    uom: str | None = "ton", location: str | None = None,
    description: str | None = None, source: str | None = None,
    movement_reason: str = "manual_add", idem_key: str | None = None,
):
    s, k = seller.strip(), sku.strip()

    cur = await database.fetch_one(
        "SELECT qty_on_hand FROM inventory_items WHERE seller=:s AND sku=:k FOR UPDATE",
        {"s": s, "k": k}
    )
    if cur is None:
        await database.execute("""
          INSERT INTO inventory_items (seller, sku, description, uom, location,
                                       qty_on_hand, qty_reserved, qty_committed, source, updated_at)
          VALUES (:s,:k,:d,:u,:loc,0,0,0,:src,NOW())
          ON CONFLICT (seller, sku) DO NOTHING
        """, {"s": s, "k": k, "d": description, "u": (uom or "ton"),
              "loc": location, "src": source or "manual"})
        cur = await database.fetch_one(
            "SELECT qty_on_hand FROM inventory_items WHERE seller=:s AND sku=:k FOR UPDATE",
            {"s": s, "k": k}
        )

    old = float(cur["qty_on_hand"]) if cur else 0.0
    new_qty = float(qty_on_hand_tons)
    delta = new_qty - old

    await database.execute("""
      UPDATE inventory_items
         SET qty_on_hand=:new, updated_at=NOW(),
             uom=:u, location=COALESCE(:loc, location),
             description=COALESCE(:desc, description),
             source=COALESCE(:src, source)
       WHERE seller=:s AND sku=:k
    """, {"new": new_qty, "u": (uom or "ton"), "loc": location, "desc": description,
          "src": (source or "manual"), "s": s, "k": k})

    meta_json = json.dumps({"from": old, "to": new_qty, "reason": movement_reason, "idem_key": idem_key})
    try:
        await database.execute("""
          INSERT INTO inventory_movements (seller, sku, movement_type, qty, ref_contract, meta)
          VALUES (:s,:k,'upsert',:q,NULL, :m::jsonb)
        """, {"s": s, "k": k, "q": delta, "m": meta_json})
    except Exception:
        await database.execute("""
          INSERT INTO inventory_movements (seller, sku, movement_type, qty, ref_contract, meta)
          VALUES (:s,:k,'upsert',:q,NULL, :m)
        """, {"s": s, "k": k, "q": delta, "m": meta_json})

    # --- webhook emit (inventory.movement)
    try:
        await emit_event_safe("inventory.movement", {
            "seller": s,
            "sku": k,
            "delta": delta,
            "new_qty": new_qty,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        })
    except Exception:
        pass

    return old, new_qty, delta

async def emit_event_safe(event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        res = await emit_event(event_type, payload)
        if not res.get("ok"):
            try:
                await database.execute("""
                  INSERT INTO webhook_dead_letters (event, payload, status_code, response_text)
                  VALUES (:e, :p::jsonb, :sc, :rt)
                """, {
                    "e": event_type,
                    "p": json.dumps(payload),
                    "sc": res.get("status_code"),
                    "rt": res.get("response")
                })
            except Exception:
                pass
        return res
    except Exception:
        try:
            await database.execute("""
              INSERT INTO webhook_dead_letters (event, payload, status_code, response_text)
              VALUES (:e, :p::jsonb, NULL, 'exception')
            """, {"e": event_type, "p": json.dumps(payload)})
        except Exception:
            pass
        return {"ok": False, "status_code": None, "response": "exception"}


import hmac, hashlib, base64, time, json

def _sign_payload(secret: str, body_bytes: bytes, ts: str) -> str:
    msg = ts.encode("utf-8") + b"." + body_bytes
    mac = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(mac).decode("ascii")

async def emit_event(event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url_env = {
        "contract.created": os.getenv("WEBHOOK_CONTRACT_CREATED"),
        "bol.updated": os.getenv("WEBHOOK_BOL_UPDATED"),
        "inventory.movement": os.getenv("WEBHOOK_INVENTORY_MOVED"),
        "signature.received": os.getenv("WEBHOOK_SIGNATURE_RECEIVED"),
    }
    url = url_env.get(event_type)
    if not url:
        return {"ok": True, "skipped": True, "reason": "no webhook url configured"}

    body = json.dumps({
        "event": event_type,
        "timestamp": int(time.time()),
        "payload": payload,
    }, default=str).encode("utf-8")

    ts = str(int(time.time()))
    secret = os.getenv("WEBHOOK_SECRET", "")
    sig = _sign_payload(secret, body, ts) if secret else ""

    headers = {
        "Content-Type": "application/json",
        "X-Bridge-Event": event_type,
        "X-Bridge-Timestamp": ts,
        "X-Bridge-Signature": sig,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, content=body)
        ok = 200 <= r.status_code < 300
        return {"ok": ok, "status_code": r.status_code, "response": r.text}

# ---------- /helpers ----------

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

@app.get(
    "/inventory",
    tags=["Inventory"],
    summary="List inventory (available view)",
    response_model=List[InventoryRowOut],
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

# -------- Inventory: Finished Goods snapshot (for seller.html) --------
class FinishedRow(BaseModel):
    sku: str
    wip_lbs: int
    finished_lbs: int
    avg_cost_per_lb: Optional[float] = None  

@app.get(
    "/inventory/finished_goods",
    tags=["Inventory"],
    summary="Finished goods by SKU for a seller (in pounds)",
    response_model=List[FinishedRow],
    status_code=200,
)
async def finished_goods(seller: str = Query(..., description="Seller (yard) name")):
    """
    Available finished = max(qty_on_hand - qty_reserved - qty_committed, 0) in TONS → return as POUNDS.
    wip_lbs is 0 until you split WIP tracking.
    """
    rows = await database.fetch_all(
        """
        SELECT
            sku,
            0::int AS wip_lbs,
            CAST(ROUND(GREATEST(
                COALESCE(qty_on_hand, 0)
              - COALESCE(qty_reserved, 0)
              - COALESCE(qty_committed, 0), 0
            ) * 2000) AS int) AS finished_lbs,
            NULL::float AS avg_cost_per_lb
        FROM inventory_items
        WHERE LOWER(seller) = LOWER(:seller)
        ORDER BY sku;
        """,
        {"seller": seller},
    )

    return [
        FinishedRow(
            sku=r["sku"],
            wip_lbs=int(r["wip_lbs"]),
            finished_lbs=int(r["finished_lbs"]),
            avg_cost_per_lb=r["avg_cost_per_lb"],
        )
        for r in rows
    ]

# ===== CONTRACTS / BOLS schema bootstrap (idempotent) =====
@app.on_event("startup")
async def _ensure_contracts_bols_schema():
    ddl = [
        # contracts
        """
        CREATE TABLE IF NOT EXISTS contracts (
          id UUID PRIMARY KEY,
          buyer TEXT NOT NULL,
          seller TEXT NOT NULL,
          material TEXT NOT NULL,
          weight_tons NUMERIC NOT NULL,
          price_per_ton NUMERIC NOT NULL,
          status TEXT NOT NULL DEFAULT 'Pending',
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          signed_at TIMESTAMPTZ,
          signature TEXT
        );
        """,
        # bols
        """
        CREATE TABLE IF NOT EXISTS bols (
          bol_id UUID PRIMARY KEY,
          contract_id UUID NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
          buyer TEXT,
          seller TEXT,
          material TEXT,
          weight_tons NUMERIC,
          price_per_unit NUMERIC,
          total_value NUMERIC,
          carrier_name TEXT,
          carrier_driver TEXT,
          carrier_truck_vin TEXT,
          pickup_signature_base64 TEXT,
          pickup_signature_time TIMESTAMPTZ,
          pickup_time TIMESTAMPTZ,
          delivery_signature_base64 TEXT,
          delivery_signature_time TIMESTAMPTZ,
          delivery_time TIMESTAMPTZ,
          status TEXT
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_bols_contract ON bols(contract_id);",
        "CREATE INDEX IF NOT EXISTS idx_bols_pickup_time ON bols(pickup_time DESC);",

        # --- Export/compliance fields (idempotent ALTERs) ---
        "ALTER TABLE bols ADD COLUMN IF NOT EXISTS origin_country TEXT",
        "ALTER TABLE bols ADD COLUMN IF NOT EXISTS destination_country TEXT",
        "ALTER TABLE bols ADD COLUMN IF NOT EXISTS port_code TEXT",
        "ALTER TABLE bols ADD COLUMN IF NOT EXISTS hs_code TEXT",
        "ALTER TABLE bols ADD COLUMN IF NOT EXISTS duty_usd NUMERIC",
        "ALTER TABLE bols ADD COLUMN IF NOT EXISTS tax_pct NUMERIC",
    ]
    for s in ddl:
        try:
            await database.execute(s)
        except Exception as e:
            logger.warn("contracts_bols_bootstrap_failed", sql=s[:100], err=str(e))

@app.on_event("startup")
async def _ensure_audit_log_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS audit_log (
        id BIGSERIAL PRIMARY KEY,
        actor TEXT,
        action TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        details JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    try:
        await database.execute(ddl)
    except Exception as e:
        logger.warn("audit_log_bootstrap_failed", err=str(e))
# ======================================================================

# -------- Inventory: Bulk Upsert (unit-aware, HMAC in prod, replay guard, logs) --------
@app.post(
    "/inventory/bulk_upsert",
    tags=["Inventory"],
    summary="Bulk upsert inventory items (absolute, unit-aware)",
    description="Partners push absolute qty_on_hand per (seller, sku). Uses HMAC in PROD; writes movements and ingest log.",
    status_code=200
)
@limiter.limit("60/minute")
async def inventory_bulk_upsert(body: dict, request: Request):    
    source = (body.get("source") or "").strip()
    seller = (body.get("seller") or "").strip()
    items  = body.get("items") or []
    if not (source and seller and isinstance(items, list)):
        raise HTTPException(400, "invalid payload: require source, seller, items[]")

    raw = await request.body()
    sig = request.headers.get("X-Signature", "")    
    if _require_hmac_in_this_env():
        if not sig or is_replay(sig) or not verify_sig(raw, sig, INVENTORY_SECRET_ENV):
            # log failure
            try:
                await database.execute("""
                  INSERT INTO inventory_ingest_log (source, seller, item_count, idem_key, sig_present, sig_valid, remote_addr, user_agent)
                  VALUES (:src, :seller, 0, NULL, :sp, FALSE, :ip, :ua)
                """, {"src": "bulk_upsert", "seller": seller, "sp": bool(sig),
                      "ip": request.client.host if request.client else None,
                      "ua": request.headers.get("user-agent")})
            except Exception:
                pass
            raise HTTPException(401, "invalid or missing signature")

    upserted = 0
    async with database.transaction():
        for it in items:
            sku = (it.get("sku") or "").strip()
            if not sku:
                continue
            desc = it.get("description")
            uom  = it.get("uom") or "ton"
            loc  = it.get("location")
            qty_raw  = float(it.get("qty_on_hand") or 0.0)
            qty_tons = _to_tons(qty_raw, uom)

            await _manual_upsert_absolute_tx(
                seller=seller, sku=sku, qty_on_hand_tons=qty_tons,
                uom=uom, location=loc, description=desc,
                source=source, movement_reason="bulk_upsert",
                idem_key=it.get("idem_key")
            )
            upserted += 1

        # success ingest log
        try:
            await database.execute("""
              INSERT INTO inventory_ingest_log (source, seller, item_count, idem_key, sig_present, sig_valid, remote_addr, user_agent)
              VALUES (:src, :seller, :cnt, :idem, :sp, TRUE, :ip, :ua)
            """, {"src": "bulk_upsert", "seller": seller, "cnt": upserted,
                  "idem": (items[0].get("idem_key") if items else None),
                  "sp": bool(sig),
                  "ip": request.client.host if request.client else None,
                  "ua": request.headers.get("user-agent")})
        except Exception:
            pass

    return {"ok": True, "upserted": upserted}

# -------- Inventory: Manual Add/Set (gated + unit-aware) --------
@app.post(
    "/inventory/manual_add",
    tags=["Inventory"],
    summary="Manual add/set qty_on_hand for a SKU (absolute set, unit-aware)",
    response_model=dict,
    status_code=200
)
@limiter.limit("60/minute")
async def inventory_manual_add(payload: dict, request: Request):    
    if _require_hmac_in_this_env() and not _is_admin_or_seller(request):
        raise HTTPException(401, "login required")

    seller = (payload.get("seller") or "").strip()
    sku    = (payload.get("sku") or "").strip()
    if not (seller and sku):
        raise HTTPException(400, "seller and sku are required")

    uom     = (payload.get("uom") or "ton")
    loc     = payload.get("location")
    desc    = payload.get("description")
    source  = payload.get("source") or "manual"
    idem    = payload.get("idem_key")
    qty_raw = float(payload.get("qty_on_hand") or 0.0)
    qty_tons = _to_tons(qty_raw, uom)

    async with database.transaction():
        old, new_qty, delta = await _manual_upsert_absolute_tx(
            seller=seller, sku=sku, qty_on_hand_tons=qty_tons,
            uom=uom, location=loc, description=desc, source=source,
            movement_reason="manual_add", idem_key=idem
        )
    return {"ok": True, "seller": seller, "sku": sku, "from": old, "to": new_qty, "delta": delta, "uom": "ton"}

# -------- Inventory: CSV template --------
@app.get("/inventory/template.csv", tags=["Inventory"], summary="CSV template")
async def inventory_template_csv():
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["seller","sku","qty_on_hand","description","uom","location","external_id"])
    w.writerow(["Winski Brothers","SHRED","100.0","Shred Steel","ton","Yard-A",""])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="inventory_template.csv"'})

# -------- Inventory: Import CSV (gated + unit-aware) --------
@app.post("/inventory/import/csv", tags=["Inventory"], summary="Import CSV (absolute set, unit-aware)")
@limiter.limit("30/minute")
async def inventory_import_csv(
    file: UploadFile = File(...),
    seller: Optional[str] = Form(None),
    request: Request = None
):
    if _require_hmac_in_this_env() and not _is_admin_or_seller(request):
        raise HTTPException(401, "login required")

    text = (await file.read()).decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    upserted, errors = 0, []

    async with database.transaction():
        for i, row in enumerate(reader, start=2):  # header at line 1
            try:
                s = (seller or row.get("seller") or "").strip()
                k = (row.get("sku") or "").strip()
                if not (s and k):
                    raise ValueError("missing seller or sku")

                uom = row.get("uom") or "ton"
                qty_raw = float(row.get("qty_on_hand") or 0.0)
                qty_tons = _to_tons(qty_raw, uom)

                await _manual_upsert_absolute_tx(
                    seller=s, sku=k, qty_on_hand_tons=qty_tons,
                    uom=uom,
                    location=row.get("location"),
                    description=row.get("description"),
                    source="import_csv",
                    movement_reason="import_csv"
                )
                upserted += 1
            except Exception as e:
                errors.append({"line": i, "error": str(e)})

    return {"ok": True, "upserted": upserted, "errors": errors}

# -------- Inventory: Import Excel (gated + unit-aware) --------
@app.post("/inventory/import/excel", tags=["Inventory"], summary="Import XLSX (absolute set, unit-aware)")
@limiter.limit("15/minute")
async def inventory_import_excel(
    file: UploadFile = File(...),
    seller: Optional[str] = Form(None),
    request: Request = None
):
    if _require_hmac_in_this_env() and not _is_admin_or_seller(request):
        raise HTTPException(401, "login required")

    try:
        import pandas as pd  # openpyxl required underneath
    except Exception:
        raise HTTPException(400, "Excel import requires pandas+openpyxl; upload CSV instead.")

    data = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(data))
    except Exception as e:
        raise HTTPException(400, f"Could not read Excel: {e}")

    cols_lower = {str(c).strip().lower() for c in df.columns}
    required = {"sku", "qty_on_hand"}
    if not required.issubset(cols_lower):
        raise HTTPException(400, "Excel must include columns: sku, qty_on_hand (plus optional seller, description, uom, location, external_id)")

    upserted, errors = 0, []
    async with database.transaction():
        for i, rec in enumerate(df.to_dict(orient="records"), start=2):
            try:
                s = (seller or rec.get("seller") or rec.get("Seller") or "").strip()
                k = (rec.get("sku") or rec.get("SKU") or "").strip()
                if not (s and k):
                    raise ValueError("missing seller or sku")

                uom = rec.get("uom") or rec.get("UOM") or "ton"
                qty_raw = rec.get("qty_on_hand") or rec.get("Qty_On_Hand") or 0.0
                qty_tons = _to_tons(float(qty_raw or 0.0), uom)

                await _manual_upsert_absolute_tx(
                    seller=s, sku=k, qty_on_hand_tons=qty_tons,
                    uom=uom,
                    location=rec.get("location") or rec.get("Location"),
                    description=rec.get("description") or rec.get("Description"),
                    source="import_excel",
                    movement_reason="import_excel"
                )
                upserted += 1
            except Exception as e:
                errors.append({"row": i, "error": str(e)})

    return {"ok": True, "upserted": upserted, "errors": errors}

@app.on_event("startup")
async def _ensure_pgcrypto():
    try:
        await database.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    except Exception as e:
        logger.warn("pgcrypto_ext_failed", err=str(e))

@app.on_event("startup")
async def _ensure_perf_indexes():
    ddl = [
        "CREATE INDEX IF NOT EXISTS idx_contracts_mat_status ON contracts(material, created_at DESC, status)",
        "CREATE INDEX IF NOT EXISTS idx_inv_mov_sku_time ON inventory_movements(seller, sku, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_orders_acct_status ON orders(account_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_orders_audit_order_time ON orders_audit(order_id, at DESC)",
    ]
    for s in ddl:
        try:
            await database.execute(s)
        except Exception as e:
            logger.warn('index_bootstrap_failed', sql=s[:80], err=str(e))

# DB safety checks (non-negative inventory quantities)
@app.on_event("startup")
async def _ensure_inventory_constraints():
    ddl = [
      "ALTER TABLE inventory_items ADD CONSTRAINT IF NOT EXISTS chk_qty_on_hand_nonneg CHECK (qty_on_hand >= 0)",
      "ALTER TABLE inventory_items ADD CONSTRAINT IF NOT EXISTS chk_qty_reserved_nonneg CHECK (qty_reserved >= 0)",
      "ALTER TABLE inventory_items ADD CONSTRAINT IF NOT EXISTS chk_qty_committed_nonneg CHECK (qty_committed >= 0)"
    ]
    for s in ddl:
        try:
            await database.execute(s)
        except Exception as e:
            logger.warn("inventory_constraints_bootstrap_failed", sql=s[:100], err=str(e))

#-------- Dead Letter Startup --------

@app.on_event("startup")
async def _ensure_dead_letters():
    try:
        await database.execute("""
        CREATE EXTENSION IF NOT EXISTS "pgcrypto";
        CREATE TABLE IF NOT EXISTS webhook_dead_letters (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          event TEXT NOT NULL,
          payload JSONB NOT NULL,
          status_code INT,
          response_text TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
    except Exception:
        pass
#-------- Dead Letter Startup --------

#------- Dossier HR Sync -------
@app.on_event("startup")
async def _nightly_dossier_sync():
    if os.getenv("DOSSIER_SYNC", "").lower() not in ("1","true","yes"):
        return
    async def _runner():
        while True:
            now = datetime.utcnow()
            target = now.replace(hour=2, minute=5, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            await asyncio.sleep((target - now).total_seconds())
            try:
                # Build events: late shipments, weight deltas, unsigned BOL aging
                # (Stub – fill with your own queries & POST to Dossier)
                pass
            except Exception:
                pass
    asyncio.create_task(_runner())
# ------- Dossier HR Sync -------

# -------- CORS --------
ALLOWED_ORIGINS = [
    "https://scrapfutures.com",
    "https://www.scrapfutures.com",
    "https://bridge.scrapfutures.com",
    "https://bridge-buyer.onrender.com",
    "https://indices.scrapfutures.com",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# -------- Host header protection --------
ALLOWED_HOSTS = [
    "scrapfutures.com",
    "www.scrapfutures.com",
    "bridge.scrapfutures.com",
    "bridge-buyer.onrender.com",  
    "localhost",
    "127.0.0.1",
]

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
        
@app.post(
    "/contracts",
    response_model=ContractOut,
    tags=["Contracts"],
    summary="Create Contract",
    description="Creates a Pending contract and reserves inventory (qty_reserved += weight_tons).",
    status_code=201
)
async def create_contract(contract: ContractIn):
    qty   = float(contract.weight_tons)
    seller = contract.seller.strip()
    sku    = contract.material.strip()  # we store material in contracts, sku in inventory

    cid = str(uuid.uuid4())

    async with database.transaction():
        # ensure inventory row exists
        await database.execute("""
            INSERT INTO inventory_items (seller, sku, qty_on_hand, qty_reserved, qty_committed)
            VALUES (:seller, :sku, 0, 0, 0)
            ON CONFLICT (seller, sku) DO NOTHING
        """, {"seller": seller, "sku": sku})

        # check availability and reserve
        inv = await database.fetch_one("""
            SELECT qty_on_hand, qty_reserved, qty_committed
            FROM inventory_items
            WHERE seller=:seller AND sku=:sku
            FOR UPDATE
        """, {"seller": seller, "sku": sku})
        on_hand   = float(inv["qty_on_hand"]) if inv else 0.0
        reserved  = float(inv["qty_reserved"]) if inv else 0.0
        committed = float(inv["qty_committed"]) if inv else 0.0
        available = on_hand - reserved - committed
        if available < qty:
            raise HTTPException(
                status_code=409,
                detail=f"Not enough inventory: available {available} ton(s) < requested {qty} ton(s)."
            )

        await database.execute("""
            UPDATE inventory_items
               SET qty_reserved = qty_reserved + :q, updated_at = NOW()
             WHERE seller=:seller AND sku=:sku
        """, {"q": qty, "seller": seller, "sku": sku})

        # movement link
        await database.execute("""
            INSERT INTO inventory_movements (seller, sku, movement_type, qty, ref_contract, meta)
            VALUES (:seller, :sku, 'reserve', :q, :cid, :meta)
        """, {"seller": seller, "sku": sku, "q": qty, "cid": cid,
              "meta": json.dumps({"reason": "contract_create"})})

        # create contract row (matches your contracts schema)
        row = await database.fetch_one("""
            INSERT INTO contracts (id, buyer, seller, material, weight_tons, price_per_ton, status, created_at)
            VALUES (:id, :buyer, :seller, :material, :weight_tons, :price_per_ton, 'Pending', NOW())
            RETURNING *
        """, {
            "id": cid,
            "buyer": contract.buyer,
            "seller": contract.seller,
            "material": contract.material,
            "weight_tons": contract.weight_tons,
            "price_per_ton": contract.price_per_ton
        })

    # optional webhook
    try:
        await emit_event("contract.created", {
            "contract_id": str(row["id"]),
            "seller": row["seller"],
            "buyer": row["buyer"],
            "sku": row["material"],                         # expose as sku to receivers
            "price_per_unit": float(row["price_per_ton"]),
            "currency": "USD",
            "created_at": (row["created_at"] or datetime.utcnow()).isoformat()
        })
    except Exception:
        pass

    return row
class PurchaseIn(BaseModel):
    op: Literal["purchase"] = "purchase"
    expected_status: Literal["Pending"] = "Pending"
    idempotency_key: Optional[str] = None

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

    # --- NEW export/compliance fields ---
    origin_country: Optional[str] = None
    destination_country: Optional[str] = None
    port_code: Optional[str] = None
    hs_code: Optional[str] = None
    duty_usd: Optional[float] = None
    tax_pct: Optional[float] = None

    class Config:
        schema_extra = {"example": {
            "contract_id": "1ec9e850-8b5a-45de-b631-f9fae4a1d4c9",
            "buyer": "Lewis Salvage", "seller": "Winski Brothers",
            "material": "Shred Steel", "weight_tons": 40, "price_per_unit": 245.00,
            "total_value": 9800.00,
            "carrier": {"name": "ABC Trucking Co.", "driver": "John Driver", "truck_vin": "1FDUF5GY3KDA12345"},
            "pickup_signature": {"base64": "data:image/png;base64,iVBOR...", "timestamp": "2025-09-01T12:00:00Z"},
            "pickup_time": "2025-09-01T12:15:00Z",
            # new example fields
            "origin_country": "US",
            "destination_country": "MX",
            "port_code": "LAX",
            "hs_code": "7404",
            "duty_usd": 125.50,
            "tax_pct": 5.0
        }}

class BOLOut(BOLIn):
    bol_id: uuid.UUID
    status: str
    delivery_signature: Optional[Signature] = None
    delivery_time: Optional[datetime] = None

    class Config:
        schema_extra = {"example": {
            "bol_id": "9fd89221-4247-4f93-bf4b-df9473ed8e57",
            "contract_id": "b1c89b94-234a-4d55-b1fc-14bfb7fce7e9",
            "buyer": "Lewis Salvage", "seller": "Winski Brothers",
            "material": "Shred Steel", "weight_tons": 40,
            "price_per_unit": 245.0, "total_value": 9800.0,
            "carrier": {"name": "ABC Trucking Co.", "driver": "Jane Doe", "truck_vin": "1FTSW21P34ED12345"},
            "pickup_signature": {"base64": "data:image/png;base64,iVBOR...", "timestamp": "2025-09-01T12:00:00Z"},
            "pickup_time": "2025-09-01T12:15:00Z",
            "delivery_signature": None, "delivery_time": None, "status": "BOL Issued",
            # new example fields
            "origin_country": "US",
            "destination_country": "MX",
            "port_code": "LAX",
            "hs_code": "7404",
            "duty_usd": 125.50,
            "tax_pct": 5.0
        }}
        
# Tighter typing for updates
ContractStatus = Literal["Pending", "Signed", "Dispatched", "Fulfilled", "Cancelled"]

class ContractUpdate(BaseModel):
    status: ContractStatus
    signature: Optional[str] = None
    # pydantic v2: use json_schema_extra (formerly schema_extra)
    class Config:
        json_schema_extra = {
            "example": {"status": "Signed", "signature": "JohnDoe123"}
        }

# ===== Idempotency cache for POST/Inventory/Purchase =====
_idem_cache = {}

@app.post("/admin/run_snapshot_now", tags=["Admin"], summary="Build & upload a snapshot now (blocking)")
async def admin_run_snapshot_now(storage: str = "supabase"):
    try:
        res = await run_daily_snapshot(storage=storage)
        return {"ok": True, **res}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/admin/run_snapshot_bg", tags=["Admin"], summary="Queue a snapshot upload (background) and return immediately")
async def admin_run_snapshot_bg(background: BackgroundTasks, storage: str = "supabase"):
    background.add_task(run_daily_snapshot, storage)
    return {"ok": True, "queued": True}

# ===== Admin export helpers =====

# --- helpers (for CSV/ZIP safety) ---
def _safe(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, uuid.UUID):
        return str(v)
    return v

def _iter_csv(rows: Iterable[dict], fieldnames: list[str]):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    yield buf.getvalue()
    buf.seek(0); buf.truncate(0)
    for r in rows:
        writer.writerow({k: _safe(r.get(k)) for k in fieldnames})
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)

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

# ===== Admin gate helper  =====
def _require_admin(request: Request):
    # Only enforce in production so local dev stays easy
    if os.getenv("ENV","").lower()=="production" and request.session.get("role")!="admin":
        raise HTTPException(403, "admin only")

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

# ===== Audit logging =====
async def log_action(actor: str, action: str, entity_id: str, details: dict):
    """Write an audit entry to audit_log."""
    await database.execute(
        """
        INSERT INTO audit_log (actor, action, entity_id, details)
        VALUES (:actor, :action, :entity_id, :details)
        """,
        {
            "actor": actor,
            "action": action,
            "entity_id": entity_id,
            "details": json.dumps(details),
        },
    )

# ===== ICE webhook signature verifier =====
LINK_SIGNING_SECRET = os.getenv("LINK_SIGNING_SECRET", "")

def verify_sig_ice(raw_body: bytes, given_sig: str) -> bool:
    """Verify ICE webhook signatures. Pass-through if no secret is set (dev/CI)."""
    if not LINK_SIGNING_SECRET:  # dev/staging/CI mode
        return True
    mac = hmac.new(LINK_SIGNING_SECRET.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, given_sig)

@app.post("/ice-digital-trade", tags=["ICE"], summary="ICE Digital Trade webhook (stub)")
async def ice_trade_webhook(request: Request):
    sig = request.headers.get("X-Signature", "")
    body = await request.body()
    if not verify_sig_ice(body, sig):
        raise HTTPException(status_code=401, detail="Invalid signature")
    # TODO: parse/process payload here (idempotency, persistence, audit)
    return {"ok": True}

# ===== Link signing (itsdangerous) =====
LINK_SIGNING_SECRET = os.getenv("LINK_SIGNING_SECRET", os.getenv("SESSION_SECRET", "dev-only-secret"))
_link_signer = URLSafeTimedSerializer(LINK_SIGNING_SECRET, salt="bridge-link-v1")

def make_signed_token(payload: dict) -> str:
    """Create a signed token encoding a small payload (e.g., {'bol_id': ...})."""
    return _link_signer.dumps(payload)

def verify_signed_token(token: str, max_age_sec: int = 900) -> dict:
    """Verify and load a signed token (default 15 min TTL)."""
    try:
        return _link_signer.loads(token, max_age=max_age_sec)
    except SignatureExpired:
        raise HTTPException(status_code=401, detail="Link expired")
    except BadSignature:
        raise HTTPException(status_code=401, detail="Bad link signature")

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
    draw("Pickup Time", row["pickup_time"].isoformat() if row["pickup_time"] else "—")
    draw("Delivery Time", row["delivery_time"].isoformat() if row["delivery_time"] else "—")
    draw("Carrier Name", row["carrier_name"])
    draw("Driver", row["carrier_driver"])
    draw("Truck VIN", row["carrier_truck_vin"])
    draw("Origin Country", row.get("origin_country") or "—")
    draw("Destination Country", row.get("destination_country") or "—")
    draw("Port Code", row.get("port_code") or "—")
    draw("HS Code", row.get("hs_code") or "—")
    if row.get("duty_usd") is not None:
        draw("Duty (USD)", f"${float(row['duty_usd']):.2f}")
    if row.get("tax_pct") is not None:
        draw("Tax (%)", f"{float(row['tax_pct']):.2f}%")

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

@app.post(
    "/bols",
    response_model=BOLOut,
    tags=["BOLs"],
    summary="Create BOL",
    status_code=201
)
async def create_bol_pg(bol: BOLIn, request: Request):
    # Optional idempotency
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

    # ---- audit log (best-effort; won't break request on failure)
    actor = request.session.get("username") if hasattr(request, "session") else None
    try:
        await log_action(actor or "system", "bol.create", str(row["bol_id"]), {
            "contract_id": str(bol.contract_id),
            "material": bol.material,
            "weight_tons": bol.weight_tons
        })
    except Exception:
        pass

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

    if buyer:
        cond.append("buyer ILIKE :buyer");         vals["buyer"] = f"%{buyer}%"
    if seller:
        cond.append("seller ILIKE :seller");       vals["seller"] = f"%{seller}%"
    if material:
        cond.append("material ILIKE :material");   vals["material"] = f"%{material}%"
    if status:
        cond.append("status ILIKE :status");       vals["status"] = f"%{status}%"
    if contract_id:
        cond.append("contract_id = :contract_id"); vals["contract_id"] = contract_id
    if start:
        cond.append("pickup_time >= :start");      vals["start"] = start
    if end:
        cond.append("pickup_time <= :end");        vals["end"] = end

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
            "price_per_unit": float(d.get("price_per_unit") or 0.0),  # column name in bols
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

# -------- Public ticker (ungated) ------
@app.get("/ticker", tags=["Market"], summary="Public ticker snapshot")
async def public_ticker(listing_id: str):
    last = await database.fetch_one("""
      SELECT fm.mark_price AS last, fm.mark_date
      FROM futures_marks fm
      WHERE fm.listing_id=:l
      ORDER BY fm.mark_date DESC
      LIMIT 1
    """, {"l": listing_id})
    bid = await database.fetch_one("""
      SELECT price, SUM(qty_open) qty FROM orders
      WHERE listing_id=:l AND side='BUY' AND status IN ('NEW','PARTIAL')
      GROUP BY price ORDER BY price DESC LIMIT 1
    """, {"l": listing_id})
    ask = await database.fetch_one("""
      SELECT price, SUM(qty_open) qty FROM orders
      WHERE listing_id=:l AND side='SELL' AND status IN ('NEW','PARTIAL')
      GROUP BY price ORDER BY price ASC LIMIT 1
    """, {"l": listing_id})
    return {
        "listing_id": listing_id,
        "last": (float(last["last"]) if last and last["last"] is not None else None),
        "last_date": (str(last["mark_date"]) if last else None),
        "best_bid": (float(bid["price"]) if bid else None),
        "best_bid_size": (float(bid["qty"]) if bid else None),
        "best_ask": (float(ask["price"]) if ask else None),
        "best_ask_size": (float(ask["qty"]) if ask else None),
    }

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
    cid = str(uuid.uuid4())  # create id up front so we can reference it in movements

    async with database.transaction():
        # ensure inventory row exists
        await database.execute("""
            INSERT INTO inventory_items (seller, sku, qty_on_hand, qty_reserved, qty_committed)
            VALUES (:seller, :sku, 0, 0, 0)
            ON CONFLICT (seller, sku) DO NOTHING
        """, {"seller": seller, "sku": sku})

        # check availability
        inv = await database.fetch_one("""
            SELECT qty_on_hand, qty_reserved, qty_committed
              FROM inventory_items
             WHERE seller=:seller AND sku=:sku
             FOR UPDATE
        """, {"seller": seller, "sku": sku})
        on_hand   = float(inv["qty_on_hand"]) if inv else 0.0
        reserved  = float(inv["qty_reserved"]) if inv else 0.0
        committed = float(inv["qty_committed"]) if inv else 0.0
        available = on_hand - reserved - committed
        if available < qty:
            raise HTTPException(
                status_code=409,
                detail=f"Not enough inventory: available {available} ton(s) < requested {qty} ton(s)."
            )

        # reserve
        await database.execute("""
            UPDATE inventory_items
               SET qty_reserved = qty_reserved + :q, updated_at = NOW()
             WHERE seller=:seller AND sku=:sku
        """, {"q": qty, "seller": seller, "sku": sku})

        # movement (now linked to contract id)
        await database.execute("""
            INSERT INTO inventory_movements (seller, sku, movement_type, qty, ref_contract, meta)
            VALUES (:seller, :sku, 'reserve', :q, :cid, :meta)
        """, {
            "seller": seller, "sku": sku, "q": qty, "cid": cid,
            "meta": json.dumps({"reason": "contract_create"})
        })

        # create contract row using the same cid
        row = await database.fetch_one("""
            INSERT INTO contracts (id, buyer, seller, material, weight_tons, price_per_ton, status)
            VALUES (:id, :buyer, :seller, :material, :weight_tons, :price_per_ton, 'Pending')
            RETURNING *
        """, {"id": cid, **contract.dict()})

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

@app.put(
    "/contracts/{contract_id}",
    response_model=ContractOut,
    tags=["Contracts"],
    summary="Update Contract",
    status_code=200
)
async def update_contract(
    contract_id: str,
    update: ContractUpdate,
    request: Request   # ✅ add this so request is available
):
    row = await database.fetch_one(
        """
        UPDATE contracts
        SET status = :status,
            signature = :signature,
            signed_at = CASE WHEN :signature IS NOT NULL THEN NOW() ELSE signed_at END
        WHERE id = :id
        RETURNING *
        """,
        {
            "id": contract_id,
            "status": update.status,
            "signature": update.signature
        }
    )

    if not row:
        raise HTTPException(status_code=404, detail="Contract not found")

    # ---- audit log
    actor = request.session.get("username") if hasattr(request, "session") else None
    try:
        await log_action(
            actor or "system",
            "contract.update",
            str(contract_id),
            {
                "status": update.status,
                "signature_present": update.signature is not None,
            }
        )
    except Exception:
        pass

    return row  

@app.get("/contracts/export_csv", tags=["Contracts"], summary="Export Contracts as CSV", status_code=200)
async def export_contracts_csv():
    rows = await database.fetch_all("SELECT * FROM contracts ORDER BY created_at DESC")
    rows = [dict(r) for r in rows]
    if not rows:
        return StreamingResponse(
            iter(["id\n"]),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="contracts.csv"'}
        )
    fieldnames = list(rows[0].keys())
    return StreamingResponse(
        _iter_csv(rows, fieldnames),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="contracts.csv"'}
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

# === INSERT: Back-compat aliases for older UI buttons ===
@app.get("/admin/export_all", include_in_schema=False)
def _alias_export_all():
    return RedirectResponse(url="/admin/exports/all.zip", status_code=307)

@app.get("/contracts/export_csv", include_in_schema=False)
def _alias_export_csv():
    return RedirectResponse(url="/admin/exports/contracts.csv", status_code=307)
# === /INSERT ===

from typing import Optional
from fastapi import HTTPException

# --- MATERIAL PRICE HISTORY (by day, avg) ---
@app.get(
    "/analytics/material_price_history",
    tags=["Analytics"],
    summary="Get historical contract prices by material",
    description="Returns daily average price_per_ton for a material (optionally filtered by seller)."
)
async def material_price_history(material: str, seller: Optional[str] = None):
    if seller:
        q = """
            SELECT (created_at AT TIME ZONE 'utc')::date AS d,
                   ROUND(AVG(price_per_ton)::numeric, 2) AS avg_price
            FROM contracts
            WHERE material = :material
              AND seller = :seller
              AND price_per_ton IS NOT NULL
            GROUP BY d
            ORDER BY d;
        """
        rows = await database.fetch_all(q, {"material": material, "seller": seller})
    else:
        q = """
            SELECT (created_at AT TIME ZONE 'utc')::date AS d,
                   ROUND(AVG(price_per_ton)::numeric, 2) AS avg_price
            FROM contracts
            WHERE material = :material
              AND price_per_ton IS NOT NULL
            GROUP BY d
            ORDER BY d;
        """
        rows = await database.fetch_all(q, {"material": material})

    return [{"date": str(r["d"]), "avg_price": float(r["avg_price"])} for r in rows]

@app.get("/analytics/rolling_bands", tags=["Analytics"], summary="Rolling 7/30/90d bands per material")
async def rolling_bands(material: str, days: int = 365):
    q = """
    WITH d AS (
      SELECT (created_at AT TIME ZONE 'utc')::date AS d, AVG(price_per_ton) AS avg_p
      FROM contracts
      WHERE material = :m
        AND price_per_ton IS NOT NULL
        AND created_at >= NOW() - make_interval(days => :days)
      GROUP BY 1
      ORDER BY 1
    )
    SELECT d::date AS date,
           avg_p   AS avg_price,
           AVG(avg_p) OVER (ORDER BY d ROWS BETWEEN 6  PRECEDING AND CURRENT ROW) AS band_7,
           AVG(avg_p) OVER (ORDER BY d ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS band_30,
           AVG(avg_p) OVER (ORDER BY d ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS band_90
    FROM d;
    """
    rows = await database.fetch_all(q, {"m": material, "days": days})
    return [dict(r) for r in rows]

@app.post("/indices/generate_snapshot", tags=["Analytics"], summary="Generate daily index snapshot for a date (default today)")
async def indices_generate_snapshot(snapshot_date: Optional[date] = None):
    d = (snapshot_date or date.today()).isoformat()
    q = """
    INSERT INTO indices_daily (as_of_date, region, material, avg_price, volume_tons)
    SELECT :d::date AS as_of_date,
           LOWER(seller) AS region,
           material,
           AVG(price_per_ton) AS avg_price,
           SUM(weight_tons)  AS volume_tons
      FROM contracts
     WHERE created_at >= :d::date
       AND created_at  < (:d::date + INTERVAL '1 day')
     GROUP BY region, material
    ON CONFLICT (as_of_date, region, material) DO UPDATE
      SET avg_price  = EXCLUDED.avg_price,
          volume_tons= EXCLUDED.volume_tons
    """
    await database.execute(q, {"d": d})
    try:
        await emit_event("index.snapshot.created", {"as_of_date": d})
    except Exception:
        pass
    METRICS_INDICES_SNAPSHOTS.inc()
    return {"ok": True, "date": d}

@app.post("/indices/backfill", tags=["Analytics"], summary="Backfill indices for a date range (inclusive)")
async def indices_backfill(start: date = Query(...), end: date = Query(...)):
    day = start; n = 0
    while day <= end:
        await indices_generate_snapshot(snapshot_date=day)
        day += timedelta(days=1); n += 1
    return {"ok": True, "days_processed": n, "from": start.isoformat(), "to": end.isoformat()}

@app.get("/public/indices/daily.json", tags=["Analytics"], summary="Public daily index JSON")
async def public_indices_json(days: int = 365, region: Optional[str] = None, material: Optional[str] = None):
    q = """
    SELECT as_of_date, region, material, avg_price, volume_tons
      FROM indices_daily
     WHERE as_of_date >= CURRENT_DATE - (:days || ' days')::interval
    """
    vals = {"days": days}
    if region:   q += " AND region = :region";   vals["region"] = region.lower()
    if material: q += " AND material = :material"; vals["material"] = material
    q += " ORDER BY as_of_date DESC, region, material"
    rows = await database.fetch_all(q, vals)
    return [{"as_of_date": r["as_of_date"].isoformat(),
             "region": r["region"], "material": r["material"],
             "avg_price": float(r["avg_price"]), "volume_tons": float(r["volume_tons"])} for r in rows]

@app.get("/public/indices/daily.csv", tags=["Analytics"], summary="Public daily index CSV")
async def public_indices_csv(days: int = 365, region: Optional[str] = None, material: Optional[str] = None):
    data = await public_indices_json(days=days, region=region, material=material)
    out = io.StringIO(); w = csv.writer(out)
    w.writerow(["as_of_date","region","material","avg_price","volume_tons"])
    for r in data:
        w.writerow([r["as_of_date"], r["region"], r["material"], r["avg_price"], r["volume_tons"]])
    out.seek(0)
    return StreamingResponse(iter([out.read()]), media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="indices_daily.csv"'})

# --- PRICE BAND ESTIMATES (min/max/avg/stddev) ---
@app.get(
    "/analytics/price_band_estimates",
    tags=["Analytics"],
    summary="Get price band stats for a material",
    description="Returns min, max, avg, and std deviation of price_per_ton for the material."
)
async def price_band_estimates(material: str):
    q = """
        SELECT
            MIN(price_per_ton) AS min_p,
            MAX(price_per_ton) AS max_p,
            AVG(price_per_ton) AS avg_p,
            COALESCE(stddev_samp(price_per_ton), 0) AS sd_p
        FROM contracts
        WHERE material = :material
          AND price_per_ton IS NOT NULL;
    """
    row = await database.fetch_one(q, {"material": material})
    if not row or row["min_p"] is None:
        raise HTTPException(status_code=404, detail="No prices found for that material.")
    return {
        "material": material,
        "min": round(float(row["min_p"]), 2),
        "max": round(float(row["max_p"]), 2),
        "average": round(float(row["avg_p"]), 2),
        "std_dev": round(float(row["sd_p"]), 2),
    }


# --- DELTA ANOMALIES (outside avg ± 2*stddev) ---
@app.get(
    "/analytics/delta_anomalies",
    tags=["Analytics"],
    summary="Flag outlier contracts by price deviation",
    description="Returns contracts where price_per_ton is outside average ± 2*std_dev for the material."
)
async def delta_anomalies(material: str):
    stats_q = """
        SELECT
            AVG(price_per_ton) AS avg_p,
            COALESCE(stddev_samp(price_per_ton), 0) AS sd_p
        FROM contracts
        WHERE material = :material
          AND price_per_ton IS NOT NULL;
    """
    stats = await database.fetch_one(stats_q, {"material": material})
    if not stats or stats["avg_p"] is None:
        return []

    avg_p = float(stats["avg_p"])
    sd_p  = float(stats["sd_p"])
    if sd_p == 0:
        return []

    lower, upper = avg_p - 2 * sd_p, avg_p + 2 * sd_p

    anomalies_q = """
        SELECT id, material, seller, price_per_ton, created_at
        FROM contracts
        WHERE material = :material
          AND price_per_ton IS NOT NULL
          AND (price_per_ton < :lower OR price_per_ton > :upper)
        ORDER BY created_at DESC
        LIMIT 200;
    """
    rows = await database.fetch_all(anomalies_q, {"material": material, "lower": lower, "upper": upper})
    return [dict(r) for r in rows]

# --- INDICES FEED ---
@app.get("/indices", tags=["Analytics"], summary="Return latest BRidge scrap index values", description="Shows the most recent average price snapshot for tracked materials in each region.")
async def get_latest_indices():
    query = """
        SELECT DISTINCT ON (region, sku)
               region, sku, avg_price, snapshot_date
        FROM index_snapshots
        ORDER BY region, sku, snapshot_date DESC;
    """
    rows = await database.fetch_all(query)
    return {
        f"{r['region'].lower()}_{r['sku'].lower()}_index": float(r["avg_price"])
        for r in rows
    }

@app.get("/export/tax_lookup", tags=["Compliance"], summary="Duty/Tax lookup by HS + destination")
async def tax_lookup(hs_code: str, dest: str):
    row = await database.fetch_one("""
      SELECT duty_usd, tax_pct
        FROM export_rules
       WHERE :hs LIKE hs_prefix || '%' AND dest = :dest
       ORDER BY length(hs_prefix) DESC
       LIMIT 1
    """, {"hs": hs_code.strip(), "dest": dest.upper()})
    return {"hs_code": hs_code.strip(), "dest": dest.upper(),
            "tax_pct": float(row["tax_pct"]) if row else 0.0,
            "duty_usd": float(row["duty_usd"]) if row else 0.0}

# --- MANUAL SNAPSHOT GENERATOR (TEMP/ADMIN) ---
@app.post("/indices/generate_snapshot", tags=["Admin"], summary="Manually generate an index snapshot", description="Aggregates avg price per SKU + region from contracts table and stores it as a snapshot.")
async def generate_snapshot():
    agg_q = """
        SELECT LOWER(seller) AS region, sku, AVG(price_per_unit) AS avg_price
        FROM contracts
        WHERE price_per_unit IS NOT NULL
        GROUP BY region, sku;
    """
    rows = await database.fetch_all(agg_q)
    if not rows:
        return {"ok": True, "snapshots_created": 0}

    insert_q = """
        INSERT INTO index_snapshots (region, sku, avg_price)
        VALUES (:region, :sku, :avg_price);
    """
    count = 0
    for r in rows:
        await database.execute(insert_q, {"region": r["region"], "sku": r["sku"], "avg_price": r["avg_price"]})
        count += 1
    return {"ok": True, "snapshots_created": count}

# ========== Admin Exports router ==========
from fastapi import APIRouter
from fastapi.responses import StreamingResponse, RedirectResponse
from sqlalchemy import text as _sqltext
import io, csv, zipfile

admin_exports = APIRouter(prefix="/admin/exports", tags=["Admin"])

@admin_exports.get("/contracts.csv", summary="Contracts CSV (streamed)")
def export_contracts_csv_admin():
    with engine.begin() as conn:
        rows = conn.execute(_sqltext("""
            SELECT 
              id, buyer, seller, material, sku, weight_tons, price_per_ton,
              total_value, status, contract_date, pickup_time, delivery_time, currency
            FROM contracts
            ORDER BY contract_date DESC NULLS LAST, id DESC
        """)).mappings().all()
        cols = rows[0].keys() if rows else []

        def _iter():
            buf = io.StringIO(); w = csv.writer(buf)
            if cols: w.writerow(cols); yield buf.getvalue(); buf.seek(0); buf.truncate(0)
            for r in rows:
                w.writerow([r.get(c) for c in cols])
                yield buf.getvalue(); buf.seek(0); buf.truncate(0)

        return StreamingResponse(
            _iter(),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="contracts.csv"'}
        )

@admin_exports.get("/all.zip", summary="All core tables as CSV (ZIP)")
def export_all_zip_admin():
    queries = {
        "contracts.csv": """
            SELECT id, buyer, seller, material, sku, weight_tons, price_per_ton,
                   total_value, status, contract_date, pickup_time, delivery_time, currency
            FROM contracts
            ORDER BY contract_date DESC NULLS LAST, id DESC
        """,
        "bols.csv": """
            SELECT bol_id, contract_id, buyer, seller, material, weight_tons, status,
                   pickup_time, delivery_time, total_value
            FROM bols
            ORDER BY pickup_time DESC NULLS LAST, bol_id DESC
        """,
        "inventory_items.csv": """
            SELECT seller, sku, description, uom, location, qty_on_hand, qty_reserved,
                   qty_committed, source, external_id, updated_at
            FROM inventory_items
            ORDER BY seller, sku
        """,
        "inventory_movements.csv": """
            SELECT seller, sku, movement_type, qty, ref_contract, created_at
            FROM inventory_movements
            ORDER BY created_at DESC
        """
    }

    with engine.begin() as conn:
        mem = io.BytesIO()
        with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for fname, sql in queries.items():
                rows = conn.execute(_sqltext(sql)).mappings().all()
                cols = rows[0].keys() if rows else []
                s = io.StringIO(); w = csv.writer(s)
                if cols: w.writerow(cols)
                for r in rows:
                    w.writerow([r.get(c) for c in cols])
                zf.writestr(fname, s.getvalue())
        mem.seek(0)
        return StreamingResponse(
            mem,
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="bridge_export_all.zip"'}
        )

# mount router (place once, near where you include other routers)
app.include_router(admin_exports)

# ========== /Admin Exports router ==========
import asyncio
import inspect
from fastapi import Request

class ContractInExtended(ContractIn):
    pricing_formula: Optional[str] = None
    reference_symbol: Optional[str] = None
    reference_price: Optional[float] = None
    reference_source: Optional[str] = None
    reference_timestamp: Optional[datetime] = None
    currency: Optional[str] = "USD"

@app.post("/contracts", response_model=ContractOut, tags=["Contracts"], summary="Create Contract", status_code=201)
async def create_contract(contract: ContractInExtended, request: Request):
    await _check_contract_quota()
    qty    = float(contract.weight_tons)
    seller = contract.seller.strip()
    sku    = contract.material.strip()
    cid    = str(uuid.uuid4())

    async with database.transaction():
        # ensure inventory row exists
        await database.execute("""
            INSERT INTO inventory_items (seller, sku, qty_on_hand, qty_reserved, qty_committed)
            VALUES (:seller, :sku, 0, 0, 0)
            ON CONFLICT (seller, sku) DO NOTHING
        """, {"seller": seller, "sku": sku})

        # check availability & reserve
        inv = await database.fetch_one("""
            SELECT qty_on_hand, qty_reserved, qty_committed
            FROM inventory_items
            WHERE seller=:seller AND sku=:sku
            FOR UPDATE
        """, {"seller": seller, "sku": sku})
        on_hand   = float(inv["qty_on_hand"]) if inv else 0.0
        reserved  = float(inv["qty_reserved"]) if inv else 0.0
        committed = float(inv["qty_committed"]) if inv else 0.0
        available = on_hand - reserved - committed
        if available < qty:
            raise HTTPException(409, f"Not enough inventory: available {available} ton(s) < requested {qty} ton(s).")

        await database.execute("""
            UPDATE inventory_items
               SET qty_reserved = qty_reserved + :q, updated_at = NOW()
             WHERE seller=:seller AND sku=:sku
        """, {"q": qty, "seller": seller, "sku": sku})

        await database.execute("""
            INSERT INTO inventory_movements (seller, sku, movement_type, qty, ref_contract, meta)
            VALUES (:seller, :sku, 'reserve', :q, :cid, :meta)
        """, {"seller": seller, "sku": sku, "q": qty, "cid": cid,
              "meta": json.dumps({"reason": "contract_create"})})

        # --- Internal ref snapshot if caller didn't pass one (BEFORE INSERT) ---
        if not (contract.reference_price and contract.reference_source):
            try:
                import httpx
                async with httpx.AsyncClient(timeout=6) as c:
                    r = await c.get(f"{request.base_url}prices/copper_last")
                    if 200 <= r.status_code < 300:
                        j = r.json()
                        contract.reference_price = float(j.get("last"))
                        contract.reference_source = "COMEX (derived/internal, delayed)"
                        contract.reference_symbol = "HG=F"
                        contract.reference_timestamp = datetime.utcnow()
                        contract.pricing_formula = contract.pricing_formula or "COMEX_Cu - 0.25"
            except Exception:
                pass

        # INSERT with pricing fields (single payload)
        payload = {
            "id": cid,
            "buyer": contract.buyer,
            "seller": contract.seller,
            "material": contract.material,
            "weight_tons": contract.weight_tons,
            "price_per_ton": contract.price_per_ton,
            "status": "Pending",
            "pricing_formula": contract.pricing_formula,
            "reference_symbol": contract.reference_symbol,
            "reference_price": contract.reference_price,
            "reference_source": contract.reference_source,
            "reference_timestamp": contract.reference_timestamp,
            "currency": contract.currency or "USD",
        }
        row = await database.fetch_one("""
            INSERT INTO contracts (id, buyer, seller, material, weight_tons, price_per_ton, status,
                                   pricing_formula, reference_symbol, reference_price, reference_source, reference_timestamp, currency)
            VALUES (:id, :buyer, :seller, :material, :weight_tons, :price_per_ton, :status,
                    :pricing_formula, :reference_symbol, :reference_price, :reference_source, :reference_timestamp, :currency)
            RETURNING *
        """, payload)

    # audit (disclaimer)
    try:
        actor = request.session.get("username") if hasattr(request, "session") else None
        await log_action(actor or "system", "contract.create", str(row["id"]), {
            "pricing_disclaimer": "Reference prices are used internally; not rebroadcast."
        })
    except Exception:
        pass

    # metric + webhook
    METRICS_CONTRACTS_CREATED.inc()
    try:
        await emit_event_safe("contract.updated", {
            "contract_id": str(row["id"]),
            "status": row["status"],
            "seller": row["seller"],
            "buyer": row["buyer"],
            "material": row["material"],
            "weight_tons": float(row["weight_tons"]),
            "price_per_ton": float(row["price_per_ton"]),
            "currency": (row.get("currency") if isinstance(row, dict) else None) or "USD",
        })
    except Exception:
        pass

    return row

@app.post("/contracts/{contract_id}/sign", tags=["Contracts"], summary="Sign a contract", status_code=200)
async def sign_contract(contract_id: str, request: Request):
    # Flip status to Signed (idempotent-ish)
    _ = await database.fetch_one("""
      UPDATE contracts
         SET status='Signed', signed_at = COALESCE(signed_at, NOW())
       WHERE id=:id AND status <> 'Signed'
       RETURNING id
    """, {"id": contract_id})

    actor = request.session.get("username") if hasattr(request, "session") else None
    bol_id = request.query_params.get("bol_id")
    signer_ip = request.client.host if request.client else None
    signed_by = actor or "unknown"

    # ---- audit log
    actor = request.session.get("username") if hasattr(request, "session") else None
    bol_id = request.query_params.get("bol_id")  # None if not provided
    signer_ip = request.client.host if request.client else None
    signed_by = actor or "unknown"
    payload = {"new_status": "Signed", "bol_id": bol_id}

    try:
        _res = log_action(actor or "system", "contract.purchase", str(contract_id), payload)
        if inspect.isawaitable(_res):
            asyncio.create_task(_res)
    except Exception:
        pass

    # ---- webhook emit (signature.received)
    try:
        await emit_event_safe("signature.received", {
            "contract_id": str(contract_id),
            "bol_id": str(bol_id) if bol_id else None,
            "signed_by": signed_by,
            "ip": signer_ip,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        })
    except Exception:
        pass
    try:
        await emit_event_safe("contract.updated", {"contract_id": str(contract_id), "status": "Signed"})
    except Exception:
        pass

    return {"status": "ok", "contract_id": contract_id}

async def emit_event_safe(event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        res = await emit_event(event_type, payload)
        if not res.get("ok"):
            try:
                await database.execute("""
                  INSERT INTO webhook_dead_letters (event, payload, status_code, response_text)
                  VALUES (:e, :p::jsonb, :sc, :rt)
                """, {
                    "e": event_type,
                    "p": json.dumps(payload),
                    "sc": res.get("status_code"),
                    "rt": res.get("response")
                })
            except Exception:
                pass
        return res
    except Exception:
        try:
            await database.execute("""
              INSERT INTO webhook_dead_letters (event, payload, status_code, response_text)
              VALUES (:e, :p::jsonb, NULL, 'exception')
            """, {"e": event_type, "p": json.dumps(payload)})
        except Exception:
            pass
        return {"ok": False, "status_code": None, "response": "exception"}
class ApplicationIn(BaseModel):
    # use Field(pattern=...) for Pydantic v2; if on v1, switch to constr(regex=...)
    entity_type: str = Field(pattern=r"^(yard|mill|industrial|manufacturer|broker)$")
    role: str = Field(pattern=r"^(buyer|seller|both)$")
    org_name: str
    ein: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None
    website: Optional[str] = None
    monthly_volume_tons: Optional[int] = None
    contact_name: str
    email: EmailStr
    phone: Optional[str] = None
    ref_code: Optional[str] = None
    materials_buy: Optional[str] = None
    materials_sell: Optional[str] = None
    lanes: Optional[str] = None
    compliance_notes: Optional[str] = None
    plan: str = Field(pattern=r"^(starter|standard|enterprise)$")
    notes: Optional[str] = None
    utm_source: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_medium: Optional[str] = None

class ApplicationOut(BaseModel):
    application_id: str
    status: str
    message: str

# --- Public endpoint (replaces /public/yard_signup) ---
@app.post(
    "/public/apply",
    tags=["Public"],
    summary="Public application (multi-entity)",
    description="Collects onboarding applications from yards, mills, industrial generators, manufacturers, and brokers.",
    response_model=ApplicationOut,
    status_code=201,
)
async def public_apply(payload: ApplicationIn, request: Request):
    application_id = str(uuid.uuid4())

    # Prevent spam/dupes for 24h by org+email
    existing = await database.fetch_one(
        """
        SELECT application_id FROM tenant_applications
        WHERE email = :email AND org_name = :org_name
          AND created_at > (now() - interval '24 hours')
        LIMIT 1
        """,
        {"email": payload.email, "org_name": payload.org_name},
    )
    if existing:
        return ApplicationOut(
            application_id=str(existing["application_id"]),
            status="pending",
            message="Application already received. We'll reach out shortly.",
        )

    await database.execute(
        """
        INSERT INTO tenant_applications (
          application_id, entity_type, role, org_name, ein, address, city, region, website,
          monthly_volume_tons, contact_name, email, phone, ref_code, materials_buy, materials_sell,
          lanes, compliance_notes, plan, notes, utm_source, utm_campaign, utm_medium
        ) VALUES (
          :application_id, :entity_type, :role, :org_name, :ein, :address, :city, :region, :website,
          :monthly_volume_tons, :contact_name, :email, :phone, :ref_code, :materials_buy, :materials_sell,
          :lanes, :compliance_notes, :plan, :notes, :utm_source, :utm_campaign, :utm_medium
        )
        """,
        {"application_id": application_id, **payload.model_dump()},
    )

    # Best-effort audit (ignore failures)
    try:
        actor = getattr(request, "session", {}).get("username") if hasattr(request, "session") else None
        await log_action(actor or "public", "application.create", application_id, payload.model_dump())
    except Exception:
        pass

    return ApplicationOut(application_id=application_id, status="pending", message="Application received.")

# --- Admin listings/approve/export ---
class ApplicationRow(BaseModel):
    application_id: str
    created_at: datetime
    status: str
    entity_type: str
    role: str
    org_name: str
    email: EmailStr
    contact_name: str
    monthly_volume_tons: Optional[int] = None
    plan: str
    ref_code: Optional[str] = None

@app.get("/admin/applications", tags=["Admin"], response_model=List[ApplicationRow], summary="List applications")
async def list_applications(limit: int = 200):
    rows = await database.fetch_all(
        """
        SELECT application_id, created_at, status, entity_type, role, org_name, email,
               contact_name, monthly_volume_tons, plan, ref_code
        FROM tenant_applications
        ORDER BY created_at DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )
    return [ApplicationRow(**dict(r)) for r in rows]

@app.post("/admin/applications/{application_id}/approve", tags=["Admin"], summary="Approve application")
async def approve_application(application_id: str):
    await database.execute(
        "UPDATE tenant_applications SET status='approved' WHERE application_id=:id",
        {"id": application_id},
    )
    return {"ok": True, "application_id": application_id}

@app.get("/admin/applications/export_csv", tags=["Admin"], summary="Export applications CSV")
async def export_applications_csv():
    rows = await database.fetch_all("SELECT * FROM tenant_applications ORDER BY created_at DESC")
    out = io.StringIO()
    if rows:
        headers = list(rows[0].keys())
        w = csv.writer(out); w.writerow(headers)
        for r in rows:
            w.writerow([r[h] for h in headers])
    data = out.getvalue().encode()
    return StreamingResponse(io.BytesIO(data), media_type="text/csv", headers={
        "Content-Disposition": "attachment; filename=tenant_applications.csv"
    })
# -------- BOLs (with PDF generation) --------
@app.post(
    "/bols",
    response_model=BOLOut,
    tags=["BOLs"],
    summary="Create BOL",
    status_code=201
    )
async def create_bol_pg(bol: BOLIn, request: Request):
   # Optional idempotency
    idem_key = request.headers.get("Idempotency-Key")
    if idem_key and idem_key in _idem_cache:
        return _idem_cache[idem_key]

    row = await database.fetch_one("""
        INSERT INTO bols (
            bol_id, contract_id, buyer, seller, material, weight_tons,
            price_per_unit, total_value,
            carrier_name, carrier_driver, carrier_truck_vin,
            pickup_signature_base64, pickup_signature_time,
            pickup_time, status,
            origin_country, destination_country, port_code, hs_code, duty_usd, tax_pct
        )
        VALUES (
            :bol_id, :contract_id, :buyer, :seller, :material, :weight_tons,
            :price_per_unit, :total_value,
            :carrier_name, :carrier_driver, :carrier_truck_vin,
            :pickup_sig_b64, :pickup_sig_time,
            :pickup_time, 'Scheduled',
            :origin_country, :destination_country, :port_code, :hs_code, :duty_usd, :tax_pct
        )
        RETURNING *
    """, {
        "bol_id": str(uuid.uuid4()),
        "contract_id": str(bol.contract_id),
        "buyer": bol.buyer, "seller": bol.seller, "material": bol.material,
        "weight_tons": bol.weight_tons, "price_per_unit": bol.price_per_unit, "total_value": bol.total_value,
        "carrier_name": bol.carrier.name, "carrier_driver": bol.carrier.driver, "carrier_truck_vin": bol.carrier.truck_vin,
        "pickup_sig_b64": bol.pickup_signature.base64, "pickup_sig_time": bol.pickup_signature.timestamp,
        "pickup_time": bol.pickup_time,
        "origin_country": bol.origin_country,
        "destination_country": bol.destination_country,
        "port_code": bol.port_code,
        "hs_code": bol.hs_code,
        "duty_usd": bol.duty_usd,
        "tax_pct": bol.tax_pct,
    })


    resp = {
        **bol.dict(),
        "bol_id": row["bol_id"],
        "status": row["status"],
        "delivery_signature": None,
        "delivery_time": None
    }

    # ---- audit
    try:
        actor = request.session.get("username") if hasattr(request, "session") else None
        await log_action(actor or "system", "bol.create", str(row["bol_id"]), {
            "contract_id": str(bol.contract_id),
            "material": bol.material,
            "weight_tons": bol.weight_tons
        })
    except Exception:
        pass

    # ---- webhook
    try:
        await emit_event_safe("bol.created", {
            "bol_id": row["bol_id"],
            "contract_id": row["contract_id"],
            "seller": row["seller"], "buyer": row["buyer"],
            "material": row["material"], "weight_tons": float(row["weight_tons"]),
            "status": row["status"]
        })
    except Exception:
        pass

    # ---- metric
    METRICS_BOLS_CREATED.inc()

    if idem_key:
        _idem_cache[idem_key] = resp
    return resp
# -------- BOLs (with PDF generation) --------

# =============== Admin Exports (core tables) ===============
   
from fastapi import APIRouter
from fastapi.responses import StreamingResponse, RedirectResponse
from sqlalchemy import text as _sqltext
import io, csv, zipfile

@admin_exports.get("/contracts.csv", summary="Contracts CSV (streamed)")
def export_contracts_csv_admin():
    with engine.begin() as conn:
        rows = conn.execute(_sqltext("""
            SELECT 
              id, buyer, seller, material, sku, weight_tons, price_per_ton,
              total_value, status, contract_date, pickup_time, delivery_time, currency
            FROM contracts
            ORDER BY contract_date DESC NULLS LAST, id DESC
        """)).mappings().all()
        cols = rows[0].keys() if rows else []

        def _iter():
            buf = io.StringIO(); w = csv.writer(buf)
            if cols:
                w.writerow(cols); yield buf.getvalue(); buf.seek(0); buf.truncate(0)
            for r in rows:
                w.writerow([r.get(c) for c in cols])
                yield buf.getvalue(); buf.seek(0); buf.truncate(0)

        return StreamingResponse(
            _iter(),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="contracts.csv"'}
        )

@admin_exports.get("/all.zip", summary="All core tables as CSV (ZIP)")
def export_all_zip_admin():
    queries = {
        "contracts.csv": """
            SELECT id, buyer, seller, material, sku, weight_tons, price_per_ton,
                   total_value, status, contract_date, pickup_time, delivery_time, currency
            FROM contracts
            ORDER BY contract_date DESC NULLS LAST, id DESC
        """,
        "bols.csv": """
            SELECT bol_id, contract_id, buyer, seller, material, weight_tons, status,
                   pickup_time, delivery_time, total_value
            FROM bols
            ORDER BY pickup_time DESC NULLS LAST, bol_id DESC
        """,
        "inventory_items.csv": """
            SELECT seller, sku, description, uom, location, qty_on_hand, qty_reserved,
                   qty_committed, source, external_id, updated_at
            FROM inventory_items
            ORDER BY seller, sku
        """,
        "inventory_movements.csv": """
            SELECT seller, sku, movement_type, qty, ref_contract, created_at
            FROM inventory_movements
            ORDER BY created_at DESC
        """
    }

    with engine.begin() as conn:
        mem = io.BytesIO()
        with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for fname, sql in queries.items():
                rows = conn.execute(_sqltext(sql)).mappings().all()
                cols = rows[0].keys() if rows else []
                s = io.StringIO(); w = csv.writer(s)
                if cols: w.writerow(cols)
                for r in rows:
                    w.writerow([r.get(c) for c in cols])
                zf.writestr(fname, s.getvalue())
        mem.seek(0)
        return StreamingResponse(
            mem,
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="bridge_export_all.zip"'}
        )

# === /INSERT ===

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


# =============== FUTURES (Admin) ===============
futures_router = APIRouter(
    prefix="/admin/futures",
    tags=["Futures"],
    dependencies=[Depends(_require_admin)]
)

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

@futures_router.post("/series/{listing_id}/finalize", summary="Finalize cash settlement at expiry")
async def finalize_series(listing_id: str, mark_date: Optional[date] = None):
    dt = mark_date or _date.today()

    cur = await database.fetch_one("""
      SELECT mark_price FROM futures_marks
      WHERE listing_id=:l AND mark_date <= :d
      ORDER BY mark_date DESC LIMIT 1
    """, {"l": listing_id, "d": dt})
    if not cur or cur["mark_price"] is None:
        raise HTTPException(400, "no settlement mark available")

    prev = await database.fetch_one("""
      SELECT mark_price FROM futures_marks
      WHERE listing_id=:l AND mark_date < :d
      ORDER BY mark_date DESC LIMIT 1
    """, {"l": listing_id, "d": dt})
    if not prev or prev["mark_price"] is None:
        raise HTTPException(400, "no prior mark to settle against")

    px_t, px_y = float(cur["mark_price"]), float(prev["mark_price"])

    cs = await database.fetch_one("""
      SELECT fp.contract_size_tons AS cs
      FROM futures_listings fl
      JOIN futures_products fp ON fp.id = fl.product_id
      WHERE fl.id=:l
    """, {"l": listing_id})
    if not cs:
        raise HTTPException(404, "listing/product not found for settlement")
    contract_size = float(cs["cs"])

    pos = await database.fetch_all("""
      SELECT account_id, net_qty
      FROM positions
      WHERE listing_id=:l
    """, {"l": listing_id})

    async with database.transaction():
        for r in pos:
            pnl = (px_t - px_y) * float(r["net_qty"]) * contract_size
            if pnl != 0.0:
                await _adjust_margin(str(r["account_id"]), pnl, "final_settlement", None)

        await database.execute("""
          UPDATE futures_listings SET trading_status='Expired', status='Expired' WHERE id=:l
        """, {"l": listing_id})
        await database.execute("""
          UPDATE orders SET status='CANCELLED' WHERE listing_id=:l AND status IN ('NEW','PARTIAL')
        """, {"l": listing_id})

    return {"ok": True, "listing_id": listing_id, "final_price": px_t, "previous_price": px_y}

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
    rows = await database.fetch_all("""
        SELECT 
          id::text AS id,
          symbol_root,
          material,
          delivery_location,
          contract_size_tons,
          tick_size,
          currency,
          price_method
        FROM futures_products
        ORDER BY created_at DESC
    """)
    return [dict(r) for r in rows]

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

# ---- MARKET price-protection helpers (NEW) ----
PRICE_BAND_PCT = float(os.getenv("PRICE_BAND_PCT", "0.10"))  # 10% default band

async def _ref_price_for(listing_id: str) -> Optional[float]:
    # Prefer official marks
    r = await database.fetch_one("""
      SELECT mark_price FROM futures_marks
      WHERE listing_id=:l ORDER BY mark_date DESC LIMIT 1
    """, {"l": listing_id})
    if r and r["mark_price"] is not None:
        return float(r["mark_price"])
    # Fallback to simple mid from top-of-book
    bid = await database.fetch_one("""
      SELECT price FROM orders WHERE listing_id=:l AND side='BUY' AND status IN ('NEW','PARTIAL')
      ORDER BY price DESC LIMIT 1
    """, {"l": listing_id})
    ask = await database.fetch_one("""
      SELECT price FROM orders WHERE listing_id=:l AND side='SELL' AND status IN ('NEW','PARTIAL')
      ORDER BY price ASC LIMIT 1
    """, {"l": listing_id})
    if bid and ask:
        return (float(bid["price"]) + float(ask["price"])) / 2.0
    return float(bid["price"]) if bid else (float(ask["price"]) if ask else None)

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

        # --- market price protection (guard band) ---
        cap_min = cap_max = None
        if is_market:
            ref = await _ref_price_for(ord_in.listing_id)
            if ref is not None:
                lo = ref * (1.0 - PRICE_BAND_PCT)
                hi = ref * (1.0 + PRICE_BAND_PCT)
                if side == "BUY":
                    cap_max = hi   # do not execute above cap_max
                else:
                    cap_min = lo   # do not execute below cap_min

        if side == "BUY":
            if is_market:
                opp = await database.fetch_all("""
                  SELECT * FROM orders
                   WHERE listing_id=:l AND side='SELL' AND status IN ('NEW','PARTIAL')
                     AND (:cap_max IS NULL OR price <= :cap_max)
                   ORDER BY price ASC, created_at ASC
                   FOR UPDATE
                """, {"l": ord_in.listing_id, "cap_max": cap_max})
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
                     AND (:cap_min IS NULL OR price >= :cap_min)
                   ORDER BY price DESC, created_at ASC
                   FOR UPDATE
                """, {"l": ord_in.listing_id, "cap_min": cap_min})
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

            # price cap enforcement for market orders
            if is_market:
                if side == "BUY" and cap_max is not None and trade_px > cap_max:
                    break
                if side == "SELL" and cap_min is not None and trade_px < cap_min:
                    break

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
        # Early initial-margin check on increased exposure 
        if delta_open > 0:
            params = await _get_margin_params(account_id)
            if params.get("is_blocked"):
                raise HTTPException(402, "account blocked pending margin call")
            contract_size = await _get_contract_size(listing_id)
            est_notional = new_price * contract_size * delta_open
            need = float(params["initial_pct"]) * est_notional
            if float(params["balance"]) < need:
                raise HTTPException(402, f"insufficient initial margin for modification; need ≥ {need:.2f}")

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
        headers={"Content-Disposition": f'attachment; filename=\"orders_{day}.csv\"'}
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
        headers={"Content-Disposition": f'attachment; filename=\"trades_{day}.csv\"'}
    )

app.include_router(trade_router)
# =================== /TRADING (Order Book) =====================
async def _fetch_csv_rows(query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    rows = await database.fetch_all(query, params or {})
    return [dict(r) for r in rows]

async def build_export_zip() -> bytes:
    """
    Builds a ZIP with CSVs for contracts, bols, inventory, users, index_snapshots.
    Mirrors /admin/export_all.zip behavior but returns bytes for reuse (cron/upload).
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
       exports = {
    # contracts table: id, buyer, seller, material, weight_tons, price_per_ton, ...
    "contracts.csv": """
        SELECT id, buyer, seller, material, weight_tons, price_per_ton,
               status, created_at, signed_at, signature
        FROM contracts
        ORDER BY created_at DESC
    """,
    # bols table: bol_id (not id) and detailed columns you actually store
    "bols.csv": """
        SELECT bol_id, contract_id, buyer, seller, material, weight_tons,
               price_per_unit, total_value,
               carrier_name, carrier_driver, carrier_truck_vin,
               pickup_signature_base64, pickup_signature_time,
               pickup_time, delivery_signature_base64, delivery_signature_time,
               delivery_time, status
        FROM bols
        ORDER BY pickup_time DESC NULLS LAST, bol_id DESC
    """,
    "inventory_movements.csv": """
        SELECT seller, sku, movement_type, qty, ref_contract, created_at
        FROM inventory_movements
        ORDER BY created_at DESC
    """,
    # users table is public.users in your bootstrap; include username if present
    "users.csv": """
        SELECT id, email, COALESCE(username, '') AS username, role, created_at
        FROM public.users
        ORDER BY created_at DESC
    """,
    "index_snapshots.csv": """
        SELECT id, region, sku, avg_price, snapshot_date
        FROM index_snapshots
        ORDER BY snapshot_date DESC
    """
}

    for fname, sql in exports.items():
            rows = await _fetch_csv_rows(sql)
            mem = io.StringIO()
            if rows:
                writer = csv.DictWriter(mem, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)
            else:
                mem.write("")  # empty file is fine
            zf.writestr(fname, mem.getvalue().encode("utf-8"))
    return buf.getvalue()

async def _upload_to_supabase(path: str, data: bytes) -> Dict[str, Any]:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_service_key = os.getenv("SUPABASE_SERVICE_ROLE")
    bucket = os.getenv("SUPABASE_BUCKET", "backups")
    if not (supabase_url and supabase_service_key):
        raise RuntimeError("Supabase env vars missing")

    # Supabase Storage REST: POST /storage/v1/object/{bucket}/{path}
    api = f"{supabase_url}/storage/v1/object/{bucket}/{path}"
    headers = {
        "Authorization": f"Bearer {supabase_service_key}",
        "Content-Type": "application/zip",
        "x-upsert": "true",
    }
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(api, headers=headers, content=data)
        r.raise_for_status()
        return {"ok": True, "path": path, "status_code": r.status_code}

async def _upload_to_s3(path: str, data: bytes) -> Dict[str, Any]:
    import boto3
    s3 = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET missing")
    s3.put_object(Bucket=bucket, Key=path, Body=data, ContentType="application/zip")
    return {"ok": True, "path": f"s3://{bucket}/{path}"}

async def run_daily_snapshot(storage: str = "supabase") -> Dict[str, Any]:
    data = await build_export_zip()
    now = datetime.utcnow()
    day = now.strftime("%Y-%m-%d")
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    filename = f"{stamp}_export_all.zip"
    path = f"{day}/{filename}"

    if storage == "s3":
        return await _upload_to_s3(path, data)
    else:
        return await _upload_to_supabase(path, data)


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
