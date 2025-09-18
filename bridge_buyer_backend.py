from fastapi import FastAPI, HTTPException, Request, Depends, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse, Response, StreamingResponse, JSONResponse, PlainTextResponse
from fastapi.testclient import TestClient
from pydantic import BaseModel, EmailStr 
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
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from fastapi import UploadFile, File, Form
from fastapi import APIRouter
from datetime import date as _date

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
app.include_router(admin_exports)

# ===== Trusted hosts + session cookie =====
allowed = ["scrapfutures.com", "www.scrapfutures.com", "bridge.scrapfutures.com", "bridge-buyer.onrender.com"]

prod = os.getenv("ENV", "development").lower() == "production"
allow_local = os.getenv("ALLOW_LOCALHOST_IN_PROD", "") in ("1", "true", "yes")
if not prod or allow_local:
    allowed += ["localhost", "127.0.0.1", "testserver", "0.0.0.0"]

app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed)

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

# =====  rate limiting (init AFTER ProxyHeaders; keep ONLY this block) =====
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

@app.exception_handler(RateLimitExceeded)
async def ratelimit_handler(request, exc):
    return PlainTextResponse("Too Many Requests", status_code=429)

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
@app.get("/healthz", tags=["Health"], summary="Health Check")
async def healthz():
    try:
        await database.execute("SELECT NOW()")
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "degraded", "error": str(e)})

# -------- Database setup (non-fatal, with bootstrap in CI/staging) --------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    # local dev fallback only; CI sets DATABASE_URL
    DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/postgres"

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
database = databases.Database(DATABASE_URL)

@app.on_event("startup")
async def startup_bootstrap_and_connect():
    # Auto-create minimal schema outside prod, or when explicitly requested
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
# ==== Quick user creation (admin-only bootstrap) =========================
import os
from typing import Literal
from pydantic import BaseModel
from fastapi import Header, HTTPException

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
from pydantic import BaseModel
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
    if role == "yard":  # back-compat
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
            raise  # likely column mismatch (e.g., no is_active)

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
        # no username column at all
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

# --- CSV export (contracts) ---
from fastapi.responses import StreamingResponse
import csv, io
from sqlalchemy import text as _sqltext

@app.get("/contracts/export_csv", tags=["Admin"], summary="Export all contracts as CSV")
def export_contracts_csv():
    # If you use a global SQLAlchemy engine:
    conn = engine.connect()
    try:
        # Pull the columns you actually have (adjust names if different)
        q = _sqltext("""
            SELECT 
              id, buyer, seller, material, sku, weight_tons, price_per_ton, total_value,
              status, contract_date, pickup_time, delivery_time, currency
            FROM contracts
            ORDER BY contract_date DESC, id DESC
        """)
        rows = conn.execute(q).fetchall()
        cols = rows[0].keys() if rows else [
            "id","buyer","seller","material","sku","weight_tons","price_per_ton",
            "total_value","status","contract_date","pickup_time","delivery_time","currency"
        ]

        def _iter_csv():
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(cols)
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)
            for r in rows:
                w.writerow([r.get(c, None) if hasattr(r, "get") else getattr(r, c, None) for c in cols])
                yield buf.getvalue(); buf.seek(0); buf.truncate(0)

        filename = "contracts.csv"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(_iter_csv(), media_type="text/csv", headers=headers)
    finally:
        conn.close()

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
                    # handle Row/RowMapping and plain tuples
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

    return old, new_qty, delta
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
    avg_cost_per_lb: Optional[float] = None  # placeholder until costing is wired

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
        "CREATE INDEX IF NOT EXISTS idx_bols_pickup_time ON bols(pickup_time DESC);"
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
    # Minimal validation
    source = (body.get("source") or "").strip()
    seller = (body.get("seller") or "").strip()
    items  = body.get("items") or []
    if not (source and seller and isinstance(items, list)):
        raise HTTPException(400, "invalid payload: require source, seller, items[]")

    raw = await request.body()
    sig = request.headers.get("X-Signature", "")
    # HMAC + anti-replay only in production
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
    # In PROD require seller/admin session
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

# === INSERT: GenericIngestBody model 
    mapping: dict
    records: List[dict]
    seller_default: Optional[str] = None
    uom_default: Optional[str] = "ton"
    source: Optional[str] = "generic"
    idem_key: Optional[str] = None

# === /INSERT ===


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

# -------- CORS --------
ALLOWED_ORIGINS = [
    "https://scrapfutures.com",
    "https://www.scrapfutures.com",
    "https://bridge.scrapfutures.com",
    "https://bridge-buyer.onrender.com",
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
        
# Optional tighter typing for updates
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
     # audit log
    actor = request.session.get("username") if hasattr(request, "session") else None
    try:
        await log_action(actor or "system", "contract.update", str(contract_id), {
            "status": update.status,
            "signature_present": update.signature is not None
        })
    except Exception:
        pass
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
        headers={"Content-Disposition": f'attachment; filename=\"contracts_export_{datetime.utcnow().isoformat()}.csv\"'}
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

# === INSERT: Back-compat aliases for older UI buttons ===
@app.get("/admin/export_all", include_in_schema=False)
def _alias_export_all():
    return RedirectResponse(url="/admin/exports/all.zip", status_code=307)

@app.get("/contracts/export_csv", include_in_schema=False)
def _alias_export_csv():
    return RedirectResponse(url="/admin/exports/contracts.csv", status_code=307)
# === /INSERT ===

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

    # ---- audit log 
actor = request.session.get("username") if hasattr(request, "session") else None
try:
        await log_action(actor or "system", "contract.purchase", str(contract_id), {
            "new_status": "Signed",
            "bol_id": bol_id
        })
except Exception:
        pass

return resp

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
