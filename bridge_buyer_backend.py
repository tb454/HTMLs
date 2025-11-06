from __future__ import annotations
# --- ensure sibling modules are importable in CI/pytest ---
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.resolve()))
# ----------------------------------------------------------
from fastapi import FastAPI, HTTPException, Request, Depends, Query, Header, params
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response, StreamingResponse, JSONResponse, PlainTextResponse
from fastapi.testclient import TestClient
from pydantic import BaseModel, EmailStr, Field 
from typing import List, Optional, Literal
from sqlalchemy import create_engine, Table, MetaData, and_, select, Column, String, DateTime, Integer, Text, Boolean
import os
import databases
import uuid
import csv
import io
from io import BytesIO
import zipfile
import tempfile
import pathlib
from decimal import Decimal, ROUND_HALF_UP
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
from datetime import date as _date, date, timedelta, datetime, timezone
import asyncio
import inspect
from fastapi import Request
from typing import Iterable
from statistics import mean, stdev
from collections import defaultdict
from typing import Optional
from fastapi import HTTPException
import io, os, csv, zipfile
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
import smtplib
from email.message import EmailMessage
import secrets
from typing import Annotated

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
_FX = {"USD": 1.0, "EUR": 0.92, "RMB": 7.10}

_KILL_SWITCH: Dict[str, bool] = defaultdict(bool)          # member_id -> True/False
_PRICE_BANDS: Dict[str, Tuple[float, float]] = {}          # symbol -> (lower, upper)
_LULD: Dict[str, Tuple[float, float]] = {}                 # symbol -> (down_pct, up_pct)
_ENTITLEMENTS: Dict[str, Set[str]] = defaultdict(set)      # username/member -> features set

# simple market-data feed state (used by WebSocket endpoint)
_md_subs: Set[WebSocket] = set()
_md_seq: int = 1

# --- market data push tuning (production) ---
MD_BOOK_PUSH       = os.getenv("MD_BOOK_PUSH", "1").lower() in ("1","true","yes")
MD_BOOK_DEPTH      = int(os.getenv("MD_BOOK_DEPTH", "10"))            # top N levels
MD_BOOK_MIN_MS     = int(os.getenv("MD_BOOK_MIN_MS", "250"))          # debounce per symbol
_md_last_push_ms: Dict[str, int] = defaultdict(lambda: 0)

# instrument registry (example; persist later)
_INSTRUMENTS = {
    # symbol: lot_size (tons), tick_size ($/lb), description
    "CU-SHRED-1M": {"lot": 20.0, "tick": 0.0005, "desc": "Copper Shred 1-Month"},
    "AL-6061-1M": {"lot": 20.0, "tick": 0.0005, "desc": "Al 6061 1-Month"},
}

# ---- common utils (hashing, dates, json, rounding, lot size) ----
def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _utc_date_now():
    return utcnow().date()

def _canon_json(d: dict) -> str:
    return json.dumps(d, sort_keys=True, separators=(",", ":"))

def _lot_size(symbol: str) -> float:
    try:
        return float(_INSTRUMENTS.get(symbol, {}).get("lot", 1.0))
    except Exception:
        return 1.0

def _round2(x: float) -> float:
    return float(Decimal(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
# ---- /common utils ----

load_dotenv()

# ---- Harvester hard block (set BLOCK_HARVESTER=1 to disable any inbound dumping) ----
BLOCK_HARVESTER = os.getenv("BLOCK_HARVESTER", "").lower() in ("1","true","yes")

def _harvester_guard():
    if BLOCK_HARVESTER:
        # Gone (410) so existing clients stop trying, with a human-readable message.
        raise HTTPException(status_code=410, detail="Inbound harvester ingestion is disabled")
# ---- /Harvester hard block ----

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
app.docs_url = "/docs"
app.redoc_url = None
app.openapi_url = "/openapi.json"

# === QBO OAuth Relay • Config ===
QBO_RELAY_AUTH = os.getenv("QBO_RELAY_AUTH", "").strip()  # shared secret for /admin/qbo/peek
# Minimal helper: header check for the relayer fetch
def _require_qbo_relay_auth(request: Request):
    if not QBO_RELAY_AUTH:  # if unset, allow in dev but warn
        return
    hdr = request.headers.get("X-Relay-Auth", "")
    if hdr != QBO_RELAY_AUTH:
        raise HTTPException(status_code=401, detail="bad relay auth")
# === QBO OAuth Relay • Config ===

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

# --- background snapshot wrapper: never crash the worker ---
async def _snapshot_task(storage: str):
    try:
        res = await run_daily_snapshot(storage=storage)
        try:
            logger.warn("snapshot_bg_result", **({"res": res} if isinstance(res, dict) else {"note": str(res)}))
        except Exception:
            pass
    except Exception as e:
        try:
            logger.warn("snapshot_bg_failed", err=str(e))
        except Exception:
            pass

@app.post("/admin/run_snapshot_bg", tags=["Admin"], summary="Queue a snapshot upload (background)")
async def admin_run_snapshot_bg(background: BackgroundTasks, storage: str = "supabase", x_auth: str = Header(default="")):
    if SNAPSHOT_AUTH and x_auth != SNAPSHOT_AUTH:
        raise HTTPException(401, "bad auth")

    # In dev/test, DO NOT queue background tasks (they can kill the worker if creds missing)
    if os.getenv("ENV", "").lower() != "production":
        return {"ok": True, "queued": False, "note": "dev-noop"}

    # prod path: safe wrapper
    background.add_task(_snapshot_task, storage)
    return {"ok": True, "queued": True}


# --- Safe latest index handler (idempotent empty state) ---
@app.get("/indices/latest", tags=["Indices"], summary="Get latest index record")
async def indices_latest(symbol: str):
    try:
        row = await database.fetch_one(
            """
            SELECT dt AS as_of, close_price, unit, currency, source_note
            FROM bridge_index_history
            WHERE symbol = :s
            ORDER BY dt DESC
            LIMIT 1
            """,
            {"s": symbol},
        )
        if not row:
            raise HTTPException(status_code=404, detail="No index history yet")
        return dict(row)
    except HTTPException:
        raise
    except Exception as e:
        # dev-safe: if table not present yet, or any other transient error,
        # report as "no history" instead of 500
        try:
            logger.warn("indices_latest_error", err=str(e))
        except Exception:
            pass
        raise HTTPException(status_code=404, detail="No index history yet")
# --- Safe latest index handler ---

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
        async with httpx.AsyncClient(timeout=6) as c:
            r = await c.get("https://comexlive.org/copper/")
            r.raise_for_status()
            html = r.text
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

@app.get("/fx/convert", tags=["Global"], summary="Convert amount between currencies (static FX)")
async def fx_convert(amount: float, from_ccy: str = "USD", to_ccy: str = "USD"):
    try:
        f = (from_ccy or "").upper()
        t = (to_ccy or "").upper()
        return {"amount": round(amount / _FX[f] * _FX[t], 2), "to": t}
    except KeyError:
        raise HTTPException(422, "Unsupported currency")

@app.get("/pricing", tags=["Pricing"], summary="Public pricing snapshot")
async def get_pricing():
    return {
        "prices": {
            "bridge_starter_monthly": 1000,
            "bridge_standard_monthly": 3000,
            "bridge_enterprise_monthly": 10000,
            "bridge_exchange_fee_nonmember": 1.25,
            "bridge_clearing_fee_nonmember": 0.85,
            "bridge_delivery_fee": 1.00,
            "bridge_cash_settle_fee": 0.75,
            "bridge_giveup_fee": 1.00,
        },
        "meters": {
            "bol_count": 1.00,
            "delivered_tons": 0.50,
            "receipts_count": 0.50,
            "warrants_count": 0.50,
            "ws_messages_per_1m": 5.00,
        },
    }
# === Prices endpoint ===

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

def send_application_email(payload: ApplyRequest, app_id: str):
    host = os.environ.get("SMTP_HOST", "")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER", "")
    pwd  = os.environ.get("SMTP_PASS", "")
    sender = os.environ.get("SMTP_FROM", "no-reply@scrapfutures.com")
    to = os.environ.get("SMTP_TO", "info@atlasipholdingsllc.com")

    subject = f"[BRidge] New Access Application — {payload.org_name} ({payload.plan})"
    lines = [
        f"Application ID: {app_id}",
        f"Received At: {datetime.utcnow().isoformat()}Z",
        "",
        f"Entity Type: {payload.entity_type}",
        f"Role: {payload.role}",
        f"Organization: {payload.org_name}",
        f"EIN: {payload.ein or '-'}",
        f"Website: {payload.website or '-'}",
        f"Monthly Volume (tons): {payload.monthly_volume_tons or '-'}",
        "",
        f"Address: {payload.address or '-'}",
        f"City/Region: {payload.city or '-'} / {payload.region or '-'}",
        "",
        f"Contact: {payload.contact_name}",
        f"Email: {payload.email}",
        f"Phone: {payload.phone or '-'}",
        "",
        f"Materials (buy): {payload.materials_buy or '-'}",
        f"Materials (sell): {payload.materials_sell or '-'}",
        f"Lanes: {payload.lanes or '-'}",
        f"Compliance: {payload.compliance_notes or '-'}",
        "",
        f"Plan: {payload.plan}",
        f"Notes: {payload.notes or '-'}",
        "",
        f"UTM Source: {payload.utm_source or '-'}",
        f"UTM Campaign: {payload.utm_campaign or '-'}",
        f"UTM Medium: {payload.utm_medium or '-'}",
    ]
    body = "\n".join(lines)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to
    msg.set_content(body)

    if not host:
        # If SMTP not configured, just log to console
        print("[WARN] SMTP not configured — application email not sent.")
        print(body)
        return

    with smtplib.SMTP(host, port, timeout=30) as s:
        s.starttls()
        if user and pwd:
            s.login(user, pwd)
        s.send_message(msg)

from sqlalchemy import MetaData
metadata = MetaData()

applications = Table(
    "applications",
    metadata,
    Column("id", String, primary_key=True),
    Column("created_at", DateTime, nullable=False),
    Column("entity_type", String, nullable=False),
    Column("role", String, nullable=False),
    Column("org_name", String, nullable=False),
    Column("ein", String),
    Column("address", String),
    Column("city", String),
    Column("region", String),
    Column("website", String),
    Column("monthly_volume_tons", Integer),
    Column("contact_name", String, nullable=False),
    Column("email", String, nullable=False),
    Column("phone", String),
    Column("ref_code", String),
    Column("materials_buy", Text),
    Column("materials_sell", Text),
    Column("lanes", String),
    Column("compliance_notes", Text),
    Column("plan", String, nullable=False),
    Column("notes", Text),
    Column("utm_source", String),
    Column("utm_campaign", String),
    Column("utm_medium", String),
    Column("is_reviewed", Boolean, nullable=False, default=False),
)

# --- Pydantic payload ---
class ApplyRequest(BaseModel):
    entity_type: str = Field(..., pattern="^(yard|buyer|broker|other)$")
    role: str = Field(..., pattern="^(buyer|seller|both)$")
    org_name: str = Field(..., min_length=2)
    ein: Optional[str] = Field(None, pattern=r"^\d{2}-\d{7}$")
    address: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None
    website: Optional[str] = None
    monthly_volume_tons: Optional[int] = Field(None, ge=0)
    contact_name: str = Field(..., min_length=2)
    email: EmailStr
    phone: Optional[str] = None
    ref_code: Optional[str] = None
    materials_buy: Optional[str] = None
    materials_sell: Optional[str] = None
    lanes: Optional[str] = None
    compliance_notes: Optional[str] = None
    plan: str = Field(..., pattern="^(starter|standard|enterprise)$")
    notes: Optional[str] = None
    # UTM passthrough (optional)
    utm_source: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_medium: Optional[str] = None
# ===== DB bootstrap =====

# ---- CSRF helpers (top-level) ----
def _csrf_issue_token(request: Request) -> str:
    """Create a new CSRF token and store it in the session."""
    token = secrets.token_urlsafe(32)
    request.session["csrf_token"] = token
    return token
# ---- role helper ----
def _require_role(request: Request, allowed: set[str]):
    role = (request.session.get("role") or "").lower()
    if role not in allowed:
        raise HTTPException(status_code=403, detail=f"forbidden: requires one of {sorted(list(allowed))}")
# ---- role helper ----

#--- CSRF helpers ----
def _csrf_get_or_create(request: Request) -> str:
    """Re-use an existing token if present; otherwise issue one."""
    tok = request.session.get("csrf_token")
    return tok if isinstance(tok, str) and len(tok) > 0 else _csrf_issue_token(request)

# --- Smart CSRF toggles ---
SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
CSRF_EXEMPT_PATHS = {
    "/login", "/signup", "/logout",
    "/stripe/webhook", "/ice/webhook", "/qbo/callback", "/admin/qbo/peek"
}

def _is_same_site_browser(request: Request) -> bool:
    """
    CSRF only applies to same-site browser flows that carry cookies.
    Heuristics: Sec-Fetch-Site header or Origin host == request host.
    """
    sfs = (request.headers.get("Sec-Fetch-Site") or "").lower()
    if sfs in ("same-origin", "same-site"):
        return True

    origin = request.headers.get("Origin")
    if not origin:
        return False
    try:
        from urllib.parse import urlparse
        o = urlparse(origin)
        return o.hostname == request.url.hostname
    except Exception:
        return False
# ---  Smart CSRF toggles ---

async def csrf_protect(
    request: Request,
    x_csrf: str | None = Header(default=None, alias="X-CSRF")
):
    """
    Enforce CSRF only in production, only for unsafe methods,
    only for same-site browser requests that carry a session cookie,
    and skip known webhook/auth endpoints.
    """
    # Only enforce in production
    if os.getenv("ENV", "").lower() != "production":
        return

    # Safe methods need no CSRF
    if request.method in SAFE_METHODS:
        return

    # Skip known non-browser endpoints / webhooks / auth
    if request.url.path in CSRF_EXEMPT_PATHS:
        return

    # Only enforce if this looks like a same-site browser request
    if not _is_same_site_browser(request):
        return

    # Require a session-bound CSRF token and a matching header
    sess_token = request.session.get("csrf_token")
    if not sess_token:
        # Issue one so the client can retry
        _csrf_issue_token(request)
        raise HTTPException(status_code=401, detail="bad csrf")

    if x_csrf != sess_token:
        raise HTTPException(status_code=401, detail="bad csrf")
# ---- CSRF helpers ----

# ---- Security headers ----
@app.middleware("http")
async def nonce_mint_mw(request: Request, call_next):
    # Per-request CSP nonce used for <script nonce> and (optionally) <style nonce>
    request.state.csp_nonce = getattr(request.state, "csp_nonce", secrets.token_urlsafe(16))
    return await call_next(request)

async def security_headers_mw(request, call_next):
    resp: Response = await call_next(request)

    # Safe defaults
    h = resp.headers
    h["X-Content-Type-Options"] = "nosniff"
    h["X-Frame-Options"] = "DENY"
    h["Referrer-Policy"] = "strict-origin-when-cross-origin"
    h["Cross-Origin-Opener-Policy"] = "same-origin"
    h["Cross-Origin-Embedder-Policy"] = "credentialless"
    h["X-Permitted-Cross-Domain-Policies"] = "none"
    h["X-Download-Options"] = "noopen"
    h["Permissions-Policy"] = "geolocation=()"

    # Only set CSP here if the route didn't set one explicitly
    if "Content-Security-Policy" not in h:
        nonce = getattr(request.state, "csp_nonce", "")
        h["Content-Security-Policy"] = (
            "default-src 'self'; "
            "base-uri 'self'; object-src 'none'; frame-ancestors 'none'; "
            "img-src 'self' data: blob: https://*.stripe.com; "
            "font-src 'self' https://fonts.gstatic.com data:; "
            f"style-src 'self' https://cdn.jsdelivr.net https://fonts.googleapis.com 'nonce-{nonce}' 'unsafe-inline'; "
            "style-src-attr 'self'; "
            f"script-src 'self' 'nonce-{nonce}' https://cdn.jsdelivr.net https://js.stripe.com; "
            "frame-src 'self' https://js.stripe.com https://checkout.stripe.com; "
            "connect-src 'self' ws: wss: https://cdn.jsdelivr.net https://*.stripe.com"
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
async def run_ddl_multi(sql: str):
    """Split and run multiple DDL statements (for asyncpg/databases)."""
    stmts = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in stmts:
        try:
            await database.execute(stmt)
        except Exception as e:
            logger.warn("ddl_failed", sql=stmt[:120], err=str(e))
# =====  request-id + structured logs =====

# ------ ID Logging ------
@app.middleware("http")
async def request_id_logging(request: Request, call_next):
    rid = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    start = time.time()
    response = await call_next(request)
    elapsed = int((time.time() - start) * 1000)
    response.headers["X-Request-ID"] = rid
    logger.info("req", id=rid, path=str(request.url.path), method=request.method, status=response.status_code, ms=elapsed)
    return response
# ------ ID Logging ------

# =====  Database (async + sync) =====
@app.on_event("startup")
async def _connect_db_first():
    if not database.is_connected:
        await database.connect()
    if not hasattr(app.state, "db_pool"):
        import asyncpg
        app.state.db_pool = await asyncpg.create_pool(ASYNC_DATABASE_URL, max_size=10)
# ----- database (async + sync) -----

# ----- idem key cache -----
@app.on_event("startup")
async def _ensure_http_idem_table():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS http_idempotency (
      key TEXT PRIMARY KEY,
      response JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_http_idem_ttl ON http_idempotency (created_at);
    """)
# ----- idem key cache -----

# =====  Prometheus metrics + optional Sentry =====
@app.on_event("startup")
async def _metrics_and_sentry():
    instrumentator.expose(app, include_in_schema=False)

dsn = (os.getenv("SENTRY_DSN") or "").strip()
if dsn.startswith("http"):  # only init if it looks like a real DSN
    sentry_sdk.init(dsn=dsn, traces_sample_rate=0.05)

from prometheus_client import Histogram
H_REQ = Histogram("bridge_http_latency_ms", "HTTP latency (ms)", ["path","method"])

@app.middleware("http")
async def _sla_timer(request: Request, call_next):
    import time as _tm
    t0 = _tm.perf_counter()
    resp = await call_next(request)
    dt = (_tm.perf_counter() - t0) * 1000.0
    try:
        H_REQ.labels(path=request.url.path, method=request.method).observe(dt)
    except Exception:
        pass
    resp.headers["X-Perf-ms"] = f"{dt:.2f}"
    return resp

@app.get("/time/sync", tags=["Health"], summary="Server time sync (UTC + monotonic)")
async def time_sync():
    return {"utc": utcnow().isoformat(), "mono_ns": time.time_ns()}
# =====  Prometheus metrics + optional Sentry =====

# =====  account - user ownership =====
@app.on_event("startup")
async def _ensure_account_user_map():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS account_users(
      account_id UUID PRIMARY KEY,
      username   TEXT NOT NULL
    );
    """)
# =====  account - user ownership =====

# ----- Uniq ref guard -----
@app.on_event("startup")
async def _uniq_ref_guard():
    try:
        await database.execute("""
          ALTER TABLE contracts
          ADD CONSTRAINT uq_contracts_ref UNIQUE (reference_source, reference_symbol)
        """)
    except Exception:
        pass  # already exists or incompatible
# ----- Uniq ref guard -----

# ----- Extra indexes -----
@app.on_event("startup")
async def _ensure_more_indexes():
    ddl = [
        "CREATE INDEX IF NOT EXISTS idx_settlements_symbol_asof ON public.settlements(symbol, as_of DESC)",
        "CREATE INDEX IF NOT EXISTS idx_clob_trades_symbol_time ON public.clob_trades(symbol, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_clob_orders_book ON public.clob_orders(symbol, side, price, created_at) WHERE status='open'",
    ]
    for s in ddl:
        try:
            await database.execute(s)
        except Exception as e:
            logger.warn("extra_index_bootstrap_failed", sql=s[:100], err=str(e))
# ----- Extra indexes -----

# ===== Billing core (fees ledger + preview/run) =====
fees_router = APIRouter(prefix="/billing", tags=["Billing"])

@app.on_event("startup")
async def _ensure_billing_core():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS fees_ledger(
      id UUID PRIMARY KEY,
      member TEXT NOT NULL,
      event_type TEXT NOT NULL,     -- trade.maker, bol.create, bol.deliver, data.overage, license.*, etc.
      ref_table TEXT NOT NULL,
      ref_id TEXT NOT NULL,
      fee_amount NUMERIC NOT NULL,
      currency TEXT NOT NULL DEFAULT 'USD',
      fee_amount_usd NUMERIC,       -- normalized for reporting (optional)
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(event_type, ref_table, ref_id)
    );
    CREATE TABLE IF NOT EXISTS billing_invoices(
      invoice_id UUID PRIMARY KEY,
      member TEXT NOT NULL,
      period_start DATE NOT NULL,
      period_end DATE NOT NULL,
      subtotal NUMERIC NOT NULL DEFAULT 0,
      tax NUMERIC NOT NULL DEFAULT 0,
      total NUMERIC NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'open',  -- open/sent/paid
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS billing_line_items(
      line_id UUID PRIMARY KEY,
      invoice_id UUID NOT NULL REFERENCES billing_invoices(invoice_id) ON DELETE CASCADE,
      event_type TEXT NOT NULL,
      description TEXT,
      amount NUMERIC NOT NULL,
      currency TEXT NOT NULL DEFAULT 'USD'
    );
    """)
    # Helpful indexes
    await run_ddl_multi("""
      CREATE INDEX IF NOT EXISTS idx_fees_ledger_member_time ON fees_ledger(member, created_at);
      CREATE INDEX IF NOT EXISTS idx_invoices_member_period ON billing_invoices(member, period_start, period_end);
    """)
    # Daily finance view
    await run_ddl_multi("""
    CREATE OR REPLACE VIEW v_fees_by_day AS
      SELECT date_trunc('day', created_at) AS d, member, SUM(fee_amount_usd) AS amt_usd
        FROM fees_ledger GROUP BY 1,2 ORDER BY 1,2;
    """)

async def _mmi_usd(member: str) -> float:
    """
    Minimum Monthly Invoice ($) for the member.
    Here we use the member's plan price_usd as the MMI.
    Return 0.0 if the member has no plan assigned.
    """
    row = await database.fetch_one("""
        SELECT bp.price_usd
          FROM member_plans mp
          JOIN billing_plans bp ON bp.plan_code = mp.plan_code
         WHERE mp.member = :m
    """, {"m": member})
    return float(row["price_usd"]) if row and row["price_usd"] is not None else 0.0

async def _member_totals(member: str, start: date, end: date):
    rows = await database.fetch_all("""
      SELECT event_type, SUM(COALESCE(fee_amount_usd, fee_amount)) AS amt
        FROM fees_ledger
       WHERE member=:m AND created_at::date >= :s AND created_at::date < :e
       GROUP BY event_type
    """, {"m": member, "s": start, "e": end})
    by_ev = {r["event_type"]: float(r["amt"] or 0) for r in rows}
    subtotal = sum(by_ev.values())
    mmi = await _mmi_usd(member) if "_mmi_usd" in globals() else 0.0
    trueup = max(0.0, mmi - subtotal) if mmi > 0 else 0.0
    return by_ev, subtotal, mmi, trueup

@fees_router.get("/preview", summary="Preview monthly charges per member")
async def billing_preview(member: str, month: str):
    y, m = map(int, month.split("-", 1))
    from datetime import date as _d
    start = _d(y, m, 1)
    end = _d(y + (m // 12), (m % 12) + 1, 1)
    by_ev, subtotal, mmi, trueup = await _member_totals(member, start, end)
    return {"member": member, "period_start": str(start), "period_end": str(end),
            "by_event": by_ev, "subtotal": round(subtotal,2),
            "mmi_usd": round(mmi,2), "trueup": round(trueup,2),
            "total": round(subtotal+trueup,2)}

@fees_router.post("/run", summary="Generate internal invoice for month (admin)")
async def billing_run(member: str, month: str, force: bool=False, request: Request=None):
    if os.getenv("ENV","").lower()=="production":
        _require_admin(request)
    y, m = map(int, month.split("-", 1))
    from datetime import date as _d
    start = _d(y, m, 1)
    end = _d(y + (m // 12), (m % 12) + 1, 1)

    # lock (don’t duplicate)
    exists = await database.fetch_one(
        "SELECT invoice_id,status FROM billing_invoices WHERE member=:m AND period_start=:s AND period_end=:e",
        {"m": member, "s": start, "e": end}
    )
    if exists and not force:
        raise HTTPException(409, f"Invoice already exists (status={exists['status']}). Pass force=1 to overwrite.")
    if exists and force:
        await database.execute("DELETE FROM billing_line_items WHERE invoice_id=:i", {"i": exists["invoice_id"]})
        await database.execute("DELETE FROM billing_invoices WHERE invoice_id=:i", {"i": exists["invoice_id"]})

    by_ev, subtotal, mmi, trueup = await _member_totals(member, start, end)
    invoice_id = str(uuid.uuid4())
    await database.execute("""
      INSERT INTO billing_invoices(invoice_id,member,period_start,period_end,subtotal,tax,total,status)
      VALUES (:i,:m,:s,:e,:sub,0,:tot,'open')
    """, {"i": invoice_id, "m": member, "s": start, "e": end,
          "sub": round(subtotal,2), "tot": round(subtotal+trueup,2)})

    for ev, amt in by_ev.items():
        await database.execute("""
          INSERT INTO billing_line_items(line_id,invoice_id,event_type,description,amount,currency)
          VALUES (:id,:inv,:ev,:desc,:amt,'USD')
        """, {"id": str(uuid.uuid4()), "inv": invoice_id, "ev": ev,
              "desc": f"{ev} ({start}–{end})", "amt": round(amt,2)})

    if trueup > 0:
        await database.execute("""
          INSERT INTO billing_line_items(line_id,invoice_id,event_type,description,amount,currency)
          VALUES (:id,:inv,'mmi.trueup',:desc,:amt,'USD')
        """, {"id": str(uuid.uuid4()), "inv": invoice_id,
              "desc": f"Minimum monthly invoice true-up ({start}–{end})",
              "amt": round(trueup,2)})

    return {"ok": True, "invoice_id": invoice_id, "member": member,
            "period_start": str(start), "period_end": str(end),
            "total": round(subtotal+trueup,2)}

app.include_router(fees_router)
# ===== /Billing core =====

# ----- billing contacts and email logs -----
@app.on_event("startup")
async def _ensure_billing_contacts_and_email_logs():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS billing_contacts (
      member TEXT PRIMARY KEY,
      email  TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS email_logs (
      id UUID PRIMARY KEY,
      to_email TEXT NOT NULL,
      subject TEXT NOT NULL,
      body TEXT NOT NULL,
      sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      status TEXT NOT NULL DEFAULT 'sent',
      ref_type TEXT,         -- e.g., 'invoice','contract','bol'
      ref_id   TEXT
    );
    """)

@app.post("/admin/billing_contact/upsert", tags=["Admin"], summary="Set billing email for a member")
async def upsert_billing_contact(member: str, email: str, request: Request=None):
    _require_admin(request)
    await database.execute("""
      INSERT INTO billing_contacts(member,email)
      VALUES (:m,:e)
      ON CONFLICT (member) DO UPDATE SET email=EXCLUDED.email, updated_at=NOW()
    """, {"m": member, "e": email})
    return {"ok": True, "member": member, "email": email}

# --- OPTIONAL SendGrid import (guarded) ---
try:
    from sendgrid import SendGridAPIClient  # type: ignore
    from sendgrid.helpers.mail import Mail  # type: ignore
    _SENDGRID_AVAILABLE = True
except Exception:
    SendGridAPIClient = None  # type: ignore
    Mail = None  # type: ignore
    _SENDGRID_AVAILABLE = False

ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "ops@example.com")

async def _send_email(to_email: str, subject: str, html: str, ref_type: str=None, ref_id: str=None):
    """
    Try SendGrid if SENDGRID_API_KEY is set and library is available.
    Otherwise, try SMTP via env (SMTP_HOST/PORT/USER/PASS/FROM).
    If neither is configured, log to email_logs and return.
    """
    # 1) SendGrid path
    sg_key = os.environ.get("SENDGRID_API_KEY", "").strip()
    if _SENDGRID_AVAILABLE and sg_key:
        try:
            sg = SendGridAPIClient(sg_key)
            msg = Mail(from_email=ADMIN_EMAIL, to_emails=to_email, subject=subject, html_content=html)
            sg.send(msg)
            try:
                await database.execute("""
                  INSERT INTO email_logs(id,to_email,subject,body,ref_type,ref_id,status)
                  VALUES (:id,:to,:sub,:body,:rt,:rid,'sent')
                """, {"id": str(uuid.uuid4()), "to": to_email, "sub": subject, "body": html, "rt": ref_type, "rid": ref_id})
            except Exception:
                pass
            return
        except Exception:           
            pass

    # 2) SMTP path (optional)
    host = os.environ.get("SMTP_HOST", "").strip()
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER", "").strip()
    pwd  = os.environ.get("SMTP_PASS", "").strip()
    sender = os.environ.get("SMTP_FROM", ADMIN_EMAIL)

    if host:
        try:
            import smtplib
            from email.message import EmailMessage
            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = sender
            msg["To"] = to_email
            msg.set_content(html, subtype="html")
            with smtplib.SMTP(host, port, timeout=30) as s:
                try:
                    s.starttls()
                except Exception:
                    pass
                if user and pwd:
                    s.login(user, pwd)
                s.send_message(msg)
            try:
                await database.execute("""
                  INSERT INTO email_logs(id,to_email,subject,body,ref_type,ref_id,status)
                  VALUES (:id,:to,:sub,:body,:rt,:rid,'sent')
                """, {"id": str(uuid.uuid4()), "to": to_email, "sub": subject, "body": html, "rt": ref_type, "rid": ref_id})
            except Exception:
                pass
            return
        except Exception:            
            pass

    # 3) Log-only (no SendGrid/SMTP configured)
    try:
        await database.execute("""
          INSERT INTO email_logs(id,to_email,subject,body,ref_type,ref_id,status)
          VALUES (:id,:to,:sub,:body,:rt,:rid,'skipped')
        """, {"id": str(uuid.uuid4()), "to": to_email, "sub": subject, "body": html, "rt": ref_type, "rid": ref_id})
    except Exception:
        pass

async def _member_billing_email(member: str) -> str | None:
    row = await database.fetch_one("SELECT email FROM billing_contacts WHERE member=:m", {"m": member})
    return (row and row["email"]) or None

async def notify_humans(event: str, *, member: str, subject: str, html: str,
                        cc_admin: bool=True, ref_type: str=None, ref_id: str=None):
    """
    event: tag like 'payment.confirmed','contract.signed','bol.delivered', etc.
    Sends to the member's billing contact (if any) and optionally CCs ADMIN_EMAIL.
    """
    to_member = await _member_billing_email(member)
    if to_member:
        await _send_email(to_member, subject, html, ref_type, ref_id)
    if cc_admin:
        await _send_email(ADMIN_EMAIL, f"[{event}] " + subject, html, ref_type, ref_id)
# ----- billing contacts and email logs -----

@app.on_event("startup")
async def _ensure_payment_profiles():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS billing_payment_profiles(
      member TEXT PRIMARY KEY,                 -- your 'org_name' or normalized member key
      email  TEXT NOT NULL,
      stripe_customer_id TEXT NOT NULL,
      default_payment_method TEXT,             -- pm_xxx
      has_default BOOLEAN NOT NULL DEFAULT FALSE,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)

import stripe, os
stripe.api_key = os.environ["STRIPE_SECRET"]
STRIPE_RETURN_BASE = os.getenv("BILLING_PUBLIC_URL", "https://bridge.scrapfutures.com")

# === Self-serve subscription via hosted Checkout (Stripe handles billing) ===
# Base plan lookup keys must match your Stripe dashboard:
BASE_PLAN_LOOKUP = {
    "starter": "bridge_starter_monthly",
    "standard": "bridge_standard_monthly",
    "enterprise": "bridge_enterprise_monthly",
}

# Add-on metered price lookup keys (you already created these):
PLAN_LOOKUP_TO_ADDON_KEYS = {
    "starter": [
        "bol_overage_starter",
        "delivered_tons_overage_starter",
        "receipts_overage_starter",
        "warrants_overage_starter",
        "ws_messages_overage_1m_starter",
    ],
    "standard": [
        "bol_overage_standard",
        "delivered_tons_overage_standard",
        "receipts_overage_standard",
        "warrants_overage_standard",
        "ws_messages_overage_1m_standard",
    ],
    "enterprise": [
        # include only the metrics you want overage for; omit if truly unlimited
        "bol_overage_enterprise",
        "delivered_tons_overage_enterprise",
        "receipts_overage_enterprise",
        "warrants_overage_enterprise",
        "ws_messages_overage_1m_enterprise",
    ],
}

def _price_id_for_lookup(lookup_key: str) -> str:
    prices = stripe.Price.list(active=True, limit=100)
    for p in prices.auto_paging_iter():
        if getattr(p, "lookup_key", None) == lookup_key:
            return p["id"]
    raise RuntimeError(f"Stripe price lookup_key not found: {lookup_key}")

@app.post("/billing/subscribe/checkout", tags=["Billing"], summary="Start subscription Checkout for plan")
async def start_subscription_checkout(member: str, plan: str, email: Optional[str] = None, _=Depends(csrf_protect)):
    plan = (plan or "").lower()
    if plan not in BASE_PLAN_LOOKUP:
        raise HTTPException(400, "plan must be starter|standard|enterprise")

    # Ensure (or create) Stripe Customer and persist locally
    row = await database.fetch_one(
        "SELECT email, stripe_customer_id FROM billing_payment_profiles WHERE member=:m", {"m": member}
    )
    if row and row["stripe_customer_id"]:
        cust_id = row["stripe_customer_id"]
        try:
            stripe.Customer.modify(cust_id, email=(email or row["email"]), name=member)
        except Exception:
            pass
    else:
        cust = stripe.Customer.create(email=(email or ""), name=member, metadata={"member": member})
        cust_id = cust.id
        await database.execute("""
            INSERT INTO billing_payment_profiles(member,email,stripe_customer_id,has_default)
            VALUES (:m,:e,:c,false)
            ON CONFLICT (member) DO UPDATE
              SET email=EXCLUDED.email, stripe_customer_id=EXCLUDED.stripe_customer_id
        """, {"m": member, "e": (email or ""), "c": cust_id})

    # Resolve base + add-on price IDs
    base_price = _price_id_for_lookup(BASE_PLAN_LOOKUP[plan])
    addon_prices = [_price_id_for_lookup(k) for k in PLAN_LOOKUP_TO_ADDON_KEYS.get(plan, [])]

    # Build line items: base plan (qty=1) + metered add-ons (no quantity needed)
    line_items = [{"price": base_price, "quantity": 1}] + [{"price": pid} for pid in addon_prices]

    # Hosted Checkout in SUBSCRIPTION mode
    session = stripe.checkout.Session.create(
        mode="subscription",
        customer=cust_id,
        line_items=line_items,
        allow_promotion_codes=True,
        subscription_data={"metadata": {"member": member, "plan": plan}},
        success_url=f"{STRIPE_RETURN_BASE}/static/apply.html?sub=ok&session={{CHECKOUT_SESSION_ID}}&member={stripe.util.utf8(member)}",
        cancel_url=f"{STRIPE_RETURN_BASE}/static/apply.html?sub=cancel",
        metadata={"member": member, "email": (email or row["email"] if row else "")},
    )
    return {"url": session.url}

@app.get("/billing/subscribe/finalize_from_session", tags=["Billing"], summary="Finalize plan after Checkout success (no webhook)")
async def finalize_subscription_from_session(sess: str, member: Optional[str] = None):
    try:
        s = stripe.checkout.Session.retrieve(sess, expand=["subscription", "customer"])
        sub = s.get("subscription")
        cust = s.get("customer")

        # Resolve plan/member from metadata (subscription first, session as fallback)
        sub_meta = (sub.get("metadata") if isinstance(sub, dict) else {}) or {}
        sess_meta = (s.get("metadata") or {})
        plan = (sub_meta.get("plan") or sess_meta.get("plan") or "").lower()
        m = member or sess_meta.get("member") or sub_meta.get("member")

        if plan not in {"starter", "standard", "enterprise"}:
            raise ValueError("Could not infer plan (starter|standard|enterprise).")
        if not m:
            raise ValueError("Missing member key.")

        # Upsert plan mapping for the member
        await database.execute("""
          INSERT INTO member_plans(member, plan_code, effective_date, updated_at)
          VALUES (:m, :p, CURRENT_DATE, NOW())
          ON CONFLICT (member) DO UPDATE
            SET plan_code=:p, updated_at=NOW()
        """, {"m": m, "p": plan})

        # Ensure column exists once; then persist stripe_subscription_id
        sub_id = (sub.get("id") if isinstance(sub, dict) else sub) or None
        if sub_id:
            await database.execute("""
              ALTER TABLE IF NOT EXISTS member_plans
              ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT
            """)
            await database.execute("""
              UPDATE member_plans SET stripe_subscription_id=:sid, updated_at=NOW()
              WHERE member=:m
            """, {"sid": sub_id, "m": m})

        # Upsert stripe_customer_id without fabricating email
        cust_id = (cust.get("id") if isinstance(cust, dict) else (cust if isinstance(cust, str) else None)) or None
        if cust_id:
            await database.execute("""
              INSERT INTO billing_payment_profiles(member, email, stripe_customer_id, has_default, updated_at)
              VALUES (
                :m,
                COALESCE((SELECT email FROM billing_payment_profiles WHERE member=:m), ''),
                :c,
                COALESCE((SELECT has_default FROM billing_payment_profiles WHERE member=:m), FALSE),
                NOW()
              )
              ON CONFLICT (member) DO UPDATE
                SET stripe_customer_id=:c, updated_at=NOW()
            """, {"m": m, "c": cust_id})

        return {"ok": True, "member": m, "plan": plan, "subscription_id": sub_id}
    except Exception as e:
        raise HTTPException(400, f"subscription finalize failed: {e}")
# === Self-serve subscription via hosted Checkout (Stripe handles billing) ===

# === Stripe Meter Events: WS messages ===
def _get_customer_id_sync(member: str) -> str | None:
    try:
        r = engine.execute(sqlalchemy.text(
            "SELECT stripe_customer_id FROM billing_payment_profiles WHERE member=:m"
        ), {"m": member}).first()
        return (r and r[0]) or None
    except Exception:
        return None

def _member_plan_sync(member: str) -> str:
    try:
        r = engine.execute(sqlalchemy.text(
            "SELECT plan_code FROM member_plans WHERE member=:m"
        ), {"m": member}).first()
        return (r and str(r[0]).lower()) or "starter"
    except Exception:
        return "starter"

# These must match the "Event name" for each Meter in your Stripe dashboard.
def _ws_meter_event_name(plan: str) -> str:
    return {
        "starter": "ws_messages_per_1m_starter",
        "standard": "ws_messages_per_1m_standard",
        "enterprise": "ws_messages_per_1m_enterprise",
    }.get(plan, "ws_messages_per_1m_starter")

def emit_ws_usage(member: str, raw_count: int) -> None:
    if raw_count <= 0:
        return
    cust = _get_customer_id_sync(member)
    if not cust:
        return
    plan = _member_plan_sync(member)
    ev = _ws_meter_event_name(plan)
    try:
        stripe.billing.MeterEvent.create(
            event_name=ev,
            payload={"stripe_customer_id": cust, "value": int(raw_count)},
            # identifier=f"{member}:ws:{int(time.time())}"  # Optional dedupe
        )
    except Exception:
        pass
# === Stripe Meter Events: WS messages ===

@app.post("/billing/pm/setup_session", tags=["Billing"], summary="Create setup-mode Checkout Session (ACH+Card)")
async def pm_setup_session(member: str, email: str, _=Depends(csrf_protect)):
    # 1) ensure (or create) Stripe Customer
    row = await database.fetch_one("SELECT stripe_customer_id FROM billing_payment_profiles WHERE member=:m", {"m": member})
    if row and row["stripe_customer_id"]:
        cust_id = row["stripe_customer_id"]
    else:
        cust = stripe.Customer.create(email=email, name=member, metadata={"member": member})
        cust_id = cust.id
        await database.execute("""
          INSERT INTO billing_payment_profiles(member,email,stripe_customer_id,has_default)
          VALUES (:m,:e,:c,false)
          ON CONFLICT (member) DO UPDATE SET email=EXCLUDED.email, stripe_customer_id=EXCLUDED.stripe_customer_id
        """, {"m": member, "e": email, "c": cust_id})

    # 2) Stripe Checkout (mode=setup) for PM collection (card + ACH bank account)
    session = stripe.checkout.Session.create(
        mode="setup",
        customer=cust_id,
        payment_method_types=["card", "us_bank_account"],
        success_url=f"{STRIPE_RETURN_BASE}/static/apply.html?pm=ok&sess={{CHECKOUT_SESSION_ID}}&member={stripe.util.utf8(member)}",
        cancel_url=f"{STRIPE_RETURN_BASE}/static/apply.html?pm=cancel",
        metadata={"member": member, "email": email},
    )
    return {"url": session.url}

@app.get("/billing/pm/finalize_from_session", tags=["Billing"], summary="Finalize default payment method from Checkout Session (no webhook)")
async def pm_finalize_from_session(sess: str, member: Optional[str] = None):
    """
    Use this when you don't have webhooks: call from your success page with the "sess" query param.
    It fetches the Checkout Session, extracts the SetupIntent → PaymentMethod, and binds as default.
    """
    try:
        s = stripe.checkout.Session.retrieve(sess, expand=["setup_intent"])
        cust_id = s.get("customer")
        si_obj = s.get("setup_intent")
        pm_id = None
        if isinstance(si_obj, dict):
            pm_id = si_obj.get("payment_method")
        elif isinstance(si_obj, str):
            # fallback fetch if expand failed
            si = stripe.SetupIntent.retrieve(si_obj)
            pm_id = si.get("payment_method")
        m = member or ((s.get("metadata") or {}).get("member"))
        await _bind_pm(m, cust_id, pm_id)
        return {"ok": True, "member": m, "customer": cust_id, "pm": pm_id}
    except Exception as e:
        raise HTTPException(400, f"pm finalize failed: {e}")

@app.get("/billing/pm/status", tags=["Billing"])
async def pm_status(member: str):
    r = await database.fetch_one("SELECT has_default, default_payment_method FROM billing_payment_profiles WHERE member=:m", {"m": member})
    return {"member": member, "has_default": bool(r and r["has_default"]), "pm": (r and r["default_payment_method"]) or None}

USE_STRIPE = os.getenv("ENABLE_STRIPE", "0").lower() in {"1","true","yes"}
STRIPE_API_KEY = os.getenv("STRIPE_SECRET", "")

# optional shim so imports don't break in CI
try:
    import stripe
except ModuleNotFoundError:
    stripe = None

# which lookup keys you want to expose
PRICE_LOOKUPS = [
    "bridge_starter_monthly",
    "bridge_standard_monthly",
    "bridge_enterprise_monthly",
    "bridge_exchange_fee_nonmember",
    "bridge_clearing_fee_nonmember",
    "bridge_delivery_fee",
    "bridge_cash_settle_fee",
    "bridge_giveup_fee",
]

# if you’re using Stripe Meters, map event_name -> displayed unit price (if any)
METER_RATES = {
    "bol_count": None,            # set None to skip or override with a fixed display value
    "delivered_tons": None,
    "receipts_count": None,
    "warrants_count": None,
    "ws_messages_per_1m": None,
}

_pricing_cache = {"data": None, "ts": 0}
_CACHE_TTL = 600  # seconds

def _fetch_prices_from_stripe():
    # fallback if disabled or stripe is missing
    if not (USE_STRIPE and stripe and STRIPE_API_KEY):
        return None

    stripe.api_key = STRIPE_API_KEY

    # fetch all prices with those lookup_keys (active only)
    out = {}
    # Stripe doesn’t filter directly by lookup_key list; we can list active and pick.
    prices = stripe.Price.list(active=True, limit=100)
    for p in prices.auto_paging_iter():
        lk = p.get("lookup_key")
        if lk in PRICE_LOOKUPS:
            # unit_amount is cents for recurring flat prices; for metered “per unit” use unit_amount as well
            amt = None
            if p.get("unit_amount") is not None:
                amt = p["unit_amount"] / 100.0
            elif p.get("unit_amount_decimal") is not None:
                amt = float(p["unit_amount_decimal"]) / 100.0
            if amt is not None:
                out[lk] = amt

    # optionally: resolve meter rates if you store them as Prices too (recommended),
    # or keep METER_RATES as env-configured constants.
    return out

def _get_pricing_snapshot():
    now = time.time()
    if _pricing_cache["data"] and (now - _pricing_cache["ts"] < _CACHE_TTL):
        return _pricing_cache["data"]

    prices = _fetch_prices_from_stripe() or {}
    # default/fallback values if Stripe didn’t return a key
    defaults = {
        "bridge_starter_monthly": 1000,
        "bridge_standard_monthly": 3000,
        "bridge_enterprise_monthly": 10000,
        "bridge_exchange_fee_nonmember": 1.25,
        "bridge_clearing_fee_nonmember": 0.85,
        "bridge_delivery_fee": 1.00,
        "bridge_cash_settle_fee": 0.75,
        "bridge_giveup_fee": 1.00,
    }
    merged_prices = {**defaults, **prices}

    # meters: either fixed numbers here, or pull from Stripe if you model them as prices too
    meters = {
        "bol_count": 1.00 if METER_RATES["bol_count"] is None else METER_RATES["bol_count"],
        "delivered_tons": 0.50 if METER_RATES["delivered_tons"] is None else METER_RATES["delivered_tons"],
        "receipts_count": 0.50 if METER_RATES["receipts_count"] is None else METER_RATES["receipts_count"],
        "warrants_count": 0.50 if METER_RATES["warrants_count"] is None else METER_RATES["warrants_count"],
        "ws_messages_per_1m": 5.00 if METER_RATES["ws_messages_per_1m"] is None else METER_RATES["ws_messages_per_1m"],
    }

    snapshot = {"prices": merged_prices, "meters": meters}
    _pricing_cache["data"], _pricing_cache["ts"] = snapshot, now
    return snapshot

# ----- Stripe billing -----
stripe.api_key = os.environ["STRIPE_SECRET"]
STRIPE_RETURN_BASE = os.getenv("BILLING_PUBLIC_URL", "https://bridge.scrapfutures.com")
SUCCESS_URL = f"{STRIPE_RETURN_BASE}/static/payment_success.html?session={{CHECKOUT_SESSION_ID}}"
CANCEL_URL  = f"{STRIPE_RETURN_BASE}/static/payment_canceled.html"

def _usd_cents(x: float) -> int:
    return int(round(float(x) * 100.0))

async def _fetch_invoice(invoice_id: str):
    return await database.fetch_one("SELECT * FROM billing_invoices WHERE invoice_id=:i", {"i": invoice_id})
# ----- Stripe billing -----

# ----- billing prefs -----
@app.on_event("startup")
async def _ensure_billing_schema():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS billing_preferences (
      member TEXT PRIMARY KEY,
      billing_day INT NOT NULL CHECK (billing_day BETWEEN 1 AND 28),
      invoice_cc_emails TEXT[] DEFAULT '{}',
      autopay BOOLEAN NOT NULL DEFAULT FALSE,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS billing_plan_limits (
      plan_code TEXT PRIMARY KEY,
      max_users INT NOT NULL DEFAULT 5,
      max_contracts_per_day INT NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)
# ----- billing prefs -----

# ---- billing cron -----
import pytz
@app.on_event("startup")
async def _billing_cron():
    # BILL_MODE=subscriptions → Stripe Subscriptions + Meters bill everything; skip internal cron
    if os.getenv("BILL_MODE", "subscriptions").lower() == "subscriptions":
        return

    async def _runner():
        while True:
            try:
                prefs = await database.fetch_all("SELECT member,billing_day,timezone,auto_charge,next_cycle_start FROM billing_preferences")
                now_utc = _dt.now(_tz.utc)
                for p in prefs:
                    if not p["auto_charge"]:
                        continue
                    tz = pytz.timezone(p["timezone"] or "America/New_York")
                    now_local = now_utc.astimezone(tz)
                    if now_local.day != int(p["billing_day"]):
                        continue

                    if p["next_cycle_start"]:
                        start = p["next_cycle_start"]
                    else:
                        s, e = _cycle_bounds(now_local, int(p["billing_day"]))
                        start = s
                    end = now_local.date()
                    if not start or start >= end:
                        continue

                    internal_id = str(uuid.uuid4())
                    # Only used in internal-billing mode:
                    await _stripe_invoice_for_member(member=p["member"], start=start, end=end, invoice_id=internal_id)

                    await database.execute("""
                      UPDATE billing_preferences SET next_cycle_start=:n, updated_at=NOW() WHERE member=:m
                    """, {"n": end, "m": p["member"]})
            except Exception:
                pass
            await asyncio.sleep(3600)
    asyncio.create_task(_runner())

# ----- billing cron -----

# ---- ws meter flush -----
@app.on_event("startup")
async def _ws_meter_flush():
    """
    Hourly: sum new WS message counts from data_msg_counters and emit to Stripe as raw counts.
    Assumes table: data_msg_counters(member TEXT, count BIGINT, ts TIMESTAMPTZ).
    """
    async def _run():
        last_ts = None
        while True:
            try:
                q = """
                  SELECT member, COALESCE(SUM(count),0) AS cnt
                    FROM data_msg_counters
                   WHERE (:ts IS NULL OR ts > :ts)
                   GROUP BY member
                """
                rows = await database.fetch_all(q, {"ts": last_ts})
                for r in rows:
                    cnt = int(r["cnt"] or 0)
                    if cnt > 0:
                        emit_ws_usage(r["member"], cnt)
                last_ts = utcnow()
            except Exception:
                pass
            await asyncio.sleep(3600)  # hourly
    asyncio.create_task(_run())
# ----- ws meter flush -----

# ----- plans tables -----
@app.on_event("startup")
async def _ensure_plans_tables():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS billing_plans (
      plan_code TEXT PRIMARY KEY,         -- 'starter','standard','enterprise'
      name TEXT NOT NULL,
      price_usd NUMERIC NOT NULL,         -- monthly fixed
      description TEXT,
      active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS member_plans (
      member TEXT PRIMARY KEY,
      plan_code TEXT NOT NULL REFERENCES billing_plans(plan_code),
      effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
      stripe_price_id TEXT,               -- used only if you choose Subscription mode
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)
# ----- plans tables -----

# ----- plan caps and limits -----
@app.on_event("startup")
async def _ensure_plan_caps_and_limits():
    await run_ddl_multi("""
    -- Plan catalog (already created earlier, kept here for clarity)
    CREATE TABLE IF NOT EXISTS billing_plans (
      plan_code TEXT PRIMARY KEY,         -- 'starter','standard','enterprise'
      name TEXT NOT NULL,
      price_usd NUMERIC NOT NULL,
      description TEXT,
      active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    -- Member -> plan mapping (already created earlier)
    CREATE TABLE IF NOT EXISTS member_plans (
      member TEXT PRIMARY KEY,
      plan_code TEXT NOT NULL REFERENCES billing_plans(plan_code),
      effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    -- Feature gates per plan (bool flags). Add more flags as you like.
    CREATE TABLE IF NOT EXISTS billing_plan_caps (
      plan_code TEXT PRIMARY KEY REFERENCES billing_plans(plan_code),
      can_contracts BOOLEAN NOT NULL DEFAULT TRUE,
      can_bols BOOLEAN NOT NULL DEFAULT TRUE,
      can_inventory BOOLEAN NOT NULL DEFAULT TRUE,
      can_receipts BOOLEAN NOT NULL DEFAULT FALSE,
      can_warrants BOOLEAN NOT NULL DEFAULT FALSE,
      can_rfq BOOLEAN NOT NULL DEFAULT FALSE,
      can_clob BOOLEAN NOT NULL DEFAULT FALSE,
      can_futures BOOLEAN NOT NULL DEFAULT FALSE,
      can_market_data BOOLEAN NOT NULL DEFAULT TRUE
    );

    -- Included usage per month + overage prices (USD) per unit
    -- Pick the metrics that matter for you;
    CREATE TABLE IF NOT EXISTS billing_plan_limits (
      plan_code TEXT PRIMARY KEY REFERENCES billing_plans(plan_code),

      -- Included quantities per monthly cycle:
      inc_bol_create INT NOT NULL DEFAULT 50,          -- included BOL creations
      inc_bol_deliver_tons NUMERIC NOT NULL DEFAULT 500, -- included delivered tons
      inc_receipts INT NOT NULL DEFAULT 0,
      inc_warrants INT NOT NULL DEFAULT 0,
      inc_ws_msgs BIGINT NOT NULL DEFAULT 50000,       -- included WS messages/month

      -- Overage pricing (USD per unit):
      over_bol_create_usd NUMERIC NOT NULL DEFAULT 1.00,      -- / BOL beyond included
      over_deliver_per_ton_usd NUMERIC NOT NULL DEFAULT 0.50, -- $/ton beyond included
      over_receipt_usd NUMERIC NOT NULL DEFAULT 0.50,         -- / receipt beyond included
      over_warrant_usd NUMERIC NOT NULL DEFAULT 0.50,         -- / warrant event beyond included
      over_ws_per_million_usd NUMERIC NOT NULL DEFAULT 5.00   -- per 1,000,000 messages
    );
    """)
# ----- plan caps and limits -----

# ----- Products -----
@app.on_event("startup")
async def _ensure_products_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS public.products (
      symbol TEXT PRIMARY KEY,
      description TEXT NOT NULL,
      unit TEXT NOT NULL,
      quality JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    try:
        # run_ddl_multi is not defined; execute each statement separately
        for stmt in ddl.split(";"):
            s = stmt.strip()
            if s:
                await database.execute(s)
    except Exception as e:
        logger.warn("products_bootstrap_failed", err=str(e))
# ----- Products -----

# ----- Anomaly scores -----
@app.on_event("startup")
async def _ensure_anomaly_scores_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS public.anomaly_scores (
      member  TEXT NOT NULL,
      symbol  TEXT NOT NULL,
      as_of   DATE NOT NULL,
      score   NUMERIC NOT NULL,
      features JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (member, symbol, as_of)
    );
    CREATE INDEX IF NOT EXISTS idx_anom_asof ON public.anomaly_scores (as_of);
    """
    try:
        await run_ddl_multi(ddl)
    except Exception as e:
        logger.warn("anomaly_scores_bootstrap_failed", err=str(e))
# ----- Anomaly scores -----

# ----- Index contracts -----
@app.on_event("startup")
async def _ensure_index_contracts_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS public.index_contracts (
      tradable_symbol TEXT PRIMARY KEY,   -- e.g., 'BR-CU-2025M'
      symbol          TEXT NOT NULL,      -- index root, e.g., 'BR-CU'
      month_code      TEXT NOT NULL,      -- e.g., 'M'
      year            INT  NOT NULL,
      expiry_date     DATE NOT NULL,
      status          TEXT NOT NULL DEFAULT 'Listed',
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    try:
        await database.execute(ddl)
    except Exception as e:
        logger.warn("index_contracts_bootstrap_failed", err=str(e))
# ----- Index contracts -----

#----- Fee schedule ----
@app.on_event("startup")
async def _ensure_fee_schedule_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS fee_schedule (
      symbol TEXT PRIMARY KEY,
      maker_bps NUMERIC NOT NULL DEFAULT 0,
      taker_bps NUMERIC NOT NULL DEFAULT 0,
      min_fee_cents NUMERIC NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    try:
        await database.execute(ddl)
    except Exception as e:
        logger.warn("fee_schedule_bootstrap_failed", err=str(e))
# ---- Fee schedule ----

# ---- Settlement Publishing ---- 
@app.on_event("startup")
async def _ensure_settlements_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS settlements (
      as_of DATE NOT NULL,
      symbol TEXT NOT NULL,
      settle NUMERIC NOT NULL,
      method TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (as_of, symbol)
    );
    """
    try:
        await database.execute(ddl)
    except Exception as e:
        logger.warn("settlements_schema_bootstrap_failed", err=str(e))
# ---- Settlement Publishing ----

# ---- Indices (daily + snapshots) bootstrap ----
@app.on_event("startup")
async def _ensure_indices_tables():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS indices_daily (
      as_of_date DATE NOT NULL,
      region TEXT NOT NULL,
      material TEXT NOT NULL,
      avg_price NUMERIC,
      volume_tons NUMERIC,
      PRIMARY KEY (as_of_date, region, material)
    );
    CREATE TABLE IF NOT EXISTS index_snapshots (
      id BIGSERIAL PRIMARY KEY,
      region TEXT NOT NULL,
      sku TEXT NOT NULL,
      avg_price NUMERIC,
      snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE
    );
    """)
# ---- /Indices (daily + snapshots) bootstrap ----

# #------ Surveillance / Alerts ------
@app.on_event("startup")
async def _ensure_surveillance_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS surveil_alerts (
      alert_id UUID PRIMARY KEY,
      rule TEXT NOT NULL,
      subject TEXT NOT NULL,
      data JSONB NOT NULL,
      severity TEXT NOT NULL CHECK (severity IN ('info','warn','high')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    try:
        await database.execute(ddl)
    except Exception as e:
        logger.warn("surveil_schema_bootstrap_failed", err=str(e))
#------ Surveillance / Alerts ------

# ----- rehydrate sequencer queue -----
@app.on_event("startup")
async def _rehydrate_queue():
    try:
        rows = await database.fetch_all(
            "SELECT id, topic, payload FROM matching_events WHERE processed_at IS NULL ORDER BY enqueued_at"
        )
        for r in rows:
            await _event_queue.put((int(r["id"]), r["topic"], dict(r["payload"])))
    except Exception:
        pass
# ----- rehydrate sequencer queue -----

# ----- Total cleanup job -----
@app.on_event("startup")
async def _http_idem_ttl_job():
    async def _run():
        while True:
            try:
                await database.execute("DELETE FROM http_idempotency WHERE created_at < NOW() - INTERVAL '2 days'")
            except Exception:
                pass
            await asyncio.sleep(24*3600)
    asyncio.create_task(_run())
# ===== Total cleanup job =====

# ===== Retention cron =====
@app.on_event("startup")
async def _retention_cron():
    async def _run():
        while True:
            try:
                # Simple defaults; wire to retention_policies if you want dynamic TTLs
                await database.execute("DELETE FROM md_ticks WHERE ts < NOW() - INTERVAL '30 days'")
                await database.execute("DELETE FROM orders_audit WHERE at < NOW() - INTERVAL '180 days'")
                await database.execute("DELETE FROM webhook_dead_letters WHERE created_at < NOW() - INTERVAL '14 days'")
                await database.execute("DELETE FROM http_idempotency WHERE created_at < NOW() - INTERVAL '2 days'")
            except Exception:
                pass
            await asyncio.sleep(24*3600)
    asyncio.create_task(_run())
# ===== Retention cron =====

# -------- DDL and hydrate ----------
@app.on_event("startup")
async def _ensure_runtime_risk_tables():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS runtime_price_bands(
      symbol TEXT PRIMARY KEY,
      lower NUMERIC,
      upper NUMERIC,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS runtime_luld(
      symbol TEXT PRIMARY KEY,
      down_pct NUMERIC,
      up_pct NUMERIC,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    """)

    # hydrate into memory
    try:
        bands = await database.fetch_all("SELECT symbol, lower, upper FROM runtime_price_bands")
        for b in bands:
            _PRICE_BANDS[b["symbol"]] = (float(b["lower"]), float(b["upper"]))
        lulds = await database.fetch_all("SELECT symbol, down_pct, up_pct FROM runtime_luld")
        for l in lulds:
            _LULD[l["symbol"]] = (float(l["down_pct"]), float(l["up_pct"]))
    except Exception:
        pass
# -------- DDL and hydrate ----------

# -------- Legal pages --------
@app.get("/legal/terms", include_in_schema=True, tags=["Legal"], summary="Terms of Service")
async def terms_page():
    return FileResponse("static/legal/terms.html")

@app.get("/legal/eula", include_in_schema=True, tags=["Legal"], summary="End User License Agreement")
async def eula_page():
    return FileResponse("static/legal/eula.html")

@app.get("/legal/privacy", include_in_schema=True, tags=["Legal"], summary="Privacy Policy")
async def privacy_page():
    return FileResponse("static/legal/privacy.html")
@app.get("/legal/aup", include_in_schema=True, tags=["Legal"], summary="Acceptable Use Policy")
async def aup_page():
    return FileResponse("static/legal/aup.html")

# Cookie Notice (alias: /legal/cookies and /cookies)
@app.get("/legal/cookies", include_in_schema=True, tags=["Legal"],
         summary="Cookie Notice",
         description="View the BRidge Cookie Notice.", status_code=200)
@app.get("/legal/cookies.html", include_in_schema=False)
async def cookies_page():
    return FileResponse("static/legal/cookies.html")

# Subprocessors (alias: /legal/subprocessors and /subprocessors)
@app.get("/legal/subprocessors", include_in_schema=True, tags=["Legal"],
         summary="Subprocessors",
         description="View the current list of BRidge subprocessors.", status_code=200)
@app.get("/legal/subprocessors.html", include_in_schema=False)
async def subprocessors_page():
    return FileResponse("static/legal/subprocessors.html")

# Data Processing Addendum (alias: /legal/dpa and /dpa)
@app.get("/legal/dpa", include_in_schema=True, tags=["Legal"],
         summary="Data Processing Addendum (DPA)",
         description="View the BRidge Data Processing Addendum.", status_code=200)
@app.get("/legal/dpa.html", include_in_schema=False)
async def dpa_page():
    return FileResponse("static/legal/dpa.html")

# Service Level Addendum (alias: /legal/sla and /sla)
@app.get("/legal/sla", include_in_schema=True, tags=["Legal"],
         summary="Service Level Addendum (SLA)",
         description="View the BRidge Service Level Addendum.", status_code=200)
@app.get("/legal/sla.html", include_in_schema=False)
async def sla_page():
    return FileResponse("static/legal/sla.html")

# Security Controls Overview (alias: /legal/security and /security)
@app.get("/legal/security", include_in_schema=True, tags=["Legal"],
         summary="Security Controls Overview",
         description="View the BRidge Security Controls Overview / Security Whitepaper.", status_code=200)
@app.get("/legal/security.html", include_in_schema=False)
async def security_page():
    return FileResponse("static/legal/security.html")

# Jurisdictional Privacy Appendix (alias: /legal/privacy-appendix and /privacy/appendix)
@app.get("/legal/privacy-appendix", include_in_schema=True, tags=["Legal"],
         summary="Jurisdictional Privacy Appendix (APAC & Canada)",
         description="View region-specific privacy disclosures for APAC & Canada.", status_code=200)
@app.get("/legal/privacy-appendix.html", include_in_schema=False)
async def privacy_appendix_page():
    return FileResponse("static/legal/privacy-appendix.html")
# -------- Legal pages --------

# -------- Regulatory: Rulebook & Policies (versioned) --------
@app.on_event("startup")
async def _ensure_rulebook_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS rulebook_versions(
      version TEXT PRIMARY KEY,
      effective_date DATE NOT NULL,
      url TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS compliance_policies(
      key TEXT PRIMARY KEY,
      url TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    await run_ddl_multi(ddl)

@app.get("/legal/rulebook", tags=["Legal"], summary="Current Rulebook (HTML)")
async def rulebook_latest():
    row = await database.fetch_one(
        "SELECT url FROM rulebook_versions ORDER BY effective_date DESC LIMIT 1"
    )
    if not row: return FileResponse("static/legal/rulebook.html")
    return FileResponse(row["url"])

@app.get("/legal/rulebook/versions", tags=["Legal"])
async def rulebook_versions():
    rows = await database.fetch_all(
        "SELECT version,effective_date,url FROM rulebook_versions ORDER BY effective_date DESC"
    )
    return [dict(r) for r in rows]

@app.post("/admin/legal/rulebook/upsert", tags=["Legal"])
async def rulebook_upsert(version: str, effective_date: date, file_path: str, request: Request):
    _require_admin(request)
    await database.execute("""
      INSERT INTO rulebook_versions(version,effective_date,url)
      VALUES (:v,:d,:u)
      ON CONFLICT (version) DO UPDATE SET effective_date=EXCLUDED.effective_date, url=EXCLUDED.url
    """, {"v":version, "d":effective_date, "u":file_path})
    return {"ok": True, "version": version}
# -------- /Regulatory --------

# === QBO OAuth Relay • Table  ===
@app.on_event("startup")
async def _ensure_qbo_oauth_events_table():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS qbo_oauth_events (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      state    TEXT UNIQUE NOT NULL,
      code     TEXT NOT NULL,
      realm_id TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_qbo_events_created ON qbo_oauth_events(created_at DESC);
    """)
# === QBO OAuth Relay • Table ===

# -------- Static HTML --------
from pathlib import Path
STATIC_DIR = Path(__file__).parent.resolve() / "static"

def _static_or_placeholder(filename: str, title: str):
    p = STATIC_DIR / filename
    if not p.exists():
        return HTMLResponse(
            f"<h1>{title}</h1><p>Upload <code>/static/{filename}</code> to replace this placeholder.</p>"
        )
    return FileResponse(str(p))
# Ferrous
@app.get("/static/spec-ferrous", include_in_schema=False)
@app.get("/static/spec-ferrous.html", include_in_schema=False)
async def _spec_ferrous():
    return _static_or_placeholder("spec-ferrous.html", "Ferrous Spec Sheet")

# Nonferrous
@app.get("/static/spec-nonferrous", include_in_schema=False)
@app.get("/static/spec-nonferrous.html", include_in_schema=False)
async def _spec_nonferrous():
    return _static_or_placeholder("spec-nonferrous.html", "Nonferrous Spec Sheet")

# Contract specs
@app.get("/static/contract-specs", include_in_schema=False)
@app.get("/static/contract-specs.html", include_in_schema=False)
async def _contract_specs():
    return _static_or_placeholder("contract-specs.html", "Contract Specifications")

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.get("/__diag/static", include_in_schema=False)
async def __diag_static():
    try:
        files = sorted(p.name for p in STATIC_DIR.iterdir())
    except Exception as e:
        return JSONResponse({"static_dir": str(STATIC_DIR), "error": str(e)}, status_code=500)
    return {"static_dir": str(STATIC_DIR), "files": files}

# -------- /Static HTML --------

@app.get("/", include_in_schema=False)
async def root(request: Request):
    """
    Serve login page and (like /buyer,/seller,/admin) also mint XSRF-TOKEN so the SPA
    can immediately echo it in X-CSRF if/when needed.
    """
    token = _csrf_get_or_create(request)
    prod = os.getenv("ENV","").lower() == "production"
    resp = FileResponse("static/bridge-login.html")
    resp.set_cookie("XSRF-TOKEN", token, httponly=False, samesite="lax", secure=prod, path="/")
    return resp

# --- Dynamic buyer page with per-request nonce + strict CSP ---
with open("static/bridge-buyer.html", "r", encoding="utf-8") as f:
    _BUYER_HTML_TEMPLATE = f.read()

with open("static/seller.html", "r", encoding="utf-8") as f:
    _SELLER_HTML_TEMPLATE = f.read()

with open("static/bridge-admin-dashboard.html", "r", encoding="utf-8") as f:
    _ADMIN_HTML_TEMPLATE = f.read()


@app.get("/buyer", include_in_schema=False)
async def buyer_page_dynamic(request: Request):
    nonce = getattr(request.state, "csp_nonce", secrets.token_urlsafe(16))
    html = _BUYER_HTML_TEMPLATE.replace("{{NONCE}}", nonce)

    # CSRF tied to session; expose via readable cookie for JS
    token = _csrf_get_or_create(request)
    prod = os.getenv("ENV","").lower() == "production"

    csp = (
        "default-src 'self'; "
        "base-uri 'self'; object-src 'none'; frame-ancestors 'none'; "
        "img-src 'self' data: blob: https://*.stripe.com; "
        "font-src 'self' https://fonts.gstatic.com data:; "
        "style-src 'self' https://cdn.jsdelivr.net https://fonts.googleapis.com 'unsafe-inline' 'nonce-" + nonce + "'; "
        "style-src-attr 'self'; "
        "script-src 'self' 'nonce-" + nonce + "' https://cdn.jsdelivr.net https://js.stripe.com; "
        "frame-src 'self' https://js.stripe.com https://checkout.stripe.com; "
        "connect-src 'self' ws: wss: https://cdn.jsdelivr.net https://*.stripe.com; "
        "form-action 'self'"
    )

    resp = HTMLResponse(content=html, headers={"Content-Security-Policy": csp})
    resp.set_cookie("XSRF-TOKEN", token, httponly=False, samesite="lax", secure=prod, path="/")
    # optional client hint (non-authoritative)
    resp.set_cookie("BRIDGE-ROLE", (request.session.get("role") or "").lower(), httponly=False, samesite="lax", secure=prod, path="/")
    return resp

@app.get("/admin", include_in_schema=False)
async def admin_page(request: Request):
    nonce = getattr(request.state, "csp_nonce", secrets.token_urlsafe(16))
    html = _ADMIN_HTML_TEMPLATE.replace("{{NONCE}}", nonce)

    token = _csrf_get_or_create(request)
    prod = os.getenv("ENV","").lower() == "production"

    csp = (
        "default-src 'self'; "
        "base-uri 'self'; object-src 'none'; frame-ancestors 'none'; "
        "img-src 'self' data: blob: https://*.stripe.com; "
        "font-src 'self' https://fonts.gstatic.com data:; "
        "style-src 'self' https://cdn.jsdelivr.net https://fonts.googleapis.com 'unsafe-inline' 'nonce-" + nonce + "'; "
        "style-src-attr 'self'; "
        "script-src 'self' 'nonce-" + nonce + "' https://cdn.jsdelivr.net https://js.stripe.com; "
        "frame-src 'self' https://js.stripe.com https://checkout.stripe.com; "
        "connect-src 'self' ws: wss: https://cdn.jsdelivr.net https://*.stripe.com; "
        "form-action 'self'"
    )
    resp = HTMLResponse(content=html, headers={"Content-Security-Policy": csp})
    resp.set_cookie("XSRF-TOKEN", token, httponly=False, samesite="lax", secure=prod, path="/")
    return resp

@app.get("/seller", include_in_schema=False)
async def seller_page(request: Request):
    nonce = getattr(request.state, "csp_nonce", secrets.token_urlsafe(16))
    html = _SELLER_HTML_TEMPLATE.replace("{{NONCE}}", nonce)

    token = _csrf_get_or_create(request)
    prod = os.getenv("ENV","").lower() == "production"

    csp = (
        "default-src 'self'; "
        "base-uri 'self'; object-src 'none'; frame-ancestors 'none'; "
        "img-src 'self' data: blob: https://*.stripe.com; "
        "font-src 'self' https://fonts.gstatic.com data:; "
        "style-src 'self' https://cdn.jsdelivr.net https://fonts.googleapis.com 'unsafe-inline' 'nonce-" + nonce + "'; "
        "style-src-attr 'self'; "
        "script-src 'self' 'nonce-" + nonce + "' https://cdn.jsdelivr.net https://js.stripe.com; "
        "frame-src 'self' https://js.stripe.com https://checkout.stripe.com; "
        "connect-src 'self' ws: wss: https://cdn.jsdelivr.net https://*.stripe.com; "
        "form-action 'self'"
    )
    resp = HTMLResponse(content=html, headers={"Content-Security-Policy": csp})
    resp.set_cookie("XSRF-TOKEN", token, httponly=False, samesite="lax", secure=prod, path="/")
    return resp

@app.get("/indices-dashboard", include_in_schema=False)
async def indices_page(request: Request):
    token = _csrf_get_or_create(request)
    prod = os.getenv("ENV","").lower() == "production"
    resp = FileResponse("static/indices.html")
    resp.set_cookie("XSRF-TOKEN", token, httponly=False, samesite="lax", secure=prod, path="/")
    return resp
# -------- Static HTML --------

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

# -------- Risk controls (kill switch, price bands, entitlements) --------
@app.on_event("startup")
async def _ensure_risk_schema():
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS clob_position_limits (
          member TEXT NOT NULL,
          symbol TEXT NOT NULL,
          limit_lots NUMERIC NOT NULL,
          PRIMARY KEY (member, symbol)
        );
        """
    ]
    for s in ddl:
        try:
            await database.execute(s)
        except Exception as e:
            logger.warn("risk_schema_bootstrap_failed", err=str(e), sql=s[:120])

@app.on_event("startup")
async def _ensure_entitlements_table():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS runtime_entitlements(
      username TEXT NOT NULL,
      feature  TEXT NOT NULL,
      PRIMARY KEY (username, feature)
    );
    """)
    # hydrate into memory
    rows = await database.fetch_all("SELECT username, feature FROM runtime_entitlements")
    for r in rows:
        _ENTITLEMENTS[r["username"]].add(r["feature"])

# Persist whenever we grant
@app.post("/admin/entitlements/grant", tags=["Admin"], summary="Grant feature to user/member")
async def grant_entitlement(user: str, feature: str, request: Request):
    _require_admin(request)  # gate in production
    _ENTITLEMENTS[user].add(feature)
    try:
        await database.execute("""
          INSERT INTO runtime_entitlements(username, feature)
          VALUES (:u,:f)
          ON CONFLICT (username, feature) DO NOTHING
        """, {"u": user, "f": feature})
    except Exception:
        pass
    return {"user": user, "features": sorted(list(_ENTITLEMENTS[user]))}
#----- Risk controls (kill switch, price bands, entitlements) -----

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
# -------- Ensure public.users has id/email/username/is_active --------

# -------- Health --------
@app.get("/healthz", tags=["Health"], summary="Health Check")
async def healthz():
    try:
        await database.execute("SELECT NOW()")
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "degraded", "error": str(e)})
    
@app.get("/health", include_in_schema=False)
async def health_alias(request: Request):
    """
    Health alias that also issues an XSRF-TOKEN cookie so static pages
    can read it and send 'X-CSRF' on subsequent requests.
    """
    # 1) issue CSRF token bound to the session    
    token = _csrf_get_or_create(request)

    # 2) do the same health check you already had
    try:
        await database.execute("SELECT NOW()")
        resp = JSONResponse({"status": "ok"})
    except Exception as e:
        resp = JSONResponse(status_code=503, content={"status": "degraded", "error": str(e)})

    # 3) set the cookie (readable by JS; Secure in prod)
    import os
    prod = os.getenv("ENV","").lower() == "production"
    resp.set_cookie(
        key="XSRF-TOKEN",
        value=token,
        httponly=False,   # must be readable by JS (_cookie('XSRF-TOKEN'))
        samesite="lax",
        secure=prod,
        path="/"
    )
    return resp
    
@app.get("/__diag/users_count", tags=["Health"])
async def diag_users_count():
    try:
        row = await database.fetch_one("SELECT COUNT(*) AS c FROM public.users")
        return {"users": int(row["c"])}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/ops/readiness", tags=["Health"])
async def readiness():
    try:
        await database.execute("SELECT 1")  # db check
        qdepth_row = await database.fetch_one(
            "SELECT COUNT(*) AS c FROM matching_events WHERE processed_at IS NULL"
        )
        qdepth = int(qdepth_row["c"]) if qdepth_row else 0
        return {"db":"ok", "queue_depth": qdepth, "sequencer_running": bool(_SEQUENCER_STARTED)}
    except Exception as e:
        return JSONResponse(status_code=503, content={"db":"error","error":str(e)})
# -------- Health --------

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
    try:
        q = """SELECT forecast_date, predicted_price, conf_low, conf_high, model_name
               FROM bridge_forecasts
               WHERE symbol=$1 AND horizon_days=$2
               ORDER BY forecast_date"""
        rows = await app.state.db_pool.fetch(q, symbol, horizon_days)
        if not rows:
            raise HTTPException(404, "No forecasts available")
        return [dict(r) for r in rows]
    except HTTPException:
        raise
    except Exception as e:
        # dev-safe: treat as "no data yet" instead of a 500
        try:
            logger.warn("forecasts_latest_error", err=str(e))
        except Exception:
            pass
        raise HTTPException(404, "No forecasts available")

@router_idx.post("/run", summary="Build today's BRidge Index closes (UTC)")
async def run_indices_now():
    await run_indices_builder()
    return {"ok": True}

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
            now = utcnow()
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
def _bootstrap_prices_indices_schema_if_needed(sqlalchemy_engine):
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
            _bootstrap_prices_indices_schema_if_needed(engine)
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
            dt = datetime.fromisoformat(ts)
            created_at = (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc))

            raw_key = f"{seller}|{buyer}|{material}|{price_per_ton}|{weight_tons}|{created_at.isoformat()}"
            dedupe_key = hashlib.sha256(raw_key.encode()).hexdigest()

            yield {
                "seller": seller, "buyer": buyer, "material": material,
                "price_per_ton": price_per_ton, "weight_tons": weight_tons,
                "created_at": created_at, "dedupe_key": dedupe_key
            }
        except (KeyError, InvalidOperation, ValueError):
            continue

@app.post("/import/contracts_csv", tags=["Data"], summary="Bulk import normalized contracts CSV (gzip or csv)", response_model=None)
async def import_contracts_csv(
    file: Annotated[UploadFile, File(...)],
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

@app.post("/admin/plans/seed_mode_a", tags=["Admin"])
async def seed_mode_a_plans(request: Request=None):
    _require_admin(request)
    plans = [
      {"plan_code":"starter",    "name":"Starter",    "price_usd":1000.00, "desc":"Contracts & BOLs"},
      {"plan_code":"standard",   "name":"Standard",   "price_usd":3000.00, "desc":"Contracts+BOLs+Inventory+Receipts"},
      {"plan_code":"enterprise", "name":"Enterprise","price_usd":10000.00, "desc":"Full stack + Futures/Clearing"}
    ]
    await database.execute_many("""
      INSERT INTO billing_plans(plan_code,name,price_usd,description,active)
      VALUES (:plan_code,:name,:price_usd,:desc,TRUE)
      ON CONFLICT (plan_code) DO UPDATE SET name=EXCLUDED.name, price_usd=EXCLUDED.price_usd,
        description=EXCLUDED.description, active=TRUE
    """, plans)

    caps = [
      {"plan_code":"starter",    "can_contracts":True,"can_bols":True,"can_inventory":True,"can_receipts":False,"can_warrants":False,"can_rfq":False,"can_clob":False,"can_futures":False,"can_market_data":True},
      {"plan_code":"standard",   "can_contracts":True,"can_bols":True,"can_inventory":True,"can_receipts":True,"can_warrants":True,"can_rfq":True,"can_clob":False,"can_futures":False,"can_market_data":True},
      {"plan_code":"enterprise", "can_contracts":True,"can_bols":True,"can_inventory":True,"can_receipts":True,"can_warrants":True,"can_rfq":True,"can_clob":True,"can_futures":True,"can_market_data":True}
    ]
    await database.execute_many("""
      INSERT INTO billing_plan_caps(plan_code,can_contracts,can_bols,can_inventory,can_receipts,can_warrants,can_rfq,can_clob,can_futures,can_market_data)
      VALUES (:plan_code,:can_contracts,:can_bols,:can_inventory,:can_receipts,:can_warrants,:can_rfq,:can_clob,:can_futures,:can_market_data)
      ON CONFLICT (plan_code) DO UPDATE SET
        can_contracts=EXCLUDED.can_contracts,
        can_bols=EXCLUDED.can_bols,
        can_inventory=EXCLUDED.can_inventory,
        can_receipts=EXCLUDED.can_receipts,
        can_warrants=EXCLUDED.can_warrants,
        can_rfq=EXCLUDED.can_rfq,
        can_clob=EXCLUDED.can_clob,
        can_futures=EXCLUDED.can_futures,
        can_market_data=EXCLUDED.can_market_data
    """, caps)

    limits = [
      # starter
      {"plan_code":"starter","inc_bol_create":50,"inc_bol_deliver_tons":500,"inc_receipts":0,"inc_warrants":0,"inc_ws_msgs":50000,
       "over_bol_create_usd":1.0,"over_deliver_per_ton_usd":0.50,"over_receipt_usd":0.50,"over_warrant_usd":0.50,"over_ws_per_million_usd":5.0},
      # standard
      {"plan_code":"standard","inc_bol_create":200,"inc_bol_deliver_tons":2000,"inc_receipts":500,"inc_warrants":200,"inc_ws_msgs":200000,
       "over_bol_create_usd":0.75,"over_deliver_per_ton_usd":0.35,"over_receipt_usd":0.30,"over_warrant_usd":0.30,"over_ws_per_million_usd":4.0},
      # enterprise
      {"plan_code":"enterprise","inc_bol_create":999999,"inc_bol_deliver_tons":999999,"inc_receipts":999999,"inc_warrants":999999,"inc_ws_msgs":5000000,
       "over_bol_create_usd":0.50,"over_deliver_per_ton_usd":0.25,"over_receipt_usd":0.10,"over_warrant_usd":0.10,"over_ws_per_million_usd":3.0},
    ]
    await database.execute_many("""
      INSERT INTO billing_plan_limits(plan_code,inc_bol_create,inc_bol_deliver_tons,inc_receipts,inc_warrants,inc_ws_msgs,
                                      over_bol_create_usd,over_deliver_per_ton_usd,over_receipt_usd,over_warrant_usd,over_ws_per_million_usd)
      VALUES (:plan_code,:inc_bol_create,:inc_bol_deliver_tons,:inc_receipts,:inc_warrants,:inc_ws_msgs,
              :over_bol_create_usd,:over_deliver_per_ton_usd,:over_receipt_usd,:over_warrant_usd,:over_ws_per_million_usd)
      ON CONFLICT (plan_code) DO UPDATE SET
        inc_bol_create=EXCLUDED.inc_bol_create,
        inc_bol_deliver_tons=EXCLUDED.inc_bol_deliver_tons,
        inc_receipts=EXCLUDED.inc_receipts,
        inc_warrants=EXCLUDED.inc_warrants,
        inc_ws_msgs=EXCLUDED.inc_ws_msgs,
        over_bol_create_usd=EXCLUDED.over_bol_create_usd,
        over_deliver_per_ton_usd=EXCLUDED.over_deliver_per_ton_usd,
        over_receipt_usd=EXCLUDED.over_receipt_usd,
        over_warrant_usd=EXCLUDED.over_warrant_usd,
        over_ws_per_million_usd=EXCLUDED.over_ws_per_million_usd
    """, limits)
    return {"ok": True}

@app.post("/admin/fees/upsert", tags=["Admin"], summary="Upsert fee schedule for a symbol")
async def upsert_fee(symbol: str, maker_bps: float = 0.0, taker_bps: float = 0.0, min_fee_cents: float = 0.0, request: Request = None):
    _require_admin(request)  # gate in production
    await database.execute("""
      INSERT INTO fee_schedule(symbol, maker_bps, taker_bps, min_fee_cents)
      VALUES (:s,:m,:t,:c)
      ON CONFLICT (symbol) DO UPDATE
        SET maker_bps = EXCLUDED.maker_bps,
            taker_bps = EXCLUDED.taker_bps,
            min_fee_cents = EXCLUDED.min_fee_cents
    """, {"s": symbol, "m": maker_bps, "t": taker_bps, "c": min_fee_cents})
    return {"symbol": symbol, "maker_bps": maker_bps, "taker_bps": taker_bps, "min_fee_cents": min_fee_cents}

from fastapi import Body

@app.post("/billing/pay/checkout", tags=["Billing"], summary="Create Stripe Checkout Session for an invoice")
async def create_checkout_session(invoice_id: str = Body(..., embed=True)):
    inv = await _fetch_invoice(invoice_id)
    if not inv:
        raise HTTPException(404, "invoice not found")
    if str(inv["status"]).lower() == "paid":
        return {"already_paid": True}

    member = inv["member"]
    amount = float(inv["total"])

    session = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card", "us_bank_account"],   # cards + ACH debit
        success_url=SUCCESS_URL,
        cancel_url=CANCEL_URL,
        line_items=[{
            "price_data": {
                "currency": "usd",
                "product_data": {"name": f"BRidge Invoice {invoice_id} — {member}"},
                "unit_amount": _usd_cents(amount),
            },
            "quantity": 1,
        }],
        metadata={"invoice_id": invoice_id, "member": member}
    )
    return {"checkout_url": session.url, "session_id": session.id}

@app.get("/billing/pay/finalize_from_session", tags=["Billing"], summary="Finalize internal invoice after Checkout success (no webhook)")
async def finalize_from_session(sess: str):
    """
    Called by your /static/payment_success.html with ?session=sessid.
    Retrieves the Checkout Session → PaymentIntent → metadata.invoice_id, and marks your invoice paid.
    """
    try:
        s = stripe.checkout.Session.retrieve(sess, expand=["payment_intent"])
        pi = s.get("payment_intent")
        if isinstance(pi, dict):
            meta = pi.get("metadata") or {}
        elif isinstance(pi, str):
            pi_obj = stripe.PaymentIntent.retrieve(pi)
            meta = pi_obj.get("metadata") or {}
        else:
            meta = {}
        invoice_id = meta.get("invoice_id")
        if not invoice_id:
            raise ValueError("No invoice_id on PaymentIntent.metadata")
        await _handle_invoice_paid(invoice_id, source="checkout.session")
        return {"ok": True, "invoice_id": invoice_id}
    except Exception as e:
        raise HTTPException(400, f"finalize from session failed: {e}")

@app.post("/billing/prefs/upsert", tags=["Billing"], summary="Set billing date/timezone")
async def billing_prefs_upsert(
    member: str = Body(...),
    billing_day: int = Body(..., ge=2, le=26),
    timezone: str = Body("America/New_York"),
    auto_charge: bool = Body(True),
):
    await database.execute("""
      INSERT INTO billing_preferences(member,billing_day,timezone,auto_charge,next_cycle_start,updated_at)
      VALUES (:m,:d,:tz,:ac,NULL,NOW())
      ON CONFLICT (member) DO UPDATE SET
        billing_day=EXCLUDED.billing_day,
        timezone=EXCLUDED.timezone,
        auto_charge=EXCLUDED.auto_charge,
        updated_at=NOW()
    """, {"m": member, "d": billing_day, "tz": timezone, "ac": auto_charge})
    return {"ok": True}

from datetime import datetime as _dt, timezone as _tz
import pytz

def _cycle_bounds(today_local: _dt, billing_day: int):
    # given local “now” and target day (2..26), compute current-cycle start & end (open interval [start, end))
    # If today is billing day, end is today; start = previous billing day (last month or this month).
    year, month, day = today_local.year, today_local.month, today_local.day
    # end (the boundary we’ll bill through) is today_local.date() if today==billing_day else None
    if day < billing_day:
        # current cycle started last month on 'billing_day', ends this month on that day
        # compute start
        sm = month - 1 if month > 1 else 12
        sy = year if month > 1 else year - 1
        start = _dt(sy, sm, billing_day, 0, 0, 0)
        end   = _dt(year, month, billing_day, 0, 0, 0)
    elif day > billing_day:
        # cycle started this month on billing_day, ends next month
        start = _dt(year, month, billing_day, 0, 0, 0)
        # end is in the future; for “is today the billing day?” logic we won’t use this branch
        end   = None
    else:
        # today is billing_day → end is today @ 00:00, start is previous billing_day
        sm = month - 1 if month > 1 else 12
        sy = year if month > 1 else year - 1
        start = _dt(sy, sm, billing_day, 0, 0, 0)
        end   = _dt(year, month, billing_day, 0, 0, 0)
    return (start.date(), (end and end.date()))

@app.post("/billing/pay/card", tags=["Billing"], summary="Create PaymentIntent (card)")
async def create_pi_card(invoice_id: str = Body(..., embed=True)):
    inv = await _fetch_invoice(invoice_id)
    if not inv: raise HTTPException(404, "invoice not found")
    if str(inv["status"]).lower() == "paid": return {"already_paid": True}
    amount = float(inv["total"])
    pi = stripe.PaymentIntent.create(
        amount=_usd_cents(amount),
        currency="usd",
        payment_method_types=["card"],
        metadata={"invoice_id": invoice_id, "member": inv["member"]}
    )
    return {"client_secret": pi.client_secret}

@app.post("/billing/pay/ach", tags=["Billing"], summary="Create PaymentIntent (ACH debit via US bank)")
async def create_pi_ach(invoice_id: str = Body(..., embed=True)):
    inv = await _fetch_invoice(invoice_id)
    if not inv: raise HTTPException(404, "invoice not found")
    if str(inv["status"]).lower() == "paid": return {"already_paid": True}
    amount = float(inv["total"])
    pi = stripe.PaymentIntent.create(
        amount=_usd_cents(amount),
        currency="usd",
        payment_method_types=["us_bank_account"],
        payment_method_options={"us_bank_account": {"verification_method": "instant"}},
        metadata={"invoice_id": invoice_id, "member": inv["member"]}
    )
    return {"client_secret": pi.client_secret}

from fastapi import Header

@app.post("/stripe/webhook", include_in_schema=False)
async def stripe_webhook(payload: bytes = Body(...), stripe_signature: str = Header(None, alias="Stripe-Signature")):
    endpoint_secret = os.environ["STRIPE_WEBHOOK_SECRET"]
    try:
        event = stripe.Webhook.construct_event(payload, stripe_signature, endpoint_secret)
    except Exception as e:
        raise HTTPException(400, f"invalid webhook: {e}")

    et = event["type"]

    # SetupIntent completed (Elements or mode=setup)
    if et == "setup_intent.succeeded":
        si = event["data"]["object"]
        cust_id = si.get("customer")
        pm_id  = si.get("payment_method")
        member = (si.get("metadata") or {}).get("member")
        await _bind_pm(member, cust_id, pm_id)
        return {"ok": True}

    # Checkout session completed (mode=setup)
    if et == "checkout.session.completed":
        sess = event["data"]["object"]
        if sess.get("mode") == "setup":
            cust_id = sess.get("customer")
            si_id = sess.get("setup_intent")
            pm_id = None
            member = (sess.get("metadata") or {}).get("member")
            if si_id:
                si = stripe.SetupIntent.retrieve(si_id)
                pm_id = si.get("payment_method")
                member = member or (si.get("metadata") or {}).get("member")
            await _bind_pm(member, cust_id, pm_id)
        return {"ok": True}

    # PaymentIntent success (manual charge or checkout)
    if et == "payment_intent.succeeded":
        pi = event["data"]["object"]
        invoice_id = (pi.get("metadata") or {}).get("invoice_id")
        await _handle_invoice_paid(invoice_id, source="payment_intent")
        return {"ok": True}

    # Stripe invoice success/failure (auto-billing)
    if et == "invoice.payment_succeeded":
        inv = event["data"]["object"]
        member = (inv.get("metadata") or {}).get("member")
        my_invoice_id = (inv.get("metadata") or {}).get("bridge_invoice_id")
        total = inv.get("amount_paid", 0) / 100.0
        if my_invoice_id:
            await database.execute("UPDATE billing_invoices SET status='paid' WHERE invoice_id=:i", {"i": my_invoice_id})
        await notify_humans("payment.confirmed", member=member,
                            subject=f"Payment Succeeded — {member}",
                            html=f"<div style='font-family:system-ui'>Stripe invoice <b>{inv['id']}</b> paid. Amount: ${total:,.2f}.</div>",
                            cc_admin=True, ref_type="stripe_invoice", ref_id=inv["id"])
        return {"ok": True}

    return {"ignored": et}
    
async def _bind_pm(member: str | None, customer_id: str | None, pm_id: str | None):
    # (optional) email “PM added”
    try:
        if member:
            await notify_humans("pm.added", member=member,
                subject="Payment Method Added",
                html=f"<div style='font-family:system-ui'>A new default payment method was added for <b>{member}</b>.</div>",
                cc_admin=True, ref_type="member", ref_id=member)
    except Exception:
        pass

    if not (member and customer_id and pm_id):
        return

    # Set default at Stripe
    try:
        stripe.Customer.modify(customer_id, invoice_settings={"default_payment_method": pm_id})
    except Exception:
        pass

    # Persist locally
    await database.execute("""
      INSERT INTO billing_payment_profiles(member,email,stripe_customer_id,default_payment_method,has_default,updated_at)
      SELECT :m, COALESCE(email,''), :c, :p, TRUE, NOW()
      FROM billing_payment_profiles WHERE member=:m
      ON CONFLICT (member) DO UPDATE
        SET default_payment_method=:p, has_default=TRUE, stripe_customer_id=:c, updated_at=NOW()
    """, {"m": member, "c": customer_id, "p": pm_id})

async def _handle_invoice_paid(invoice_id: str | None, source: str):
    if not invoice_id: return
    inv = await _fetch_invoice(invoice_id)
    if not inv: return
    if str(inv["status"]).lower() == "paid":
        return  # idempotent

    # mark paid
    await database.execute("UPDATE billing_invoices SET status='paid' WHERE invoice_id=:i", {"i": invoice_id})

    # email user + admin
    member = inv["member"]
    subject = f"Payment Received — Invoice {invoice_id}"
    html = f"""
    <div style="font-family:system-ui">
      <h2>BRidge Payment Confirmation</h2>
      <p><b>Invoice:</b> {invoice_id}<br/>
         <b>Member:</b> {member}<br/>
         <b>Period:</b> {inv['period_start']} → {inv['period_end']}<br/>
         <b>Total:</b> ${float(inv['total']):,.2f} USD<br/>
         <b>Source:</b> {source}
      </p>
      <p>Thank you. A PDF/CSV copy is available in your dashboard.</p>
    </div>
    """
    await notify_humans("payment.confirmed", member=member, subject=subject, html=html,
                        cc_admin=True, ref_type="invoice", ref_id=invoice_id)
    
async def _stripe_invoice_for_member(member: str, start: date, end: date, invoice_id: str | None=None) -> dict:
        # BILL_MODE=subscriptions → Stripe Subscriptions + Meters generate the invoice; skip manual items
    if os.getenv("BILL_MODE", "subscriptions").lower() == "subscriptions":
        return {"skipped": "subscriptions_mode"}
    prof = await database.fetch_one("SELECT email, stripe_customer_id FROM billing_payment_profiles WHERE member=:m", {"m": member})
    if not (prof and prof["stripe_customer_id"]):
        raise HTTPException(402, f"{member} has no Stripe customer / PM")
    cust_id = prof["stripe_customer_id"]

    # usage aggregation from fees_ledger
    rows = await database.fetch_all("""
      SELECT event_type, SUM(COALESCE(fee_amount_usd, fee_amount)) AS amt
        FROM fees_ledger
       WHERE member=:m AND created_at::date >= :s AND created_at::date < :e
       GROUP BY event_type
    """, {"m": member, "s": start, "e": end})
    by_ev = {r["event_type"]: float(r["amt"] or 0) for r in rows}
    subtotal = sum(by_ev.values())
    mmi = await _mmi_usd(member) if "_mmi_usd" in globals() else 0.0
    trueup = max(0.0, mmi - subtotal) if mmi > 0 else 0.0

    # plan line + overages
    plan = await database.fetch_one("""
      SELECT mp.plan_code, bp.name, bp.price_usd
        FROM member_plans mp JOIN billing_plans bp ON bp.plan_code = mp.plan_code
       WHERE mp.member=:m
    """, {"m": member})
    limits = await database.fetch_one("SELECT * FROM billing_plan_limits WHERE plan_code=:c", {"c": plan["plan_code"]}) if plan else None

    # Guarded: only push manual items if explicitly allowed
    if os.getenv("PUSH_INTERNAL_ITEMS_TO_STRIPE", "0").lower() in ("1", "true", "yes"):
        # usage items
        for ev, amt in by_ev.items():
            if amt == 0:
                continue
        stripe.InvoiceItem.create(
            customer=cust_id, currency="usd", amount=_usd_cents(amt),
            description=f"{ev} ({start}–{end})",
            metadata={"member": member, "period_start": str(start), "period_end": str(end), "event_type": ev}
        )

    # Guarded: plan line only if explicitly allowed
    if os.getenv("PUSH_INTERNAL_ITEMS_TO_STRIPE", "0").lower() in ("1", "true", "yes"):
        if plan and float(plan["price_usd"]) > 0:
            stripe.InvoiceItem.create(
                customer=cust_id, currency="usd", amount=_usd_cents(float(plan["price_usd"])),
                description=f"SaaS Package — {plan['name']} ({plan['plan_code']})",
                metadata={"member": member, "plan_code": plan["plan_code"], "period_start": str(start), "period_end": str(end)}
            )


    # overages (Mode A)
    if limits and os.getenv("PUSH_INTERNAL_ITEMS_TO_STRIPE", "0").lower() in ("1", "true", "yes"):
        # BOL creates
        row_bolc = await database.fetch_one("""
          SELECT COUNT(*) AS c FROM bols WHERE created_at::date >= :s AND created_at::date < :e AND seller=:m
        """, {"s": start, "e": end, "m": member})
        bol_creates = int(row_bolc["c"] or 0)
        if bol_creates > int(limits["inc_bol_create"]):
            over = bol_creates - int(limits["inc_bol_create"])
            fee = over * float(limits["over_bol_create_usd"])
            stripe.InvoiceItem.create(customer=cust_id, currency="usd", amount=_usd_cents(fee),
              description=f"Overage: BOL creates ({over} over {int(limits['inc_bol_create'])})",
              metadata={"member": member, "metric":"bol_create_overage","qty_over":str(over)})

        # Delivered tons
        row_dtons = await database.fetch_one("""
          SELECT COALESCE(SUM(weight_tons),0) AS t FROM bols
           WHERE status ILIKE 'Delivered' AND delivery_time::date >= :s AND delivery_time::date < :e AND seller=:m
        """, {"s": start, "e": end, "m": member})
        dtons = float(row_dtons["t"] or 0.0)
        if dtons > float(limits["inc_bol_deliver_tons"]):
            over_t = dtons - float(limits["inc_bol_deliver_tons"])
            fee = over_t * float(limits["over_deliver_per_ton_usd"])
            stripe.InvoiceItem.create(customer=cust_id, currency="usd", amount=_usd_cents(fee),
              description=f"Overage: Delivered tons ({over_t:.2f} over {float(limits['inc_bol_deliver_tons']):.2f})",
              metadata={"member": member, "metric":"deliver_tons_overage","qty_over":f"{over_t:.2f}"})

        # Receipts
        row_rcp = await database.fetch_one("""
          SELECT COUNT(*) AS c FROM receipts WHERE created_at::date >= :s AND created_at::date < :e AND seller=:m
        """, {"s": start, "e": end, "m": member})
        receipts_c = int(row_rcp["c"] or 0)
        if receipts_c > int(limits["inc_receipts"]):
            over = receipts_c - int(limits["inc_receipts"])
            fee = over * float(limits["over_receipt_usd"])
            stripe.InvoiceItem.create(customer=cust_id, currency="usd", amount=_usd_cents(fee),
              description=f"Overage: Receipts ({over} over {int(limits['inc_receipts'])})",
              metadata={"member": member, "metric":"receipts_overage","qty_over":str(over)})

        # Warrant events (coarse)
        row_w = await database.fetch_one("""
          SELECT COUNT(*) AS c FROM warrants WHERE updated_at::date >= :s AND updated_at::date < :e AND holder=:m
        """, {"s": start, "e": end, "m": member})
        w_events = int(row_w["c"] or 0)
        if w_events > int(limits["inc_warrants"]):
            over = w_events - int(limits["inc_warrants"])
            fee = over * float(limits["over_warrant_usd"])
            stripe.InvoiceItem.create(customer=cust_id, currency="usd", amount=_usd_cents(fee),
              description=f"Overage: Warrant events ({over} over {int(limits['inc_warrants'])})",
              metadata={"member": member, "metric":"warrant_overage","qty_over":str(over)})

        # WebSocket messages (DISABLED: Stripe Meters handle WS billing)
        if os.getenv("BILL_INTERNAL_WS", "0").lower() in ("1","true","yes"):
            row_msg = await database.fetch_one("""
              SELECT COALESCE(SUM(count),0) AS c FROM data_msg_counters WHERE member=:m AND ts::date >= :s AND ts::date < :e
            """, {"m": member, "s": start, "e": end})
            msgs = int(row_msg["c"] or 0)
            if msgs > int(limits["inc_ws_msgs"]):
                over = msgs - int(limits["inc_ws_msgs"])
                fee = (over / 1_000_000.0) * float(limits["over_ws_per_million_usd"])
                stripe.InvoiceItem.create(customer=cust_id, currency="usd", amount=_usd_cents(fee),
                  description=f"Overage: Market data messages ({over} over {int(limits['inc_ws_msgs'])})",
                  metadata={"member": member, "metric":"ws_msgs_overage","qty_over":str(over)})


    # MMI true-up
    if trueup > 0:
        stripe.InvoiceItem.create(customer=cust_id, currency="usd", amount=_usd_cents(trueup),
          description=f"Minimum monthly invoice true-up ({start}–{end})",
          metadata={"member": member, "mmi": str(mmi)})

    inv = stripe.Invoice.create(
        customer=cust_id,
        collection_method="charge_automatically",
        auto_advance=True,
        metadata={"member": member, "period_start": str(start), "period_end": str(end), "bridge_invoice_id": invoice_id or ""}
    )
    inv = stripe.Invoice.finalize_invoice(inv.id)
    return {"stripe_invoice_id": inv.id}

    # Finalize (auto-advance will finalize and attempt payment; we can finalize now to trigger immediately)
    inv = stripe.Invoice.finalize_invoice(inv.id)
    return {"stripe_invoice_id": inv.id}

@app.post("/billing/pay/charge_now", tags=["Billing"], summary="Charge stored PM for an invoice (off-session)")
async def billing_charge_now(invoice_id: str = Body(..., embed=True)):
    inv = await _fetch_invoice(invoice_id)
    if not inv:
        raise HTTPException(404, "invoice not found")

    if str(inv["status"]).lower() == "paid":
        return {"ok": True, "already_paid": True}

    member = inv["member"]
    prof = await database.fetch_one("""
      SELECT stripe_customer_id, default_payment_method, has_default
      FROM billing_payment_profiles WHERE member=:m
    """, {"m": member})
    if not (prof and prof["has_default"] and prof["stripe_customer_id"] and prof["default_payment_method"]):
        raise HTTPException(402, "No default payment method on file for this member")

    amount = float(inv["total"])
    try:
        pi = stripe.PaymentIntent.create(
            amount=_usd_cents(amount),
            currency="usd",
            customer=prof["stripe_customer_id"],
            payment_method=prof["default_payment_method"],
            off_session=True,
            confirm=True,
            description=f"BRidge Invoice {invoice_id} — {member}",
            metadata={"invoice_id": invoice_id, "member": member}
        )
    except stripe.error.CardError as e:
        # authentication_required etc. → fall back to a hosted checkout if you want
        raise HTTPException(402, f"Payment failed: {getattr(e, 'user_message', str(e))}")
    except Exception as e:
        raise HTTPException(400, f"Error creating charge: {e}")

    if pi.status not in ("succeeded", "requires_capture"):
        # If you use capture later, handle requires_capture; else treat as failure
        raise HTTPException(400, f"Unexpected payment status: {pi.status}")

    # mark paid
    await database.execute("UPDATE billing_invoices SET status='paid' WHERE invoice_id=:i", {"i": invoice_id})

    # email human confirmation
    subject = f"Payment Received — Invoice {invoice_id}"
    html = f"""
    <div style="font-family:system-ui">
      <h2>BRidge Payment Confirmation</h2>
      <p><b>Invoice:</b> {invoice_id}<br/>
         <b>Member:</b> {member}<br/>
         <b>Period:</b> {inv['period_start']} → {inv['period_end']}<br/>
         <b>Total:</b> ${amount:,.2f} USD<br/>
         <b>Method:</b> Stored default (Stripe)<br/>
      </p>
    </div>"""
    await notify_humans("payment.confirmed", member=member, subject=subject, html=html,
                        cc_admin=True, ref_type="invoice", ref_id=invoice_id)

    return {"ok": True, "invoice_id": invoice_id, "status": "paid", "pi": pi.id}

@app.post("/billing/pm/change_session", tags=["Billing"], summary="Start PM change flow (mode=setup)")
async def pm_change_session(member: str = Body(..., embed=True)):
    row = await database.fetch_one("SELECT email, stripe_customer_id FROM billing_payment_profiles WHERE member=:m", {"m": member})
    if not row:
        raise HTTPException(404, "member not found in billing profiles; create a profile first")

    cust_id = row["stripe_customer_id"]
    email   = row["email"]

    session = stripe.checkout.Session.create(
        mode="setup",
        customer=cust_id,
        payment_method_types=["card", "us_bank_account"],
        success_url=f"{STRIPE_RETURN_BASE}/static/settings.html?pm=ok&member={stripe.util.utf8(member)}",
        cancel_url=f"{STRIPE_RETURN_BASE}/static/settings.html?pm=cancel&member={stripe.util.utf8(member)}",
        metadata={"member": member, "email": email}
    )
    return {"url": session.url}

@app.get("/billing/pm/details", tags=["Billing"])
async def pm_details(member: str):
    row = await database.fetch_one("SELECT stripe_customer_id, default_payment_method, has_default FROM billing_payment_profiles WHERE member=:m", {"m": member})
    if not (row and row["has_default"] and row["default_payment_method"]):
        return {"member": member, "has_default": False}
    pm = stripe.PaymentMethod.retrieve(row["default_payment_method"])
    masked = None
    if pm["type"] == "card":
        masked = f"Card •••• {pm['card']['last4']} ({pm['card']['brand']})"
    elif pm["type"] == "us_bank_account":
        masked = f"Bank •••• {pm['us_bank_account']['last4']} ({pm['us_bank_account']['bank_name']})"
    return {"member": member, "has_default": True, "label": masked, "type": pm["type"]}

@app.post("/admin/statements/run", tags=["Admin"], summary="Generate nightly statements ZIP for all members")
async def statements_run(as_of: date, request: Request):
    _require_admin(request)  # gate in production

    members = await _distinct_members(as_of)
    run_id = str(uuid.uuid4())
    await database.execute(
        "INSERT INTO statement_runs(run_id, as_of, member_count) VALUES (:id,:d,:n)",
        {"id": run_id, "d": as_of, "n": len(members)}
    )

    memfile = BytesIO()
    with zipfile.ZipFile(memfile, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for m in members:
            csv_text, pdf_bytes = await _build_member_statement(m, as_of)
            safe_m = "".join(ch if ch.isalnum() or ch in ("-","_",".") else "_" for ch in m)
            zf.writestr(f"{as_of}/statements/{safe_m}.csv", csv_text)
            zf.writestr(f"{as_of}/statements/{safe_m}.pdf", pdf_bytes)
    memfile.seek(0)

    headers = {"Content-Disposition": f'attachment; filename="bridge_statements_{as_of}.zip"'}
    return StreamingResponse(memfile, media_type="application/zip", headers=headers)

@app.get("/statements/{member}/{as_of}.pdf", tags=["Admin"], summary="Generate a single member statement PDF")
async def statement_single_pdf(member: str, as_of: date, request: Request):
    _require_admin(request)  # remove this line if you want it public
    _, pdf_bytes = await _build_member_statement(member, as_of)
    headers = {"Content-Disposition": f'attachment; filename="{member}_{as_of}.pdf"'}
    return StreamingResponse(BytesIO(pdf_bytes), media_type="application/pdf", headers=headers)

@app.on_event("shutdown")
async def shutdown_disconnect():
    try:
        await database.disconnect()
    except Exception:
        pass
# ==== /Quick user creation (admin-only bootstrap) ========================

# ======= AUTH ========
class LoginIn(BaseModel):
    username: str   # can be email OR username
    password: str

class LoginOut(BaseModel):
    ok: bool
    role: str
    redirect: str | None = None

@app.post("/login", tags=["Auth"], response_model=LoginOut, summary="Login with email or username")
@limiter.limit("5/minute")
async def login(request: Request):
    # Accept JSON or classic HTML form
    try:
        data = await request.json()
    except Exception:
        form = await request.form()
        data = {"username": (form.get("username") or ""), "password": (form.get("password") or "")}

    body = LoginIn(**data)
    ident = (body.username or "").strip().lower()
    pwd   = body.password or ""

    row = await database.fetch_one(
        """
        SELECT
        COALESCE(username, '') AS username,
        COALESCE(email, '')    AS email,
        role
        FROM public.users
        WHERE (LOWER(COALESCE(email, '')) = :ident OR LOWER(COALESCE(username, '')) = :ident)
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

    request.session.clear()
    request.session["username"] = (row["username"] or row["email"])
    request.session["role"] = role

    return LoginOut(ok=True, role=role, redirect=f"/{role}")
# ======= AUTH ========

@app.get("/me", tags=["Auth"])
async def me(request: Request):
    return {
        "username": (request.session.get("username") or "Guest"),
        "role": (request.session.get("role") or "")
    }

# -------- Compliance: KYC/AML flags + recordkeeping toggle --------
@app.on_event("startup")
async def _ensure_compliance_schema():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS compliance_members(
      username TEXT PRIMARY KEY,
      kyc_passed BOOLEAN NOT NULL DEFAULT FALSE,
      aml_passed BOOLEAN NOT NULL DEFAULT FALSE,
      bsa_risk TEXT NOT NULL DEFAULT 'low',
      sanctions_screened BOOLEAN NOT NULL DEFAULT FALSE,
      boi_collected BOOLEAN NOT NULL DEFAULT FALSE,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS retention_policies(
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)

@app.post("/admin/compliance/member/set", tags=["Admin"])
async def compliance_member_set(username: str, kyc: bool=False, aml: bool=False,
                                sanctions: bool=False, boi: bool=False, bsa_risk: str="low",
                                request: Request=None):
    _require_admin(request)
    await database.execute("""
      INSERT INTO compliance_members(username,kyc_passed,aml_passed,sanctions_screened,boi_collected,bsa_risk)
      VALUES (:u,:k,:a,:s,:b,:r)
      ON CONFLICT (username) DO UPDATE SET
        kyc_passed=EXCLUDED.kyc_passed,
        aml_passed=EXCLUDED.aml_passed,
        sanctions_screened=EXCLUDED.sanctions_screened,
        boi_collected=EXCLUDED.boi_collected,
        bsa_risk=EXCLUDED.bsa_risk,
        updated_at=NOW()
    """, {"u":username,"k":kyc,"a":aml,"s":sanctions,"b":boi,"r":bsa_risk})
    return {"ok": True, "user": username}
# -------- Compliance: KYC/AML flags + recordkeeping toggle --------

# # ======= fee schedule ========================      
@app.get("/legal/fees", tags=["Legal"], summary="BRidge Fee Schedule")
async def fees_doc_alias():
    # Use the shared static-or-placeholder helper to serve the fees page
    return _static_or_placeholder("legal/fees.html", "BRidge Fee Schedule")
# ======= fee schedule ========================

# ======= Member plan ========================
@app.post("/admin/member/plan/set", tags=["Admin"], summary="Assign plan to member")
async def set_member_plan(member: str, plan_code: str, request: Request=None):
    _require_admin(request)
    # validate plan exists & active
    p = await database.fetch_one("SELECT 1 FROM billing_plans WHERE plan_code=:c AND active=TRUE", {"c": plan_code})
    if not p: raise HTTPException(404, "plan not found or inactive")
    await database.execute("""
      INSERT INTO member_plans(member,plan_code,effective_date,updated_at)
      VALUES (:m,:c,CURRENT_DATE,NOW())
      ON CONFLICT (member) DO UPDATE SET plan_code=:c, updated_at=NOW()
    """, {"m": member, "c": plan_code})
    return {"ok": True, "member": member, "plan": plan_code}
# ======= Member plan ========================

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

# --- Canonical contracts export (maps raw cols -> your header set) ---
CONTRACTS_EXPORT_SQL = """
SELECT
  c.id,
  c.buyer,
  c.seller,
  c.material,
  /* sku -> use material as the SKU for now */
  c.material AS sku,
  c.weight_tons,
  c.price_per_ton,
  /* total_value -> compute */
  (c.price_per_ton * c.weight_tons) AS total_value,
  c.status,
  /* contract_date -> created_at */
  c.created_at AS contract_date,
  /* pickup_time/delivery_time -> from linked BOLs */
  (SELECT MIN(b.pickup_time)   FROM bols b WHERE b.contract_id = c.id) AS pickup_time,
  (SELECT MAX(b.delivery_time) FROM bols b WHERE b.contract_id = c.id) AS delivery_time,
  COALESCE(c.currency, 'USD') AS currency
FROM contracts c
ORDER BY c.created_at DESC NULLS LAST, c.id DESC
"""

# --- ZIP export (all core data) ---
from fastapi.responses import StreamingResponse
import zipfile, io, csv
from sqlalchemy import text as _sqltext

@app.get("/admin/export_all", tags=["Admin"], summary="Download ZIP of all CSVs")
def admin_export_all():
    
    exports = {
        "contracts.csv": CONTRACTS_EXPORT_SQL,

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

# -------- DR: snapshot self-verify & RTO/RPO exposure --------
@app.get("/admin/dr/objectives", tags=["Admin"])
def dr_objectives():
    return {"RTO_seconds": 900, "RPO_seconds": 60, "note": "Targets; enforce via ops runbook."}

@app.get("/admin/backup/selfcheck", tags=["Admin"])
def backup_selfcheck():
    with engine.begin() as conn:
        counts = {}
        for tbl in ["contracts","bols","inventory_items","orders","trades","audit_events","settlements"]:
            try:
                counts[tbl] = conn.execute(_sqltext(f"SELECT COUNT(*) c FROM {tbl}")).scalar()
            except Exception:
                counts[tbl] = None
        return {"ok": True, "table_counts": counts}
# -------- DR: snapshot self-verify & RTO/RPO exposure --------

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

async def _ensure_futures_tables_if_missing():
    try:
        # this will raise if the table doesn't exist yet
        await database.fetch_one("SELECT 1 FROM futures_products LIMIT 1")
    except Exception:
        # run the bootstrap once (safe to call repeatedly)
        await _ensure_futures_schema()
# ===== /FUTURES schema bootstrap =====

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

@app.on_event("startup")
async def _ensure_eventlog_schema():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS matching_events(
      id BIGSERIAL PRIMARY KEY,
      topic TEXT NOT NULL,        -- 'ORDER','CANCEL','MODIFY'
      payload JSONB NOT NULL,
      enqueued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      processed_at TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS idx_matching_events_unproc ON matching_events(processed_at) WHERE processed_at IS NULL;
    """)

# ===== TRADING HARDENING: risk limits, audit, trading status =====
@app.on_event("startup")
async def _ensure_trading_hardening():
    # 1) columns / tables (safe to run every boot)
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
        # split the column add from the named constraint add
        "ALTER TABLE futures_listings ADD COLUMN IF NOT EXISTS trading_status TEXT NOT NULL DEFAULT 'Trading';",
    ]
    for stmt in ddl:
        try:
            await database.execute(stmt)
        except Exception as e:
            logger.warn("trading_hardening_bootstrap_failed", err=str(e), sql=stmt[:120])

    # 2) add the named constraint with a guard (don’t fail if it already exists)
    try:
        await database.execute(
            "ALTER TABLE futures_listings "
            "ADD CONSTRAINT chk_trading_status "
            "CHECK (trading_status IN ('Trading','Halted','Expired'))"
        )
    except Exception:
        # Already exists (or other benign condition) — ignore
        pass


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
# ===== INVENTORY schema bootstrap (idempotent) =====

# ------ RECEIPTS schema bootstrap (idempotent) =====
@app.on_event("startup")
async def _ensure_receipts_schema():
    ddl = """
CREATE TABLE IF NOT EXISTS public.receipts (
  receipt_id      UUID PRIMARY KEY,
  seller          TEXT,
  sku             TEXT,
  qty_tons        NUMERIC,
  status          TEXT NOT NULL DEFAULT 'created',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  consumed_at     TIMESTAMPTZ,
  consumed_bol_id UUID,
  provenance      JSONB NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE public.receipts ADD COLUMN IF NOT EXISTS qty_tons NUMERIC;
ALTER TABLE public.receipts ADD COLUMN IF NOT EXISTS sku TEXT;
ALTER TABLE public.receipts ADD COLUMN IF NOT EXISTS symbol    TEXT;
ALTER TABLE public.receipts ADD COLUMN IF NOT EXISTS location  TEXT;
ALTER TABLE public.receipts ADD COLUMN IF NOT EXISTS qty_lots  NUMERIC;
ALTER TABLE public.receipts ADD COLUMN IF NOT EXISTS lot_size  NUMERIC;

CREATE OR REPLACE VIEW public.receipts_live AS
  SELECT * FROM public.receipts
  WHERE consumed_at IS NULL AND status IN ('created','pledged');
"""
    try:
        await run_ddl_multi(ddl)
    except Exception as e:
        logger.warn("receipts_bootstrap_failed", err=str(e))
# ===== RECEIPTS schema bootstrap (idempotent) =====


@app.on_event("startup")
async def _receipts_backfill_once():
    try:
        # set symbol from sku where missing
        await database.execute("""
            UPDATE public.receipts
               SET symbol = COALESCE(symbol, sku)
             WHERE (symbol IS NULL OR symbol = '')
        """)
        # default lot_size to 20 tons where missing
        await database.execute("""
            UPDATE public.receipts
               SET lot_size = 20.0
             WHERE lot_size IS NULL
        """)
        # compute qty_lots from qty_tons when missing (tons -> pounds -> lots)
        await database.execute("""
            UPDATE public.receipts
               SET qty_lots = (COALESCE(qty_tons,0) * 2000.0) / NULLIF(lot_size,0)
             WHERE (qty_lots IS NULL) AND qty_tons IS NOT NULL AND lot_size IS NOT NULL
        """)
    except Exception as e:
        logger.warn("receipts_backfill_failed", err=str(e))

@app.on_event("startup")
async def _receipts_backfill_columns():
    """
    Idempotent backfill for receipts so /stocks/snapshot can aggregate.
    Convention: lot_size is in TONS per lot (e.g., 20.0).
    """
    try:
        # 0) Ensure lot_size default (TONS per lot)
        await database.execute("""
            UPDATE public.receipts
               SET lot_size = 20.0
             WHERE lot_size IS NULL
        """)

        # 1) Ensure symbol (default to sku)
        await database.execute("""
            UPDATE public.receipts
               SET symbol = sku
             WHERE (symbol IS NULL OR symbol = '')
               AND sku IS NOT NULL
        """)

        # 2) Normalize location
        await database.execute("""
            UPDATE public.receipts
               SET location = COALESCE(NULLIF(location,''), 'UNKNOWN')
             WHERE location IS NULL OR location = ''
        """)

        # 3) If qty_tons is missing but we have qty_lots and lot_size → derive tons
        await database.execute("""
            UPDATE public.receipts
               SET qty_tons = qty_lots * lot_size
             WHERE qty_tons IS NULL
               AND qty_lots IS NOT NULL
               AND lot_size IS NOT NULL
        """)

        # 4) If qty_lots is missing but we have qty_tons and lot_size → derive lots
        await database.execute("""
            UPDATE public.receipts
               SET qty_lots = CASE
                                WHEN lot_size IS NOT NULL AND lot_size > 0
                                  THEN qty_tons / lot_size
                                ELSE NULL
                              END
             WHERE qty_lots IS NULL
               AND qty_tons IS NOT NULL
               AND lot_size IS NOT NULL
        """)
    except Exception as e:
        logger.warn("receipts_backfill_failed", err=str(e))


@app.on_event("startup")
async def _ensure_stocks_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS public.locations(
      location TEXT PRIMARY KEY,
      name TEXT,
      city TEXT,
      region TEXT,
      storage_fee_usd_per_ton_day NUMERIC DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS public.stocks_daily(
      as_of DATE NOT NULL,
      symbol TEXT NOT NULL,
      location TEXT NOT NULL,
      qty_lots NUMERIC NOT NULL,
      lot_size NUMERIC NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (as_of, symbol, location)
    );
    """
    try:
        await run_ddl_multi(ddl)
    except Exception as e:
        logger.warn("stocks_schema", err=str(e))

# -------- Warranting & Chain of Title --------
@app.on_event("startup")
async def _ensure_warrant_schema():
    # 1) Create table without FK (order-safe)
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS public.warrants(
      warrant_id UUID PRIMARY KEY,
      receipt_id UUID NOT NULL,
      status     TEXT NOT NULL DEFAULT 'on_warrant', -- on_warrant | off_warrant | pledged
      holder     TEXT NOT NULL,                      -- member/beneficial owner
      pledged_to TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)
    # 2) Add FK separately, guarded (idempotent-ish)
    try:
        await database.execute("""
          ALTER TABLE public.warrants
          ADD CONSTRAINT fk_warrants_receipt
          FOREIGN KEY (receipt_id)
          REFERENCES public.receipts(receipt_id)
          ON DELETE CASCADE
        """)
    except Exception:
        # already exists or receipts not ready yet in this boot — safe to ignore
        pass

class WarrantIn(BaseModel):
    receipt_id: str
    holder: str

@app.post("/warrants/mint", tags=["Warehousing"])
async def warrant_mint(w: WarrantIn):
    wid = str(uuid.uuid4())
    await database.execute("""
      INSERT INTO public.warrants(warrant_id,receipt_id,holder)
      VALUES (:w,:r,:h)
    """, {"w": wid, "r": w.receipt_id, "h": w.holder})
    return {"warrant_id": wid, "status": "on_warrant"}

@app.post("/warrants/transfer", tags=["Warehousing"])
async def warrant_transfer(warrant_id: str, new_holder: str):
    await database.execute("""
      UPDATE public.warrants
         SET holder=:h, updated_at=NOW()
       WHERE warrant_id=:w AND status <> 'pledged'
    """, {"w": warrant_id, "h": new_holder})
    return {"ok": True}

@app.post("/warrants/pledge", tags=["Warehousing"])
async def warrant_pledge(warrant_id: str, lender: str):
    await database.execute("""
      UPDATE public.warrants
         SET status='pledged', pledged_to=:p, updated_at=NOW()
       WHERE warrant_id=:w
    """, {"w": warrant_id, "p": lender})
    return {"ok": True}

@app.post("/warrants/release", tags=["Warehousing"])
async def warrant_release(warrant_id: str):
    await database.execute("""
      UPDATE public.warrants
         SET status='on_warrant', pledged_to=NULL, updated_at=NOW()
       WHERE warrant_id=:w
    """, {"w": warrant_id})
    return {"ok": True}
# -------- /Warranting & Chain of Title --------


# ------ RECEIVABLES schema bootstrap (idempotent) =====
@app.on_event("startup")
async def _ensure_receivables_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS public.receivables (
      id UUID PRIMARY KEY,
      receipt_id UUID NOT NULL,
      face_value_usd NUMERIC NOT NULL,
      due_date DATE NOT NULL,
      debtor TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'open',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_receivables_receipt ON public.receivables (receipt_id);
    """
    try:
        await run_ddl_multi(ddl)
    except Exception as e:
        logger.warn("receivables_bootstrap_failed", err=str(e))
# ===== Receivables schema bootstrap (idempotent) =====

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
    k_norm = k.upper()

    cur = await database.fetch_one(
        "SELECT qty_on_hand FROM inventory_items WHERE LOWER(seller)=LOWER(:s) AND LOWER(sku)=LOWER(:k) FOR UPDATE",
        {"s": s, "k": k_norm}
    )
    if cur is None:
        await database.execute("""
          INSERT INTO inventory_items (seller, sku, description, uom, location,
                                       qty_on_hand, qty_reserved, qty_committed, source, updated_at)
          VALUES (:s,:k,:d,:u,:loc,0,0,0,:src,NOW())
          ON CONFLICT (seller, sku) DO NOTHING
        """, {"s": s, "k": k_norm, "d": description, "u": (uom or "ton"),
                "loc": location, "src": source or "manual"}
        )
        cur = await database.fetch_one(
            "SELECT qty_on_hand FROM inventory_items WHERE LOWER(seller)=LOWER(:s) AND LOWER(sku)=LOWER(:k) FOR UPDATE",
            {"s": s, "k": k_norm}
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
       WHERE LOWER(seller)=LOWER(:s) AND LOWER(sku)=LOWER(:k)
    """, {"new": new_qty, "u": (uom or "ton"), "loc": location, "desc": description,
          "src": (source or "manual"), "s": s, "k": k_norm})

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
            "timestamp": utcnow().isoformat(),
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
    q = "SELECT * FROM inventory_available WHERE LOWER(seller) = LOWER(:seller)"
    vals = {"seller": seller}
    if sku:
        q += " AND LOWER(sku) = LOWER(:sku)"
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
    q = "SELECT * FROM inventory_movements WHERE LOWER(seller)=LOWER(:seller)"
    vals = {"seller": seller, "limit": limit, "offset": offset}
    if sku:
        q += " AND LOWER(sku)=LOWER(:sku)"; vals["sku"] = sku.upper()
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
        # contracts (base shape)
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
        
        # idempotent key for imports/backfills
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS dedupe_key TEXT;",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_contracts_dedupe ON contracts(dedupe_key);",

        # extend contracts with pricing fields used by create_contract()
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS pricing_formula     TEXT;",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS reference_symbol    TEXT;",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS reference_price     NUMERIC;",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS reference_source    TEXT;",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS reference_timestamp TIMESTAMPTZ;",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS currency            TEXT DEFAULT 'USD';",

        # bols (base shape)
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

        "CREATE INDEX IF NOT EXISTS idx_bols_contract     ON bols(contract_id);",
        "CREATE INDEX IF NOT EXISTS idx_bols_pickup_time  ON bols(pickup_time DESC);",

        # export/compliance fields (idempotent)
        "ALTER TABLE bols ADD COLUMN IF NOT EXISTS origin_country TEXT;",
        "ALTER TABLE bols ADD COLUMN IF NOT EXISTS destination_country TEXT;",
        "ALTER TABLE bols ADD COLUMN IF NOT EXISTS port_code TEXT;",
        "ALTER TABLE bols ADD COLUMN IF NOT EXISTS hs_code TEXT;",
        "ALTER TABLE bols ADD COLUMN IF NOT EXISTS duty_usd NUMERIC;",
        "ALTER TABLE bols ADD COLUMN IF NOT EXISTS tax_pct NUMERIC;",
    ]

    for s in ddl:
        try:
            await database.execute(s)
        except Exception as e:
            logger.warn("contracts_bols_bootstrap_failed", sql=s[:100], err=str(e))
# ===== /CONTRACTS / BOLS schema bootstrap =====

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
# ----------- /schema bootstrap -----------

# ===== AUDIT CHAIN schema bootstrap (idempotent) =====
@app.on_event("startup")
async def _ensure_audit_chain_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS audit_events (
      chain_date DATE NOT NULL,
      seq        BIGINT NOT NULL,
      actor      TEXT,
      action     TEXT NOT NULL,
      entity_type TEXT NOT NULL,
      entity_id   TEXT NOT NULL,
      payload     JSONB NOT NULL,
      prev_hash   TEXT NOT NULL DEFAULT '',
      event_hash  TEXT NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (chain_date, seq)
    );
    CREATE INDEX IF NOT EXISTS idx_audit_events_entity ON audit_events(entity_type, entity_id, chain_date, seq);
    """
    try:
        await run_ddl_multi(ddl)
    except Exception as e:
        logger.warn("audit_chain_bootstrap_failed", err=str(e))

@app.on_event("startup")
async def _ensure_audit_seal_schema():
    ddl = [
        "ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS sealed BOOLEAN NOT NULL DEFAULT FALSE;",
        """
        CREATE TABLE IF NOT EXISTS audit_seals (
          chain_date DATE PRIMARY KEY,
          final_hash TEXT NOT NULL,
          sealed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    ]
    for s in ddl:
        try:
            await database.execute(s)
        except Exception as e:
            logger.warn("audit_seal_bootstrap_failed", err=str(e), sql=s[:120])

# -------- AUDIT CHAIN /schema bootstrap --------

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
    _harvester_guard()
    key = _idem_key(request)
    hit = await idem_get(key) if key else None
    if hit: return hit   
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

    resp = {"ok": True, "upserted": upserted}              # bulk_upsert
    return await _idem_guard(request, key, resp)

async def idem_get(key: str):
    if not key: return None
    row = await database.fetch_one("SELECT response FROM http_idempotency WHERE key=:k", {"k": key})
    return (row and row["response"])

# Legacy helper kept for backward compatibility; prefer `_idem_guard(...)` in handlers.
async def idem_put(key: str, resp: dict):
    if not key: return resp
    try:
        await database.execute(
            "INSERT INTO http_idempotency(key,response) VALUES (:k,:r::jsonb) ON CONFLICT DO NOTHING",
            {"k": key, "r": json.dumps(resp, default=str)}
        )
    except Exception:
        pass
    return resp

# -------- Inventory: Manual Add/Set (gated + unit-aware) --------
@app.post(
    "/inventory/manual_add",
    tags=["Inventory"],
    summary="Manual add/set qty_on_hand for a SKU (absolute set, unit-aware)",
    response_model=dict,
    status_code=200
)
@limiter.limit("60/minute")
async def inventory_manual_add(payload: dict, request: Request, _=Depends(csrf_protect)):
    key = _idem_key(request)
    if key and key in _idem_cache:
        return _idem_cache[key]   
    if _require_hmac_in_this_env() and not _is_admin_or_seller(request):
        raise HTTPException(401, "login required")

    seller = (payload.get("seller") or "").strip()
    sku    = (payload.get("sku") or "").strip().upper()
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
    resp = {"ok": True, "seller": seller, "sku": sku, "from": old, "to": new_qty, "delta": delta, "uom": "ton"}
    return await _idem_guard(request, key, resp)           # manual_add

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
@app.post("/inventory/import/csv", tags=["Inventory"], summary="Import CSV (absolute set, unit-aware)", response_model=None)
@limiter.limit("30/minute")
async def inventory_import_csv(
    file: Annotated[UploadFile, File(...)],
    seller: Optional[str] = Form(None),
    request: Request = None,
    _=Depends(csrf_protect)
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
@app.post("/inventory/import/excel", tags=["Inventory"], summary="Import XLSX (absolute set, unit-aware)", response_model=None)
@limiter.limit("15/minute")
async def inventory_import_excel(
    file: Annotated[UploadFile, File(...)],
    seller: Optional[str] = Form(None),
    request: Request = None,
    _=Depends(csrf_protect)
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

# ----------- Startup tasks -----------
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
        "CREATE INDEX IF NOT EXISTS idx_inventory_items_norm ON inventory_items (LOWER(seller), LOWER(sku))",
        "CREATE INDEX IF NOT EXISTS idx_inventory_movements_norm ON inventory_movements (LOWER(seller), LOWER(sku), created_at DESC)"
    ]
    for s in ddl:
        try:
            await database.execute(s)
        except Exception as e:
            logger.warn('index_bootstrap_failed', sql=s[:80], err=str(e))

# DB safety checks (non-negative inventory quantities)
@app.on_event("startup")
async def _ensure_inventory_constraints():
    try:
        await database.execute(
            "ALTER TABLE inventory_items ADD CONSTRAINT chk_qty_on_hand_nonneg CHECK (qty_on_hand >= 0)"
        )
    except Exception:
        pass  # already exists

    try:
        await database.execute(
            "ALTER TABLE inventory_items ADD CONSTRAINT chk_qty_reserved_nonneg CHECK (qty_reserved >= 0)"
        )
    except Exception:
        pass  # already exists

    try:
        await database.execute(
            "ALTER TABLE inventory_items ADD CONSTRAINT chk_qty_committed_nonneg CHECK (qty_committed >= 0)"
        )
    except Exception:
        pass  # already exists

#-------- Dead Letter Startup --------
@app.on_event("startup")
async def _ensure_dead_letters():
    try:
        await run_ddl_multi("""
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

# ------- Webhook DLQ replayer -------
@limiter.limit("30/minute")
@app.post("/admin/webhooks/replay", tags=["Admin"])
async def webhook_replay(request: Request, limit: int = 100):
    _require_admin(request)  # gate in production
    rows = await database.fetch_all(
        """
        SELECT id, event, payload
        FROM webhook_dead_letters
        ORDER BY created_at ASC
        LIMIT :l
        """,
        {"l": limit},
    )
    ok = fail = 0
    for r in rows:
        res = await emit_event(r["event"], dict(r["payload"]))
        if res.get("ok"):
            ok += 1
            await database.execute(
                "DELETE FROM webhook_dead_letters WHERE id=:i", {"i": r["id"]}
            )
        else:
            fail += 1
    return {"replayed_ok": ok, "failed": fail}
# ------- Webhook DLQ replayer -------


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

#------- RFQs -------
@app.on_event("startup")
async def _ensure_rfq_schema():
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS rfqs (
          rfq_id UUID PRIMARY KEY,
          symbol TEXT NOT NULL,             -- use symbol_root like 'CU-SHRED-1M'
          side TEXT NOT NULL CHECK (side IN ('buy','sell')),
          quantity_lots NUMERIC NOT NULL,
          price_limit NUMERIC,
          expires_at TIMESTAMPTZ NOT NULL,
          status TEXT NOT NULL DEFAULT 'open',
          creator TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS rfq_quotes (
          quote_id UUID PRIMARY KEY,
          rfq_id UUID NOT NULL REFERENCES rfqs(rfq_id) ON DELETE CASCADE,
          responder TEXT NOT NULL,
          price NUMERIC NOT NULL,
          qty_lots NUMERIC NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS rfq_deals (
          deal_id UUID PRIMARY KEY,
          rfq_id UUID NOT NULL REFERENCES rfqs(rfq_id) ON DELETE CASCADE,
          quote_id UUID NOT NULL REFERENCES rfq_quotes(quote_id) ON DELETE CASCADE,
          symbol TEXT NOT NULL,
          price NUMERIC NOT NULL,
          qty_lots NUMERIC NOT NULL,
          buy_owner TEXT NOT NULL,
          sell_owner TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
    ]
    for s in ddl:
        try:
            await database.execute(s)
        except Exception as e:
            logger.warn("rfq_schema_bootstrap_failed", err=str(e), sql=s[:90])
#------- RFQs -------

# ------ Contracts refs index ------
@app.on_event("startup")
async def _idx_refs():
    try:
        await database.execute("CREATE INDEX IF NOT EXISTS idx_contracts_ref ON contracts(reference_source, reference_symbol)")
    except Exception:
        pass
# ------ Contracts refs index ------

# ------ Statements router ------
@app.on_event("startup")
async def _ensure_statements_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS statement_runs (
      run_id UUID PRIMARY KEY,
      as_of DATE NOT NULL,
      member_count INT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    try:
        await database.execute(ddl)
    except Exception as e:
        logger.warn("statements_schema_bootstrap_failed", err=str(e))
# ------ Statements router ------

# ------ CLOB router ------
@app.on_event("startup")
async def _ensure_clob_schema():
    ddl = [
        # simple symbol-level order book (separate from futures_* tables)
        """
        CREATE TABLE IF NOT EXISTS clob_orders (
          order_id UUID PRIMARY KEY,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL CHECK (side IN ('buy','sell')),
          price NUMERIC NOT NULL,
          qty_lots NUMERIC NOT NULL,
          qty_open NUMERIC NOT NULL,
          owner TEXT NOT NULL,
          tif TEXT NOT NULL CHECK (tif IN ('day','ioc','fok')),
          status TEXT NOT NULL DEFAULT 'open',
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS clob_trades (
          trade_id UUID PRIMARY KEY,
          symbol TEXT NOT NULL,
          price NUMERIC NOT NULL,
          qty_lots NUMERIC NOT NULL,
          buy_owner TEXT NOT NULL,
          sell_owner TEXT NOT NULL,
          maker_order UUID,
          taker_order UUID,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS md_ticks (
          seq BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
          symbol TEXT NOT NULL,
          price NUMERIC NOT NULL,
          qty_lots NUMERIC NOT NULL,
          kind TEXT NOT NULL,
          ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS clob_positions (
          owner TEXT NOT NULL,
          symbol TEXT NOT NULL,
          qty_lots NUMERIC NOT NULL DEFAULT 0,
          PRIMARY KEY (owner, symbol)
        );
        """,
    ]
    for s in ddl:
        try:
            await database.execute(s)
        except Exception as e:
            logger.warn("clob_schema_bootstrap_failed", err=str(e), sql=s[:120])
# ------ CLOB router ------

# -------- CORS --------
ALLOWED_ORIGINS = [
    "https://scrapfutures.com",
    "https://www.scrapfutures.com",
    "https://bridge.scrapfutures.com",
    "https://bridge-buyer.onrender.com",
    "https://indices.scrapfutures.com",
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
    "indices.scrapfutures.com",
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

class ReceiptProvenance(BaseModel):
    truck_id: Optional[str] = None
    scale_in_lbs: Optional[float] = None
    scale_out_lbs: Optional[float] = None
    gps_polyline: Optional[list[list[float]]] = None  # [[lat,lon], ...]
    photos: Optional[list[str]] = None                 # URLs/ids

class ProductIn(BaseModel):
    symbol: str
    description: str
    unit: str
    quality: Optional[dict] = {}

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

# -------- Contracts & BOLs --------

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
# ----- Idempotency helper -----
def _idem_key(request: Request) -> Optional[str]:
    return request.headers.get("Idempotency-Key") or request.headers.get("X-Idempotency-Key")

async def _idem_guard(request: Request, key: Optional[str], resp: dict):
    if key:
        _idem_cache[key] = resp
        try:
            await idem_put(key, resp)  # persist to http_idempotency so idem_get() works
        except Exception:
            pass
    return resp
# ===== Idempotency cache for POST/Inventory/Purchase =====

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
# ------------------ Hash-chained audit events (per-day chain) ---------
async def audit_append(actor: str, action: str, entity_type: str, entity_id: str, payload: dict, chain_date: Optional[date]=None):
    cd = chain_date or _utc_date_now()
    # last link
    last = await database.fetch_one("""
        SELECT seq, event_hash FROM audit_events
        WHERE chain_date=:d ORDER BY seq DESC LIMIT 1
    """, {"d": cd})
    prev_seq = int(last["seq"]) if last else 0
    prev_hash = last["event_hash"] if last else ""
    seq = prev_seq + 1

    body = {
        "chain_date": str(cd),
        "seq": seq,
        "actor": actor,
        "action": action,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "payload": payload,
    }
    event_hash = _sha256_hex((prev_hash or "") + _canon_json(body))
    await database.execute("""
        INSERT INTO audit_events(chain_date,seq,actor,action,entity_type,entity_id,payload,prev_hash,event_hash)
        VALUES (:d,:seq,:actor,:action,:etype,:eid,:payload::jsonb,:prev,:eh)
    """, {"d": cd, "seq": seq, "actor": actor, "action": action, "etype": entity_type, "eid": entity_id,
          "payload": json.dumps(payload), "prev": prev_hash, "eh": event_hash})
    return {"chain_date": str(cd), "seq": seq, "event_hash": event_hash}
# ------------------- Hash-chained audit events (per-day chain) ----

# --------- AUDIT Logging (safe models + guarded) ---------------------
from pydantic import BaseModel
from fastapi import Body

class AuditAppendIn(BaseModel):
    actor: str = "system"
    action: str = "note"
    entity_type: str = ""
    entity_id: str = ""
    payload: dict = {}

@app.post("/admin/audit/log", tags=["Admin"], summary="Append audit event")
async def admin_audit_log(body: AuditAppendIn = Body(...), request: Request = None):
    # Only enforce admin in production
    if os.getenv("ENV","").lower()=="production":
        _require_admin(request)

    try:
        await audit_append(
            body.actor, body.action, body.entity_type, body.entity_id, body.payload
        )
        return {"ok": True}
    except Exception as e:
        # dev-safe: never 500 here
        try:
            logger.warn("audit_append_failed", err=str(e))
        except Exception:
            pass
        return {"ok": True, "skipped": True}

class AuditSealIn(BaseModel):
    chain_date: Optional[date] = None

@app.post("/admin/audit/seal", tags=["Admin"], summary="Seal a day's audit chain")
async def admin_audit_seal(request: Request, body: Optional[AuditSealIn] = None):
    _require_admin(request)
    try:
        cd = (body.chain_date if (body and body.chain_date) else _utc_date_now())
        tail = await database.fetch_one("""
            SELECT seq, event_hash FROM audit_events
            WHERE chain_date=:d ORDER BY seq DESC LIMIT 1
        """, {"d": cd})
        final_hash = tail["event_hash"] if tail else _sha256_hex(f"empty:{cd}")
        await database.execute("UPDATE audit_events SET sealed=TRUE WHERE chain_date=:d", {"d": cd})
        await database.execute("""
            INSERT INTO audit_seals(chain_date, final_hash)
            VALUES (:d, :h)
            ON CONFLICT (chain_date) DO UPDATE
              SET final_hash=EXCLUDED.final_hash, sealed_at=now()
        """, {"d": cd, "h": final_hash})
        return {"chain_date": str(cd), "final_hash": final_hash}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "audit_seal_failed", "detail": str(e)})

@app.get("/admin/audit/verify", tags=["Admin"], summary="Recompute and verify audit chain")
async def admin_audit_verify(request: Request, chain_date: date):
    _require_admin(request)
    try:
        rows = await database.fetch_all("""
            SELECT seq, actor, action, entity_type, entity_id, payload, prev_hash, event_hash
            FROM audit_events
            WHERE chain_date=:d
            ORDER BY seq ASC
        """, {"d": chain_date})
        prev = ""
        for r in rows:
            body = {
                "chain_date": str(chain_date),
                "seq": int(r["seq"]),
                "actor": r["actor"],
                "action": r["action"],
                "entity_type": r["entity_type"],
                "entity_id": r["entity_id"],
                "payload": r["payload"],
            }
            expect = _sha256_hex((prev or "") + _canon_json(body))
            if expect != r["event_hash"]:
                return {"ok": False, "seq": int(r["seq"]), "expected": expect, "got": r["event_hash"]}
            prev = r["event_hash"]
        seal = await database.fetch_one("SELECT final_hash FROM audit_seals WHERE chain_date=:d", {"d": chain_date})
        final_hash = prev or _sha256_hex(f"empty:{chain_date}")
        return {"ok": bool(seal and seal["final_hash"] == final_hash), "final_hash": final_hash}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "audit_verify_failed", "detail": str(e)})
# ---------- AUDIT logging --------------------------------

# ===== ICE webhook signature verifier =====
ICE_LINK_SIGNING_SECRET = os.getenv("LINK_SIGNING_SECRET", "")

def verify_sig_ice(raw_body: bytes, given_sig: str) -> bool:
    """Verify ICE webhook signatures. Pass-through if no secret is set (dev/CI)."""
    if not ICE_LINK_SIGNING_SECRET:  # dev/staging/CI mode
        return True
    mac = hmac.new(ICE_LINK_SIGNING_SECRET.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, given_sig)

@app.post("/ice-digital-trade", tags=["ICE"], summary="ICE Digital Trade webhook (stub)")
async def ice_trade_webhook(request: Request):
    sig = request.headers.get("X-Signature", "")
    body = await request.body()
    if not verify_sig_ice(body, sig):
        raise HTTPException(status_code=401, detail="Invalid signature")
    # TODO: parse/process payload here (idempotency, persistence, audit)
    return {"ok": True}

@app.post("/ice/webhook", tags=["Integrations"], summary="ICE DT webhook (HMAC)")
async def ice_webhook(request: Request, x_signature: str = Header(None, alias="X-Signature")):
    body = await request.body()
    secret = os.getenv("ICE_WEBHOOK_SECRET", "")
    mac = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(mac, x_signature or ""):
        raise HTTPException(401, "Bad signature")
    try:
        payload = json.loads(body.decode())
    except Exception:
        raise HTTPException(400, "Invalid JSON")
    # best-effort audit
    try:
        await audit_append("ice", "webhook.recv", "ice_dt", "", payload)
    except Exception:
        pass
    return {"ok": True}
# ---------- Ice webhook signature verifier -----

# === QBO OAuth Relay • Endpoints  ===
from fastapi import Query

@app.get("/qbo/callback", include_in_schema=False)
async def qbo_callback(code: str = Query(...), state: str = Query(...), realmId: str = Query(...)):
    """
    Public endpoint hit by Intuit after user approves OAuth.
    Writes (state, code, realmId) into qbo_oauth_events for the local harvester to fetch.
    """
    # Basic sanity check on state
    if not state or len(state) < 8:
        raise HTTPException(status_code=400, detail="invalid state")

    # idempotent upsert on state
    await database.execute("""
        INSERT INTO qbo_oauth_events(state, code, realm_id)
        VALUES (:s, :c, :r)
        ON CONFLICT (state) DO UPDATE SET code=EXCLUDED.code, realm_id=EXCLUDED.realm_id, created_at=NOW()
    """, {"s": state, "c": code, "r": realmId})

    # Plaintext so the browser tab just says "You can close this tab."
    return PlainTextResponse("You can close this tab.")

@app.get("/admin/qbo/peek", include_in_schema=False)
async def qbo_peek(state: str = Query(...), request: Request = None):
    """
    Private fetch used by your local harvester.
    Requires X-Relay-Auth header == QBO_RELAY_AUTH (env).
    Returns {"code": "...","realmId":"..."} once the callback has landed.
    """
    _require_qbo_relay_auth(request)
    row = await database.fetch_one(
        "SELECT code, realm_id FROM qbo_oauth_events WHERE state = :s",
        {"s": state}
    )
    if not row:
        raise HTTPException(status_code=404, detail="pending")
    return {"code": row["code"], "realmId": row["realm_id"]}
# === QBO OAuth Relay • Endpoints ===

# -------- Security: key registry & rotation (scaffold) --------
@app.on_event("startup")
async def _ensure_key_registry():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS key_registry(
      key_name TEXT PRIMARY KEY,
      version INT NOT NULL DEFAULT 1,
      material TEXT NOT NULL,        -- store KMS alias or encrypted blob, NOT plaintext keys
      rotated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)

@app.post("/admin/keys/rotate", tags=["Admin"])
async def rotate_key(key_name: str, new_material_ref: str, request: Request):
    _require_admin(request)
    await database.execute("""
      INSERT INTO key_registry(key_name,version,material)
      VALUES (:n,1,:m)
      ON CONFLICT (key_name) DO UPDATE SET version = key_registry.version + 1, material=:m, rotated_at=NOW()
    """, {"n": key_name, "m": new_material_ref})
    return {"ok": True, "key_name": key_name}
# -------- Security: key registry & rotation (scaffold) --------

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
from pathlib import Path
import tempfile

@app.get("/bol/{bol_id}/pdf", tags=["Documents"], summary="Download BOL as PDF", status_code=200)
async def generate_bol_pdf(bol_id: str):
    row = await database.fetch_one("SELECT * FROM bols WHERE bol_id = :bol_id", {"bol_id": bol_id})
    if not row:
        raise HTTPException(status_code=404, detail="BOL not found")

    d = dict(row)  # <-- use a plain dict everywhere below

    filename = f"bol_{bol_id}.pdf"
    filepath = str(Path(tempfile.gettempdir()) / filename)  # <-- Windows-safe

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

    draw("BOL ID", d["bol_id"])
    draw("Contract ID", d["contract_id"])
    draw("Status", d.get("status") or "—")
    draw("Buyer", d.get("buyer") or "—")
    draw("Seller", d.get("seller") or "—")
    draw("Material", d.get("material") or "—")
    draw("Weight (tons)", d.get("weight_tons") or "—")

    ppu = float(d["price_per_unit"]) if d.get("price_per_unit") is not None else 0.0
    tv  = float(d["total_value"])    if d.get("total_value")    is not None else 0.0
    draw("Price per ton", f"${ppu:.2f}")
    draw("Total Value", f"${tv:.2f}")

    draw("Pickup Time", d["pickup_time"].isoformat() if d.get("pickup_time") else "—")
    draw("Delivery Time", d["delivery_time"].isoformat() if d.get("delivery_time") else "—")
    draw("Carrier Name", d.get("carrier_name") or "—")
    draw("Driver", d.get("carrier_driver") or "—")
    draw("Truck VIN", d.get("carrier_truck_vin") or "—")
    draw("Origin Country", d.get("origin_country") or "—")
    draw("Destination Country", d.get("destination_country") or "—")
    draw("Port Code", d.get("port_code") or "—")
    draw("HS Code", d.get("hs_code") or "—")
    if d.get("duty_usd") is not None:
        draw("Duty (USD)", f"${float(d['duty_usd']):.2f}")
    if d.get("tax_pct") is not None:
        draw("Tax (%)",  f"{float(d['tax_pct']):.2f}%")

    y -= line_height
    c.setFont("Helvetica-Oblique", 10)
    c.drawString(margin, y, f"Generated by BRidge on {utcnow().isoformat()}")

    try:
        fingerprint = hashlib.sha256(json.dumps(d, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()[:12]
        y -= line_height
        c.setFont("Helvetica", 8)
        c.drawString(margin, y, f"Verify: https://bridge-buyer.onrender.com/bol/{d['bol_id']}  •  Hash: {fingerprint}")
    except Exception:
        pass

    c.save()
    return FileResponse(filepath, media_type="application/pdf", filename=filename)
# -------- Documents: BOL PDF --------

# -------- Receipts lifecycle --------
class ReceiptCreateIn(BaseModel):
    # business fields you already store
    seller: str
    sku: str              # your current schema stores `sku`
    qty_tons: float
    # NEW: fields we want to populate for stocks snapshot
    symbol: Optional[str] = None        # if omitted we’ll default to `sku`
    location: Optional[str] = None      # yard / warehouse code

class ReceiptCreateOut(BaseModel):
    receipt_id: str
    seller: str
    sku: str
    qty_tons: float
    symbol: str
    location: Optional[str] = None
    lot_size: float
    qty_lots: float
    status: str

@app.post("/receipts", tags=["Receipts"], summary="Create/mint a live receipt")
async def receipt_create(body: ReceiptCreateIn, request: Request) -> ReceiptCreateOut:
    key = _idem_key(request)
    hit = None
    if key:
        hit = await idem_get(key)
    if hit: return hit


    """
    Populates symbol, lot_size, qty_lots, location at write time.
    """
    # pick a symbol (default to SKU if caller didn’t supply a separate tradable symbol)
    symbol = (body.symbol or body.sku).strip()

    # look up lot_size from your in-memory registry (falls back to 1.0 if missing)
    lot_size = _lot_size(symbol)  # <-- you already have this helper defined above

    # compute qty_lots
    qty_lots = float(body.qty_tons) / float(lot_size) if lot_size > 0 else None

    receipt_id = str(uuid.uuid4())
    status = "created"

    await database.execute("""
        INSERT INTO public.receipts(
            receipt_id, seller, sku, qty_tons, status,
            symbol, location, qty_lots, lot_size, created_at, updated_at
        ) VALUES (
            :id, :seller, :sku, :qty_tons, :status,
            :symbol, :location, :qty_lots, :lot_size, NOW(), NOW()
        )
    """, {
        "id": receipt_id,
        "seller": body.seller.strip(),
        "sku": body.sku.strip(),
        "qty_tons": float(body.qty_tons),
        "status": status,
        "symbol": symbol,
        "location": (body.location or "UNKNOWN").strip(),
        "qty_lots": qty_lots,
        "lot_size": lot_size,
    })

    resp = ReceiptCreateOut(
         receipt_id=receipt_id,
         seller=body.seller.strip(),
         sku=body.sku.strip(),
         qty_tons=float(body.qty_tons),
         symbol=symbol,
         location=(body.location or "UNKNOWN").strip(),
         lot_size=float(lot_size),
         qty_lots=float(qty_lots) if qty_lots is not None else 0.0,
         status=status,
     )
    return await _idem_guard(request, key, resp)


@app.post("/receipts/{receipt_id}/consume", tags=["Receipts"], summary="Auto-expire at melt")
async def receipt_consume(receipt_id: str, bol_id: Optional[str] = None, prov: ReceiptProvenance = ReceiptProvenance()):
    r = await database.fetch_one("SELECT 1 FROM public.receipts WHERE receipt_id=:id", {"id": receipt_id})
    if not r:
        raise HTTPException(404, "receipt not found")

    await database.execute("""
      UPDATE public.receipts
         SET consumed_at=NOW(),
             consumed_bol_id=:bol,
             provenance=:prov::jsonb,
             status='delivered',
             updated_at=NOW()
       WHERE receipt_id=:id AND consumed_at IS NULL
    """, {"id":receipt_id, "bol":bol_id, "prov": json.dumps(prov.dict())})

    try:
        await audit_append("system", "receipt.consume", "receipt", receipt_id, {"bol_id": bol_id})
    except Exception:
        pass

    return {"receipt_id": receipt_id, "status": "consumed"}
#-------- Receipts lifecycle --------

# ---- Finance: receivables mint ----
class ReceivableIn(BaseModel):
    receipt_id: str
    face_value_usd: float
    due_date: date
    debtor: str  # buyer name/id

@app.post("/finance/receivable", tags=["Finance"], summary="Mint receivable from a live receipt")
async def receivable_create(r: ReceivableIn):
    rec = await database.fetch_one("SELECT * FROM public.receipts WHERE receipt_id=:id", {"id": r.receipt_id})
    if not rec:
        raise HTTPException(404, "Receipt not found")
    if rec.get("consumed_at"):
        raise HTTPException(409, "Receipt already consumed")

    rid = str(uuid.uuid4())
    await database.execute("""
      INSERT INTO public.receivables (id, receipt_id, face_value_usd, due_date, debtor, status, created_at)
      VALUES (:id, :rid, :fv, :dd, :deb, 'open', NOW())
    """, {"id": rid, "rid": r.receipt_id, "fv": r.face_value_usd, "dd": r.due_date, "deb": r.debtor})

    try:
        await audit_append("system", "receivable.create", "receivable", rid, r.dict())
    except Exception:
        pass

    return {"receivable_id": rid, "status": "open"}
# ---- Finance: receivables mint ----

# ---- Insurance: transport coverage quote (stub) ----
class InsuranceQuoteIn(BaseModel):
    receipt_id: str
    coverage_usd: float
    route_miles: Optional[float] = None

@app.post("/insurance/quote", tags=["Insurance"], summary="Get a stubbed transport insurance quote")
async def insurance_quote(i: InsuranceQuoteIn):
    # TODO: call carrier API; stub a rate
    rate_bps = 15.0  # 0.15%
    prem = round(i.coverage_usd * rate_bps / 10000.0, 2)
    try:
        await audit_append("system", "insurance.quote", "receipt", i.receipt_id, {"premium": prem, "bps": rate_bps})
    except Exception:
        pass
    return {"premium_usd": prem, "rate_bps": rate_bps}
# ---- Insurance: transport coverage quote (stub) ----

# ========== Stocks (daily snapshots from receipts) ==========
@app.post("/stocks/snapshot", tags=["Stocks"], summary="Snapshot live receipts into stocks_daily")
async def stocks_snapshot(as_of: date):
    rows = await database.fetch_all("""
      SELECT symbol, location, SUM(qty_lots) qty_lots, MAX(lot_size) lot_size
      FROM public.receipts
      WHERE status IN ('created','pledged') AND consumed_at IS NULL
      GROUP BY symbol, location
    """)
    await database.execute("DELETE FROM public.stocks_daily WHERE as_of=:d", {"d": as_of})
    for r in rows:
        await database.execute("""
          INSERT INTO public.stocks_daily(as_of, symbol, location, qty_lots, lot_size)
          VALUES (:d,:s,:loc,:q,:lot)
        """, {"d": as_of, "s": r["symbol"], "loc": r["location"],
              "q": float(r["qty_lots"]), "lot": float(r["lot_size"])})
    return {"as_of": str(as_of), "rows": len(rows)}

@app.get("/stocks", tags=["Stocks"], summary="Read stocks snapshot (JSON)")
async def stocks(as_of: date | None = None):
    d = as_of or datetime.utcnow().date()
    rows = await database.fetch_all("""
      SELECT as_of, symbol, location, qty_lots, lot_size
      FROM public.stocks_daily
      WHERE as_of=:d
      ORDER BY symbol, location
    """, {"d": d})
    return [dict(r) for r in rows]

@app.get("/stocks.csv", tags=["Stocks"], summary="Read stocks snapshot (CSV)")
async def stocks_csv(as_of: date | None = None):
    d = as_of or datetime.utcnow().date()
    rows = await database.fetch_all("""
      SELECT as_of, symbol, location, qty_lots, lot_size
      FROM public.stocks_daily
      WHERE as_of=:d
      ORDER BY symbol, location
    """, {"d": d})
    out = io.StringIO(); w = csv.writer(out)
    w.writerow(["date","symbol","location","qty_lots","lot_size","qty_lb"])
    for r in rows:
        qty_lb = float(r["qty_lots"]) * float(r["lot_size"]) * 2000.0
        w.writerow([r["as_of"], r["symbol"], r["location"],
                    float(r["qty_lots"]), float(r["lot_size"]), qty_lb])
    out.seek(0)
    return StreamingResponse(iter([out.read()]), media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="stocks_{d}.csv"'} )
# ========== Stocks (daily snapshots from receipts) ==========

# -------- BOLs (with filtering) --------
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
# -------- BOLs (with filtering) --------

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
        "best_bid": (float(bid["price"]) if (bid and bid["price"] is not None) else None),
        "best_bid_size": (float(bid["qty"]) if (bid and bid["qty"] is not None) else None),
        "best_ask": (float(ask["price"]) if (ask and ask["price"] is not None) else None),
        "best_ask_size": (float(ask["qty"]) if (ask and ask["qty"] is not None) else None),
    }

# -------- Contracts (with Inventory linkage) --------
from fastapi import Response, Query
from typing import Optional
from datetime import datetime

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
    reference_source: Optional[str] = Query(None),
    reference_symbol: Optional[str] = Query(None),
    sort: Optional[str] = Query(
        None,
        description="created_at_desc|price_per_ton_asc|price_per_ton_desc|weight_tons_desc"
    ),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    response: Response = None,   # lets us set X-Total-Count
):
    # normalize inputs
    buyer    = buyer.strip()    if isinstance(buyer, str) else buyer
    seller   = seller.strip()   if isinstance(seller, str) else seller
    material = material.strip() if isinstance(material, str) else material
    status   = status.strip()   if isinstance(status, str) else status
    reference_source = reference_source.strip() if isinstance(reference_source, str) else reference_source
    reference_symbol = reference_symbol.strip() if isinstance(reference_symbol, str) else reference_symbol

    # base query
    query = "SELECT * FROM contracts"
    conditions, values = [], {}

    if buyer:
        conditions.append("buyer ILIKE :buyer");               values["buyer"] = f"%{buyer}%"
    if seller:
        conditions.append("seller ILIKE :seller");             values["seller"] = f"%{seller}%"
    if material:
        conditions.append("material ILIKE :material");         values["material"] = f"%{material}%"
    if status:
        conditions.append("status ILIKE :status");             values["status"] = f"%{status}%"
    if start:
        conditions.append("created_at >= :start");             values["start"] = start
    if end:
        conditions.append("created_at <= :end");               values["end"] = end
    if reference_source:
        conditions.append("reference_source = :ref_src");      values["ref_src"] = reference_source
    if reference_symbol:
        conditions.append("reference_symbol ILIKE :ref_sym");  values["ref_sym"] = f"%{reference_symbol}%"

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    # --- total count header ---
    count_sql = "SELECT COUNT(*) AS c FROM contracts"
    if conditions:
        count_sql += " WHERE " + " AND ".join(conditions)
    row_count = await database.fetch_one(count_sql, values)
    try:
        if response is not None:
            response.headers["X-Total-Count"] = str(int(row_count["c"] or 0))
    except Exception:
        pass

    # --- sort mapping ---
    order_clause = "created_at DESC NULLS LAST"
    if sort:
        s = sort.lower()
        if s == "price_per_ton_asc":
            order_clause = "price_per_ton ASC NULLS LAST, created_at DESC NULLS LAST"
        elif s == "price_per_ton_desc":
            order_clause = "price_per_ton DESC NULLS LAST, created_at DESC NULLS LAST"
        elif s == "weight_tons_desc":
            order_clause = "weight_tons DESC NULLS LAST, created_at DESC NULLS LAST"
        else:
            order_clause = "created_at DESC NULLS LAST"

    # finalize + fetch
    query += f" ORDER BY {order_clause} LIMIT :limit OFFSET :offset"
    values["limit"], values["offset"] = limit, offset
    return await database.fetch_all(query=query, values=values)

# ------- Contract ID--------
@app.get("/contracts/{contract_id}", tags=["Contracts"], summary="Fetch contract by id")
async def get_contract_by_id(contract_id: str):
    row = await database.fetch_one("SELECT * FROM contracts WHERE id = :id", {"id": contract_id})
    if not row:
        raise HTTPException(status_code=404, detail="contract not found")
    return dict(row) 

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
    request: Request
):
    try:
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

        # Best-effort audit; never break the request
        try:
            actor = (request.session.get("username") if hasattr(request, "session") else None)
            await log_action(
                actor or "system",
                "contract.update",
                str(contract_id),
                {"status": update.status, "signature_present": update.signature is not None}
            )
        except Exception as e:
            try:
                logger.warn("contract_update_audit_failed", err=str(e))
            except Exception:
                pass

        return row

    except HTTPException:
        raise
    except Exception as e:
        # dev-safe fallback: return current row without failing the socket
        try:
            logger.warn("contract_update_failed", err=str(e))
        except Exception:
            pass
        cur = await database.fetch_one("SELECT * FROM contracts WHERE id=:id", {"id": contract_id})
        if cur:
            return cur
        raise HTTPException(status_code=500, detail="contract update failed")
    
@app.post("/contracts/{id}/cancel", tags=["Contracts"], summary="Cancel Pending contract (unreserve inventory)")
async def contract_cancel(id: str):    
    row = await database.fetch_one("SELECT * FROM contracts WHERE id = :id FOR UPDATE", {"id": id})
    if not row:
        raise HTTPException(status_code=404, detail="contract not found")

    status = (row["status"] if not isinstance(row, dict) else row.get("status")) or ""
    status = str(status)

    # Idempotent
    if status == "Cancelled":
        return {"ok": True, "contract_id": id, "status": "Cancelled"}

    # Only allow cancel from Pending. Block once Signed/Dispatched/Fulfilled.
    if status in ("Signed", "Dispatched", "Fulfilled"):
        raise HTTPException(status_code=409, detail=f"cannot cancel contract in state {status}")
    if status != "Pending":
        raise HTTPException(status_code=409, detail=f"only Pending contracts can be cancelled (got {status})")

    # Unreserve the inventory that was reserved at create time
    qty = float(row["weight_tons"] or 0.0)
    seller = (row["seller"] or "").strip()
    sku    = (row["material"] or "").strip()

    async with database.transaction():        
        await database.execute("""
            UPDATE inventory_items
               SET qty_reserved = GREATEST(qty_reserved - :q, 0),
                   updated_at   = NOW()
             WHERE LOWER(seller)=LOWER(:s) AND LOWER(sku)=LOWER(:k)
        """, {"q": qty, "s": seller, "k": sku})

        try:
            await database.execute("""
              INSERT INTO inventory_movements (seller, sku, movement_type, qty, ref_contract, meta)
              VALUES (:s,:k,'unreserve',:q,:cid,:m)
            """, {"s": seller, "k": sku, "q": qty, "cid": id, "m": json.dumps({"reason":"cancel"})})
        except Exception:            
            pass

        await database.execute(
            "UPDATE contracts SET status='Cancelled', updated_at=NOW() WHERE id=:id",
            {"id": id}
        )

    out = await database.fetch_one("SELECT * FROM contracts WHERE id=:id", {"id": id})
    return dict(out)
# ------- Contract ID--------       

#-------- Export Contracts as CSV --------
from fastapi import Response
import uuid

def _normalize(v):
    # keep this near the top of the file so it's used by export_contracts_csv
    from datetime import date, datetime as _dt
    if isinstance(v, (date, _dt)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"), ensure_ascii=False)
    # let csv write str(value) for everything else
    return v

@app.get("/contracts/export_csv", tags=["Contracts"], summary="Export Contracts as CSV", status_code=200)
async def export_contracts_csv():
    # Streamed CSV, ultra-guarded. Never crashes the worker, even with odd types/NULLs.
    try:
        rows = await database.fetch_all("SELECT * FROM contracts ORDER BY created_at DESC")
    except Exception as e:
        try:
            logger.warn("contracts_export_csv_query_failed", err=str(e))
        except Exception:
            pass
        return StreamingResponse(
            iter(["id\n"]), media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="contracts.csv"'}
        )

    # Normalize to list of dicts up front
    dict_rows = []
    try:
        for r in rows:
            dict_rows.append(dict(r))
    except Exception as e:
        try:
            logger.warn("contracts_export_csv_row_cast_failed", err=str(e))
        except Exception:
            pass
        return StreamingResponse(
            iter(["id\n"]),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="contracts.csv"'}
        )

    fieldnames = sorted({k for r in dict_rows for k in r.keys()}) if dict_rows else ["id"]

    def _norm(v):
        try:            
            if v is None: return ""
            if isinstance(v, (int, float, str)): return v
            from datetime import date, datetime as _dt
            import uuid as _uuid
            if isinstance(v, (date, _dt)): return v.isoformat()
            if isinstance(v, Decimal): return float(v)
            if isinstance(v, _uuid.UUID): return str(v)
            if isinstance(v, (dict, list)): return json.dumps(v, separators=(",", ":"), ensure_ascii=False)
            return str(v)
        except Exception:
            return ""

    async def _gen():
        buf = io.StringIO(newline="")
        w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        # header
        w.writeheader(); yield buf.getvalue(); buf.seek(0); buf.truncate(0)
        # rows
        for r in dict_rows:
            try:
                w.writerow({k: _norm(r.get(k)) for k in fieldnames})
            except Exception:
                # write a minimal, non-breaking row
                safe = {}
                for k in fieldnames:
                    try:
                        safe[k] = _norm(r.get(k))
                    except Exception:
                        safe[k] = ""
                w.writerow(safe)
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)

    return StreamingResponse(
        _gen(), media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="contracts.csv"'}
    )
#-------- Export Contracts as CSV --------

# --- Idempotent purchase (atomic contract signing + inventory commit + BOL create) ---
@app.patch("/contracts/{contract_id}/purchase",
           tags=["Contracts"],
           summary="Purchase (atomic)",
           description="Atomically change a Pending contract to Signed, move reserved→committed, and auto-create a Scheduled BOL.",
           status_code=200)
async def purchase_contract(contract_id: str, body: PurchaseIn, request: Request, _=Depends(csrf_protect)):    
    key = _idem_key(request) or getattr(body, "idempotency_key", None)
    hit = None
    if key:
        hit = await idem_get(key)
    if hit: return hit


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
            WHERE LOWER(seller)=LOWER(:s) AND LOWER(sku)=LOWER(:k)
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
            WHERE LOWER(seller)=LOWER(:s) AND LOWER(sku)=LOWER(:k)
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
            "pickup_time": utcnow()
        })

    resp = {"ok": True, "contract_id": contract_id, "new_status": "Signed", "bol_id": bol_id}
    return await _idem_guard(request, key, resp)
# ----- Idempotent purchase (atomic contract signing + inventory commit + BOL create) -----

# --- MATERIAL PRICE HISTORY (by day, avg) ---
from typing import Optional
from fastapi import HTTPException

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

# ========== Admin Exports router ==========

#======== Contracts (with Inventory linkage) ==========
class ContractInExtended(ContractIn):
    pricing_formula: Optional[str] = None
    reference_symbol: Optional[str] = None
    reference_price: Optional[float] = None
    reference_source: Optional[str] = None
    reference_timestamp: Optional[datetime] = None
    currency: Optional[str] = "USD"

@app.post("/contracts", response_model=ContractOut, tags=["Contracts"], summary="Create Contract", status_code=201)
async def create_contract(contract: ContractInExtended, request: Request, _=Depends(csrf_protect)):
    key = _idem_key(request)
    if key:
        hit = await idem_get(key)
        if hit:
            return hit

    await _check_contract_quota()

    cid    = str(uuid.uuid4())
    qty    = float(contract.weight_tons)
    seller = (contract.seller or "").strip()
    sku    = (contract.material or "").strip()

    # ---------- best-effort reference price (NO transaction, tiny timeout) ----------
    if not (contract.reference_price and contract.reference_source):
        try:
            async with httpx.AsyncClient(timeout=5.0) as c:
                r = await c.get(f"{request.base_url}prices/copper_last")
                if 200 <= r.status_code < 300:
                    j = r.json()
                    contract.reference_price     = float(j.get("last"))
                    contract.reference_source    = "COMEX (derived/internal, delayed)"
                    contract.reference_symbol    = "HG=F"
                    contract.reference_timestamp = utcnow() 
                    if not contract.pricing_formula:
                        contract.pricing_formula = "COMEX_Cu - 0.25"
        except Exception:
            pass

    # -------- historical mode detection + created_at override -----------
    import_mode = request.headers.get("X-Import-Mode", "").lower() == "historical"
    if os.getenv("ENV", "").lower() == "production" and import_mode:
        pass  # TEMP: allow historical import without admin during backfill

    created_at_override: datetime | None = None
    if import_mode:
        hdr = (request.headers.get("X-Import-Created-At") or "").strip()
        try:
            if hdr:
                # Accept 'YYYY-MM-DD' or full ISO, then coerce to aware UTC
                if len(hdr) == 10 and hdr[4] == "-" and hdr[7] == "-":
                    dt = datetime.fromisoformat(hdr + "T00:00:00")
                else:
                    dt = datetime.fromisoformat(hdr.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    from datetime import timezone as _tz
                    dt = dt.replace(tzinfo=_tz.utc)
                else:
                    dt = dt.astimezone(_tz.utc)
                created_at_override = dt
            elif contract.reference_timestamp:
                # ensure aware UTC
                rt = contract.reference_timestamp
                created_at_override = (rt.astimezone(_tz.utc)
                                       if rt.tzinfo else rt.replace(tzinfo=_tz.utc))
        except Exception:
            created_at_override = None

    # --------------------- primary path with inventory reserve ----------------------
    try:
        if import_mode:
            # Historical import: Signed, optional back-dated created_at
            payload = {
                "id": cid,
                "buyer": contract.buyer, "seller": contract.seller,
                "material": contract.material, "weight_tons": contract.weight_tons,
                "price_per_ton": contract.price_per_ton, "status": "Signed",
                "pricing_formula": contract.pricing_formula,
                "reference_symbol": contract.reference_symbol,
                "reference_price": contract.reference_price,
                "reference_source": contract.reference_source,
                "reference_timestamp": contract.reference_timestamp,
                "currency": contract.currency or "USD",
            }

            if created_at_override is not None:
                row = await database.fetch_one("""
                    INSERT INTO contracts (
                        id,buyer,seller,material,weight_tons,price_per_ton,status,
                        pricing_formula,reference_symbol,reference_price,reference_source,
                        reference_timestamp,currency,created_at
                    )
                    VALUES (
                        :id,:buyer,:seller,:material,:weight_tons,:price_per_ton,:status,
                        :pricing_formula,:reference_symbol,:reference_price,:reference_source,
                        :reference_timestamp,:currency,:created_at
                    )
                    RETURNING *
                """, {**payload, "created_at": created_at_override})
            else:
                row = await database.fetch_one("""
                    INSERT INTO contracts (
                        id,buyer,seller,material,weight_tons,price_per_ton,status,
                        pricing_formula,reference_symbol,reference_price,reference_source,
                        reference_timestamp,currency
                    )
                    VALUES (
                        :id,:buyer,:seller,:material,:weight_tons,:price_per_ton,:status,
                        :pricing_formula,:reference_symbol,:reference_price,:reference_source,
                        :reference_timestamp,:currency
                    )
                    RETURNING *
                """, payload)

        else:
            # LIVE path: reserve inventory, write Pending
            async with database.transaction():
                await database.execute("""
                    INSERT INTO inventory_items (seller, sku, qty_on_hand, qty_reserved, qty_committed)
                    VALUES (:s,:k,0,0,0)
                    ON CONFLICT (seller, sku) DO NOTHING
                """, {"s": seller, "k": sku})

                inv = await database.fetch_one("""
                    SELECT qty_on_hand, qty_reserved, qty_committed
                    FROM inventory_items
                    WHERE LOWER(seller)=LOWER(:s) AND LOWER(sku)=LOWER(:k)
                    FOR UPDATE
                """, {"s": seller, "k": sku})

                on_hand   = float(inv["qty_on_hand"]) if inv else 0.0
                reserved  = float(inv["qty_reserved"]) if inv else 0.0
                committed = float(inv["qty_committed"]) if inv else 0.0
                available = on_hand - reserved - committed
                if available < qty:
                    raise HTTPException(409, f"Not enough inventory: available {available} ton(s) < requested {qty} ton(s).")

                await database.execute("""
                    UPDATE inventory_items
                       SET qty_reserved = qty_reserved + :q, updated_at = NOW()
                     WHERE LOWER(seller)=LOWER(:s) AND LOWER(sku)=LOWER(:k)
                """, {"q": qty, "s": seller, "k": sku})

                await database.execute("""
                    INSERT INTO inventory_movements (seller, sku, movement_type, qty, ref_contract, meta)
                    VALUES (:s,:k,'reserve',:q,:cid,:m)
                """, {"s": seller, "k": sku, "q": qty, "cid": cid,
                      "m": json.dumps({"reason": "contract_create"})})

                payload = {
                    "id": cid,
                    "buyer": contract.buyer, "seller": contract.seller,
                    "material": contract.material, "weight_tons": contract.weight_tons,
                    "price_per_ton": contract.price_per_ton, "status": "Pending",
                    "pricing_formula": contract.pricing_formula,
                    "reference_symbol": contract.reference_symbol,
                    "reference_price": contract.reference_price,
                    "reference_source": contract.reference_source,
                    "reference_timestamp": contract.reference_timestamp,
                    "currency": contract.currency or "USD",
                }

                row = await database.fetch_one("""
                    INSERT INTO contracts (
                        id,buyer,seller,material,weight_tons,price_per_ton,status,
                        pricing_formula,reference_symbol,reference_price,reference_source,
                        reference_timestamp,currency
                    )
                    VALUES (
                        :id,:buyer,:seller,:material,:weight_tons,:price_per_ton,:status,
                        :pricing_formula,:reference_symbol,:reference_price,:reference_source,
                        :reference_timestamp,:currency
                    )
                    RETURNING *
                """, payload)

        # best-effort audit/webhook; never crash
        try:
            actor = request.session.get("username") if hasattr(request, "session") else None
            await log_action(actor or "system", "contract.create", str(row["id"]),
                             {"pricing_disclaimer": "Reference prices are internal only."})
        except Exception:
            pass
        try:
            METRICS_CONTRACTS_CREATED.inc()
        except Exception:
            pass
        try:
            await emit_event_safe("contract.updated", {
                "contract_id": str(row["id"]), "status": row["status"],
                "seller": row["seller"], "buyer": row["buyer"],
                "material": row["material"],
                "weight_tons": float(row["weight_tons"]), "price_per_ton": float(row["price_per_ton"]),
                "currency": (row.get("currency") if isinstance(row, dict) else None) or "USD",
            })
        except Exception:
            pass

        resp = row
        return await _idem_guard(request, key, resp)

    # --------------------- fallback path: minimal insert -----------------------------
    except HTTPException:
        raise
    except Exception as e:
        try:
            logger.warn("contract_create_failed_primary", err=str(e))
        except Exception:
            pass
        try:
            await database.execute("""
                INSERT INTO contracts (id,buyer,seller,material,weight_tons,price_per_ton,status,currency)
                VALUES (:id,:buyer,:seller,:material,:wt,:ppt,'Pending',COALESCE(:ccy,'USD'))
                ON CONFLICT (id) DO NOTHING
            """, {"id": cid, "buyer": contract.buyer, "seller": contract.seller,
                  "material": contract.material, "wt": contract.weight_tons,
                  "ppt": contract.price_per_ton, "ccy": contract.currency})
            row = await database.fetch_one("SELECT * FROM contracts WHERE id=:id", {"id": cid})
            return await _idem_guard(request, key, row if row else {"id": cid, "status": "Pending"})
        except Exception as e2:
            try:
                logger.warn("contract_create_failed_fallback", err=str(e2))
            except Exception:
                pass
            raise HTTPException(status_code=500, detail="contract create failed")

    # ========= Admin Exports router ==========

# -------- Products --------
@app.post("/products", tags=["Products"], summary="Upsert a tradable product")
async def products_add(p: ProductIn):
    quality_json = json.dumps(p.quality or {}, separators=(",", ":"), ensure_ascii=False)
    await database.execute("""
      INSERT INTO public.products(symbol, description, unit, quality)
      VALUES (:s, :d, :u, :q::jsonb)
      ON CONFLICT (symbol) DO UPDATE
        SET description = EXCLUDED.description,
            unit        = EXCLUDED.unit,
            quality     = EXCLUDED.quality
    """, {"s": p.symbol, "d": p.description, "u": p.unit, "q": quality_json})

    _INSTRUMENTS[p.symbol] = {"lot": 1.0, "tick": 0.01, "desc": p.description}
    return {"symbol": p.symbol, "tradable": True}
#-------- Products --------

# ------- Signed Contract -------
@app.post("/contracts/{contract_id}/sign", tags=["Contracts"], summary="Sign a contract", status_code=200)
async def sign_contract(contract_id: str, request: Request):
    try:
            await notify_humans(
                "contract.signed",
                member=(request.session.get("username") or "unknown"),
                subject=f"Contract Signed — {contract_id}",
                html=f"<div style='font-family:system-ui'>Contract <b>{contract_id}</b> has been signed.</div>",
                ref_type="contract", ref_id=contract_id
            )
    except Exception: pass

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
            "timestamp": utcnow().isoformat(),
        })
    except Exception:
        pass
    try:
        await emit_event_safe("contract.updated", {"contract_id": str(contract_id), "status": "Signed"})
    except Exception:
        pass

    return {"status": "ok", "contract_id": contract_id}
#-------- Signed Contract --------

# ========== Tenant Applications ==========
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

@app.on_event("startup")
async def _ensure_tenant_applications_schema():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS tenant_applications (
      application_id UUID PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      status TEXT NOT NULL DEFAULT 'pending',
      entity_type TEXT NOT NULL,
      role TEXT NOT NULL,
      org_name TEXT NOT NULL,
      ein TEXT,
      address TEXT,
      city TEXT,
      region TEXT,
      website TEXT,
      monthly_volume_tons INT,
      contact_name TEXT NOT NULL,
      email TEXT NOT NULL,
      phone TEXT,
      ref_code TEXT,
      materials_buy TEXT,
      materials_sell TEXT,
      lanes TEXT,
      compliance_notes TEXT,
      plan TEXT NOT NULL,
      notes TEXT,
      utm_source TEXT,
      utm_campaign TEXT,
      utm_medium TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_tenant_apps_created ON tenant_applications(created_at DESC);
    """)
# ========== Tenant Applications ==========

# --- Public endpoint (replaces /public/yard_signup) ---
@app.post(
    "/public/apply",
    tags=["Public"],
    summary="Public application (multi-entity)",
    description="Collects onboarding applications from yards, mills, industrial generators, manufacturers, and brokers.",
    response_model=ApplicationOut,
    status_code=201,
)
async def public_apply(payload: ApplicationIn, request: Request, _=Depends(csrf_protect)):
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

        # Best-effort email (reuses the earlier helper)
    try:
        # Build the simple ApplyRequest the email helper expects
        shim = ApplyRequest(
            entity_type=payload.entity_type if payload.entity_type in ("yard","buyer","broker","other")
                         else ("buyer" if payload.entity_type in ("mill","manufacturer","industrial") else "other"),
            role=payload.role,
            org_name=payload.org_name,
            ein=payload.ein,
            address=payload.address,
            city=payload.city,
            region=payload.region,
            website=payload.website,
            monthly_volume_tons=payload.monthly_volume_tons,
            contact_name=payload.contact_name,
            email=payload.email,
            phone=payload.phone,
            ref_code=payload.ref_code,
            materials_buy=payload.materials_buy,
            materials_sell=payload.materials_sell,
            lanes=payload.lanes,
            compliance_notes=payload.compliance_notes,
            plan=payload.plan,
            notes=payload.notes,
            utm_source=payload.utm_source,
            utm_campaign=payload.utm_campaign,
            utm_medium=payload.utm_medium,
        )
        send_application_email(shim, application_id)
    except Exception as e:
        print("[WARN] application email failed:", e)

         # Require payment method on file BEFORE accepting
    pay = await database.fetch_one("""
    SELECT has_default FROM billing_payment_profiles WHERE member=:m OR member=:alt
    """, {"m": payload.org_name, "alt": (payload.org_name or "").strip()})
    if not (pay and pay["has_default"]):
        raise HTTPException(402, detail="Payment method required. Please add a payment method before submitting.")

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
from uuid import uuid4, uuid5, NAMESPACE_URL

@app.post("/bols", response_model=BOLOut, tags=["BOLs"], summary="Create BOL", status_code=201)
async def create_bol_pg(bol: BOLIn, request: Request):
    key = _idem_key(request)
    hit = await idem_get(key) if key else None
    if hit:
        return hit

    # Deterministic UUID when an Idempotency-Key is present
    bol_uuid = uuid5(NAMESPACE_URL, f"bol:{key}") if key else uuid4()
    bol_id_str = str(bol_uuid)

    # Try to insert; if it already exists (same key), fetch the existing row
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
        ON CONFLICT (bol_id) DO NOTHING
        RETURNING *
    """, {
        "bol_id": bol_id_str,
        "contract_id": str(bol.contract_id),
        "buyer": bol.buyer, "seller": bol.seller, "material": bol.material,
        "weight_tons": bol.weight_tons, "price_per_unit": bol.price_per_unit, "total_value": bol.total_value,
        "carrier_name": bol.carrier.name, "carrier_driver": bol.carrier.driver, "carrier_truck_vin": bol.carrier.truck_vin,
        "pickup_sig_b64": bol.pickup_signature.base64, "pickup_sig_time": bol.pickup_signature.timestamp,
        "pickup_time": bol.pickup_time,
        "origin_country": bol.origin_country, "destination_country": bol.destination_country,
        "port_code": bol.port_code, "hs_code": bol.hs_code,
        "duty_usd": bol.duty_usd, "tax_pct": bol.tax_pct,
    })
    we_created = row is not None
    if row is None:
        row = await database.fetch_one("SELECT * FROM bols WHERE bol_id = :id", {"id": bol_id_str})

    # Auto-mint a receipt ONLY on first creation
    if we_created:
        try:
            _symbol = row["material"]
            _lot = _lot_size(_symbol)
            _qty_lots = (float(row["weight_tons"]) / _lot) if _lot > 0 else None
            await database.execute("""
                INSERT INTO public.receipts(
                    receipt_id, seller, sku, qty_tons, status,
                    symbol, location, qty_lots, lot_size, created_at, updated_at, consumed_bol_id
                ) VALUES (
                    :id, :seller, :sku, :qty_tons, 'created',
                    :symbol, :location, :qty_lots, :lot_size, NOW(), NOW(), NULL
                )
            """, {
                "id": str(uuid4()),
                "seller": row["seller"],
                "sku": row["material"],
                "qty_tons": float(row["weight_tons"]),
                "symbol": _symbol,
                "location": "UNKNOWN",
                "qty_lots": _qty_lots,
                "lot_size": _lot,
            })
        except Exception:
            pass

    # Best-effort audit / webhook / metric
    try:
        actor = request.session.get("username") if hasattr(request, "session") else None
        await log_action(actor or "system", "bol.create", str(row["bol_id"]), {
            "contract_id": str(bol.contract_id), "material": bol.material, "weight_tons": bol.weight_tons
        })
    except Exception:
        pass
    try:
        await emit_event_safe("bol.created", {
            "bol_id": row["bol_id"], "contract_id": row["contract_id"],
            "seller": row["seller"], "buyer": row["buyer"],
            "material": row["material"], "weight_tons": float(row["weight_tons"]),
            "status": row["status"]
        })
    except Exception:
        pass
    try:
        METRICS_BOLS_CREATED.inc()
    except Exception:
        pass

    resp = {
        **bol.model_dump(),  # Pydantic v2
        "bol_id": row["bol_id"],
        "status": row["status"],
        "delivery_signature": None,
        "delivery_time": None,
    }
    return await _idem_guard(request, key, resp)

@app.post("/bols/{bol_id}/deliver", tags=["BOLs"], summary="Mark BOL delivered and expire linked receipts")
async def bols_mark_delivered(
    bol_id: str,
    receipt_ids: Optional[List[str]] = Query(None),
    request: Request = None
):
    # mark delivered
    row = await database.fetch_one("""
      UPDATE bols
         SET status='Delivered',
             delivery_time = COALESCE(delivery_time, NOW())
       WHERE bol_id=:id
       RETURNING bol_id
    """, {"id": bol_id})
    if not row:
        raise HTTPException(404, "BOL not found")

    # consume linked receipts (best-effort)
    if receipt_ids:
        for rid in receipt_ids:
            try:
                await receipt_consume(rid, bol_id=bol_id)  # reuse the endpoint logic
            except Exception:
                pass

    # audit
    try:
        await audit_append(
            (request.session.get("username") if hasattr(request, "session") else "system"),
            "bol.deliver", "bol", bol_id, {"receipts": receipt_ids or []}
        )
    except Exception:
        pass

    return {"bol_id": bol_id, "status": "Delivered", "receipts_consumed": receipt_ids or []}

# -------- BOLs (with PDF generation) --------

# =============== Admin Exports (core tables) ===============
   
from fastapi import APIRouter
from fastapi.responses import StreamingResponse, RedirectResponse
from sqlalchemy import text as _sqltext
import io, csv, zipfile

admin_exports = APIRouter(prefix="/admin/exports", tags=["Admin"])

@admin_exports.get("/contracts.csv", summary="Contracts CSV (streamed)")
def export_contracts_csv_admin():
    with engine.begin() as conn:
        rows = conn.execute(_sqltext(CONTRACTS_EXPORT_SQL)).mappings().all()

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
        "contracts.csv": CONTRACTS_EXPORT_SQL,

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
app.include_router(admin_exports)

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
            WHERE LOWER(seller)=LOWER(:s) AND LOWER(sku)=LOWER(:k)
            FOR UPDATE
        """, {"s": seller, "k": sku})

        await database.execute("""
            UPDATE inventory_items
            SET qty_reserved = GREATEST(0, qty_reserved - :q),
                updated_at = NOW()
            WHERE LOWER(seller)=LOWER(:s) AND LOWER(sku)=LOWER(:k)
        """, {"q": qty, "s": seller, "k": sku})

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

class FuturesProductIn(BaseModel):
    symbol_root: str
    material: str
    delivery_location: str
    contract_size_tons: float = 20.0
    tick_size: float = 0.5
    currency: str = "USD"
    price_method: Literal["VWAP_BASIS", "MANUAL", "EXTERNAL"] = "VWAP_BASIS"

class FuturesProductOut(FuturesProductIn):
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

@futures_router.post("/products", response_model=FuturesProductOut, summary="Create/Upsert a futures product")
async def create_product(p: FuturesProductIn):
    await _ensure_futures_tables_if_missing()
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
    await _ensure_futures_tables_if_missing()
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
    await _ensure_futures_tables_if_missing()
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
    await _ensure_futures_tables_if_missing()
    await database.execute("UPDATE futures_listings SET status='Listed' WHERE id=:id", {"id": listing_id})
    return {"ok": True}

class TradingStatusIn(BaseModel):
    trading_status: Literal["Trading","Halted","Expired"]

@futures_router.post("/series/{listing_id}/trading_status", summary="Set listing trading status")
async def set_trading_status(listing_id: str, body: TradingStatusIn):
    await _ensure_futures_tables_if_missing()
    await database.execute("UPDATE futures_listings SET trading_status=:st WHERE id=:id",
                           {"st": body.trading_status, "id": listing_id})
    return {"ok": True, "listing_id": listing_id, "trading_status": body.trading_status}

@futures_router.post("/series/{listing_id}/expire", summary="Expire listing (cash-settled)")
async def expire_listing(listing_id: str):
    await _ensure_futures_tables_if_missing()
    await database.execute("""
      UPDATE futures_listings SET trading_status='Expired', status='Expired' WHERE id=:id
    """, {"id": listing_id})
    return {"ok": True, "listing_id": listing_id, "status": "Expired"}

@futures_router.post("/series/{listing_id}/finalize", summary="Finalize cash settlement at expiry")
async def finalize_series(listing_id: str, mark_date: Optional[date] = None):
    await _ensure_futures_tables_if_missing()
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
    await _ensure_futures_tables_if_missing()
    mark_price, method = await _compute_mark_for_listing(body.listing_id, body.mark_date)
    await database.execute("""
      INSERT INTO futures_marks (id, listing_id, mark_date, mark_price, method)
      VALUES (:id,:lid,:dt,:px,:m)
    """, {"id": str(uuid.uuid4()), "lid": body.listing_id, "dt": (body.mark_date or _date.today()), "px": mark_price, "m": method})
    return {"listing_id": body.listing_id, "mark_date": str(body.mark_date or _date.today()), "mark_price": mark_price, "method": method}

@futures_router.get("/products", response_model=List[FuturesProductOut], summary="List products")
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

async def _fut_check_position_limit(member: str, listing_id: str, inc: float, side: str):
    # Map listing -> symbol_root so futures & spot share the same limit keys
    symbol = await _symbol_for_listing(listing_id)
    if not symbol:
        return
    row = await database.fetch_one(
        "SELECT net_qty FROM positions WHERE account_id=:a AND listing_id=:l",
        {"a": member, "l": listing_id}
    )
    cur = float(row["net_qty"]) if row else 0.0
    new = cur + (inc if side == "BUY" else -inc)

    lim = await database.fetch_one(
        "SELECT limit_lots FROM clob_position_limits WHERE member=:m AND symbol=:s",
        {"m": member, "s": symbol}
    )
    if lim and abs(new) > float(lim["limit_lots"]):
        raise HTTPException(
            429,
            f"Position limit exceeded for {member} on {symbol}: {new} lots (max {float(lim['limit_lots'])})"
        )

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

async def _require_plan_cap(member: str, cap: str):
    row = await database.fetch_one("""
      SELECT c.* FROM member_plans mp
      JOIN billing_plan_caps c ON c.plan_code = mp.plan_code
      WHERE mp.member = :m
    """, {"m": member})
    if not row or not bool(row.get(cap)):
        raise HTTPException(403, f"Your plan does not include '{cap}'. Please upgrade.")


# ---- MARKET price-protection helpers ----
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

# ---- ENTITLEMENTS & PRICE BANDS/LULD ----
def require_entitlement(user: str, feature: str):
    feats = _ENTITLEMENTS.get(user, set())
    if feature not in feats:
        raise HTTPException(status_code=403, detail=f"Missing entitlement: {feature}")

async def price_band_check(symbol: str, price: float):
    if band := _PRICE_BANDS.get(symbol):
        lo, hi = band
        if price < lo or price > hi:
            raise HTTPException(status_code=422, detail=f"Price {price} outside band [{lo}, {hi}]")
    # LULD on last settle 
    if symbol in _LULD:
        down, up = _LULD[symbol]
        row = await database.fetch_one(
            "SELECT settle FROM settlements WHERE symbol=:s ORDER BY as_of DESC LIMIT 1",
            {"s": symbol}
        )
        if row:
            base = float(row["settle"])
            if price < base*(1.0 - down) or price > base*(1.0 + up):
                raise HTTPException(status_code=422, detail="LULD violation")

async def _symbol_for_listing(listing_id: str) -> str:
    row = await database.fetch_one("""
        SELECT fp.symbol_root
        FROM futures_listings fl
        JOIN futures_products fp ON fp.id = fl.product_id
        WHERE fl.id = :id
    """, {"id": listing_id})
    if not row:
        return None
    return row["symbol_root"]

async def _fut_get_fees(listing_id: str):
    sym = await _symbol_for_listing(listing_id)
    if not sym:
        return (0.0, 0.0, 0.0)
    row = await database.fetch_one(
        "SELECT maker_bps, taker_bps, min_fee_cents FROM fee_schedule WHERE symbol=:s",
        {"s": sym}
    )
    if not row:
        return (0.0, 0.0, 0.0)
    return (float(row["maker_bps"]), float(row["taker_bps"]), float(row["min_fee_cents"]))

# ===== Risk controls (admin/runtime) =====
@app.post("/risk/kill_switch/{member_id}", tags=["Risk"], summary="Toggle kill switch")
async def kill_switch(member_id: str, enabled: bool = True):
    _KILL_SWITCH[member_id] = enabled
    # --- audit (best-effort) ---
    try:
        await audit_append("admin", "risk.kill_switch", "member", member_id, {"enabled": enabled})
    except Exception:
        pass
    return {"member_id": member_id, "enabled": enabled}

@app.post("/risk/price_band/{symbol}", tags=["Risk"], summary="Set absolute price band")
async def set_price_band(symbol: str, lower: float, upper: float):
    _PRICE_BANDS[symbol] = (lower, upper)
    await database.execute("""
      INSERT INTO runtime_price_bands(symbol, lower, upper, updated_at)
      VALUES (:s,:l,:u, NOW())
      ON CONFLICT (symbol) DO UPDATE SET lower=EXCLUDED.lower, upper=EXCLUDED.upper, updated_at=NOW()
    """, {"s": symbol, "l": lower, "u": upper})
    return {"symbol": symbol, "band": _PRICE_BANDS[symbol]}

@app.post("/risk/luld/{symbol}", tags=["Risk"], summary="Set LULD pct (0.05=5%)")
async def set_luld(symbol: str, down_pct: float, up_pct: float):
    _LULD[symbol] = (down_pct, up_pct)
    await database.execute("""
      INSERT INTO runtime_luld(symbol, down_pct, up_pct, updated_at)
      VALUES (:s,:d,:u, NOW())
      ON CONFLICT (symbol) DO UPDATE SET down_pct=EXCLUDED.down_pct, up_pct=EXCLUDED.up_pct, updated_at=NOW()
    """, {"s": symbol, "d": down_pct, "u": up_pct})
    return {"symbol": symbol, "luld": _LULD[symbol]}

@app.post("/risk/limits", tags=["Risk"], summary="Upsert position limit (member+symbol)")
async def upsert_limit(member: str, symbol: str, limit_lots: float):
    await database.execute("""
      INSERT INTO clob_position_limits(member, symbol, limit_lots)
      VALUES (:m, :s, :l)
      ON CONFLICT (member, symbol) DO UPDATE SET limit_lots = EXCLUDED.limit_lots
    """, {"m": member, "s": symbol, "l": limit_lots})
    return {"member": member, "symbol": symbol, "limit_lots": limit_lots}

# ---- helpers for risk ----
async def _latest_settle_for_symbol(symbol_root: str) -> float | None:
    # Try most recent official mark for any listing of this product
    r = await database.fetch_one("""
        SELECT fm.mark_price
        FROM futures_marks fm
        JOIN futures_listings fl ON fl.id = fm.listing_id
        JOIN futures_products fp ON fp.id = fl.product_id
        WHERE fp.symbol_root = :s
        ORDER BY fm.mark_date DESC
        LIMIT 1
    """, {"s": symbol_root})
    return float(r["mark_price"]) if r and r["mark_price"] is not None else None

async def _risk_check_position_limit(member: str, symbol: str, delta_lots: float, side: str):
    row = await database.fetch_one(
        "SELECT qty_lots FROM clob_positions WHERE owner=:o AND symbol=:s",
        {"o": member, "s": symbol}
    )
    cur = float(row["qty_lots"]) if row else 0.0
    proposed = cur + (delta_lots if side == "buy" else -delta_lots)

    lim = await database.fetch_one(
        "SELECT limit_lots FROM clob_position_limits WHERE member=:m AND symbol=:s",
        {"m": member, "s": symbol}
    )
    if lim and abs(proposed) > float(lim["limit_lots"]):
        raise HTTPException(422, detail=f"Position limit exceeded: {proposed} vs {float(lim['limit_lots'])}")

class MarginIn(BaseModel):
    member: str
    symbol: str          # symbol_root, e.g. 'CU-SHRED-1M'
    net_lots: Optional[Decimal] = None  # if None, read from clob_positions
    price: float
    vol_pct: float = 0.15
    lot_value_per_tick: float = 1.0     # (kept for future use)

@app.post("/risk/margin/calc", tags=["Risk"], summary="SPAN-lite initial & daily PnL")
async def margin_calc(m: MarginIn):
    # net exposure
    if m.net_lots is None:
        row = await database.fetch_one(
            "SELECT qty_lots FROM clob_positions WHERE owner=:o AND symbol=:s",
            {"o": m.member, "s": m.symbol}
        )
        net = float(row["qty_lots"]) if row else 0.0
    else:
        net = float(m.net_lots)

    # Initial Margin (very simple model)
    im = abs(net) * m.price * m.vol_pct

    # Reference settle (latest official mark if available)
    settle = await _latest_settle_for_symbol(m.symbol)
    ref = settle if settle is not None else m.price
    daily_pnl = (m.price - ref) * net

    return {
        "member": m.member,
        "symbol": m.symbol,
        "initial_margin": round(im, 2),
        "daily_pnl": round(daily_pnl, 2),
        "ref_settle": ref
    }

@app.post("/risk/portfolio_margin", tags=["Risk"], summary="Scenario-based PM")
async def portfolio_margin(member: str, symbol: str, price: float, net_lots: float,
                           shocks: List[float] = Query([-0.1,-0.05,0,0.05,0.1])):
    lot = _lot_size(symbol)
    exposures = []
    worst = 0.0
    for s in shocks:
        px = price * (1.0 + s)
        pnl = (px - price) * net_lots * lot
        exposures.append({"shock": s, "mtm": round(pnl,2)})
        worst = min(worst, pnl)
    im = round(abs(worst), 2)  # conservative: worst loss across scenarios
    return {"member": member, "symbol": symbol, "net_lots": net_lots, "scenarios": exposures, "initial_margin_PM": im}
# ===== Risk controls (admin/runtime) =====

# ===== Surveillance / Alerts =====
class AlertIn(BaseModel):
    rule: str
    subject: str
    data: Dict[str, Any]
    severity: Literal["info","warn","high"] = "info"

@app.get("/surveil/alerts", tags=["Surveillance"], summary="List alerts")
async def surveil_list(limit:int=100):
    rows = await database.fetch_all(
        "SELECT * FROM surveil_alerts ORDER BY created_at DESC LIMIT :l",
        {"l": limit}
    )
    return [dict(r) for r in rows]

@app.post("/surveil/alert", tags=["Surveillance"], summary="Create alert")
async def surveil_create(a: AlertIn):
    aid = str(uuid.uuid4())
    await database.execute("""
      INSERT INTO surveil_alerts(alert_id, rule, subject, data, severity)
      VALUES (:id,:r,:s,:d::jsonb,:sev)
    """, {"id": aid, "r": a.rule, "s": a.subject, "d": json.dumps(a.data), "sev": a.severity})
    return {"alert_id": aid}

# Example: lightweight spoofing detector you can call on trade/order events
async def surveil_spoof_check(symbol: str, owner: str, side: str, placed: float, canceled: float, window_s:int=5):
    try:
        if placed >= 5 and canceled/placed > 0.8:
            await surveil_create(AlertIn(
                rule="spoof_like",
                subject=owner,
                data={"symbol":symbol,"side":side,"placed":placed,"canceled":canceled,"window_s":window_s},
            ))
    except Exception:
        pass

class AnomIn(BaseModel):
    member: str
    symbol: str
    as_of: date
    features: dict

@app.post("/ml/anomaly", tags=["Surveillance"])
async def ml_anomaly(a: AnomIn):
    # Placeholder: simple z-score style
    score = min(1.0, max(0.0, (a.features.get("cancel_rate", 0)*0.6 + a.features.get("gps_var", 0)*0.4)))
    await database.execute("""
      INSERT INTO public.anomaly_scores(member, symbol, as_of, score, features)
      VALUES (:m, :s, :d, :sc, :f::jsonb)
    """, {"m": a.member, "s": a.symbol, "d": a.as_of, "sc": score, "f": json.dumps(a.features)})

    if score > 0.85:
        await surveil_create(AlertIn(
            rule="ml_anomaly_high",
            subject=a.member,
            data={"symbol": a.symbol, "score": score, "features": a.features},
            severity="high"
        ))
    return {"score": score}
# ===== Surveillance / Alerts =====

# -------- Surveillance: case management + rules --------
@app.on_event("startup")
async def _ensure_surv_cases():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS surveil_cases(
      case_id UUID PRIMARY KEY,
      rule TEXT NOT NULL,
      subject TEXT NOT NULL,
      opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      status TEXT NOT NULL DEFAULT 'open',
      notes TEXT
    );
    """)

@app.post("/surveil/case/open", tags=["Surveillance"])
async def case_open(rule: str, subject: str, notes: str=""):
    cid = str(uuid.uuid4())
    await database.execute("""INSERT INTO surveil_cases(case_id,rule,subject,notes) VALUES (:i,:r,:s,:n)""",
                           {"i":cid,"r":rule,"s":subject,"n":notes})
    return {"case_id": cid, "status": "open"}

@app.post("/surveil/rules/run", tags=["Surveillance"])
async def run_rules(symbol: str, window_minutes: int = 5):
    # Example: wash trade heuristic (same owner both sides within window)
    rows = await database.fetch_all("""
      SELECT t1.trade_id a, t2.trade_id b, t1.buy_owner, t1.sell_owner, t2.buy_owner, t2.sell_owner
      FROM clob_trades t1 JOIN clob_trades t2 ON t1.symbol=t2.symbol
      WHERE t1.symbol=:s AND abs(EXTRACT(EPOCH FROM (t1.created_at - t2.created_at))) <= :w*60
        AND (t1.buy_owner=t2.sell_owner OR t1.sell_owner=t2.buy_owner)
      LIMIT 100
    """, {"s":symbol, "w":window_minutes})
    flagged = len(rows)
    if flagged:
        await surveil_create(AlertIn(rule="wash_like", subject=symbol, data={"pairs": flagged}, severity="high"))
    return {"ok": True, "wash_like_pairs": flagged}
# -------- Surveillance: case management + rules --------

# ---- Market data websocket ----
@app.websocket("/md/ws")
async def md_ws(ws: WebSocket):
    await ws.accept()
    _md_subs.add(ws)
    try:
        await ws.send_json({"type": "hello", "seq": _md_seq})
        while True:
            # Optional: receive pings or client filter updates
            _ = await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _md_subs.discard(ws)

async def _md_broadcast(msg: Dict[str, Any]):
    global _md_seq
    _md_seq += 1
    msg["seq"] = _md_seq
    dead = []
    for s in list(_md_subs):
        try:
            await s.send_json(msg)
        except Exception:
            dead.append(s)
    for s in dead:
        _md_subs.discard(s)

async def _publish_book(symbol: str) -> None:
    if not MD_BOOK_PUSH:
        return
    try:
        now_ms = time.time_ns() // 1_000_000
        last = _md_last_push_ms.get(symbol, 0)
        if now_ms - last < MD_BOOK_MIN_MS:
            return
        _md_last_push_ms[symbol] = now_ms

        bids = await database.fetch_all("""
          SELECT price, SUM(qty_open) AS qty
            FROM clob_orders
           WHERE symbol=:s AND side='buy' AND status='open'
           GROUP BY price ORDER BY price DESC LIMIT :d
        """, {"s": symbol, "d": MD_BOOK_DEPTH})
        asks = await database.fetch_all("""
          SELECT price, SUM(qty_open) AS qty
            FROM clob_orders
           WHERE symbol=:s AND side='sell' AND status='open'
           GROUP BY price ORDER BY price ASC LIMIT :d
        """, {"s": symbol, "d": MD_BOOK_DEPTH})

        await _md_broadcast({
            "type": "book",
            "symbol": symbol,
            "bids": [{"price": float(r["price"]), "qty": float(r["qty"])} for r in bids],
            "asks": [{"price": float(r["price"]), "qty": float(r["qty"])} for r in asks],
        })
    except Exception:
        pass

# ---- RFQ (Request for Quote) ----
class RFQIn(BaseModel):
    symbol: str
    side: Literal["buy","sell"]
    quantity_lots: Decimal
    price_limit: Optional[Decimal] = None
    expires_at: datetime

class RFQQuoteIn(BaseModel):
    price: Decimal
    qty_lots: Decimal

async def _rfq_symbol(rfq_id: str) -> str:
    row = await database.fetch_one("SELECT symbol FROM rfqs WHERE rfq_id=:id", {"id": rfq_id})
    if not row:
        raise HTTPException(404, "RFQ not found")
    return row["symbol"]

@limiter.limit("30/minute")
@app.post("/rfq", tags=["RFQ"], summary="Create RFQ")
async def create_rfq(r: RFQIn, request: Request, _=Depends(csrf_protect)):
    key = _idem_key(request)
    hit = None
    if key:
        hit = await idem_get(key)
    if hit: return hit

    user = (request.session.get("username") if hasattr(request, "session") else None) or "anon"
    require_entitlement(user, "rfq.post")
    rfq_id = str(uuid.uuid4())
    await database.execute(
        """
        INSERT INTO rfqs(rfq_id, symbol, side, quantity_lots, price_limit, expires_at, creator)
        VALUES (:id, :symbol, :side, :qty, :pl, :exp, :creator)
        """,
        {
            "id": rfq_id,
            "symbol": r.symbol,
            "side": r.side,
            "qty": str(r.quantity_lots),
            "pl": str(r.price_limit) if r.price_limit is not None else None,
            "exp": r.expires_at,
            "creator": user,
        },
    )
    # optional: broadcast a “new RFQ” tick
    try:
        await _md_broadcast({"type": "rfq.new", "rfq_id": rfq_id, "symbol": r.symbol, "side": r.side, "qty_lots": float(r.quantity_lots)})
    except Exception:
        pass
    resp = {"rfq_id": rfq_id, "status": "open"}
    return await _idem_guard(request, key, resp)

@limiter.limit("60/minute")
@app.post("/rfq/{rfq_id}/quote", tags=["RFQ"], summary="Respond to RFQ")
async def quote_rfq(rfq_id: str, q: RFQQuoteIn, request: Request, _=Depends(csrf_protect)):
    key = _idem_key(request)
    hit = None
    if key:
        hit = await idem_get(key)
    if hit: return hit

    user = (request.session.get("username") if hasattr(request, "session") else None) or "anon"
    require_entitlement(user, "rfq.quote")
    symbol = await _rfq_symbol(rfq_id)
    await price_band_check(symbol, float(q.price))
    quote_id = str(uuid.uuid4())
    await database.execute(
        """
        INSERT INTO rfq_quotes(quote_id, rfq_id, responder, price, qty_lots)
        VALUES (:qid, :rid, :resp, :p, :q)
        """,
        {"qid": quote_id, "rid": rfq_id, "resp": user, "p": str(q.price), "q": str(q.qty_lots)},
    )
    try:
        await _md_broadcast({"type": "rfq.quote", "rfq_id": rfq_id, "quote_id": quote_id, "symbol": symbol, "price": float(q.price), "qty_lots": float(q.qty_lots)})
    except Exception:
        pass
    resp = {"quote_id": quote_id}
    return await _idem_guard(request, key, resp)

@limiter.limit("30/minute")
@app.post("/rfq/{rfq_id}/award", tags=["RFQ"], summary="Award RFQ to a quote → records deal & broadcasts")
async def award_rfq(rfq_id: str, quote_id: str, request: Request):
    key = _idem_key(request)
    hit = None
    if key:
        hit = await idem_get(key)
    if hit: return hit
    user = (request.session.get("username") if hasattr(request, "session") else None) or "anon"
    require_entitlement(user, "rfq.award")

    rfq = await database.fetch_one("SELECT * FROM rfqs WHERE rfq_id=:id", {"id": rfq_id})
    if not rfq:
        raise HTTPException(404, "RFQ not found")
    if rfq["status"] != "open":
        raise HTTPException(409, "RFQ not open")

    q = await database.fetch_one("SELECT * FROM rfq_quotes WHERE quote_id=:q AND rfq_id=:r", {"q": quote_id, "r": rfq_id})
    if not q:
        raise HTTPException(404, "Quote not found")

    price = float(q["price"])
    qty   = float(q["qty_lots"])
    symbol = rfq["symbol"]
    await price_band_check(symbol, price)

    # Determine sides for record
    creator = rfq["creator"]
    responder = q["responder"]
    buy_owner  = creator if rfq["side"] == "buy" else responder
    sell_owner = creator if rfq["side"] == "sell" else responder

    deal_id = str(uuid.uuid4())
    async with database.transaction():
        await database.execute(
            """
            INSERT INTO rfq_deals(deal_id, rfq_id, quote_id, symbol, price, qty_lots, buy_owner, sell_owner)
            VALUES (:id, :rid, :qid, :s, :p, :q, :b, :s2)
            """,
            {"id": deal_id, "rid": rfq_id, "qid": quote_id, "s": symbol, "p": price, "q": qty, "b": buy_owner, "s2": sell_owner},
        )
        await database.execute("UPDATE rfqs SET status='filled' WHERE rfq_id=:id", {"id": rfq_id})

    # Broadcast trade tick (RFQ-style)
    try:
        await _md_broadcast({"type": "trade", "venue": "RFQ", "symbol": symbol, "price": price, "qty_lots": qty})
    except Exception:
        pass

    resp = {"deal_id": deal_id, "status": "done"}
    return await _idem_guard(request, key, resp)

# ===================== CLOB (Symbol-level order book) =====================
from fastapi import APIRouter
clob_router = APIRouter(prefix="/clob", tags=["CLOB"])

# -- CLOB helpers (positions & limits) --
async def _add_position(owner: str, symbol: str, delta_qty: float) -> None:
    await database.execute("""
      INSERT INTO clob_positions(owner, symbol, qty_lots)
      VALUES (:o, :s, :q)
      ON CONFLICT (owner, symbol) DO UPDATE
      SET qty_lots = clob_positions.qty_lots + EXCLUDED.qty_lots
    """, {"o": owner, "s": symbol, "q": delta_qty})

async def _check_position_limit(owner: str, symbol: str, inc: float, side: str, is_sell: bool=False) -> None:
    # simple example limits; adjust or wire to a config table
    max_abs = float(os.getenv("CLOB_MAX_LOTS_PER_SYMBOL", "200"))
    row = await database.fetch_one(
        "SELECT qty_lots FROM clob_positions WHERE owner=:o AND symbol=:s",
        {"o": owner, "s": symbol}
    )
    cur = float(row["qty_lots"]) if row else 0.0
    new = cur + (inc if side == "buy" else -inc)
    if abs(new) > max_abs:
        raise HTTPException(429, f"Position limit exceeded for {owner} on {symbol}: {new} lots (max {max_abs})")

class CLOBOrderIn(BaseModel):
    symbol: str
    side: Literal["buy","sell"]
    price: Decimal
    qty_lots: Decimal
    tif: Literal["day","ioc","fok"] = "day"

async def _get_fees(symbol: str):
    row = await database.fetch_one(
        "SELECT maker_bps, taker_bps, min_fee_cents FROM public.fee_schedule WHERE symbol=:s", {"s": symbol}
    )
    if not row: return (0.0, 0.0, 0.0)
    return (float(row["maker_bps"]), float(row["taker_bps"]), float(row["min_fee_cents"]))

async def _latest_settle_on_or_before(symbol: str, d: date) -> Optional[float]:
    row = await database.fetch_one("""
      SELECT settle FROM public.settlements WHERE symbol=:s AND as_of<=:d
      ORDER BY as_of DESC LIMIT 1
    """, {"s":symbol, "d":d})
    return float(row["settle"]) if row else None

async def _distinct_members(as_of: date) -> List[str]:
    rows = await database.fetch_all("""
      WITH m1 AS (
        SELECT buy_owner AS m FROM public.clob_trades WHERE created_at::date<=:d
        UNION
        SELECT sell_owner AS m FROM public.clob_trades WHERE created_at::date<=:d
      ), m2 AS (
        SELECT owner AS m FROM public.clob_positions
      )
      SELECT DISTINCT m FROM (SELECT * FROM m1 UNION SELECT * FROM m2) x WHERE m IS NOT NULL
    """, {"d":as_of})
    return [r["m"] for r in rows]

async def _member_day_trades(member: str, as_of: date):
    rows = await database.fetch_all("""
      SELECT t.trade_id, t.symbol, t.price, t.qty_lots, t.buy_owner, t.sell_owner,
             t.maker_order, t.taker_order, t.created_at,
             ob.owner AS maker_owner, ot.owner AS taker_owner
      FROM public.clob_trades t
      LEFT JOIN public.clob_orders ob ON ob.order_id = t.maker_order
      LEFT JOIN public.clob_orders ot ON ot.order_id = t.taker_order
      WHERE t.created_at::date = :d AND (t.buy_owner=:m OR t.sell_owner=:m)
      ORDER BY t.created_at ASC
    """, {"d":as_of, "m":member})
    return [dict(r) for r in rows]

async def _position_net(member: str, symbol: str) -> float:
    row = await database.fetch_one(
        "SELECT qty_lots FROM public.clob_positions WHERE owner=:o AND symbol=:s", {"o":member,"s":symbol}
    )
    return float(row["qty_lots"]) if row else 0.0

def _fee_amount(notional: float, bps: float, min_cents: float) -> float:
    fee = max(notional * (bps/10000.0), (min_cents/100.0))
    return _round2(fee)

async def _build_member_statement(member: str, as_of: date):
    trades = await _member_day_trades(member, as_of)
    # aggregate by symbol
    per_symbol = {}
    total_fees = 0.0
    for tr in trades:
        sym = tr["symbol"]
        lot = _lot_size(sym)
        qty = float(tr["qty_lots"])
        px  = float(tr["price"])
        notional = qty * lot * px
        maker_bps, taker_bps, min_cents = await _get_fees(sym)
        # maker/taker role
        is_maker = (tr.get("maker_owner") == member)
        bps = maker_bps if is_maker else taker_bps
        fee = _fee_amount(notional, bps, min_cents)
        total_fees += fee
        side = "BUY" if tr["buy_owner"] == member else "SELL"
        per_symbol.setdefault(sym, {"qty_lots":0.0, "notional":0.0, "trades":[]})
        per_symbol[sym]["qty_lots"] += qty if side=="BUY" else -qty
        per_symbol[sym]["notional"] += notional if side=="BUY" else -notional
        per_symbol[sym]["trades"].append({
            "time": str(tr["created_at"]),
            "side": side, "price": px, "qty_lots": qty, "lot": lot,
            "notional": _round2(notional), "role": "MAKER" if is_maker else "TAKER", "fee": fee
        })

    # MTM versus prior settle for end-of-day net
    mtm_total = 0.0
    mtm_lines = []
    for sym, agg in per_symbol.items():
        settle_today = await _latest_settle_on_or_before(sym, as_of)
        settle_prev  = await _latest_settle_on_or_before(sym, (as_of - timedelta(days=1)))
        net_pos = await _position_net(member, sym)   # end-of-day net (lots)
        if settle_today is not None and settle_prev is not None:
            mtm = (settle_today - settle_prev) * net_pos * _lot_size(sym)
            mtm_total += mtm
            mtm_lines.append((sym, net_pos, settle_prev, settle_today, _round2(mtm)))

    # CSV (as string)
    csv_lines = []
    csv_lines.append("Member,As Of")
    csv_lines.append(f"{member},{as_of}")
    csv_lines.append("")
    csv_lines.append("Trades (time, symbol, side, qty_lots, lot, price, notional, role, fee)")
    csv_lines.append("time,symbol,side,qty_lots,lot,price,notional,role,fee")
    for sym, agg in per_symbol.items():
        for t in agg["trades"]:
            csv_lines.append(f"{t['time']},{sym},{t['side']},{t['qty_lots']},{t['lot']},{t['price']},{t['notional']},{t['role']},{t['fee']}")
    csv_lines.append("")
    csv_lines.append("MTM (symbol, net_lots_eod, settle_prev, settle_today, mtm)")
    csv_lines.append("symbol,net_lots_eod,settle_prev,settle_today,mtm")
    for (sym, net, sp, st, mtm) in mtm_lines:
        csv_lines.append(f"{sym},{net},{sp},{st},{mtm}")
    csv_lines.append("")
    csv_lines.append(f"Total Fees USD,{_round2(total_fees)}")
    csv_lines.append(f"Total MTM USD,{_round2(mtm_total)}")
    csv_text = "\n".join(csv_lines)

    # PDF
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=LETTER)
    w, h = LETTER
    y = h - 1*inch
    c.setFont("Helvetica-Bold", 14); c.drawString(1*inch, y, f"BRidge Statement — {member}"); y -= 16
    c.setFont("Helvetica", 10); c.drawString(1*inch, y, f"As Of: {as_of}"); y -= 18
    c.drawString(1*inch, y, f"Total Fees: ${_round2(total_fees)}   Total MTM: ${_round2(mtm_total)}"); y -= 22
    c.setFont("Helvetica-Bold", 10); c.drawString(1*inch, y, "Trades"); y -= 14
    c.setFont("Helvetica", 9)
    for sym, agg in per_symbol.items():
        c.drawString(1*inch, y, f"{sym}"); y -= 12
        for t in agg["trades"]:
            line = f"{t['time']}  {t['side']}  {t['qty_lots']}@{t['price']}  lot={t['lot']}  ${t['notional']}  {t['role']} fee=${t['fee']}"
            c.drawString(1.2*inch, y, line); y -= 11
            if y < 1*inch: c.showPage(); y = h - 1*inch; c.setFont("Helvetica", 9)
    c.setFont("Helvetica-Bold", 10); c.drawString(1*inch, y, "MTM"); y -= 14
    c.setFont("Helvetica", 9)
    for (sym, net, sp, st, mtm) in mtm_lines:
        c.drawString(1.2*inch, y, f"{sym}: net={net}  prev={sp}  today={st}  MTM=${mtm}"); y -= 11
        if y < 1*inch: c.showPage(); y = h - 1*inch; c.setFont("Helvetica", 9)
    c.showPage(); c.save()
    pdf_bytes = buf.getvalue(); buf.close()

    return csv_text, pdf_bytes

@clob_router.get("/statement", summary="Generate statement (CSV or PDF)")
async def clob_statement(member: str, as_of: date, fmt: Literal["csv","pdf"]="pdf"):
    csv_text, pdf_bytes = await _build_member_statement(member, as_of)
    if fmt == "csv":
        return StreamingResponse(iter([csv_text]), media_type="text/csv",
                                 headers={"Content-Disposition": f'attachment; filename="{member}_{as_of}.csv"'})
    return StreamingResponse(BytesIO(pdf_bytes), media_type="application/pdf",
                             headers={"Content-Disposition": f'attachment; filename="{member}_{as_of}.pdf"'})

# ===== Deterministic sequencer (single worker) =====
_event_queue: asyncio.Queue = asyncio.Queue()
_SEQUENCER_STARTED = False

async def _sequencer_worker():
    while True:
        ev_id, topic, payload = await _event_queue.get()
        try:
            # mark as processing (best-effort)
            try:
                await database.execute(
                    "UPDATE matching_events SET processed_at=NOW() WHERE id=:i",
                    {"i": ev_id}
                )
            except Exception:
                pass

            if topic == "ORDER":
                await _clob_match(payload["order_id"])

            elif topic == "CANCEL":
                req = type("Req", (), {"session": {"username": payload.get("user", "system")}})
                await clob_cancel_order(payload["order_id"], request=req)

            elif topic == "ORDER_FUTURES":
                await _fut_order_match(payload["order_id"])

            elif topic == "MODIFY_FUTURES":
                await _fut_modify(payload["order_id"], payload.get("new_price"), payload.get("new_qty"))

            elif topic == "CANCEL_FUTURES":
                await _fut_cancel(payload["order_id"], payload.get("user"))

        except Exception as e:
            try:
                logger.warn("sequencer_fail", topic=topic, err=str(e))
            except Exception:
                pass
        finally:
            _event_queue.task_done()


@app.on_event("startup")
async def _start_sequencer():
    global _SEQUENCER_STARTED
    if not _SEQUENCER_STARTED:
        asyncio.create_task(_sequencer_worker())
        _SEQUENCER_STARTED = True

# ===== Futures matching handlers (sequenced) =====
async def _fut_match(order_id: str):
    """Sequenced matching for a single futures order id (already inserted)."""
    MAX_RETRY = 3
    for attempt in range(1, MAX_RETRY+1):
        try:
            async with database.transaction():
                row = await database.fetch_one("SELECT * FROM orders WHERE id=:id FOR UPDATE", {"id": order_id})
                if not row:
                    return

                listing_id = row["listing_id"]
                side = row["side"]
                price = float(row["price"])
                remaining = float(row["qty_open"])

                await _require_listing_trading(listing_id)
                contract_size = await _get_contract_size(listing_id)

                is_market = (side == "SELL" and price == 0.0)

                cap_min = cap_max = None
                if is_market:
                    ref = await _ref_price_for(listing_id)
                    if ref is not None:
                        lo = ref * (1.0 - PRICE_BAND_PCT)
                        hi = ref * (1.0 + PRICE_BAND_PCT)
                        if side == "BUY": cap_max = hi
                        else:             cap_min = lo

                # ----- TIF handling helpers -----
                tif = (row.get("tif") or "GTC").upper()
                # for FOK precheck: compute total fillable (<= price for BUY, >= price for SELL)
                async def _total_fillable():
                    if side == "BUY":
                        cond = "price <= :p"
                        vals = {"l": listing_id, "p": price}
                        if is_market and cap_max is not None:
                            cond = "(:cap IS NULL OR price <= :cap)"
                            vals = {"l": listing_id, "cap": cap_max}
                        q = f"""SELECT COALESCE(SUM(qty_open),0) q
                                FROM orders WHERE listing_id=:l AND side='SELL' AND status IN ('NEW','PARTIAL')
                                  AND {cond}"""
                        r = await database.fetch_one(q, vals)
                    else:
                        cond = "price >= :p"
                        vals = {"l": listing_id, "p": price}
                        if is_market and cap_min is not None:
                            cond = "(:cap IS NULL OR price >= :cap)"
                            vals = {"l": listing_id, "cap": cap_min}
                        q = f"""SELECT COALESCE(SUM(qty_open),0) q
                                FROM orders WHERE listing_id=:l AND side='BUY' AND status IN ('NEW','PARTIAL')
                                  AND {cond}"""
                        r = await database.fetch_one(q, vals)
                    return float(r["q"] or 0.0)

                if tif == "FOK":
                    fillable = await _total_fillable()
                    if fillable + 1e-12 < remaining:
                        # all-or-nothing: cancel
                        await database.execute("UPDATE orders SET status='CANCELLED', qty_open=0 WHERE id=:id", {"id": order_id})
                        await _audit_order(order_id, "CANCELLED", 0, "FOK not fillable")
                        return

                # pull opposite orders with FOR UPDATE, price/time priority
                if side == "BUY":
                    if is_market:
                        opp = await database.fetch_all("""
                          SELECT * FROM orders
                           WHERE listing_id=:l AND side='SELL' AND status IN ('NEW','PARTIAL')
                             AND (:cap_max IS NULL OR price <= :cap_max)
                           ORDER BY price ASC, created_at ASC
                           FOR UPDATE
                        """, {"l": listing_id, "cap_max": cap_max})
                    else:
                        opp = await database.fetch_all("""
                          SELECT * FROM orders
                           WHERE listing_id=:l AND side='SELL' AND status IN ('NEW','PARTIAL') AND price <= :p
                           ORDER BY price ASC, created_at ASC
                           FOR UPDATE
                        """, {"l": listing_id, "p": price})
                else:  # SELL
                    if is_market:
                        opp = await database.fetch_all("""
                          SELECT * FROM orders
                           WHERE listing_id=:l AND side='BUY' AND status IN ('NEW','PARTIAL')
                             AND (:cap_min IS NULL OR price >= :cap_min)
                           ORDER BY price DESC, created_at ASC
                           FOR UPDATE
                        """, {"l": listing_id, "cap_min": cap_min})
                    else:
                        opp = await database.fetch_all("""
                          SELECT * FROM orders
                           WHERE listing_id=:l AND side='BUY' AND status IN ('NEW','PARTIAL') AND price >= :p
                           ORDER BY price DESC, created_at ASC
                           FOR UPDATE
                        """, {"l": listing_id, "p": price})

                for r in opp:
                    if remaining <= 0:
                        break

                    open_qty = float(r["qty_open"])
                    if open_qty <= 0:
                        continue

                    trade_qty = min(remaining, open_qty)
                    trade_px  = float(r["price"])

                    if is_market:
                        if side == "BUY" and cap_max is not None and trade_px > cap_max: break
                        if side == "SELL" and cap_min is not None and trade_px < cap_min: break

                    buy_id  = order_id if side == "BUY" else r["id"]
                    sell_id = r["id"]   if side == "BUY" else order_id

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
                    """, {"oid": r["id"]})
                    if not part_a or not part_b:
                        raise HTTPException(400, "margin account not found")

                    need_a = float(part_a["initial_pct"]) * notional
                    need_b = float(part_b["initial_pct"]) * notional
                    if float(part_a["balance"]) < need_a: raise HTTPException(402, f"insufficient initial margin for account {part_a['aid']}")
                    if float(part_b["balance"]) < need_b: raise HTTPException(402, f"insufficient initial margin for account {part_b['aid']}")

                    await _adjust_margin(part_a["aid"], -need_a, "initial_margin", order_id)
                    await _adjust_margin(part_b["aid"], -need_b, "initial_margin", r["id"])

                    trade_id = str(uuid.uuid4())
                    await database.execute("""
                      INSERT INTO trades (id, buy_order_id, sell_order_id, listing_id, price, qty)
                      VALUES (:id,:b,:s,:l,:px,:q)
                    """, {"id": trade_id, "b": buy_id, "s": sell_id, "l": listing_id, "px": trade_px, "q": trade_qty})

                    # positions (NEW)
                    await _update_position(part_a["aid"], listing_id, +trade_qty if side == "BUY" else -trade_qty)
                    await _update_position(part_b["aid"], listing_id, -trade_qty if side == "BUY" else +trade_qty)

                    # ---- fees (maker/taker) for futures ----
                    maker_bps, taker_bps, min_cents = await _fut_get_fees(listing_id)

                    # notional = contract_size * qty * price
                    notional = contract_size * trade_qty * trade_px

                    # reuse your global _fee_amount helper (already defined for CLOB)
                    taker_fee = _fee_amount(notional, taker_bps, min_cents)
                    maker_fee = _fee_amount(notional, maker_bps, min_cents)

                    taker_acct = part_a["aid"]   # incoming order's account (taker)
                    maker_acct = part_b["aid"]   # resting order's account (maker)

                    if taker_fee:
                        await _adjust_margin(taker_acct, -taker_fee, "fee_taker", trade_id)
                    if maker_fee:
                        await _adjust_margin(maker_acct, -maker_fee, "fee_maker", trade_id)
                    # ---- /fees ----
                    
                    # ---- surveillance: futures fill soft-signal ----
                    try:
                        await audit_append("surveil", "fut.trade", "trade", trade_id, {
                            "listing": str(listing_id),
                            "price": trade_px,
                            "qty": trade_qty
                        })
                    except Exception:
                        pass
                    # ---- /surveillance ----

                    # ------- update contra ------
                    r_new_open = open_qty - trade_qty
                    r_status = "FILLED" if r_new_open <= 0 else "PARTIAL"
                    await database.execute("UPDATE orders SET qty_open=:qo, status=:st WHERE id=:id",
                                           {"qo": r_new_open, "st": r_status, "id": r["id"]})
                    await _audit_order(r["id"], r_status, r_new_open, "matched")

                    # update this order
                    remaining -= trade_qty
                    my_status = "FILLED" if remaining <= 0 else "PARTIAL"
                    await database.execute("UPDATE orders SET qty_open=:qo, status=:st WHERE id=:id",
                                           {"qo": remaining, "st": my_status, "id": order_id})
                    await _audit_order(order_id, my_status, remaining, "matched")

                # TIF: IOC (cancel remainder for both limit & market)
                if tif == "IOC" and remaining > 0:
                    await database.execute("UPDATE orders SET status='CANCELLED', qty_open=0 WHERE id=:id", {"id": order_id})
                    await _audit_order(order_id, "CANCELLED", 0, "IOC remainder")

                # market fallback (legacy SELL-market IOC) remains covered by IOC logic
            return
        except Exception as e:
            # retry only on serialization/deadlock
            msg = (str(e) or "").lower()
            if attempt < MAX_RETRY and ("serialization" in msg or "deadlock" in msg):
                continue
            raise
# --------- Futures matching handlers (sequenced) -------------

async def _fut_modify_match(order_id: str, new_price: float | None, new_qty: float | None):
    """Apply modify (price/qty), then rematch the order sequenced."""
    async with database.transaction():
        row = await database.fetch_one("SELECT * FROM orders WHERE id=:id FOR UPDATE", {"id": order_id})
        if not row:
            return
        if row["status"] in ("FILLED", "CANCELLED"):
            return

        old_price = float(row["price"])
        old_qty   = float(row["qty"])
        old_open  = float(row["qty_open"])
        filled    = old_qty - old_open

        total = float(new_qty) if new_qty is not None else old_qty
        if total < filled:
            # don't throw in sequencer; just ignore bad amend
            return

        price = float(new_price) if new_price is not None else old_price
        new_open = total - filled
        new_status = "FILLED" if new_open <= 0 else ("PARTIAL" if filled > 0 else "NEW")

        await database.execute("""
          UPDATE orders SET price=:p, qty=:qt, qty_open=:qo, status=:st WHERE id=:id
        """, {"p": price, "qt": total, "qo": new_open, "st": new_status, "id": order_id})
        await _audit_order(order_id, "MODIFY", new_open, "sequenced amend")

    # Attempt rematch after modify
    await _fut_match(order_id)


async def _fut_order_match(order_id: str):
    """Order entry handler -> run sequenced matching."""
    await _fut_match(order_id)


async def _fut_modify(order_id: str, new_price: float | None, new_qty: float | None):
    await _fut_modify_match(order_id, new_price, new_qty)


# ===== Futures cancel (orders table, not CLOB) =====
async def _fut_cancel(order_id: str, user: str | None = None):
    row = await database.fetch_one("SELECT account_id, status FROM orders WHERE id=:id", {"id": order_id})
    if not row:
        return  # nothing to do
    if row["status"] in ("FILLED","CANCELLED"):
        return
    # (OPTIONAL) if you map user -> account ownership, enforce here. Otherwise skip.
    await database.execute(
        "UPDATE orders SET status='CANCELLED', qty_open=0 WHERE id=:id",
        {"id": order_id}
    )
    await _audit_order(order_id, "CANCELLED", 0, f"user cancel ({user or 'system'})")

# ===== /Futures matching handlers =====

@limiter.limit("120/minute")
@clob_router.post("/orders", summary="Place order")
async def clob_place_order(o: CLOBOrderIn, request: Request):
    key = _idem_key(request)
    hit = None
    if key:
        hit = await idem_get(key)
    if hit: return hit

    user = (request.session.get("username") if hasattr(request, "session") else None) or "anon"
    require_entitlement(user, "clob.trade")
    if _KILL_SWITCH.get(user):
        raise HTTPException(423, "Kill switch active")
    await price_band_check(o.symbol, float(o.price))
    await _check_position_limit(user, o.symbol, float(o.qty_lots), o.side)

    order_id = str(uuid.uuid4())
    qty = float(o.qty_lots)

    await database.execute("""
      INSERT INTO clob_orders(order_id,symbol,side,price,qty_lots,qty_open,owner,tif,status)
      VALUES (:id,:s,:side,:p,:q,:q,:owner,:tif,'open')
    """, {"id": order_id, "s": o.symbol, "side": o.side, "p": str(o.price), "q": qty, "owner": user, "tif": o.tif})

    # Enqueue for deterministic matching (must be inside this async function)
    ev = await database.fetch_one(
        "INSERT INTO matching_events(topic,payload) VALUES ('ORDER', :p) RETURNING id",
        {"p": json.dumps({"order_id": order_id})}
    )
    await _event_queue.put((int(ev["id"]), "ORDER", {"order_id": order_id}))
    await _publish_book(o.symbol)
    resp = {"order_id": order_id, "queued": True}
    return await _idem_guard(request, key, resp)

# ===== Settlement (VWAP from recent CLOB trades) =====
@app.post("/settlement/publish", tags=["Settlement"], summary="Publish daily settle")
async def settle_publish(symbol: str, as_of: date, method: str = "vwap_last60m"):
    # naive vwap: last 60m clob trades
    rows = await database.fetch_all("""
      SELECT price, qty_lots FROM clob_trades
      WHERE symbol = :s AND created_at >= now() - interval '60 minutes'
    """, {"s": symbol})
    if rows:
        qty_sum = sum(float(r["qty_lots"]) for r in rows)
        vwap = (
            sum(float(r["price"]) * float(r["qty_lots"]) for r in rows)
            / max(qty_sum, 1e-9)
        )
    else:
        # fallback to last trade
        r = await database.fetch_one(
            "SELECT price FROM clob_trades WHERE symbol = :s ORDER BY created_at DESC LIMIT 1",
            {"s": symbol}
        )
        vwap = float(r["price"]) if r else 0.0

    await database.execute("""
      INSERT INTO settlements(as_of, symbol, settle, method)
      VALUES (:d, :s, :p, :m)
      ON CONFLICT (as_of, symbol) DO UPDATE
        SET settle = EXCLUDED.settle, method = EXCLUDED.method
    """, {"d": as_of, "s": symbol, "p": vwap, "m": method})

    # --- audit (best-effort) ---
    try:
        await audit_append("admin", "settlement.publish", "settlement",
                           f"{symbol}:{as_of}", {"method": method, "settle": vwap})
    except Exception:
        pass

    return {"symbol": symbol, "as_of": str(as_of), "settle": round(vwap, 5), "method": method}

@app.post("/index/contracts/expire", tags=["Settlement"], summary="Expire monthly index futures to BR-Settle")
async def expire_month(tradable_symbol: str, as_of: date, request: Request):
    # _require_admin(request)  # uncomment to gate in production

    ic = await database.fetch_one(
        "SELECT * FROM index_contracts WHERE tradable_symbol = :t",
        {"t": tradable_symbol}
    )
    if not ic:
        raise HTTPException(404, "No such contract")

    root = ic["symbol"]  # e.g., "BR-CU"
    # map index root to underlying settle symbol
    under = {"BR-CU": "CU-SHRED-1M", "BR-AL": "AL-6061-1M"}.get(root)
    if not under:
        raise HTTPException(422, "No underlying mapping")

    row = await database.fetch_one(
        "SELECT settle FROM settlements WHERE symbol = :s AND as_of = :d",
        {"s": under, "d": as_of}
    )
    if not row:
        raise HTTPException(409, "No BR-Settle for expiry date")

    settle_px = float(row["settle"])

    # best-effort audit
    try:
        await audit_append(
            "system", "futures.expire", "index_contract", tradable_symbol,
            {"settle": settle_px, "as_of": str(as_of)}
        )
    except Exception:
        pass

    return {"tradable_symbol": tradable_symbol, "final_settle": settle_px, "as_of": str(as_of)}

# ===== Index (read APIs over settlements) =====
@app.get("/index/latest", tags=["Index"], summary="Latest settle per symbol")
async def index_latest():
    rows = await database.fetch_all("""
      SELECT DISTINCT ON (symbol) symbol, as_of, settle, method, created_at
      FROM public.settlements
      ORDER BY symbol, as_of DESC, created_at DESC
    """)
    return [dict(r) for r in rows]

@app.get("/index/history", tags=["Index"], summary="Historical settles for a symbol (JSON)")
async def index_history(symbol: str, start: date | None = None, end: date | None = None):
    sql = "SELECT as_of, settle, method, created_at FROM public.settlements WHERE symbol=:s"
    args = {"s": symbol}
    if start: sql += " AND as_of >= :a"; args["a"] = start
    if end:   sql += " AND as_of <= :b"; args["b"] = end
    sql += " ORDER BY as_of ASC"
    rows = await database.fetch_all(sql, args)
    return {"symbol": symbol, "rows": [dict(r) for r in rows]}

@app.get("/index/history.csv", tags=["Index"], summary="Historical settles for a symbol (CSV)")
async def index_history_csv(symbol: str, start: date | None = None, end: date | None = None):
    sql = "SELECT as_of, settle, method FROM public.settlements WHERE symbol=:s"
    args = {"s": symbol}
    if start: sql += " AND as_of >= :a"; args["a"] = start
    if end:   sql += " AND as_of <= :b"; args["b"] = end
    sql += " ORDER BY as_of ASC"
    rows = await database.fetch_all(sql, args)
    out = io.StringIO(); w = csv.writer(out)
    w.writerow(["date","settle","method"])
    for r in rows:
        w.writerow([r["as_of"], float(r["settle"]), r["method"]])
    out.seek(0)
    return StreamingResponse(
    iter([out.getvalue()]),
    media_type="text/csv",
    headers={"Content-Disposition": f'attachment; filename="{symbol}_history.csv"'}
)

@app.get("/index/tweet", tags=["Index"], summary="One-line tweet text for today’s BR-Settle")
async def index_tweet(as_of: date | None = None):
    d = as_of or datetime.utcnow().date()
    rows = await database.fetch_all(
        "SELECT symbol, settle FROM public.settlements WHERE as_of=:d ORDER BY symbol",
        {"d": d}
    )
    if not rows:
        return {"as_of": str(d), "text": "No BR-Settle today."}

    parts = [f"{r['symbol']} ${float(r['settle']):.4f}/lb" for r in rows]
    text = (
        f"Official BR-Settle ({d}): "
        + "  •  ".join(parts)
        + " — Method: VWAP last 60m (fallback last trade)."
    )
    return {"as_of": str(d), "text": text}

#-------------------- Settlement (VWAP from recent CLOB trades) =====

# ===== FIX shim → CLOB =====
class FixOrder(BaseModel):
    ClOrdID: str
    Symbol: str
    Side: Literal["1","2"]  # 1=buy,2=sell
    Price: float
    OrderQty: float
    TimeInForce: Optional[Literal["0","3","4"]] = "0"  # 0=Day,3=IOC,4=FOK
    SenderCompID: Optional[str] = None
@app.post("/fix/order", tags=["FIX"], summary="FIX Order shim → CLOB")
async def fix_order(o: FixOrder):
    side = "buy" if o.Side == "1" else "sell"
    tif  = {"0":"day","3":"ioc","4":"fok"}[o.TimeInForce or "0"]

    member = o.SenderCompID or "fix_member"
    # grant runtime entitlement for the member (sessionless)
    _ENTITLEMENTS[member].add("clob.trade")

    # Build a minimal Request-like object with session
    class _Req:
        session = {"username": member}

    # Route through the regular CLOB endpoint (it handles kill switch, bands, limits)
    try:
        return await clob_place_order(
            CLOBOrderIn(
                symbol=o.Symbol,
                side=side,
                price=Decimal(str(o.Price)),
                qty_lots=Decimal(str(o.OrderQty)),
                tif=tif,
            ),
            _Req,
        )
    except HTTPException:
        raise
    except Exception as e:
        try:
            logger.warn("fix_order_error", err=str(e))
        except Exception:
            pass
        raise HTTPException(400, "invalid FIX order")

@app.post("/fix/dropcopy", tags=["FIX"], summary="Receive Drop Copy (stub)")
async def fix_dropcopy(payload: dict):
    # Ack & optionally persist elsewhere for audit
    return {"status": "received", "bytes": len(json.dumps(payload))}
# ===== FIX shim → CLOB =====

@clob_router.delete("/orders/{order_id}", summary="Cancel order")
async def clob_cancel_order(order_id: str, request: Request):
    user = (request.session.get("username") if hasattr(request, "session") else None) or "anon"
    row = await database.fetch_one(
    "SELECT owner,status,symbol FROM clob_orders WHERE order_id=:id",
    {"id": order_id}
)
    if not row: raise HTTPException(404, "Order not found")
    if row["owner"] != user: raise HTTPException(403, "Not owner")
    if row["status"] != "open": return {"status": row["status"]}

    await database.execute(
        "UPDATE clob_orders SET status='cancelled', qty_open=0 WHERE order_id=:id",
        {"id": order_id}
    )
    # enqueue cancel (safe if already cancelled)
    ev = await database.fetch_one(
        "INSERT INTO matching_events(topic,payload) VALUES ('CANCEL', :p) RETURNING id",
        {"p": json.dumps({"order_id": order_id, "user": user})}
    )
    await _event_queue.put((int(ev["id"]), "CANCEL", {"order_id": order_id, "user": user}))
    await _publish_book(row["symbol"])   
    return {"status": "cancelled"}

@clob_router.get("/orderbook", summary="Best bid/ask + depth (simple)")
async def clob_orderbook(symbol: str, depth: int = 10):
    bids = await database.fetch_all("""
      SELECT price, SUM(qty_open) qty FROM clob_orders
      WHERE symbol=:s AND side='buy' AND status='open'
      GROUP BY price ORDER BY price DESC LIMIT :d
    """, {"s": symbol, "d": depth})
    asks = await database.fetch_all("""
      SELECT price, SUM(qty_open) qty FROM clob_orders
      WHERE symbol=:s AND side='sell' AND status='open'
      GROUP BY price ORDER BY price ASC LIMIT :d
    """, {"s": symbol, "d": depth})
    return {"symbol": symbol, "bids": [dict(b) for b in bids], "asks": [dict(a) for a in asks]}

async def _clob_match(order_id: str) -> float:
    o = await database.fetch_one("SELECT * FROM clob_orders WHERE order_id=:id", {"id": order_id})
    if not o or o["status"] != "open":
        return 0.0
    symbol, side, owner = o["symbol"], o["side"], o["owner"]
    price, open_qty = float(o["price"]), float(o["qty_open"])
    contra_side = "sell" if side == "buy" else "buy"
    price_sort = "ASC" if side == "buy" else "DESC"
    price_cmp = "<=" if side == "buy" else ">="

    rows = await database.fetch_all(f"""
      SELECT * FROM clob_orders
      WHERE symbol=:s AND side=:cs AND status='open' AND price {price_cmp} :p
      ORDER BY price {price_sort}, created_at ASC
    """, {"s": symbol, "cs": contra_side, "p": price})

    filled_total = 0.0
    for row in rows:
        if open_qty <= 0:
            break
        if row["owner"] == owner:
            continue  # self-match prevention

        trade_qty = min(open_qty, float(row["qty_open"]))
        trade_price = float(row["price"])
        trade_id = str(uuid.uuid4())
        buy_owner  = owner if side == "buy" else row["owner"]
        sell_owner = row["owner"] if side == "buy" else owner

        await _check_position_limit(buy_owner, symbol, trade_qty, "buy")
        await _check_position_limit(sell_owner, symbol, trade_qty, "sell", is_sell=True)

        await database.execute("""
          INSERT INTO clob_trades(trade_id,symbol,price,qty_lots,buy_owner,sell_owner,maker_order,taker_order)
          VALUES (:id,:s,:p,:q,:b,:se,:maker,:taker)
        """, {"id": trade_id, "s": symbol, "p": trade_price, "q": trade_qty, "b": buy_owner, "se": sell_owner,
              "maker": row["order_id"], "taker": order_id})

        # --- audit (best-effort; never break matching) ---
        try:
            await audit_append(buy_owner, "trade.fill", "trade", trade_id, {
                "symbol": symbol, "price": trade_price, "qty_lots": trade_qty,
                "maker_order": row["order_id"], "taker_order": order_id
            })
            await audit_append(sell_owner, "trade.fill", "trade", trade_id, {
                "symbol": symbol, "price": trade_price, "qty_lots": trade_qty,
                "maker_order": row["order_id"], "taker_order": order_id
            })
        except Exception:
            pass

        # order updates
        await database.execute(
            "UPDATE clob_orders SET qty_open = qty_open - :q WHERE order_id=:id",
            {"q": trade_qty, "id": row["order_id"]}
        )
        await database.execute(
            "UPDATE clob_orders SET qty_open = GREATEST(qty_open - :q, 0) WHERE order_id=:id",
            {"q": trade_qty, "id": order_id}
        )
        await database.execute(
            "UPDATE clob_orders SET status='filled' WHERE order_id=:id AND qty_open <= 0.0000001",
            {"id": row["order_id"]}
        )
        await database.execute(
            "UPDATE clob_orders SET status='filled' WHERE order_id=:id AND qty_open <= 0.0000001",
            {"id": order_id}
        )

        # positions + ticks
        await _add_position(buy_owner, symbol, +trade_qty)
        await _add_position(sell_owner, symbol, -trade_qty)

        await database.execute("""
          INSERT INTO md_ticks(symbol, price, qty_lots, kind)
          VALUES (:s,:p,:q,'trade')
        """, {"s": symbol, "p": trade_price, "q": trade_qty})
        await _md_broadcast({"type": "trade", "symbol": symbol, "price": trade_price, "qty_lots": trade_qty})

        open_qty -= trade_qty
        filled_total += trade_qty
    try:
        await _publish_book(symbol)
    except Exception:
        pass

@limiter.limit("180/minute")
@trade_router.post("/orders", summary="Place order (limit or market) and match")
async def place_order(ord_in: OrderIn, request: Request):    
    key = _idem_key(request)
    hit = None
    if key:
        hit = await idem_get(key)
        if hit: return hit

    user = (request.session.get("username") if hasattr(request, "session") else None) or ""
    require_entitlement(user, "trade.place")

    if _KILL_SWITCH.get(user):
        raise HTTPException(423, "Kill switch active")
    await _ensure_margin_account(ord_in.account_id)

    # LIMIT pre-check (MARKET guarded in matching)
    if ord_in.order_type == "LIMIT" and ord_in.price is not None:
        sym = await _symbol_for_listing(ord_in.listing_id)
        if sym:
            await price_band_check(sym, float(ord_in.price))

    order_id = str(uuid.uuid4())
    side = ord_in.side.upper()
    is_market = (ord_in.order_type == "MARKET")
    price = float(ord_in.price) if (ord_in.price is not None) else (0.0 if is_market and side == "SELL" else float("inf"))
    qty   = float(ord_in.qty)
    # validations
    if qty <= 0:
        raise HTTPException(400, "qty must be positive")
    if not is_market and (ord_in.price is None or float(ord_in.price) <= 0):
        raise HTTPException(400, "price must be positive for LIMIT orders")

    # --- cheap risk before enqueue (single copy only) ---
    params = await _get_margin_params(ord_in.account_id)
    if params.get("is_blocked"):
        raise HTTPException(402, "account blocked pending margin call")

    open_lots = await _sum_open_lots(ord_in.account_id)
    limit_lots = float(params.get("risk_limit_open_lots", 50))
    if open_lots + qty > limit_lots:
        raise HTTPException(429, f"risk limit exceeded: {open_lots}+{qty}>{limit_lots}")

    await _fut_check_position_limit(ord_in.account_id, ord_in.listing_id, qty, side)


    async with database.transaction():
        await database.execute("""
          INSERT INTO orders (id, account_id, listing_id, side, price, qty, qty_open, status, tif)
          VALUES (:id,:a,:l,:s,:p,:q,:q,'NEW',:tif)
        """, {"id": order_id, "a": ord_in.account_id, "l": ord_in.listing_id,
              "s": side, "p": (0 if is_market else price), "q": qty, "tif": ord_in.tif or "GTC"})
        await _audit_order(order_id, "NEW", qty, "order accepted")
        ev = await database.fetch_one(
            "INSERT INTO matching_events(topic,payload) VALUES ('ORDER_FUTURES', :p) RETURNING id",
            {"p": json.dumps({"order_id": order_id})}
        )

    await _event_queue.put((int(ev["id"]), "ORDER_FUTURES", {"order_id": order_id}))
    resp = {"order_id": order_id, "queued": True}
    return await _idem_guard(request, key, resp)

@trade_router.patch("/orders/{order_id}", summary="Modify order (price/qty) and rematch")
async def modify_order(order_id: str, body: ModifyOrderIn, request: Request):
    user = (request.session.get("username") if hasattr(request, "session") else None) or ""
    require_entitlement(user, "trade.modify")

    if body.price is None and body.qty is None:
        raise HTTPException(400, "provide price and/or qty to modify")

    # basic checks / enforce user↔account ownership mapping here
    row = await database.fetch_one("SELECT account_id, listing_id, side, status FROM orders WHERE id=:id", {"id": order_id})
    if not row:
        raise HTTPException(404, "order not found")
    if row["status"] in ("FILLED", "CANCELLED"):
        raise HTTPException(409, f"order is {row['status']} and cannot be modified")

    # ---- surveillance: spoof-like signal on amend ----
    try:
        sym_for_mod = await _symbol_for_listing(row["listing_id"])
        await surveil_spoof_check(
            symbol=sym_for_mod or "",
            owner=user or "unknown",
            side=row["side"] or "BUY",   
            placed=1,
            canceled=0,
            window_s=5
        )
    except Exception:
        pass
    # ---- surveillance spoof-like signal on amend ----

    # enqueue for sequencer; actual modify+rematch happens there
    ev = await database.fetch_one(
        "INSERT INTO matching_events(topic,payload) VALUES ('MODIFY_FUTURES', :p) RETURNING id",
        {"p": json.dumps({"order_id": order_id, "new_price": body.price, "new_qty": body.qty})}
    )
    await _event_queue.put((int(ev["id"]), "MODIFY_FUTURES", {
        "order_id": order_id, "new_price": body.price, "new_qty": body.qty
    }))
    return {"order_id": order_id, "queued": True}
    
@trade_router.delete("/orders/{order_id}", response_model=CancelOut, summary="Cancel remaining qty")
async def cancel_order(order_id: str, request: Request):
    user = (request.session.get("username") if hasattr(request, "session") else None) or ""

    # ---- surveillance: possible spoof-like signal on cancel ----
    try:
        o = await database.fetch_one(
            "SELECT listing_id, side FROM orders WHERE id=:id",
            {"id": order_id}
        )
        sym_for_cancel = (await _symbol_for_listing(o["listing_id"])) if o else None
        await surveil_spoof_check(
            symbol=sym_for_cancel or "",
            owner=user or "unknown",
            side=(o["side"] if (o and o.get("side")) else "BUY"),
            placed=0,
            canceled=1,
            window_s=5
        )
    except Exception:
        pass
    # ---- /surveillance ----

    # enqueue; sequencer will perform futures cancel (_fut_cancel)
    ev = await database.fetch_one(
        "INSERT INTO matching_events(topic,payload) VALUES ('CANCEL_FUTURES', :p) RETURNING id",
        {"p": json.dumps({"order_id": order_id, "user": user})}
    )
    await _event_queue.put((int(ev["id"]), "CANCEL_FUTURES", {"order_id": order_id, "user": user}))
    return {"id": order_id, "status": "QUEUED"}

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
app.include_router(clob_router)

#=================== /TRADING (Order Book) =====================
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
    "contracts.csv": CONTRACTS_EXPORT_SQL,

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

    try:
        if storage == "s3":
            if not os.getenv("S3_BUCKET"):
                return {"ok": True, "skipped": "s3 env missing", "path": path}
            return await _upload_to_s3(path, data)

        if not (os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_ROLE")):
            return {"ok": True, "skipped": "supabase env missing", "path": path}
        return await _upload_to_supabase(path, data)

    except Exception as e:
        try:
            logger.warn("snapshot_upload_failed", err=str(e), path=path)
        except Exception:
            pass
        return {"ok": False, "error": str(e), "path": path}

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

    # --- pull current marks/settles + positions/margin accounts ---
    pos = await database.fetch_all("""
      SELECT p.account_id, p.listing_id, p.net_qty,
             COALESCE(
               (SELECT fm.mark_price
                  FROM futures_marks fm
                 WHERE fm.listing_id = p.listing_id
                   AND fm.mark_date   <= :d
                 ORDER BY fm.mark_date DESC
                 LIMIT 1),
               (SELECT settle_price
                  FROM v_latest_settle v
                 WHERE v.listing_id = p.listing_id)
             ) AS settle_price,
             fp.contract_size_tons,
             m.balance,
             m.maintenance_pct
        FROM positions p
        JOIN futures_listings fl ON fl.id = p.listing_id
        JOIN futures_products fp ON fp.id = fl.product_id
        JOIN margin_accounts m   ON m.account_id = p.account_id
    """, {"d": dt})

    # --- previous day marks per listing (for variation PnL) ---
    prev_map: dict[str, float] = {}
    prev = await database.fetch_all("""
        SELECT DISTINCT ON (fm.listing_id) fm.listing_id, fm.mark_price
          FROM futures_marks fm
         WHERE fm.mark_date < :d
      ORDER BY fm.listing_id, fm.mark_date DESC
    """, {"d": dt})
    for r in prev:
        prev_map[str(r["listing_id"])] = float(r["mark_price"])

    results = []

    # --- atomic run ---
    async with database.transaction():
        for r in pos:
            lst  = str(r["listing_id"])
            px_t = float(r["settle_price"]) if r["settle_price"] is not None else None
            px_y = prev_map.get(lst)

            # need both prices to compute variation
            if px_t is None or px_y is None:
                continue

            qty  = float(r["net_qty"])
            size = float(r["contract_size_tons"])
            pnl  = (px_t - px_y) * qty * size

            if pnl != 0.0:
                await _adjust_margin(str(r["account_id"]), pnl, "variation_margin", None)

           # --- maintenance margin check / block-unblock logic (state-change emits) ---
            req = abs(qty) * px_t * size * float(r["maintenance_pct"])

            cur = await database.fetch_one(
                "SELECT balance, is_blocked FROM margin_accounts WHERE account_id = :a",
                {"a": r["account_id"]},
)

            cur_bal = float(cur["balance"]) if cur else 0.0
            was_blocked = bool(cur and cur.get("is_blocked"))
            needs_block = cur_bal < req

            if needs_block and not was_blocked:
                # transition → UNBLOCKED -> BLOCKED
                await _adjust_margin(str(r["account_id"]), 0.0, f"margin_call_required: need >= {req:.2f}", None)
                await database.execute(
                    "UPDATE margin_accounts SET is_blocked = TRUE WHERE account_id = :a",
                    {"a": r["account_id"]},
                )
                try:
                    await emit_event_safe("risk.margin_call", {
                        "account_id": str(r["account_id"]),
                        "required": round(req, 2),
                        "balance": round(cur_bal, 2),
                        "listing_id": lst,
                        "mark_date": str(dt),
                    })
                except Exception:
                    pass

            elif not needs_block and was_blocked:
                # transition → BLOCKED -> UNBLOCKED
                await database.execute(
                    "UPDATE margin_accounts SET is_blocked = FALSE WHERE account_id = :a",
                    {"a": r["account_id"]},
                )
                try:
                    await emit_event_safe("risk.unblocked", {
                        "account_id": str(r["account_id"]),
                        "balance": round(cur_bal, 2),
                        "listing_id": lst,
                        "mark_date": str(dt),
                    })
                except Exception:
                    pass

            # append a row for this position regardless of state change
            results.append({
                "account_id": str(r["account_id"]),
                "listing_id": lst,
                "qty": qty,
                "prev_settle": px_y,
                "settle": px_t,
                "variation_pnl": pnl,
                "maintenance_required": req,
                "balance_after": cur_bal,
            })
        return {"as_of": str(dt), "results": results}
# --- maintenance margin check / block-unblock logic (state-change emits) ---

# -------- Clearinghouse Economics (guaranty fund + waterfall) --------
@app.on_event("startup")
async def _ensure_ccp_schema():
    await run_ddl_multi("""
    CREATE TABLE IF NOT EXISTS guaranty_fund(
      member TEXT PRIMARY KEY,
      contribution_usd NUMERIC NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS default_events(
      id UUID PRIMARY KEY,
      member TEXT NOT NULL,
      occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      amount_usd NUMERIC NOT NULL,
      notes TEXT
    );
    """)

@clearing_router.post("/guaranty/deposit")
async def gf_deposit(member: str, amount: float):
    await database.execute("""
      INSERT INTO guaranty_fund(member,contribution_usd)
      VALUES (:m,:a)
      ON CONFLICT (member) DO UPDATE SET contribution_usd = guaranty_fund.contribution_usd + EXCLUDED.contribution_usd,
                                         updated_at=NOW()
    """, {"m":member,"a":amount})
    return {"ok": True}

@clearing_router.post("/waterfall/apply")
async def waterfall_apply(member: str, shortfall_usd: float):
    # 1) Use IM/VM balances (margin_accounts) → 2) Guaranty fund → 3) Skin-in-the-game (not modeled here)
    row = await database.fetch_one("SELECT balance FROM margin_accounts WHERE account_id=:a", {"a": member})
    bal = float(row["balance"]) if row else 0.0
    use_imvm = min(bal, shortfall_usd)
    rem = shortfall_usd - use_imvm
    if use_imvm:
        await _adjust_margin(member, -use_imvm, "default_waterfall", None)
    if rem > 0:
        gf = await database.fetch_one("SELECT contribution_usd FROM guaranty_fund WHERE member=:m", {"m": member})
        g = float(gf["contribution_usd"]) if gf else 0.0
        use_gf = min(g, rem)
        if use_gf:
            await database.execute("UPDATE guaranty_fund SET contribution_usd = contribution_usd - :x WHERE member=:m",
                                   {"x":use_gf,"m":member})
        rem -= use_gf
    await database.execute("INSERT INTO default_events(id,member,amount_usd,notes) VALUES (:i,:m,:a,:n)",
                           {"i":str(uuid.uuid4()),"m":member,"a":shortfall_usd,"n":f"IM/VM used={use_imvm}"})
    return {"ok": rem <= 0, "remaining_shortfall": rem}
app.include_router(clearing_router)
# ===================== /CLEARING (Margin & Variation) =====================