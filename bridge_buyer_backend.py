from fastapi import FastAPI, HTTPException, Request, Depends, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse, Response, StreamingResponse, JSONResponse
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

# ===== middleware & observability deps =====
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
import structlog, time

# rate limiting
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi.responses import PlainTextResponse

# metrics & errors
from prometheus_fastapi_instrumentator import Instrumentator
import sentry_sdk

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

# ===== Trusted hosts + session cookie =====
allowed = ["scrapfutures.com", "www.scrapfutures.com", "bridge-buyer.onrender.com"]
# Allow local/pytest hosts when not in production
if os.getenv("ENV", "development").lower() != "production":
    allowed += ["localhost", "127.0.0.1", "testserver", "0.0.0.0"]

app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed)
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SESSION_SECRET", "change-me"))

# ===== Security headers (incl. CSP that allows jsDelivr you use in HTML) =====
async def security_headers_mw(request, call_next):
    resp: Response = await call_next(request)
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    resp.headers["Permissions-Policy"] = "geolocation=()"
    resp.headers["Content-Security-Policy"] = (
        "default-src 'self' https://cdn.jsdelivr.net; "
        "img-src 'self' data:; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
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
from fastapi.staticfiles import StaticFiles
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
    return FileResponse("static/index.html")

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
PASSWORD_HASH_COL = "password"
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
        schema_extra = {"example":{
            "buyer":"Lewis Salvage","seller":"Winski Brothers","material":"Shred Steel","weight_tons":40.0,"price_per_ton":245.00
        }}

class ContractOut(ContractIn):
    id: uuid.UUID
    status: str
    created_at: datetime
    signed_at: Optional[datetime]
    signature: Optional[str]
    class Config:
        schema_extra = {"example":{
            "id":"b1c89b94-234a-4d55-b1fc-14bfb7fce7e9","buyer":"Lewis Salvage","seller":"Winski Brothers",
            "material":"Shred Steel","weight_tons":40,"price_per_ton":245.00,
            "status":"Signed","created_at":"2025-09-01T10:00:00Z","signed_at":"2025-09-01T10:15:00Z","signature":"abc123signature"
        }}

# Optional tighter typing for updates
ContractStatus = Literal["Pending","Signed","Dispatched","Fulfilled","Cancelled"]

class ContractUpdate(BaseModel):
    status: ContractStatus
    signature: Optional[str] = None
    class Config:
        schema_extra = {"example":{"status":"Signed","signature":"JohnDoe123"}}

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
        schema_extra = {"example":{
            "contract_id":"1ec9e850-8b5a-45de-b631-f9fae4a1d4c9","buyer":"Lewis Salvage","seller":"Winski Brothers",
            "material":"Shred Steel","weight_tons":40,"price_per_unit":245.00,"total_value":9800.00,
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
        schema_extra = {"example":{
            "bol_id":"9fd89221-4247-4f93-bf4b-df9473ed8e57","contract_id":"b1c89b94-234a-4d55-b1fc-14bfb7fce7e9",
            "buyer":"Lewis Salvage","seller":"Winski Brothers","material":"Shred Steel","weight_tons":40,
            "price_per_unit":245.0,"total_value":9800.0,
            "carrier":{"name":"ABC Trucking Co.","driver":"Jane Doe","truck_vin":"1FTSW21P34ED12345"},
            "pickup_signature":{"base64":"data:image/png;base64,iVBOR...","timestamp":"2025-09-01T12:00:00Z"},
            "pickup_time":"2025-09-01T12:15:00Z","delivery_signature":None,"delivery_time":None,"status":"BOL Issued"
        }}

# ===== Idempotency cache for POST /bols (starter) =====
_idem_cache = {}

# ===== Admin export helpers (CSV normalization + token) =====
ADMIN_EXPORT_TOKEN = os.getenv("ADMIN_EXPORT_TOKEN", "")

def _normalize(v):
    """Make DB values CSV-safe and deterministic."""
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"), ensure_ascii=False)
    return v

def _rows_to_csv_bytes(rows):
    """Dict rows -> CSV bytes, with stable header order."""
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

# ===== Webhook HMAC + replay protection (enabled when secret set) =====
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

    # === Verification footer: short SHA-256 over the DB row ===
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

# -------- Contracts --------
@app.post("/contracts", response_model=ContractOut, tags=["Contracts"], summary="Create Contract", description="Create a new contract with buyer, seller, material, weight, and price.", status_code=201)
async def create_contract(contract: ContractIn):
    row = await database.fetch_one("""
        INSERT INTO contracts (id, buyer, seller, material, weight_tons, price_per_ton)
        VALUES (:id, :buyer, :seller, :material, :weight_tons, :price_per_ton)
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
    description="Retrieve contracts with optional filters: buyer, seller, status, created_at date range.",
    status_code=200
)
async def get_all_contracts(
    buyer: Optional[str] = Query(None, description="Filter by buyer name"),
    seller: Optional[str] = Query(None, description="Filter by seller name"),
    status: Optional[str] = Query(None, description="Filter by contract status"),
    start: Optional[datetime] = Query(None, description="Start date (created_at >=)"),
    end: Optional[datetime] = Query(None, description="End date (created_at <=)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    query = "SELECT * FROM contracts"
    conditions, values = [], {}
    if buyer: conditions.append("buyer ILIKE :buyer"); values["buyer"] = f"%{buyer}%"
    if seller: conditions.append("seller ILIKE :seller"); values["seller"] = f"%{seller}%"
    if status: conditions.append("status ILIKE :status"); values["status"] = f"%{status}%"
    if start: conditions.append("created_at >= :start"); values["start"] = start
    if end:   conditions.append("created_at <= :end");   values["end"] = end
    if conditions: query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
    values["limit"], values["offset"] = limit, offset
    return await database.fetch_all(query=query, values=values)

@app.get("/contracts/{contract_id}", response_model=ContractOut, tags=["Contracts"], summary="Get Contract by ID", description="Retrieve a specific contract by its unique ID.", status_code=200)
async def get_contract_by_id(contract_id: str):
    row = await database.fetch_one("SELECT * FROM contracts WHERE id = :id", {"id": contract_id})
    if not row:
        raise HTTPException(status_code=404, detail="Contract not found")
    return row

@app.put("/contracts/{contract_id}", response_model=ContractOut, tags=["Contracts"], summary="Update Contract", description="Update a contract’s status or signature using its ID.", status_code=200)
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

@app.get("/contracts/export_csv", tags=["Contracts"], summary="Export Contracts as CSV", description="Export all contract records to a downloadable CSV file.", status_code=200)
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
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="contracts_export_{datetime.utcnow().isoformat()}.csv"'})

# -------- BOLs --------
@app.post("/bols", response_model=BOLOut, tags=["BOLs"], summary="Create BOL", description="Create a new Bill of Lading for a contract with carrier and signature data.", status_code=201)
async def create_bol_pg(bol: BOLIn, request: Request):
    # =====  idempotency support =====
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

@app.get("/bols", response_model=List[BOLOut], tags=["BOLs"], summary="List BOLs", description="Retrieve all BOLs. Supports optional filtering by buyer, seller, status, contract_id, and pickup date range.", status_code=200)
async def get_all_bols_pg(
    buyer: Optional[str] = Query(None, description="Filter by buyer name"),
    seller: Optional[str] = Query(None, description="Filter by seller name"),
    status: Optional[str] = Query(None, description="Filter by BOL status"),
    contract_id: Optional[str] = Query(None, description="Filter by contract ID"),
    start: Optional[datetime] = Query(None, description="Start date (pickup_time >=)"),
    end: Optional[datetime] = Query(None, description="End date (pickup_time <=)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    query = "SELECT * FROM bols"
    conditions, values = [], {}
    if buyer: conditions.append("buyer ILIKE :buyer"); values["buyer"] = f"%{buyer}%"
    if seller: conditions.append("seller ILIKE :seller"); values["seller"] = f"%{seller}%"
    if status: conditions.append("status ILIKE :status"); values["status"] = f"%{status}%"
    if contract_id: conditions.append("contract_id = :contract_id"); values["contract_id"] = contract_id
    if start: conditions.append("pickup_time >= :start"); values["start"] = start
    if end:   conditions.append("pickup_time <= :end");   values["end"] = end
    if conditions: query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY pickup_time DESC LIMIT :limit OFFSET :offset"
    values["limit"], values["offset"] = limit, offset

    rows = await database.fetch_all(query=query, values=values)
    result = []
    for row in rows:
        result.append({
            "bol_id": row["bol_id"],
            "contract_id": row["contract_id"],
            "buyer": row["buyer"],
            "seller": row["seller"],
            "material": row["material"],
            "weight_tons": row["weight_tons"],
            "price_per_unit": row["price_per_unit"],
            "total_value": row["total_value"],
            "carrier": {
                "name": row["carrier_name"], "driver": row["carrier_driver"], "truck_vin": row["carrier_truck_vin"]
            },
            "pickup_signature": {
                "base64": row["pickup_signature_base64"], "timestamp": row["pickup_signature_time"]
            },
            "delivery_signature": (
                {"base64": row["delivery_signature_base64"], "timestamp": row["delivery_signature_time"]}
                if row.get("delivery_signature_base64") else None
            ),
            "pickup_time": row["pickup_time"],
            "delivery_time": row["delivery_time"],
            "status": row["status"]
        })
    return result

@app.post("/bols/{bol_id}/update_status", tags=["BOLs"], summary="Update BOL Status", description="Update the status of a BOL (e.g., to In Transit or Delivered).", status_code=200)
async def update_bol_status_pg(bol_id: str, new_status: str):
    row = await database.fetch_one("""
        UPDATE bols
        SET status = :status,
            delivery_time = CASE WHEN :status ILIKE 'delivered' THEN NOW() ELSE delivery_time END
        WHERE bol_id = :bol_id
        RETURNING bol_id
    """, {"bol_id": bol_id, "status": new_status})
    if not row:
        raise HTTPException(status_code=404, detail="BOL not found")
    return {"message": "Status updated"}

@app.post("/bols/{bol_id}/add_delivery_signature", tags=["BOLs"], summary="Add Delivery Signature", description="Attach a delivery signature and mark the BOL as Delivered. Also auto-fulfills the linked contract.", status_code=200)
async def add_delivery_signature_pg(bol_id: str, sig: Signature):
    row = await database.fetch_one("""
        UPDATE bols
        SET delivery_signature_base64 = :b64,
            delivery_signature_time = :ts,
            status = 'Delivered',
            delivery_time = NOW()
        WHERE bol_id = :bol_id
        RETURNING bol_id, contract_id
    """, {"bol_id": bol_id, "b64": sig.base64, "ts": sig.timestamp})
    if not row:
        raise HTTPException(status_code=404, detail="BOL not found")

    # Auto-update the linked contract to Fulfilled
    await database.execute("""
        UPDATE contracts SET status = 'Fulfilled'
        WHERE id = :cid
    """, {"cid": row["contract_id"]})
    return {"message": "Delivery signature added, contract fulfilled"}

# -------- Auth --------
@limiter.limit("5/minute")  # ===== NEW: rate limit login =====
@app.post(
    "/login",
    tags=["Auth"],
    summary="User Login",
    description="Authenticate a user based on username and password. Returns role and redirect path on success.",
    status_code=200
)
async def login(data: LoginRequest, request: Request):
    if users is None:
        raise HTTPException(status_code=500, detail="Users table not initialized")

    stmt = (
        select(
            users.c[USERNAME_COL].label("username"),
            users.c[PASSWORD_HASH_COL].label("password_hash"),
            users.c[ROLE_COL].label("role")
        )
        .where(and_(users.c[USERNAME_COL] == data.username.strip()))
        .limit(1)
    )
    row = await database.fetch_one(stmt)
    if not row:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    stored_hash_or_plain = row["password_hash"]
    ok = False
    try:
        ok = bcrypt.verify(data.password.strip(), stored_hash_or_plain)
    except Exception:
        ok = data.password.strip() == str(stored_hash_or_plain)
    if not ok:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    role = row["role"]
    accepts = request.headers.get("accept", "")
    if "text/html" in accepts:
        if role == "admin": return RedirectResponse(url="/admin", status_code=303)
        elif role == "buyer": return RedirectResponse(url="/buyer", status_code=303)
        elif role == "seller": return RedirectResponse(url="/seller", status_code=303)
        else: raise HTTPException(status_code=400, detail="Unknown user role")
    else:
        return JSONResponse({"ok": True, "role": role, "redirect": f"/{role if role in ('admin','buyer','seller') else 'buyer'}"})

# -------- Integrations (hardened ICE webhook) --------
@app.post("/ice-digital-trade", tags=["Integrations"], summary="ICE DT Webhook")
async def ice_dt_webhook(request: Request):
    raw = await request.body()
    ice_secret = os.getenv("ICE_WEBHOOK_SECRET", "")
    sig = request.headers.get("X-Signature")

    # Enforce HMAC + replay **only if** secret is configured
    if ice_secret:
        if not (verify_sig(raw, sig, "ICE_WEBHOOK_SECRET") and not is_replay(sig)):
            raise HTTPException(401, "Bad signature")

    payload = await request.json()
    contract_id = payload.get("contract_id")
    external_ref = payload.get("external_ref")
    if contract_id:
        await database.execute("""
            UPDATE contracts
            SET status = COALESCE(status,'Pending'),
                signature = COALESCE(signature,'ICE'),
                signed_at = NOW()
            WHERE id = :id
        """, {"id": contract_id})
    return {"ok": True, "received": payload, "external_ref": external_ref, "hmac_enforced": bool(ice_secret)}

@app.post("/docsign", tags=["Integrations"], summary="DocSign Stub", description="Stub endpoint to simulate doc-sign webhooks.", status_code=200)
async def docsign_stub(payload: dict):
    bol_id = payload.get("bol_id")
    sig_b64 = payload.get("signature_base64", "stub")
    if bol_id:
        row = await database.fetch_one("""
            UPDATE bols
            SET delivery_signature_base64 = :sig_b64,
                delivery_signature_time = NOW(),
                status = 'Delivered',
                delivery_time = NOW()
            WHERE bol_id = :bol_id
            RETURNING contract_id
        """, {"sig_b64": sig_b64, "bol_id": bol_id})
        if row:
            await database.execute("UPDATE contracts SET status = 'Fulfilled' WHERE id = :cid", {"cid": row["contract_id"]})
    return {"ok": True, "received": payload, "note": "stub only"}

# -------- Analytics --------
@app.get("/analytics/contracts_by_day", tags=["Analytics"], summary="Contracts per day", description="Counts of contracts grouped by day (last 30).", status_code=200)
async def contracts_by_day():
    rows = await database.fetch_all("""
        SELECT DATE(created_at) as day, COUNT(*) as count
        FROM contracts
        GROUP BY day ORDER BY day DESC LIMIT 30
    """)
    return [{"day": str(r["day"]), "count": r["count"]} for r in rows]

# -------- Admin export (retention helper) --------
@app.get(
    "/admin/export_all",
    tags=["Admin"],
    summary="Export all data (ZIP)",
    description="Exports contracts.csv and bols.csv in a ZIP. Accepts X-Admin-Token header OR admin session.",
    status_code=200
)
async def admin_export_all(request: Request, x_admin_token: str | None = Header(None)):
    # Auth: allow either admin session **or** header token
    if not (_is_admin_session(request) or (ADMIN_EXPORT_TOKEN and x_admin_token == ADMIN_EXPORT_TOKEN)):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch rows
    contracts = await database.fetch_all("SELECT * FROM contracts ORDER BY created_at DESC")
    bols = await database.fetch_all("SELECT * FROM bols ORDER BY COALESCE(pickup_time, created_at) DESC")

    # Build ZIP in-memory
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("contracts.csv", _rows_to_csv_bytes(contracts))
        zf.writestr("bols.csv", _rows_to_csv_bytes(bols))
    zip_bytes.seek(0)

    ts = datetime.utcnow().strftime("%Y-%m-%d")
    headers = {"Content-Disposition": f'attachment; filename="bridge_export_{ts}.zip"'}
    return StreamingResponse(zip_bytes, media_type="application/zip", headers=headers)
