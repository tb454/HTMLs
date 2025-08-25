from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse, Response, StreamingResponse, JSONResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData, and_, select
import os
import databases
import uuid
import csv
import io
from passlib.hash import bcrypt
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="BRidge API")

# --- Serve static HTML from /static ---
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", include_in_schema=False)
async def root():
    return FileResponse("static/bridge-login.html")

@app.get("/healthz", include_in_schema=False)
async def healthz():
    return {"ok": True, "service": "bridge-buyer"}

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

@app.get("/buyer")
async def buyer_page():
    return FileResponse("static/bridge-buyer.html")

@app.get("/admin")
async def admin_page():
    return FileResponse("static/bridge-admin-dashboard.html")

@app.get("/seller")
async def seller_page():
    return FileResponse("static/index.html")

# --- Database Setup ---
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

# --- CORS ---
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

# --- Data Models ---
class CarrierInfo(BaseModel):
    name: str
    driver: str
    truck_vin: str

class Signature(BaseModel):
    base64: str
    timestamp: datetime

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

# ✅ NEW CONTRACT MODELS HERE:
class ContractIn(BaseModel):
    buyer: str
    seller: str
    material: str
    weight_tons: float
    price_per_ton: float

class ContractOut(ContractIn):
    id: uuid.UUID
    status: str
    created_at: datetime
    signed_at: Optional[datetime]
    signature: Optional[str]

# --- In-memory store (temporary for BOLs only) ---
bol_records: List[BOLRecord] = []

# --- BOL Endpoints ---
@app.post("/create_bol")
def create_bol(record: BOLRecord):
    bol_records.append(record)
    return {"message": "BOL created", "bol_id": record.bol_id}

@app.get("/bols")
def get_all_bols():
    return bol_records

@app.get("/bol/{bol_id}")
def get_bol_by_id(bol_id: str):
    for record in bol_records:
        if record.bol_id == bol_id:
            return record
    raise HTTPException(status_code=404, detail="BOL not found")

@app.get("/export_csv")
def export_csv():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "BOL_ID", "Contract_ID", "Buyer", "Seller", "Material", "Weight_Tons",
        "Status", "Pickup_Time", "Delivery_Time", "Total_Value"
    ])
    for r in bol_records:
        writer.writerow([
            r.bol_id, r.contract_id, r.buyer, r.seller, r.material, r.weight_tons, r.status,
            r.pickup_time.isoformat(),
            r.delivery_time.isoformat() if r.delivery_time else "",
            f"{r.total_value:.2f}"
        ])
    output.seek(0)
    headers = {
        "Content-Disposition": f'attachment; filename="bol_export_{datetime.utcnow().isoformat()}.csv"'
    }
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers=headers)

@app.post("/update_status/{bol_id}")
def update_status(bol_id: str, new_status: str):
    for record in bol_records:
        if record.bol_id == bol_id:
            record.status = new_status
            if new_status.lower() == "delivered":
                record.delivery_time = datetime.utcnow()
            return {"message": "Status updated"}
    raise HTTPException(status_code=404, detail="BOL not found")

@app.post("/add_delivery_signature/{bol_id}")
def add_delivery_signature(bol_id: str, signature: Signature):
    for record in bol_records:
        if record.bol_id == bol_id:
            record.delivery_signature = signature
            record.delivery_time = datetime.utcnow()
            record.status = "Delivered"
            return {"message": "Delivery signature added"}
    raise HTTPException(status_code=404, detail="BOL not found")

# --- Login Endpoint with Redirects or JSON ---
@app.post("/login")
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
        if role == "admin":
            return RedirectResponse(url="/admin", status_code=303)
        elif role == "buyer":
            return RedirectResponse(url="/buyer", status_code=303)
        elif role == "seller":
            return RedirectResponse(url="/seller", status_code=303)
        else:
            raise HTTPException(status_code=400, detail="Unknown user role")
    else:
        return JSONResponse({"ok": True, "role": role, "redirect": f"/{role if role in ('admin','buyer','seller') else 'buyer'}"})
