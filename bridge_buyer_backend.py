from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData
from fastapi import Request
import os
import databases
import uuid
import csv


app = FastAPI()

# --- Database Setup ---
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

database = databases.Database(DATABASE_URL)
metadata = MetaData()
engine = create_engine(DATABASE_URL)
users = None

@app.on_event("startup")
async def startup():
    global users
    await database.connect()
    metadata.reflect(bind=engine)
    users = metadata.tables["users"]
    if users is None:
        raise RuntimeError("The 'users' table was not found in the database schema.")
@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
    filename = f"bol_export_{datetime.utcnow().isoformat()}.csv"
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["BOL_ID", "Contract_ID", "Buyer", "Material", "Weight", "Status", "Pickup_Time", "Delivery_Time", "Total_Value"])
        for r in bol_records:
            writer.writerow([
                r.bol_id, r.contract_id, r.buyer, r.material, r.weight_tons, r.status,
                r.pickup_time.isoformat(), r.delivery_time.isoformat() if r.delivery_time else "",
                f"${r.total_value:,.2f}"
            ])
    return {"message": "CSV exported", "filename": filename}

@app.post("/update_status/{bol_id}")
def update_status(bol_id: str, new_status: str):
    for record in bol_records:
        if record.bol_id == bol_id:
            record.status = new_status
            if new_status == "Delivered":
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

# --- Login Endpoint ---
@app.post("/login")
async def login(data: LoginRequest):
    query = users.select().where(
        users.c.username == data.username.strip(),
        users.c.password == data.password.strip()
    )
    result = await database.fetch_one(query)

    if not result:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return {
        "username": result["username"],
        "role": result["role"]
    }