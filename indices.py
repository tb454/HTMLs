from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from datetime import datetime
from sqlalchemy import text
router = APIRouter(prefix="/indices", tags=["Indices"])

async def get_db(request: Request):
    db = getattr(request.app.state, "db", None)
    if db is None:
        raise RuntimeError("Database connection not initialized")
    return db

class SnapshotIn(BaseModel):
    material_code: str
    region: str
    price_per_ton: float
    basis_points: int = 0
    sample_count: int = Field(ge=1)
    notes: str | None = None

@router.post("/snapshot", status_code=201)
async def create_snapshot(payload: SnapshotIn, db=Depends(get_db)):
    q = text("""
      INSERT INTO br_index_snapshots(material_code, region, price_per_ton, basis_points, sample_count, notes)
      VALUES (:m,:r,:p,:b,:c,:n)
      RETURNING id, snapshot_ts
    """)
    row = await db.fetch_one(q, dict(m=payload.material_code, r=payload.region,
                                     p=payload.price_per_ton, b=payload.basis_points,
                                     c=payload.sample_count, n=payload.notes))
    return {"id": str(row["id"]), "snapshot_ts": row["snapshot_ts"]}

@router.get("/latest")
async def latest(material_code: str, region: str, db=Depends(get_db)):
    q = text("""
      SELECT * FROM br_index_snapshots
      WHERE material_code=:m AND region=:r
      ORDER BY snapshot_ts DESC LIMIT 1
    """)
    row = await db.fetch_one(q, dict(m=material_code, r=region))
    if not row: raise HTTPException(404, "no snapshot")
    return dict(row)
