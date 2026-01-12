# routers/carriers.py
from typing import Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from providers.dat_provider import get_carrier_by_dot, subscribe_monitoring, DATError
from providers.fmcsa_safer import fetch_snapshot, SAFERError

router = APIRouter(prefix="/carriers", tags=["Carriers"])

class VerifyRequest(BaseModel):
    dot_number: Optional[str] = Field(None)
    mc_number: Optional[str] = Field(None)
    legal_name_hint: Optional[str] = None

class VerifyResponse(BaseModel):
    carrier_id: int
    status: str
    fmcsa_snapshot: Dict[str, Any] | None = None
    dat_snapshot: Dict[str, Any] | None = None
    monitoring_subscribed: bool = False

async def _upsert_carrier(request: Request, dot, mc, legal_name, dba_name, addr):
    database = request.app.state.database
    row = await database.fetch_one(
        "select id from public.carriers where dot_number = :dot limit 1",
        {"dot": dot} if dot else {"dot": None}
    ) if dot else None

    if row:
        cid = row["id"]
        await database.execute("""
          update public.carriers
             set mc_number = coalesce(:mc, mc_number),
                 legal_name = coalesce(:legal_name, legal_name),
                 dba_name   = coalesce(:dba_name, dba_name),
                 country    = coalesce(:country, country),
                 state      = coalesce(:state, state),
                 city       = coalesce(:city, city),
                 postal_code= coalesce(:postal_code, postal_code)
           where id = :id
        """, {
            "mc": mc, "legal_name": legal_name, "dba_name": dba_name,
            "country": (addr or {}).get("country"),
            "state": (addr or {}).get("state"),
            "city": (addr or {}).get("city"),
            "postal_code": (addr or {}).get("postal_code"),
            "id": cid
        })
        return int(cid)

    cid = await database.execute("""
      insert into public.carriers
        (dot_number, mc_number, legal_name, dba_name, country, state, city, postal_code)
      values (:dot, :mc, :legal_name, :dba_name, :country, :state, :city, :postal_code)
      returning id
    """, {
        "dot": dot, "mc": mc, "legal_name": legal_name, "dba_name": dba_name,
        "country": (addr or {}).get("country"),
        "state": (addr or {}).get("state"),
        "city": (addr or {}).get("city"),
        "postal_code": (addr or {}).get("postal_code"),
    })
    return int(cid)

async def _record_verification(request: Request, cid: int, source: str, snap: Dict[str, Any]):
    database = request.app.state.database
    await database.execute("""
      insert into public.carrier_verifications
        (carrier_id, source, authority_status, safety_rating, oos, insurance_exp, payload_json)
      values
        (:cid, :source, :auth, :safety, :oos, :ins_exp, :payload)
    """, {
        "cid": cid,
        "source": source,
        "auth": snap.get("authority_status"),
        "safety": snap.get("safety_rating"),
        "oos": snap.get("oos"),
        "ins_exp": snap.get("insurance_exp"),
        "payload": snap
    })
    status = "oos" if snap.get("oos") else ("active" if (snap.get("authority_status") or "").lower().startswith("auth") else "unknown")
    await database.execute("update public.carriers set status = :s where id = :id", {"s": status, "id": cid})

@router.post("/verify", response_model=VerifyResponse,
    summary="Verify a carrier (FMCSA stub + local DAT-mock); upsert carrier & snapshot results")
async def carriers_verify(request: Request, payload: VerifyRequest):
    if not payload.dot_number and not payload.mc_number:
        raise HTTPException(400, "Provide dot_number or mc_number")

    # 1) FMCSA
    fmcsa = None
    try:
        fmcsa = await fetch_snapshot(payload.dot_number, payload.mc_number)
    except SAFERError:
        pass

    cid = await _upsert_carrier(
        request,
        dot=(fmcsa or {}).get("dot_number") or payload.dot_number,
        mc=(fmcsa or {}).get("mc_number") or payload.mc_number,
        legal_name=(fmcsa or {}).get("legal_name") or payload.legal_name_hint,
        dba_name=(fmcsa or {}).get("dba_name"),
        addr=(fmcsa or {}).get("address"),
    )
    if fmcsa:
        await _record_verification(request, cid, "fmcsa", fmcsa)

    # 2) local DAT-mock lookup
    dat = None
    monitoring = False
    try:
        if payload.dot_number:
            dm = await get_carrier_by_dot(request, payload.dot_number)
            if dm:
                mapped = {
                    "authority_status": dm.get("authority_status"),
                    "safety_rating":   dm.get("safety_rating"),
                    "oos":             dm.get("oos"),
                    "insurance_exp":   dm.get("insurance_exp"),
                    "raw":             dm
                }
                await _record_verification(request, cid, "dat", mapped)

                ext = dm.get("external_id") or f"mock-{payload.dot_number}"
                database = request.app.state.database
                await database.execute("""
                  insert into public.carrier_provider_links (carrier_id, provider, external_id, monitoring)
                  values (:cid, 'dat', :eid, true)
                  on conflict (carrier_id, provider) do update set external_id = excluded.external_id, monitoring = true
                """, {"cid": cid, "eid": str(ext)})
                monitoring = True
    except DATError:
        pass

    database = request.app.state.database
    row = await database.fetch_one("select status from public.carriers where id = :id", {"id": cid})
    return VerifyResponse(
        carrier_id=cid,
        status=row["status"] if row else "unknown",
        fmcsa_snapshot=fmcsa,
        dat_snapshot=dat,
        monitoring_subscribed=monitoring
    )

@router.get("/{carrier_id}", summary="Carrier with recent snapshots")
async def get_carrier(request: Request, carrier_id: int):
    database = request.app.state.database
    c = await database.fetch_one("select * from public.carriers where id = :id", {"id": carrier_id})
    if not c: raise HTTPException(404, "Carrier not found")
    snaps = await database.fetch_all("""
      select source, authority_status, safety_rating, oos, insurance_exp, payload_json, fetched_at
        from public.carrier_verifications
       where carrier_id = :id
       order by fetched_at desc limit 5
    """, {"id": carrier_id})
    return {"carrier": dict(c), "verifications": [dict(s) for s in snaps]}
