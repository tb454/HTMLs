# routers/carriers_li_admin.py
import csv, io, json, httpx, asyncio, re
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from fastapi import APIRouter, UploadFile, File, HTTPException, Body, Query, Request, Depends


router = APIRouter(prefix="/admin/carriers", tags=["Admin/Carriers"])

# --- column resolver (case/space tolerant) ---
def _col(row: Dict[str, Any], *cands: str) -> Optional[str]:
    if not row: return None
    lower_map = {str(k).strip().lower(): k for k in row.keys()}
    for c in cands:
        k = lower_map.get(c.strip().lower())
        if k is not None:
            v = row.get(k)
            return None if v in (None, "", "nan", "Null", "NULL") else str(v).strip()
    return None

def _to_bool(v: Any) -> Optional[bool]:
    if v is None: return None
    s = str(v).strip().lower()
    if s in {"1","true","yes","y","t"}: return True
    if s in {"0","false","no","n","f"}: return False
    return None

def _to_date(s: Any) -> Optional[str]:
    if not s: return None
    x = str(s).strip()
    # accept YYYY-MM-DD, MM/DD/YYYY, YYYYMMDD
    for pat in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            d = datetime.strptime(x, pat).date()
            return d.isoformat()
        except Exception:
            pass
    return None

async def _upsert_batch(ref_rows: list[Dict[str, Any]], database) -> dict:
    """Upserts into carriers_ref, mirrors into dat_mock_carriers, logs changes for monitor."""
    if not ref_rows:
        return {"inserted": 0, "updated": 0, "changes": 0}

    inserted = updated = changes = 0

    for r in ref_rows:
        dot = r["dot_number"]
        # 1) carriers_ref upsert
        prev = await database.fetch_one("select dot_number, authority_status, oos, insurance_exp from public.carriers_ref where dot_number=:d", {"d": dot})
        if prev:
            await database.execute("""
              update public.carriers_ref
                 set mc_number=:mc, legal_name=:ln, dba_name=:dba,
                     authority_status=:auth, oos=:oos, insurance_exp=:ins,
                     address_state=:st, address_city=:ct, address_zip=:zip,
                     raw_json=:raw, updated_at=now()
               where dot_number=:dot
            """, r)
            updated += 1
        else:
            await database.execute("""
              insert into public.carriers_ref(
                dot_number, mc_number, legal_name, dba_name,
                authority_status, oos, insurance_exp,
                address_state, address_city, address_zip, raw_json
              )
              values (:dot_number,:mc_number,:legal_name,:dba_name,
                      :authority_status,:oos,:insurance_exp,
                      :address_state,:address_city,:address_zip,:raw_json)
            """, r)
            inserted += 1

        # 2) mirror row into dat_mock_carriers (so /carriers/verify works without vendor)
        #    and append to dat_mock_changes so the nightly monitor raises alerts
        await database.execute("""
          insert into public.dat_mock_carriers(dot_number, mc_number, legal_name, authority_status, safety_rating, oos, insurance_exp, external_id, raw_json)
          values (:dot_number, :mc_number, :legal_name, :authority_status, null, :oos, :insurance_exp, :dot_number, :raw_json)
          on conflict (dot_number) do update set
            mc_number      = excluded.mc_number,
            legal_name     = excluded.legal_name,
            authority_status = excluded.authority_status,
            oos            = excluded.oos,
            insurance_exp  = excluded.insurance_exp,
            raw_json       = excluded.raw_json
        """, r)
        await database.execute("""
          insert into public.dat_mock_changes(dot_number, payload_json)
          values (:dot_number, :payload)
        """, {"dot_number": dot, "payload": json.dumps({
              "dot_number": dot,
              "mc_number": r["mc_number"],
              "legal_name": r["legal_name"],
              "authority_status": r["authority_status"],
              "oos": r["oos"],
              "insurance_exp": r["insurance_exp"]
        })})
        changes += 1

    return {"inserted": inserted, "updated": updated, "changes": changes}

def _normalize_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Map common FMCSA L&I column names (datasets vary—this is tolerant)
    dot  = _col(row, "dot_number", "usdot", "dot", "usdOT number", "number_dot", "carrier_dot")
    if not dot or not dot.isdigit():
        return None

    mc   = _col(row, "mc_number", "docket_number", "mc_mx_ff_number", "docket #", "docket")
    name = _col(row, "legal_name", "legalname", "company_legal_name", "name")
    dba  = _col(row, "dba_name", "dba", "doing_business_as")
    auth = _col(row, "operating_status", "authority_status", "operatingstatus", "operation_status")
    oos  = _to_bool(_col(row, "out_of_service", "oos", "out_of_service_date_flag", "is_oos"))
    ins  = _to_date(_col(row, "insurance_exp", "insurance_expiration", "policy_exp", "policy_expiration"))

    st   = _col(row, "phy_state", "mailing_state", "state")
    city = _col(row, "phy_city", "mailing_city", "city")
    zipc = _col(row, "phy_zip", "mailing_zip", "zip", "zipcode")

    return {
        "dot_number": dot,
        "mc_number": mc,
        "legal_name": name,
        "dba_name": dba,
        "authority_status": auth,
        "oos": oos,
        "insurance_exp": ins,
        "address_state": st,
        "address_city": city,
        "address_zip": zipc,
        "raw_json": json.dumps(row, default=str),
    }

def _read_csv_text(text: str) -> list[Dict[str, Any]]:
    rdr = csv.DictReader(io.StringIO(text))
    out: list[Dict[str, Any]] = []
    for r in rdr:
        n = _normalize_row(r)
        if n: out.append(n)
    return out

@router.post("/li_import", summary="Upload FMCSA Licensing & Insurance CSV (free dataset)")
async def li_import(file: UploadFile = File(...), database = Depends(lambda: None)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Upload a .csv")
    text = (await file.read()).decode("utf-8-sig", errors="replace")
    rows = _read_csv_text(text)
    res = await _upsert_batch(rows, database)
    return {"ok": True, "rows_in": len(rows), **res}

@router.post("/li_sync_url", summary="Fetch L&I CSV from URL and ingest")
async def li_sync_url(url: str = Body(..., embed=True), database = Depends(lambda: None)):
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.get(url)
        if r.status_code >= 300:
            raise HTTPException(400, f"fetch failed: {r.status_code}")
    except Exception as e:
        raise HTTPException(400, f"download failed: {e}")
    text = r.text
    rows = _read_csv_text(text)
    res = await _upsert_batch(rows, database)
    return {"ok": True, "url": url, "rows_in": len(rows), **res}
