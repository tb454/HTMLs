# routers/dat_mock_admin.py
import csv, io, json
from fastapi import APIRouter, UploadFile, File, HTTPException, Request

router = APIRouter(prefix="/admin/dat_mock", tags=["Admin"])

@router.post("/import", summary="Import DAT-mock carriers from CSV")
async def import_dat_mock(request: Request, file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Upload a CSV")
    data = (await file.read()).decode("utf-8", errors="ignore")
    rdr = csv.DictReader(io.StringIO(data))
    database = request.app.state.database
    n = 0
    for r in rdr:
        dot = (r.get("dot_number") or "").strip()
        if not dot: continue
        payload = {
            "dot_number": dot,
            "mc_number": (r.get("mc_number") or "").strip() or None,
            "legal_name": (r.get("legal_name") or "").strip() or None,
            "authority_status": (r.get("authority_status") or "").strip() or None,
            "safety_rating": (r.get("safety_rating") or "").strip() or None,
            "oos": str(r.get("oos") or "").strip().lower() in {"true","1","yes","y"},
            "insurance_exp": (r.get("insurance_exp") or "").strip() or None,
            "external_id": (r.get("external_id") or "").strip() or None,
            "raw_json": r
        }
        await database.execute("""
          insert into public.dat_mock_carriers
            (dot_number, mc_number, legal_name, authority_status, safety_rating, oos, insurance_exp, external_id, raw_json)
          values
            (:dot_number,:mc_number,:legal_name,:authority_status,:safety_rating,:oos,:insurance_exp,:external_id, to_jsonb(:raw_json::json))
          on conflict (dot_number) do update set
            mc_number = excluded.mc_number,
            legal_name = excluded.legal_name,
            authority_status = excluded.authority_status,
            safety_rating = excluded.safety_rating,
            oos = excluded.oos,
            insurance_exp = excluded.insurance_exp,
            external_id = excluded.external_id,
            raw_json = excluded.raw_json
        """, {"raw_json": json.dumps(payload["raw_json"]), **payload})

        await database.execute("""
          insert into public.dat_mock_changes (dot_number, payload_json)
          values (:dot, :payload)
        """, {"dot": dot, "payload": payload})
        n += 1
    return {"ok": True, "rows": n}
