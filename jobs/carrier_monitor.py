# jobs/carrier_monitor.py
from datetime import datetime, timedelta, timezone
from fastapi import Request
from providers.dat_provider import fetch_changes_since

async def carrier_monitor_run(request: Request):
    database = request.app.state.database
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    changes = await fetch_changes_since(request, since)
    items = (changes or {}).get("items") or []
    alerts = 0

    for it in items:
        dot = str(it.get("dot_number") or "")
        if not dot: continue

        carrier = await database.fetch_one("select id from public.carriers where dot_number = :dot", {"dot": dot})
        if not carrier:
            cid = await database.execute("""
              insert into public.carriers (dot_number, legal_name, status)
              values (:dot, :name, 'unknown') returning id
            """, {"dot": dot, "name": it.get("legal_name")})
        else:
            cid = carrier["id"]

        await database.execute("""
          insert into public.carrier_verifications (carrier_id, source, authority_status, safety_rating, oos, insurance_exp, payload_json)
          values (:cid, 'dat', :auth, :safety, :oos, :ins, :payload)
        """, {
          "cid": cid,
          "auth": it.get("authority_status"),
          "safety": it.get("safety_rating"),
          "oos": it.get("oos"),
          "ins": it.get("insurance_exp"),
          "payload": it
        })

        new_status = "oos" if it.get("oos") else ("active" if (it.get("authority_status") or "").lower().startswith("auth") else "unknown")
        await database.execute("update public.carriers set status = :s where id = :cid", {"s": new_status, "cid": cid})

        if it.get("oos"):
            await database.execute("""
              insert into public.carrier_alerts (carrier_id, kind, severity, details_json)
              values (:cid, 'oos', 'critical', :d)
            """, {"cid": cid, "d": it})
            alerts += 1
        elif it.get("insurance_exp"):
            await database.execute("""
              insert into public.carrier_alerts (carrier_id, kind, severity, details_json)
              values (:cid, 'insurance_lapse', 'warn', :d)
            """, {"cid": cid, "d": it})
            alerts += 1

    return {"ok": True, "checked": len(items), "alerts": alerts}
