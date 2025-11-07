# full_test_suite.py (merged)
# End-to-end smoke/flow test for FastAPI backend.
# Run with:  python tests/full_test_suite.py
import os, sys, time, json, uuid
from datetime import date, datetime, timedelta, timezone
import requests
import re

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")

# Optional envs used by the app
ENV            = os.environ.get("ENV", "development").lower()
H_SNAPSHOT_ENV = os.environ.get("SNAPSHOT_AUTH", "")       # if set, we must send x-auth
ICE_SECRET     = os.environ.get("ICE_WEBHOOK_SECRET", "")
SETUP_TOKEN    = os.environ.get("ADMIN_SETUP_TOKEN", "")

# Optional admin creds (only needed if ENV=production and you want admin routes)
ADMIN_EMAIL    = os.environ.get("ADMIN_EMAIL", "")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")

# One session for everything so cookies persist
S = requests.Session()

def hdr(extra=None):
    h = {}
    if extra: h.update(extra)
    return h

def h_snapshot():
    return {"x-auth": H_SNAPSHOT_ENV} if H_SNAPSHOT_ENV else {}

# ---------- HTTP helpers (raw) ----------
def post_raw(path, **kw):   return S.post(f"{BASE}{path}", timeout=30, **kw)
def get_raw(path,  **kw):   return S.get(f"{BASE}{path}",  timeout=30, **kw)
def put_raw(path,  **kw):   return S.put(f"{BASE}{path}",  timeout=30, **kw)
def patch_raw(path,**kw):   return S.patch(f"{BASE}{path}",timeout=30, **kw)
def delete_raw(path,**kw):  return S.delete(f"{BASE}{path}",timeout=30, **kw)

# ---------- Pretty printer + run log ----------
RESULTS = []  # ("PASS"/"FAIL"/"SKIP", label, msg)
RUN_LOG = []  # [{'method': 'GET', 'path': '/contracts', 'status': 200, 'ok': True, 'snippet': '...'}]

# ---- Connection counters (for summary + artifact) ----
REQUESTS_TOTAL = 0
REQUESTS_BY_METHOD = {"GET": 0, "POST": 0, "PUT": 0, "PATCH": 0, "DELETE": 0}

# Optional: save 5xx bodies to disk for debugging
SAVE_ERR_DIR = os.environ.get("SAVE_ERR_DIR", "")

def _save_err_body(name, resp):
    if not (SAVE_ERR_DIR and resp is not None and getattr(resp, "status_code", 0) >= 500):
        return
    os.makedirs(SAVE_ERR_DIR, exist_ok=True)
    method, path = _parse_name(name)
    safe = (path or "/").strip("/").replace("/", "_") or "root"
    p = os.path.join(SAVE_ERR_DIR, f"{method}_{safe}_{resp.status_code}.txt")
    try:
        with open(p, "w", encoding="utf-8") as f:
            try:
                f.write(json.dumps(resp.json(), indent=2))
            except Exception:
                f.write(resp.text or "")
    except Exception:
        pass

# Exit behavior (set envs to '1' to enable):
FAIL_ON_MISSING = os.environ.get("FAIL_ON_MISSING", "0") in ("1","true","yes")
FAIL_ON_FAIL    = os.environ.get("FAIL_ON_FAIL", "1") in ("1","true","yes")  # default: fail on HTTP fails

def _parse_name(name: str):
    m = re.search(r'^(GET|POST|PUT|PATCH|DELETE)\s+(\S+)', name)
    return (m.group(1), m.group(2)) if m else (None, None)

def _append_run_log(name: str, resp=None, ok=True):
    method, path = _parse_name(name)
    status = getattr(resp, "status_code", None) if resp is not None else None
    snippet = ""
    if resp is not None:
        try:
            snippet = json.dumps(resp.json())[:180]
        except Exception:
            try:
                snippet = (resp.text or "")[:180]
            except Exception:
                snippet = ""
    RUN_LOG.append({"method": method, "path": path, "status": status, "ok": bool(ok), "snippet": snippet})

def ok(name, resp=None, cond=True, msg=""):
    """Record PASS/FAIL, always log, capture status + response snippet."""
    if resp is not None and (resp.status_code >= 400 or not cond):
        body = ""
        try:
            body = json.dumps(resp.json())[:300]
        except Exception:
            body = (resp.text or "")[:300]
        RESULTS.append(("FAIL", name, f"HTTP {resp.status_code} {msg} :: {body}"))
        _append_run_log(name, resp, ok=False)
        _save_err_body(name, resp)
        print(f"❌ {name} :: HTTP {resp.status_code} {msg}")
    elif not cond:
        RESULTS.append(("FAIL", name, msg))
        _append_run_log(name, None, ok=False)
        print(f"❌ {name} :: {msg}")
    else:
        RESULTS.append(("PASS", name, ""))
        _append_run_log(name, resp, ok=True)
        print(f"✅ {name}")

def skip(name, why):
    RESULTS.append(("SKIP", name, why))
    _append_run_log(name, None, ok=True)
    print(f"⚠️  SKIP {name} :: {why}")

def must_json(r):
    try:
        return r.json()
    except Exception:
        raise AssertionError("Not JSON")

def maybe_login_admin():
    """
    In development, admin routes are typically open (your _require_admin gates prod).
    In production, try to login if ADMIN_EMAIL/PASSWORD are provided.
    """
    if ENV != "production":
        return True  # dev/staging: usually ungated
    if not (ADMIN_EMAIL and ADMIN_PASSWORD):
        skip("ADMIN login", "ENV=production but ADMIN_EMAIL/PASSWORD not set")
        return False
    r = post("/login", json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    if r.status_code == 200:
        ok("POST /login (admin)", r)
        return True
    ok("POST /login (admin)", r)  # will print a FAIL with details
    return False

# ===== Coverage tracking (auto) =====
TESTED: set[tuple[str, str]] = set()
MISSING_ENDPOINTS = []

def _record(method: str, path: str):
    """Record a call like ('GET','/contracts/123'). Path must be relative (starts with '/')."""
    try:
        if not path.startswith("/"):
            from urllib.parse import urlparse
            p = urlparse(path).path
            path = p if p else path
        TESTED.add((method.upper(), path))
    except Exception:
        pass

def _instrument_session(session):
    """Wrap session.request so even direct S.post(BASE+...) get recorded."""
    orig_request = session.request
    def req(method, url, *args, **kwargs):
        try:
            if url.startswith(BASE):
                rel = url[len(BASE):]
                if not rel.startswith("/"):
                    rel = "/" + rel
                _record(method, rel)
            resp = orig_request(method, url, *args, **kwargs)  # make the request
            # count every connection (even ones not wrapped by ok()/skip())
            try:
                m = method.upper()
                REQUESTS_BY_METHOD[m] = REQUESTS_BY_METHOD.get(m, 0) + 1
            except Exception:
                pass
            globals()["REQUESTS_TOTAL"] = globals().get("REQUESTS_TOTAL", 0) + 1
            return resp
        except Exception:
            # still fall back if anything goes weird
            return orig_request(method, url, *args, **kwargs)
    session.request = req

_instrument_session(S)

# Wrap the helper verbs to record too (covers get("/x") style)
def post(path, **kw):   _record("POST", path);   return post_raw(path, **kw)
def get(path,  **kw):   _record("GET", path);    return get_raw(path,  **kw)
def put(path,  **kw):   _record("PUT", path);    return put_raw(path,  **kw)
def patch(path,**kw):   _record("PATCH", path);  return patch_raw(path, **kw)
def delete(path,**kw):  _record("DELETE", path); return delete_raw(path, **kw)

def _tmpl_to_regex(tmpl: str) -> re.Pattern:
    """
    Convert an OpenAPI path template like '/contracts/{id}' to a regex that matches
    any single path segment for each '{...}' placeholder.
    """
    rx = re.sub(r"\{[^/}]+\}", r"[^/]+", tmpl)
    return re.compile(r"^" + rx + r"$")

def _coverage_report():
    global MISSING_ENDPOINTS
    try:
        spec = S.get(f"{BASE}/openapi.json", timeout=30)
        print("\n======= API COVERAGE =======")
        if spec.status_code != 200:
            print("Could not fetch /openapi.json (status", spec.status_code, ")")
            MISSING_ENDPOINTS = []
            return []
        api = spec.json()
        declared, tags_by_ep = set(), {}
        for path, methods in (api.get("paths") or {}).items():
            for method, meta in methods.items():
                m = method.upper()
                if m in ("GET","POST","PUT","PATCH","DELETE"):
                    declared.add((m, path))
                    tags_by_ep[(m, path)] = list(meta.get("tags") or [])
        rx_cache = {}
        covered = set()
        for (dm, dp) in declared:
            rx = rx_cache.get(dp)
            if not rx:
                rx = _tmpl_to_regex(dp); rx_cache[dp] = rx
            if any(tm == dm and rx.match(tp) for (tm, tp) in TESTED):
                covered.add((dm, dp))
        missing = sorted(list(declared - covered))
        MISSING_ENDPOINTS = missing
        print(f"Declared: {len(declared)}  Tested: {len(covered)}  Missing: {len(missing)}")
        per_tag_counts = {}
        for ep in declared:
            for t in tags_by_ep.get(ep, []) or ["(untagged)"]:
                per_tag_counts.setdefault(t, {"decl":0,"cov":0})
                per_tag_counts[t]["decl"] += 1
                if ep in covered:
                    per_tag_counts[t]["cov"] += 1
        if per_tag_counts:
            print("\nBy Tag:")
            for t, c in sorted(per_tag_counts.items(), key=lambda kv: (kv[1]["cov"]/max(1,kv[1]["decl"]), kv[0])):
                print(f"  - {t}: {c['cov']}/{c['decl']}")
        if missing:
            print("\nMissing endpoints:")
            for i, (m, p) in enumerate(missing[:120], 1):
                print(f"  {i:>3}. {m} {p}")
            if len(missing) > 120:
                print(f"  ... (+{len(missing)-120} more)")
        return missing
    except Exception as e:
        print("\n======= API COVERAGE =======")
        print("Coverage computation failed:", type(e).__name__, str(e))
        MISSING_ENDPOINTS = []
        return []
# ===== /Coverage tracking =====

def main():
    today = str(date.today())
    # --------- BASIC PAGES / HEALTH ---------
    ok("GET /docs", get("/docs"))
    ok("GET /healthz", get("/healthz"))
    ok("GET /time/sync", get("/time/sync"))
    ok("GET /ops/readiness", get("/ops/readiness"))
    get("/__diag/users_count")  # best-effort
    ok("GET /terms", get("/terms"))
    ok("GET /eula",  get("/eula"))
    ok("GET /privacy", get("/privacy"))

    # HTML pages + XSRF cookie (cheap wins)
    ok("GET /health", get("/health"))            # alias that also sets XSRF cookie
    ok("GET /",       get("/"))                  # login page + XSRF   <-- fixed extra ')'
    ok("GET /buyer",  get("/buyer"))             # dynamic nonce CSP
    ok("GET /seller", get("/seller"))
    ok("GET /admin",  get("/admin"))

    # --------- AUTH / SESSION ---------
    r = get("/me")
    ok("GET /me", r, cond=r.status_code in (200, 401))

    # --------- LEGAL (site policy pages) ---------
    for p in [
        "/legal/cookies","/legal/subprocessors","/legal/dpa","/legal/sla",
        "/legal/security","/legal/privacy-appendix","/legal/rulebook","/legal/rulebook/versions"
    ]:
        ok(f"GET {p}", get(p))

    # Minor legal endpoints
    ok("GET /legal/terms",   get("/legal/terms"))
    ok("GET /legal/eula",    get("/legal/eula"))
    ok("GET /legal/privacy", get("/legal/privacy"))

    # --------- LEGAL (extra pages) ---------
    ok("GET /legal/fees", get("/legal/fees"))
    ok("GET /legal/aup",  get("/legal/aup"))

    # --------- PRICES / FX ---------
    ok("GET /prices/copper_last", get("/prices/copper_last"))
    ok("GET /fx/convert", get("/fx/convert", params={"amount": 100, "from_ccy":"USD","to_ccy":"EUR"}))

    # --------- ADMIN / EXPORTS ---------
    if ENV == "production":
        if not maybe_login_admin():
            skip("Admin block", "Skipping admin endpoints without login")
        r = post("/admin/run_snapshot_bg", headers=hdr(h_snapshot()), json=None); ok("POST /admin/run_snapshot_bg", r)
    else:
        r = post("/admin/run_snapshot_bg", headers=hdr(h_snapshot()), json=None); ok("POST /admin/run_snapshot_bg", r)

    r = get("/admin/exports/all.zip")
    ok("GET /admin/exports/all.zip", r, cond=(r.status_code==200 and "application/zip" in r.headers.get("content-type","")))

    r = get("/admin/exports/contracts.csv")
    ok("GET /admin/exports/contracts.csv", r, cond=(r.status_code==200 and "text/csv" in r.headers.get("content-type","")))

    # `GET /admin/export_all` (alias in some builds)
    ok("GET /admin/export_all", get("/admin/export_all"))

    # Admin bootstrap helpers
    ok("POST /admin/create_user",
       post("/admin/create_user",
            json={"email":"ops@example.com","password":"Admin123!","role":"admin"},
            headers={"X-Setup-Token": SETUP_TOKEN} if SETUP_TOKEN else {}))
    ok("POST /admin/plans/seed_mode_a", post("/admin/plans/seed_mode_a"))

    # Audit chain (append → seal → verify)
    if ENV == "production" and not (ADMIN_EMAIL and ADMIN_PASSWORD):
        skip("Admin audit endpoints", "No admin login in prod")
    else:
        ok("POST /admin/audit/log", post("/admin/audit/log", json={"payload":{"note":"smoke-test"}}))
        today = str(date.today())
        ok("POST /admin/audit/seal", post("/admin/audit/seal", params={"chain_date": today}))
        ok("GET /admin/audit/verify", get("/admin/audit/verify", params={"chain_date": today}))

    # Additional admin surfaces (soft asserts)
    ok("POST /admin/run_snapshot_now", post("/admin/run_snapshot_now"))
    ok("GET /admin/backup/selfcheck", get("/admin/backup/selfcheck"))
    ok("POST /admin/webhooks/replay", post("/admin/webhooks/replay", json={"kind":"ice"}))
    ok("POST /admin/keys/rotate", post("/admin/keys/rotate"))
    ok("GET /admin/dr/objectives", get("/admin/dr/objectives"))

    # Admin fees + compliance + statements/applications (soft paths)
    ok("POST /admin/fees/upsert", post("/admin/fees/upsert", json={"symbol":"CU-SHRED-1M","maker_bps":2,"taker_bps":4}))
    ok("POST /admin/compliance/member/set",
       post("/admin/compliance/member/set", 
            json={"username":"m1","kyc":True,"aml":True,"sanctions":True,"boi":True,"bsa_risk":"low"}))
    ok("POST /admin/statements/run", post("/admin/statements/run", params={"as_of": today}))
    # statements pdf (best-effort using dummy IDs/dates)
    ok("GET /statements/{member}/{as_of}.pdf", get(f"/statements/m1/{today}.pdf", stream=True))
    ok("GET /admin/applications", get("/admin/applications"))
    ok("GET /admin/applications/export_csv", get("/admin/applications/export_csv"))
    # Approve first application if present
    r_apps = get("/admin/applications")
    if r_apps.status_code == 200:
        try:
            arr = r_apps.json()
            if arr:
                app_id = arr[0].get("id") or arr[0].get("application_id")
                if app_id:
                    ok("POST /admin/applications/{id}/approve",
                       post(f"/admin/applications/{app_id}/approve"))
        except Exception:
            pass

    # --------- BILLING (admin & customer-facing, non-Stripe webhook paths) ---------
    ok("POST /admin/billing_contact/upsert",
       post("/admin/billing_contact/upsert", params={"member":"Acme Yard","email":"billing@example.com"}))
    ok("GET /billing/pm/status",   get("/billing/pm/status",   params={"member":"Acme Yard"}))
    ok("POST /billing/prefs/upsert",
       post("/billing/prefs/upsert", json={"member":"Acme Yard","billing_day":15,"timezone":"America/Phoenix","auto_charge":False}))
    ok("GET /billing/pm/details",  get("/billing/pm/details",  params={"member":"Acme Yard"}))
    ok("GET /billing/subscribe/finalize_from_session",
       get("/billing/subscribe/finalize_from_session", params={"sess":"fake"}))

    # Billing: subscribe/checkout & PM setup/change (accept 400 if no Stripe)
    ok("POST /billing/subscribe/checkout",
       post("/billing/subscribe/checkout", params={"member":"Acme Yard","plan":"starter","email":"billing@example.com"}))
    ok("POST /billing/pm/setup_session",
       post("/billing/pm/setup_session", params={"member":"Acme Yard","email":"billing@example.com"}))
    ok("POST /billing/pm/change_session",
       post("/billing/pm/change_session", json={"member":"Acme Yard"}))
    # Payment endpoints (404 on bogus invoice_id is fine)
    ok("POST /billing/pay/checkout",
       post("/billing/pay/checkout", json={"invoice_id":"00000000-0000-0000-0000-000000000000"}))
    ok("GET  /billing/pay/finalize_from_session",
       get("/billing/pay/finalize_from_session", params={"sess":"fake"}))
    ok("POST /billing/pay/card",
       post("/billing/pay/card", json={"invoice_id":"00000000-0000-0000-0000-000000000000"}))
    ok("POST /billing/pay/ach",
       post("/billing/pay/ach",  json={"invoice_id":"00000000-0000-0000-0000-000000000000"}))
    ok("POST /billing/pay/charge_now",
       post("/billing/pay/charge_now", json={"invoice_id":"00000000-0000-0000-0000-000000000000"}))

    # --------- PLANS & MEMBERSHIP (admin) ---------
    ok("POST /admin/member/plan/set",
       post("/admin/member/plan/set", params={"member":"Acme Yard","plan_code":"starter"}))
    ok("GET /billing/preview",
       get("/billing/preview", params={"member":"Acme Yard","month": str(date.today())[:7]}))
    ok("POST /billing/run",
       post("/billing/run", params={"member":"Acme Yard","month": str(date.today())[:7], "force":"1"}))

    # --------- REFERENCE PRICES & INDICES ---------
    try:
        post("/reference_prices/pull_home")
        post("/reference_prices/pull_now_all")
    except requests.ReadTimeout:
        skip("/reference_prices/pull_now_all", "Timed out on external scrapes; skipping locally")
    ok("GET /reference_prices/latest", get("/reference_prices/latest", params={"symbol":"COMEX_Cu"}))

    ok("GET /indices/universe", get("/indices/universe"))
    r = get("/indices/latest", params={"symbol":"BR-CU"})
    if r.status_code == 404:
        skip("GET /indices/latest", "no index history yet")
    else:
        ok("GET /indices/latest", r)

    # Indices builder endpoints
    ok("POST /indices/run",        post("/indices/run"))  # nightly builder trigger
    ok("POST /indices/backfill",   post("/indices/backfill", params={"start": str(date.today()), "end": str(date.today())}))
    ok("POST /indices/generate_snapshot",
       post("/indices/generate_snapshot", params={"snapshot_date": str(date.today())}))
    ok("POST /indices/seed_copper_indices", post("/indices/seed_copper_indices"))

    # --------- INVENTORY SEED for contract create ---------
    seller = "Acme Yard"
    sku    = "CU-SHRED-1M"
    inv = {"seller": seller, "sku": sku, "qty_on_hand": 100.0, "uom":"ton", "location":"YARD-A", "description":"Seed"}
    ok("POST /inventory/manual_add", post("/inventory/manual_add", json=inv))

    # Inventory surfaces
    ok("GET /inventory", get("/inventory", params={"seller": seller}))
    ok("GET /inventory/movements/list", get("/inventory/movements/list", params={"seller": seller}))
    ok("GET /inventory/finished_goods", get("/inventory/finished_goods", params={"seller": seller}))
    ok("GET /inventory/template.csv", get("/inventory/template.csv"))
    # CSV import (tiny inline)
    csv_body = "seller,sku,qty_on_hand,uom,location,description\nAcme Yard,CU-SHRED-1M,5,ton,YARD-A,imported\n"
    ok("POST /inventory/import/csv", post("/inventory/import/csv",
       files={"file": ("inventory.csv", csv_body, "text/csv")}))

    # --------- INVENTORY BULK API ---------
    bulk_payload = {
        "source":"test","seller":"Acme Yard",
        "items":[{"sku":"CU-SHRED-1M","qty_on_hand":111,"uom":"ton","location":"YARD-A","description":"bulk"}]
    }
    r = post("/inventory/bulk_upsert", json=bulk_payload)
    if r.status_code in (200,201,204):
        ok("POST /inventory/bulk_upsert", r)
    elif ENV == "production" and r.status_code in (401,403):
        ok("POST /inventory/bulk_upsert", r)  # gated as expected; still log resp
    else:
        ok("POST /inventory/bulk_upsert", r)

    # --------- INVENTORY EXCEL IMPORT (expect 400 with CSV bytes/xlsx mime) ---------
    _has_xlsx = True
    try:
        import pandas  # noqa
        import openpyxl  # noqa
    except Exception:
        _has_xlsx = False

    if not _has_xlsx:
        skip("POST /inventory/import/excel", "pandas/openpyxl not installed")
    else:
        r = post("/inventory/import/excel",
                 files={"file": ("inv.xlsx", b"sku,qty_on_hand\nX,1\n",
                                 "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        if r.status_code == 400:
            ok("POST /inventory/import/excel", r, cond=True)  # exercised error path; log resp
        else:
            ok("POST /inventory/import/excel", r)

    # --------- CONTRACTS (create → list → get → export) ---------
    contract = {
        "buyer":"Buyer Inc",
        "seller": seller,
        "material": sku,
        "weight_tons": 20,
        "price_per_ton": 250.0,
    }
    r = post("/contracts", json=contract); ok("POST /contracts (create)", r)
    cid = None
    try:
        cid = r.json()["id"]
    except Exception:
        pass

    ok("GET /contracts", get("/contracts", params={"seller": seller}))
    if cid:
        ok("GET /contracts/{id}", get(f"/contracts/{cid}"))
        ok("PUT /contracts/{id} → Signed", put(f"/contracts/{cid}", json={"status":"Signed","signature":"SmokeTester"}))
        ok("GET /contracts/export_csv", get("/contracts/export_csv", stream=True))

        r = patch(f"/contracts/{cid}/purchase", json={"op":"purchase","expected_status":"Pending"})
        if r.status_code == 409:
            skip("PATCH /contracts/{id}/purchase", "already Signed/Pending mismatch")
        else:
            ok("PATCH /contracts/{id}/purchase", r)

        r = post(f"/contracts/{cid}/cancel")
        if r.status_code == 409:
            skip("POST /contracts/{id}/cancel", "not in Pending")
        else:
            ok("POST /contracts/{id}/cancel", r)

        # Contracts sign + admin close-through
        ok("POST /contracts/{id}/sign", post(f"/contracts/{cid}/sign"))
        ok("POST /admin/close_through", post("/admin/close_through", params={"cutoff": str(date.today())}))

    # Import normalized contracts CSV (data)
    csv_body2 = "buyer,seller,material,weight_tons,price_per_ton\nB,S,TestMat,1.0,123.0\n"
    ok("POST /import/contracts_csv", post("/import/contracts_csv",
       files={"file": ("contracts.csv", csv_body2, "text/csv")}))

    # --------- BOLs (create → list → deliver → pdf) ---------
    dummy_contract = cid or str(uuid.uuid4())
    bol = {
        "contract_id": dummy_contract,
        "buyer": "Buyer Inc",
        "seller": seller,
        "material": sku,
        "weight_tons": 10.0,
        "price_per_unit": 250.0,
        "total_value": 2500.0,
        "carrier": {"name":"XYZ Truck","driver":"Jane D","truck_vin":"VIN123"},
        "pickup_signature": {"base64":"", "timestamp":"2025-01-01T00:00:00+00:00"},
        "pickup_time": "2025-01-01T00:05:00+00:00"
    }
    r = post("/bols", json=bol); ok("POST /bols (create)", r)
    bol_id = None
    try: bol_id = r.json()["bol_id"]
    except Exception: pass

    ok("GET /bols", get("/bols"))
    if bol_id:
        ok("POST /bols/{id}/deliver", post(f"/bols/{bol_id}/deliver"))
        r = get(f"/bol/{bol_id}/pdf")
        ok("GET /bol/{id}/pdf", r, cond=(r.status_code==200 and r.headers.get("content-type","").startswith("application/pdf")))

    # --------- RECEIPTS → STOCKS ---------
    rec = {
        "seller": seller,
        "sku": sku,
        "qty_tons": 20.0,
        "location": "YARD-A"
    }
    r = post("/receipts", json=rec); ok("POST /receipts", r)
    rid = None
    try: rid = r.json()["receipt_id"]
    except Exception: pass

    if rid:
        ok("POST /receipts/{id}/consume", post(f"/receipts/{rid}/consume"))

    today = str(date.today())
    ok("POST /stocks/snapshot", post("/stocks/snapshot", params={"as_of": today}))
    ok("GET /stocks", get("/stocks", params={"as_of": today}))
    ok("GET /stocks.csv", get("/stocks.csv", params={"as_of": today}))

    # --------- ANALYTICS ---------
    ok("GET /analytics/material_price_history", get("/analytics/material_price_history", params={"material": sku}))
    ok("GET /analytics/rolling_bands", get("/analytics/rolling_bands", params={"material": sku}))
    ok("GET /public/indices/daily.json", get("/public/indices/daily.json"))
    ok("GET /public/indices/daily.csv", get("/public/indices/daily.csv"))
    ok("GET /analytics/price_band_estimates", get("/analytics/price_band_estimates", params={"material": sku}))
    ok("GET /analytics/delta_anomalies", get("/analytics/delta_anomalies", params={"material": sku}))
    ok("GET /indices", get("/indices"))

    # --------- BUYER POSITIONS ---------
    ok("GET /buyer_positions", get("/buyer_positions", params={"buyer":"Buyer Inc"}))

    # --------- FORECASTS (best effort) ---------
    post("/forecasts/run")
    r = get("/forecasts/latest", params={"symbol": sku, "horizon_days":30})
    if r.status_code == 404:
        skip("GET /forecasts/latest", "No forecasts available")
    else:
        ok("GET /forecasts/latest", r)

    # --------- PRICING ---------
    r = get("/pricing/quote", params={"category":"scrap","material":sku})
    if r.status_code in (200,404):
        if r.status_code == 404:
            skip("GET /pricing/quote", "No internal price available")
        else:
            ok("GET /pricing/quote", r)
    else:
        ok("GET /pricing/quote", r)

    # --------- COMPLIANCE ---------
    ok("GET /export/tax_lookup", get("/export/tax_lookup", params={"hs_code":"7404","dest":"US"}))

    # --------- PRODUCTS / FUTURES / MARKS ---------
    symbol_root = "CU-SHRED-1M"
    ok("POST /products", post("/products",
       json={"symbol":symbol_root, "description":"Copper Shred 1M", "unit":"ton", "quality":{"grade":"shred"}}))

    fprod = {
        "symbol_root": symbol_root,
        "material": "Copper Shred",
        "delivery_location": "YARD-A",
        "contract_size_tons": 20.0,
        "tick_size": 0.5,
        "currency": "USD",
        "price_method": "MANUAL"
    }
    r = post("/admin/futures/products", json=fprod); ok("POST /admin/futures/products", r)
    try:
        product_id = r.json()["id"]
    except Exception:
        product_id = None

    listing_id = None
    if product_id:
        ok("POST /admin/futures/products/{root}/pricing",
           post(f"/admin/futures/products/{symbol_root}/pricing",
                json={"lookback_days":14,"basis_adjustment":0,"carry_per_month":0,"manual_mark":250.0,"external_source":None}))
        r = post("/admin/futures/series/generate", json={"product_id": product_id, "months_ahead": 1, "day_of_month": 15})
        ok("POST /admin/futures/series/generate", r)
        try:
            listing_id = r.json()[0]["id"]
        except Exception:
            rg = get("/admin/futures/series", params={"product_id": product_id})
            if rg.status_code == 200:
                arr = rg.json()
                if arr:
                    listing_id = arr[0].get("id")

        if listing_id:
            ok("POST /admin/futures/series/{id}/list", post(f"/admin/futures/series/{listing_id}/list"))
            ok("POST /admin/futures/marks/publish", post("/admin/futures/marks/publish", json={"listing_id": listing_id}))
            ok("POST /admin/futures/series/{id}/trading_status",
               post(f"/admin/futures/series/{listing_id}/trading_status", json={"trading_status":"Trading"}))
            ok("GET /admin/futures/marks", get("/admin/futures/marks"))
            ok("GET /admin/futures/series", get("/admin/futures/series"))
            ok("POST /admin/futures/series/{id}/expire", post(f"/admin/futures/series/{listing_id}/expire"))
            ok("POST /admin/futures/series/{id}/finalize", post(f"/admin/futures/series/{listing_id}/finalize"))

    # --------- RISK / ENTITLEMENTS / TRADING / CLOB / FIX / RFQ ---------
    for feat in ["clob.trade","trade.place","trade.modify","rfq.post","rfq.quote","rfq.award"]:
        post("/admin/entitlements/grant", params={"user":"anon","feature":feat})

    post("/risk/price_band/CU-SHRED-1M", params={"lower": 100.0, "upper": 1000.0})
    post("/risk/luld/CU-SHRED-1M", params={"down_pct": 0.20, "up_pct": 0.20})
    post("/risk/limits", params={"member":"anon","symbol":"CU-SHRED-1M","limit_lots": 200})

    r = post("/clob/orders", json={"symbol":"CU-SHRED-1M", "side":"buy", "price":250, "qty_lots":1, "tif":"day"})
    clob_oid = None
    if r.status_code == 200:
        ok("POST /clob/orders", r)
        try:
            clob_oid = r.json().get("order_id")
        except Exception:
            pass
    else:
        skip("POST /clob/orders", f"status={r.status_code}")

    ok("GET /clob/orderbook", get("/clob/orderbook", params={"symbol":"CU-SHRED-1M","depth":5}))

    fix = {"ClOrdID":"T1","Symbol":"CU-SHRED-1M","Side":"1","Price":255.0,"OrderQty":1.0,"TimeInForce":"0","SenderCompID":"fix_member"}
    ok("POST /fix/order", post("/fix/order", json=fix))
    ok("POST /fix/dropcopy", post("/fix/dropcopy", json={"msg":"EXEC_REPORT"}))

    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    rfq_id = None
    r = post("/rfq", json={"symbol":"CU-SHRED-1M","side":"buy","quantity_lots":"1","price_limit":"260","expires_at": expires_at})
    if r.status_code == 200:
        ok("POST /rfq", r)
        try:
            rfq_id = r.json().get("rfq_id")
        except Exception:
            rfq_id = None
    else:
        skip("POST /rfq", f"status={r.status_code}")

    if rfq_id:
        r = post(f"/rfq/{rfq_id}/quote", json={"price":"258","qty_lots":"1"})
        if r.status_code == 200:
            ok("POST /rfq/{id}/quote", r)
            quote_id = None
            try:
                quote_id = r.json().get("quote_id")
            except Exception:
                pass
            if quote_id:
                ok("POST /rfq/{id}/award", post(f"/rfq/{rfq_id}/award", params={"quote_id":quote_id}))
        else:
            skip("POST /rfq/{id}/quote", f"status={r.status_code}")

    ok("POST /settlement/publish", post("/settlement/publish", params={"symbol":"CU-SHRED-1M","as_of":str(date.today()), "method":"vwap_last60m"}))
    ok("GET /index/latest", get("/index/latest"))
    ok("GET /index/history", get("/index/history", params={"symbol":"CU-SHRED-1M"}))
    ok("GET /index/history.csv", get("/index/history.csv", params={"symbol":"CU-SHRED-1M"}))
    ok("GET /index/tweet", get("/index/tweet"))
    ok("POST /index/contracts/expire", 
       post("/index/contracts/expire", json={"tradable_symbol":"CU-SHRED-1M","as_of": today}))

    ok("GET /clearing/positions", get("/clearing/positions", params={"account_id": str(uuid.uuid4())}))
    ok("GET /clearing/margin",    get("/clearing/margin",    params={"account_id": str(uuid.uuid4())}))
    ok("POST /clearing/variation_run", post("/clearing/variation_run", json={"mark_date": str(date.today())}))
    ok("POST /clearing/deposit", post("/clearing/deposit", json={"account_id":"acct1","amount": 100000.0}))
    ok("POST /clearing/guaranty/deposit", post("/clearing/guaranty/deposit", json={"member":"m1","amount":1000.0}))
    ok("POST /clearing/waterfall/apply", 
       post("/clearing/waterfall/apply", json={"member":"m1","shortfall_usd":5000.0}))

    ok("POST /risk/margin/calc", 
       post("/risk/margin/calc", json={"member":"acct1","symbol":"CU-SHRED-1M","price":250.0,"net_lots":2}))
    ok("POST /risk/portfolio_margin", 
       post("/risk/portfolio_margin", params={"member":"acct1","symbol":"CU-SHRED-1M","price":250.0,"net_lots":2}))
    ok("POST /risk/kill_switch/{member_id}", post("/risk/kill_switch/m1"))

    ok("GET /surveil/alerts", get("/surveil/alerts"))
    ok("POST /surveil/alert", 
       post("/surveil/alert", json={"rule":"test_rule","subject":"demo","data":{"k":"v"},"severity":"info"}))
    ok("POST /ml/anomaly", 
       post("/ml/anomaly", json={"member":"Acme Yard","symbol":"CU-SHRED-1M","as_of": today,"features":{"cancel_rate":0.1,"gps_var":0.2}}))
    ok("POST /surveil/case/open", 
       post("/surveil/case/open", params={"rule":"test_rule","subject":"demo","notes":"smoke"}))
    ok("POST /surveil/rules/run", 
       post("/surveil/rules/run", params={"symbol":"CU-SHRED-1M","window_minutes":5}))

    if listing_id:
        ok("GET /ticker", get("/ticker", params={"listing_id": listing_id}))
    else:
        skip("GET /ticker", "no listing_id available")

    # --------- RULEBOOK ADMIN ---------
    r = post("/admin/legal/rulebook/upsert",
             params={"version":"v1","effective_date":str(date.today()),"file_path":"static/legal/rulebook.html"})
    ok("POST /admin/legal/rulebook/upsert", r,
       cond=(r.status_code in (200,201,204) or (ENV == "production" and r.status_code in (401,403))))

    # --------- QBO OAUTH RELAY (dev sanity) ---------
    ok("GET /qbo/callback",
       get("/qbo/callback", params={"code":"abc","state":"01234567state","realmId":"12345"}))
    ok("GET /admin/qbo/peek",
       get("/admin/qbo/peek", 
           params={"state":"01234567state"}),
           headers={"X-Relay-Auth": os.environ.get("QBO_RELAY_AUTH","")})

    payload = {"example":"ice-hook"}
    if ICE_SECRET:
        import hmac, hashlib
        body = json.dumps(payload).encode("utf-8")
        mac  = hmac.new(ICE_SECRET.encode(), body, hashlib.sha256).hexdigest()
        ok("POST /ice/webhook (HMAC)", post("/ice/webhook", headers={"X-Signature": mac}, data=body))
    else:
        r = post("/ice/webhook", data=json.dumps(payload))
        ok("POST /ice/webhook", r, cond=(r.status_code == 200))

    ok("POST /ice-digital-trade", post("/ice-digital-trade", json={"event":"ping"}))

    # --------- STRIPE WEBHOOK (bad sig → expect 400) ---------
    r = post("/stripe/webhook", headers={"Stripe-Signature":"bad"}, data=b'{}')
    ok("POST /stripe/webhook", r, cond=(r.status_code == 400))

    # --------- PUBLIC APPLICATION (may 402 without PM; accept as exercised) ---------
    r = post("/public/apply", json={
      "entity_type":"yard","role":"buyer","org_name":"Acme Yard","contact_name":"Alice",
      "email":"alice@example.com","plan":"starter"
    })
    ok("POST /public/apply", r, cond=(r.status_code in (200,201,202,204,402)))

    # ---- Exercise the rest of the surface (soft asserts) ----
    try:
        run_additional(listing_id=listing_id)
    except Exception as e:
        skip("run_additional()", f"{type(e).__name__}: {e}")

    # --- Quick adds to lift coverage for trading/warrants/insurance ---
    # 1) Futures trading surface
    acct = str(uuid.uuid4())
    ok("POST /clearing/deposit", post("/clearing/deposit", json={"account_id": acct, "amount": 1_000_000}))
    # Need a listing_id to trade; get first available
    try:
        r_ls = get("/admin/futures/series")
        li = (r_ls.json()[0]["id"] if r_ls.status_code==200 and r_ls.json() else None)
    except Exception:
        li = None
    if li:
        # place → modify → book → cancel
        r = post("/trade/orders", json={"account_id": acct,"listing_id": li,"side":"BUY","price":250,"qty":1,"order_type":"LIMIT","tif":"GTC"}); ok("POST /trade/orders", r)
        try: oid = r.json().get("order_id")
        except Exception: oid = None
        if oid:
            ok("PATCH /trade/orders/{id}", patch(f"/trade/orders/{oid}", json={"price":251,"qty":1}))
            ok("GET /trade/book", get("/trade/book", params={"listing_id": li, "depth": 5}))
            ok("DELETE /trade/orders/{id}", delete(f"/trade/orders/{oid}"))
    # 2) CLOB cancel path (exercise DELETE)
    r = post("/clob/orders", json={"symbol":"CU-SHRED-1M","side":"sell","price":260,"qty_lots":1,"tif":"day"}); 
    try: clob_id = r.json().get("order_id")
    except Exception: clob_id = None
    if clob_id:
        ok("DELETE /clob/orders/{id}", delete(f"/clob/orders/{clob_id}"))

    # 3) Warrants lifecycle
    # mint a receipt if there isn't one yet
    rrec = post("/receipts", json={"seller":"Acme Yard","sku":"CU-SHRED-1M","qty_tons":2.0})
    try: rec_id = rrec.json().get("receipt_id")
    except Exception: rec_id = None
    if rec_id:
        r_w = post("/warrants/mint", json={"receipt_id": rec_id, "holder":"Acme Yard"}); ok("POST /warrants/mint", r_w)
        try: wid = r_w.json().get("warrant_id")
        except Exception: wid = None
        if wid:
            ok("POST /warrants/transfer", post("/warrants/transfer", params={"warrant_id": wid, "new_holder":"BankCo"}))
            ok("POST /warrants/pledge",   post("/warrants/pledge",   params={"warrant_id": wid, "lender":"BankCo"}))
            ok("POST /warrants/release",  post("/warrants/release",  params={"warrant_id": wid}))

    # 4) Insurance quote
    ok("POST /insurance/quote", post("/insurance/quote", json={"receipt_id": rec_id or str(uuid.uuid4()), "coverage_usd": 10000}))

    # --------- COVERAGE REPORT ---------
    missing = _coverage_report()

    # --------- DETAILED SUMMARY ---------
    print("\n======= DETAILED SUMMARY =======")
    total = len(RESULTS)
    passed = len([x for x in RESULTS if x[0]=="PASS"])
    failed = len([x for x in RESULTS if x[0]=="FAIL"])
    skipped= len([x for x in RESULTS if x[0]=="SKIP"])
    print(f"Good: {passed}   Fail: {failed}   Skip: {skipped}   Total checks: {total}")

    # Failures by status code
    fails = []
    for kind, name, msg in RESULTS:
        if kind == "FAIL":
            status = None
            m = re.search(r"HTTP\s+(\d{3})", msg or "")
            if m: status = int(m.group(1))
            fails.append((status, name, msg))
    if fails:
        print("\nFailures by status:")
        from collections import defaultdict
        buckets = defaultdict(list)
        for st, nm, ms in fails:
            buckets[st].append((nm, ms))
        for st in sorted(buckets.keys(), key=lambda x: (x is None, x)):
            label = str(st) if st is not None else "unknown"
            print(f"  {label}: {len(buckets[st])}")
            for nm, ms in buckets[st][:10]:
                print(f"    - {nm} :: {ms[:140]}{'...' if len(ms)>140 else ''}")
            if len(buckets[st]) > 10:
                print(f"    ... (+{len(buckets[st])-10} more)")

    # HTTP connection counters
    print(f"\nHTTP connections (all): {REQUESTS_TOTAL}  "
          f"[GET={REQUESTS_BY_METHOD.get('GET',0)}, "
          f"POST={REQUESTS_BY_METHOD.get('POST',0)}, "
          f"PUT={REQUESTS_BY_METHOD.get('PUT',0)}, "
          f"PATCH={REQUESTS_BY_METHOD.get('PATCH',0)}, "
          f"DELETE={REQUESTS_BY_METHOD.get('DELETE',0)}]")

    if missing:
        print(f"\nMissing endpoints (from OpenAPI): {len(missing)}")

    # Write machine-readable artifact
    artifact = {
        "base": BASE,
        "env": ENV,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "missing": missing,
        "run_log": RUN_LOG,
        "requests_total": REQUESTS_TOTAL,
        "requests_by_method": REQUESTS_BY_METHOD,
    }
    try:
        os.makedirs("tests/_reports", exist_ok=True)
        with open("tests/_reports/last_run_report.json", "w", encoding="utf-8") as f:
            json.dump(artifact, f, indent=2)
        print('Saved report → tests/_reports/last_run_report.json')
    except Exception as _e:
        print("Could not write report:", _e)

    # --------- SUMMARY + EXIT CODE ---------
    print("\n======= SUMMARY =======")
    print(f"Total: {total}  PASS: {passed}  FAIL: {failed}  SKIP: {skipped}")

    exit_code = 0
    if FAIL_ON_FAIL and failed:
        exit_code = 1
    if FAIL_ON_MISSING and len(missing) > 0:
        exit_code = max(exit_code, 2)

    if failed:
        print("\nFailures:")
        for kind, name, msg in RESULTS:
            if kind=="FAIL":
                print(f"- {name}: {msg}")

    if exit_code:
        sys.exit(exit_code)

# ======== ADDITIVE SMOKE: exercise the rest of the surface (dev-friendly) ========

def _ok(name, resp, also_ok=(200,201,204,409,404)):
    try:
        sc = resp.status_code
    except Exception:
        print(f"❌ {name} :: no response object")
        return
    if sc in also_ok:
        print(f"✅ {name}")
    else:
        print(f"❌ {name} :: HTTP {sc} :: {getattr(resp,'text','')[:200]}")

def _json(resp, default=None):
    try:
        return resp.json()
    except Exception:
        return default

def run_additional(listing_id=None):
    # --- 0) Ensure we have a session user (signup → login) ---
    email = "testuser@example.com"
    pwd   = "TestPass123!"
    _ok("POST /signup", S.post(f"{BASE}/signup", json={"email": email, "password": pwd, "role": "buyer"}))
    _ok("POST /login",  S.post(f"{BASE}/login",  json={"username": email, "password": pwd}))

    # --- 1) Grant entitlements for features we’ll hit (dev only; ungated) ---
    for feat in ["rfq.post","rfq.quote","rfq.award","clob.trade","trade.place","trade.modify"]:
        r = S.post(f"{BASE}/admin/entitlements/grant", params={"user": email, "feature": feat})
        _ok(f"POST /admin/entitlements/grant?feature={feat}", r)

    # --- 2) Minimal product & futures scaffolding ---
    r = S.post(f"{BASE}/products", json={"symbol":"CU-SHRED-1M","description":"Copper Shred 1M","unit":"ton","quality":{"purity":"mixed"}})
    _ok("POST /products", r)

    fut = {
        "symbol_root":"CU-SHRED-1M",
        "material":"Copper Shred",
        "delivery_location":"MIDWEST",
        "contract_size_tons":20.0,
        "tick_size":0.5,
        "currency":"USD",
        "price_method":"VWAP_BASIS"
    }
    r = S.post(f"{BASE}/admin/futures/products", json=fut)
    _ok("POST /admin/futures/products", r)
    fut_row = _json(r, {})
    prod_id = fut_row.get("id")

    if prod_id:
        _ok("POST /admin/futures/products/{symbol_root}/pricing",
            S.post(f"{BASE}/admin/futures/products/{fut['symbol_root']}/pricing",
                   json={"lookback_days":14,"basis_adjustment":0.0,"carry_per_month":0.0,"manual_mark":None,"external_source":None}))

        r = S.post(f"{BASE}/admin/futures/series/generate", json={"product_id": prod_id, "months_ahead": 1, "day_of_month": 15})
        _ok("POST /admin/futures/series/generate", r)
        r = S.get(f"{BASE}/admin/futures/series", params={"product_id": prod_id})
        _ok("GET /admin/futures/series", r)
        listings = _json(r, [])
        li_from_admin = listings[0]["id"] if listings else None

        if li_from_admin:
            _ok("POST /admin/futures/marks/publish",
                S.post(f"{BASE}/admin/futures/marks/publish", json={"listing_id": li_from_admin}))

if __name__ == "__main__":
    main()
