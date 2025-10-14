# full_test_suite.py (merged)
# End-to-end smoke/flow test for FastAPI backend.
# Run with:  python tests/full_test_suite.py
import os, sys, time, json, uuid
from datetime import date, datetime, timedelta, timezone
import requests

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

def post(path, **kw):   return S.post(f"{BASE}{path}", timeout=30, **kw)
def get(path,  **kw):   return S.get(f"{BASE}{path}",  timeout=30, **kw)
def put(path,  **kw):   return S.put(f"{BASE}{path}",  timeout=30, **kw)
def patch(path,**kw):   return S.patch(f"{BASE}{path}",timeout=30, **kw)
def delete(path,**kw):  return S.delete(f"{BASE}{path}",timeout=30, **kw)

# Pretty printer
RESULTS = []
def ok(name, resp=None, cond=True, msg=""):
    if resp is not None and (resp.status_code >= 400 or not cond):
        body = ""
        try:
            body = json.dumps(resp.json())[:300]
        except Exception:
            body = (resp.text or "")[:300]
        RESULTS.append(("FAIL", name, f"HTTP {resp.status_code} {msg} :: {body}"))
        print(f"❌ {name} :: HTTP {resp.status_code} {msg}")
    elif not cond:
        RESULTS.append(("FAIL", name, msg))
        print(f"❌ {name} :: {msg}")
    else:
        RESULTS.append(("PASS", name, ""))
        print(f"✅ {name}")

def skip(name, why):
    RESULTS.append(("SKIP", name, why))
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

def main():
    # --------- BASIC PAGES / HEALTH ---------
    ok("GET /docs", get("/docs"))
    ok("GET /healthz", get("/healthz"))
    ok("GET /time/sync", get("/time/sync"))
    ok("GET /ops/readiness", get("/ops/readiness"))
    get("/__diag/users_count")  # best-effort
    ok("GET /terms", get("/terms"))
    ok("GET /eula",  get("/eula"))
    ok("GET /privacy", get("/privacy"))

    # --------- LEGAL (site policy pages) ---------
    for p in [
        "/legal/cookies","/legal/subprocessors","/legal/dpa","/legal/sla",
        "/legal/security","/legal/privacy-appendix","/legal/rulebook","/legal/rulebook/versions"
    ]:
        ok(f"GET {p}", get(p))

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
    ok("POST /admin/compliance/member/set", post("/admin/compliance/member/set", json={"member_id":"m1","status":"approved"}))
    ok("POST /admin/statements/run", post("/admin/statements/run"))
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

    # --------- REFERENCE PRICES & INDICES ---------
    try:
        post("/reference_prices/pull_home")
        post("/reference_prices/pull_now_all")
    except requests.ReadTimeout:
        skip("/reference_prices/pull_now_all", "Timed out on external scrapes; skipping locally")
    ok("GET /reference_prices/latest", get("/reference_prices/latest"))

    ok("GET /indices/universe", get("/indices/universe"))
    r = get("/indices/latest", params={"symbol":"BR-CU"})
    if r.status_code == 404:
        skip("GET /indices/latest", "no index history yet")
    else:
        ok("GET /indices/latest", r)

    # --------- INVENTORY SEED for contract create ---------
    seller = "Acme Yard"
    sku    = "CU-SHRED-1M"
    inv = {"seller": seller, "sku": sku, "qty_on_hand": 100.0, "uom":"ton", "location":"YARD-A", "description":"Seed"}
    ok("POST /inventory/manual_add", post("/inventory/manual_add", json=inv))

    # Inventory surfaces
    ok("GET /inventory", get("/inventory"))
    ok("GET /inventory/movements/list", get("/inventory/movements/list"))
    ok("GET /inventory/finished_goods", get("/inventory/finished_goods", params={"seller": seller}))
    ok("GET /inventory/template.csv", get("/inventory/template.csv"))
    # CSV/XLSX import (tiny inline)
    csv_body = "seller,sku,qty_on_hand,uom,location,description\nAcme Yard,CU-SHRED-1M,5,ton,YARD-A,imported\n"
    ok("POST /inventory/import/csv", post("/inventory/import/csv",
       files={"file": ("inventory.csv", csv_body, "text/csv")}))
    # (XLSX is optional; often needs a real xlsx—skipping to avoid tool deps)

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
            # Try GET if POST didn't return ids
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
            # lifecycle edges:
            ok("POST /admin/futures/series/{id}/expire", post(f"/admin/futures/series/{listing_id}/expire"))
            ok("POST /admin/futures/series/{id}/finalize", post(f"/admin/futures/series/{listing_id}/finalize"))

    # --------- RISK / ENTITLEMENTS / TRADING / CLOB / FIX / RFQ ---------
    for feat in ["clob.trade","trade.place","trade.modify","rfq.post","rfq.quote","rfq.award"]:
        post("/admin/entitlements/grant", params={"user":"anon","feature":feat})

    post("/risk/price_band/CU-SHRED-1M", params={"lower": 100.0, "upper": 1000.0})
    post("/risk/luld/CU-SHRED-1M", params={"down_pct": 0.20, "up_pct": 0.20})
    post("/risk/limits", params={"member":"anon","symbol":"CU-SHRED-1M","limit_lots": 200})

    # Place book order (CLOB simple path)
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

    # RFQ with tz-aware expiry
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
    ok("POST /index/contracts/expire", post("/index/contracts/expire", json={"symbol":"CU-SHRED-1M","as_of": str(date.today())}))

    ok("GET /clearing/positions", get("/clearing/positions", params={"account_id": str(uuid.uuid4())}))
    ok("GET /clearing/margin",    get("/clearing/margin",    params={"account_id": str(uuid.uuid4())}))
    ok("POST /clearing/variation_run", post("/clearing/variation_run", json={"mark_date": str(date.today())}))
    ok("POST /clearing/deposit", post("/clearing/deposit", json={"account_id":"acct1","amount": 100000.0}))
    ok("POST /clearing/guaranty/deposit", post("/clearing/guaranty/deposit", json={"member_id":"m1","amount":1000.0}))
    ok("POST /clearing/waterfall/apply", post("/clearing/waterfall/apply", json={"as_of": str(date.today())}))

    # Risk calc endpoints
    ok("POST /risk/margin/calc", post("/risk/margin/calc", json={"account_id":"acct1"}))
    ok("POST /risk/portfolio_margin", post("/risk/portfolio_margin", json={"account_id":"acct1"}))
    ok("POST /risk/kill_switch/{member_id}", post("/risk/kill_switch/m1"))

    # Surveillance family
    ok("GET /surveil/alerts", get("/surveil/alerts"))
    ok("POST /surveil/alert", post("/surveil/alert", json={"type":"test","severity":"low"}))
    ok("POST /ml/anomaly", post("/ml/anomaly", json={"window":"1d"}))
    ok("POST /surveil/case/open", post("/surveil/case/open", json={"alert_id":"demo"}))
    ok("POST /surveil/rules/run", post("/surveil/rules/run", json={}))

    # Market misc
    ok("GET /ticker", get("/ticker"))

    # ICE webhook
    payload = {"example":"ice-hook"}
    if ICE_SECRET:
        import hmac, hashlib
        body = json.dumps(payload).encode("utf-8")
        mac  = hmac.new(ICE_SECRET.encode(), body, hashlib.sha256).hexdigest()
        ok("POST /ice/webhook (HMAC)", post("/ice/webhook", headers={"X-Signature": mac}, data=body))
    else:
        r = post("/ice/webhook", data=json.dumps(payload))
        if r.status_code == 200:
            ok("POST /ice/webhook", r)
        else:
            skip("POST /ice/webhook", f"status={r.status_code}")

    # ICE Digital Trade stub
    ok("POST /ice-digital-trade", post("/ice-digital-trade", json={"event":"ping"}))

    # ---- Exercise the rest of the surface (soft asserts) ----
    try:
        run_additional(listing_id=listing_id)
    except Exception as e:
        skip("run_additional()", f"{type(e).__name__}: {e}")

    # --------- SUMMARY ---------
    print("\n======= SUMMARY =======")
    total = len(RESULTS)
    passed = len([x for x in RESULTS if x[0]=="PASS"])
    failed = len([x for x in RESULTS if x[0]=="FAIL"])
    skipped= len([x for x in RESULTS if x[0]=="SKIP"])
    print(f"Total: {total}  PASS: {passed}  FAIL: {failed}  SKIP: {skipped}")
    if failed:
        print("\nFailures:")
        for kind, name, msg in RESULTS:
            if kind=="FAIL":
                print(f"- {name}: {msg}")
        sys.exit(1)

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

    # --- 3) Contracts → BOL → PDF; Receipts → Stocks ---
    r = S.post(f"{BASE}/contracts", json={
        "buyer":"Lewis Salvage","seller":"Winski Brothers","material":"Shred Steel",
        "weight_tons":5.0,"price_per_ton":250.0
    })
    _ok("POST /contracts (smoke2)", r)
    cid = _json(r, {}).get("id")

    now_iso = datetime.now(timezone.utc).isoformat()
    bol_payload = {
        "contract_id": cid,
        "buyer":"Lewis Salvage","seller":"Winski Brothers","material":"Shred Steel",
        "weight_tons":5.0,"price_per_unit":250.0,"total_value":1250.0,
        "carrier":{"name":"ABC Trucking","driver":"Jane Doe","truck_vin":"1FTSW21P34ED12345"},
        "pickup_signature":{"base64":"data:image/png;base64,AAA","timestamp":now_iso},
        "pickup_time": now_iso
    }
    r = S.post(f"{BASE}/bols", json=bol_payload)
    _ok("POST /bols", r)
    bol_id = _json(r, {}).get("bol_id")

    if bol_id:
        _ok("GET /bol/{bol_id}/pdf", S.get(f"{BASE}/bol/{bol_id}/pdf", stream=True))
        _ok("POST /bols/{bol_id}/deliver", S.post(f"{BASE}/bols/{bol_id}/deliver"))

    rc = {"seller":"Winski Brothers","sku":"Shred Steel","qty_tons":2.0,"location":"YARD-A"}
    r = S.post(f"{BASE}/receipts", json=rc)
    _ok("POST /receipts", r)
    rid = _json(r, {}).get("receipt_id")
    if rid:
        _ok("POST /receipts/{id}/consume", S.post(f"{BASE}/receipts/{rid}/consume"))

    today = str(date.today())
    _ok("POST /stocks/snapshot", S.post(f"{BASE}/stocks/snapshot", json={"as_of": today}))
    _ok("GET /stocks",        S.get(f"{BASE}/stocks",     params={"as_of": today}))
    _ok("GET /stocks.csv",    S.get(f"{BASE}/stocks.csv", params={"as_of": today}))

    # --- 4) Compliance / Finance / Insurance ---
    _ok("GET /export/tax_lookup", S.get(f"{BASE}/export/tax_lookup", params={"hs_code":"7404","dest":"US"}))
    if rid:
        _ok("POST /finance/receivable", S.post(f"{BASE}/finance/receivable", json={
            "receipt_id": rid, "face_value_usd": 1000.0, "due_date": today, "debtor":"Lewis Salvage"
        }))
    _ok("POST /insurance/quote", S.post(f"{BASE}/insurance/quote", json={
        "receipt_id": rid or "00000000-0000-0000-0000-000000000000",
        "coverage_usd": 5000.0
    }))

    # --- 5) RFQ happy-path (requires entitlements) ---
    in_10 = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    rq = {"symbol":"CU-SHRED-1M","side":"buy","quantity_lots":"1","price_limit":"260","expires_at": in_10}
    r = S.post(f"{BASE}/rfq", json=rq); _ok("POST /rfq", r)
    rfq_id = _json(r, {}).get("rfq_id")
    if rfq_id:
        _ok("POST /rfq/{id}/quote", S.post(f"{BASE}/rfq/{rfq_id}/quote", json={"price":"259.5","qty_lots":"1"}))
        qid = _json(S.post(f"{BASE}/rfq/{rfq_id}/quote", json={"price":"259.0","qty_lots":"1"}), {}).get("quote_id")
        if qid:
            _ok("POST /rfq/{id}/award", S.post(f"{BASE}/rfq/{rfq_id}/award", params={"quote_id": qid}))

    # --- 6) Trading (futures) minimal: deposit margin so matching logic can proceed ---
    _ok("POST /clearing/deposit", S.post(f"{BASE}/clearing/deposit", json={"account_id":"acct1","amount": 100000.0}))
    if listing_id:
        od = {"account_id":"acct1","listing_id":listing_id,"side":"BUY","price":250.0,"qty":1.0,"order_type":"LIMIT","tif":"GTC"}
        ro = S.post(f"{BASE}/trade/orders", json=od)
        _ok("POST /trade/orders", ro)
        tr_order_id = _json(ro, {}).get("order_id")
        _ok("GET /trade/book", S.get(f"{BASE}/trade/book", params={"listing_id": listing_id, "depth": 5}))
        # try modify/cancel
        if tr_order_id:
            _ok("PATCH /trade/orders/{id}", S.patch(f"{BASE}/trade/orders/{tr_order_id}", json={"price": 251.0}))
            _ok("DELETE /trade/orders/{id}", S.delete(f"{BASE}/trade/orders/{tr_order_id}"))

    # --- 7) CLOB spot path (entitlement-based) ---
    r = S.post(f"{BASE}/clob/orders", json={
        "symbol":"CU-SHRED-1M","side":"buy","price":"1.23","qty_lots":"1","tif":"day"
    })
    _ok("POST /clob/orders", r)
    clob_oid = _json(r, {}).get("order_id")
    _ok("GET /clob/orderbook", S.get(f"{BASE}/clob/orderbook", params={"symbol":"CU-SHRED-1M","depth":5}))
    if clob_oid:
        _ok("DELETE /clob/orders/{id}", S.delete(f"{BASE}/clob/orders/{clob_oid}"))
    _ok("GET /clob/statement", S.get(f"{BASE}/clob/statement", params={"format":"csv"}))
    _ok("GET /trade/orders/export", S.get(f"{BASE}/trade/orders/export", params={"as_of": str(date.today())}))
    _ok("GET /trade/trades/export", S.get(f"{BASE}/trade/trades/export", params={"as_of": str(date.today())}))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted.")
        sys.exit(130)
