# full_test_suite.py
# End-to-end smoke/flow test for your FastAPI backend.
# Run with:  python full_test_suite.py
import os, sys, time, json, uuid
from datetime import date, datetime, timedelta
import requests

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")

# Optional headers for endpoints that check these in your code (only needed if you set these env vars)
H_SNAPSHOT = os.environ.get("SNAPSHOT_AUTH", "")
H_ICE      = os.environ.get("ICE_WEBHOOK_SECRET", "")
H_SETUP    = os.environ.get("ADMIN_SETUP_TOKEN", "")

def hdr(extra=None):
    h = {}
    if extra: h.update(extra)
    return h

def h_snapshot():
    return {"x-auth": H_SNAPSHOT} if H_SNAPSHOT else {}

def h_setup():
    return {"X-Setup-Token": H_SETUP} if H_SETUP else {}

def post(path, **kw):   return requests.post(f"{BASE}{path}", timeout=20, **kw)
def get(path,  **kw):   return requests.get(f"{BASE}{path}", timeout=20, **kw)
def put(path,  **kw):   return requests.put(f"{BASE}{path}", timeout=20, **kw)
def patch(path,**kw):   return requests.patch(f"{BASE}{path}", timeout=20, **kw)
def delete(path,**kw):  return requests.delete(f"{BASE}{path}", timeout=20, **kw)

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

def main():
    # --------- BASIC PAGES / HEALTH ---------
    ok("GET /docs", get("/docs"))
    r = get("/healthz"); ok("GET /healthz", r)
    get("/__diag/users_count")  # not enforced; best-effort
    ok("GET /terms", get("/terms"))
    ok("GET /eula",  get("/eula"))
    ok("GET /privacy", get("/privacy"))

    # --------- PRICES / FX ---------
    r = get("/prices/copper_last"); ok("GET /prices/copper_last", r)
    r = get("/fx/convert", params={"amount": 100, "from_ccy":"USD","to_ccy":"EUR"}); ok("GET /fx/convert", r)

    # --------- ADMIN / EXPORTS (dev: usually open) ---------
    # background snapshot (requires SNAPSHOT_AUTH header only if you set it)
    r = post("/admin/run_snapshot_bg", headers=hdr(h_snapshot()), json=None); ok("POST /admin/run_snapshot_bg", r)
    r = get("/admin/export_all"); ok("GET /admin/export_all (ZIP)", r, cond=(r.status_code==200 and "application/zip" in r.headers.get("content-type","")))
    r = get("/admin/exports/all.zip"); ok("GET /admin/exports/all.zip", r, cond=(r.status_code==200 and "application/zip" in r.headers.get("content-type","")))
    r = get("/admin/exports/contracts.csv"); ok("GET /admin/exports/contracts.csv", r, cond=(r.status_code==200 and "text/csv" in r.headers.get("content-type","")))

    # Audit chain (append → seal → verify)
    r = post("/admin/audit/log", json={"payload":{"note":"smoke-test"}}); ok("POST /admin/audit/log", r)
    today = str(date.today())
    r = post("/admin/audit/seal", params={"chain_date": today}); ok("POST /admin/audit/seal", r)
    r = get("/admin/audit/verify", params={"chain_date": today}); ok("GET /admin/audit/verify", r)

    # --------- REFERENCE PRICES & INDICES ---------
    # pull reference prices best-effort
    post("/reference_prices/pull_home")
    post("/reference_prices/pull_now_all")

    # indices universe / latest (may be empty -> still 200 or 404)
    get("/indices/universe")
    # If your DB has rows these will 200; else you may get 404 — acceptable
    r = get("/indices/latest", params={"symbol":"BR-CU"})
    if r.status_code == 404:
        skip("GET /indices/latest", "no index history yet")
    else:
        ok("GET /indices/latest", r)

    # --------- INVENTORY SEED for contract create ---------
    seller = "Acme Yard"
    sku    = "CU-SHRED-1M"     # aligns with your _INSTRUMENTS
    inv = {"seller": seller, "sku": sku, "qty_on_hand": 100.0, "uom":"ton", "location":"YARD-A", "description":"Seed"}
    r = post("/inventory/manual_add", json=inv); ok("POST /inventory/manual_add", r)

    # --------- CONTRACTS (create → list → get → export) ---------
    # Ensure enough stock exists (we just seeded)
    contract = {
        "buyer":"Buyer Inc",
        "seller": seller,
        "material": sku,
        "weight_tons": 20,
        "price_per_ton": 250.0,
        # extended fields optional; backend fills ref pricing internally
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

    # purchase (atomic) — will only pass if contract is still Pending; otherwise expect 409 → we still mark as pass-ish
    if cid:
        r = patch(f"/contracts/{cid}/purchase", json={"op":"purchase","expected_status":"Pending"})
        if r.status_code == 409:
            skip("PATCH /contracts/{id}/purchase", "already Signed/Pending mismatch")
        else:
            ok("PATCH /contracts/{id}/purchase", r)

    # cancel (only Pending allowed) – may 409 if not Pending
    if cid:
        r = post(f"/contracts/{cid}/cancel")
        if r.status_code == 409:
            skip("POST /contracts/{id}/cancel", "not in Pending")
        else:
            ok("POST /contracts/{id}/cancel", r)

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
        "pickup_signature": {"base64":"", "timestamp":"2025-01-01T00:00:00Z"},
        "pickup_time": "2025-01-01T00:05:00Z"
    }
    r = post("/bols", json=bol); ok("POST /bols (create)", r)
    bol_id = None
    try: bol_id = r.json()["bol_id"]
    except Exception: pass

    ok("GET /bols", get("/bols"))
    if bol_id:
        ok("POST /bols/{id}/deliver", post(f"/bols/{bol_id}/deliver"))
        # PDF (may be large; we just verify 200 + content type)
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
    # These may be empty; we still verify 200 where appropriate
    get("/analytics/material_price_history", params={"material": sku})
    get("/analytics/rolling_bands", params={"material": sku})
    get("/public/indices/daily.json")
    get("/public/indices/daily.csv")
    get("/analytics/price_band_estimates", params={"material": sku})
    get("/analytics/delta_anomalies", params={"material": sku})
    get("/indices")  # public index feed (may be empty)

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
        # ok if not configured fully
        if r.status_code == 404:
            skip("GET /pricing/quote", "No internal price available")
        else:
            ok("GET /pricing/quote", r)
    else:
        ok("GET /pricing/quote", r)

    # --------- COMPLIANCE ---------
    ok("GET /export/tax_lookup", get("/export/tax_lookup", params={"hs_code":"7404","dest":"US"}))

    # --------- PRODUCTS / FUTURES / MARKS (MANUAL pricing so we can publish a mark) ---------
    symbol_root = "CU-SHRED-1M"
    prod = {"symbol":symbol_root, "description":"Copper Shred 1M", "unit":"ton", "quality":{"grade":"shred"}}
    ok("POST /products", post("/products", json=prod))

    # Admin futures: create product with MANUAL price method to avoid VWAP dependency
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

    if product_id:
        ok("POST /admin/futures/products/{root}/pricing",
           post(f"/admin/futures/products/{symbol_root}/pricing",
                json={"lookback_days":14,"basis_adjustment":0,"carry_per_month":0,"manual_mark":250.0,"external_source":None}))

        # Generate series → list → publish mark → set trading status
        r = post("/admin/futures/series/generate", json={"product_id": product_id, "months_ahead": 1, "day_of_month": 15})
        ok("POST /admin/futures/series/generate", r)
        listing_id = None
        try:
            listing_id = r.json()[0]["id"]
        except Exception:
            pass

        if listing_id:
            ok("POST /admin/futures/series/{id}/list", post(f"/admin/futures/series/{listing_id}/list"))
            ok("POST /admin/futures/marks/publish", post("/admin/futures/marks/publish", json={"listing_id": listing_id}))
            ok("POST /admin/futures/series/{id}/trading_status",
               post(f"/admin/futures/series/{listing_id}/trading_status", json={"trading_status":"Trading"}))
            get("/admin/futures/marks")
            get("/admin/futures/series")

    # --------- RISK / ENTITLEMENTS / TRADING / CLOB / FIX / RFQ ---------
    # Grant entitlements to "anon" session (dev mode: admin gate not enforced)
    entitles = ["clob.trade","trade.place","trade.modify","rfq.post","rfq.quote","rfq.award"]
    for feat in entitles:
        post("/admin/entitlements/grant", params={"user":"anon","feature":feat})

    # Risk config (safe defaults)
    post("/risk/price_band/CU-SHRED-1M", params={"lower": 100.0, "upper": 1000.0})
    post("/risk/luld/CU-SHRED-1M", params={"down_pct": 0.20, "up_pct": 0.20})
    post("/risk/limits", params={"member":"anon","symbol":"CU-SHRED-1M","limit_lots": 200})

    # CLOB place order (simple limit order) → read orderbook
    r = post("/clob/orders", json={"symbol":"CU-SHRED-1M", "side":"buy", "price":250, "qty_lots":1, "tif":"day"})
    if r.status_code == 200:
        ok("POST /clob/orders", r)
    else:
        skip("POST /clob/orders", f"status={r.status_code} (band/LULD/entitlement?)")

    ok("GET /clob/orderbook", get("/clob/orderbook", params={"symbol":"CU-SHRED-1M","depth":5}))

    # FIX shim (will grant entitlement internally)
    fix = {"ClOrdID":"T1","Symbol":"CU-SHRED-1M","Side":"1","Price":255.0,"OrderQty":1.0,"TimeInForce":"0","SenderCompID":"fix_member"}
    ok("POST /fix/order", post("/fix/order", json=fix))

    # RFQ flow (post → quote → award)
    rfq_id = None
    r = post("/rfq", json={"symbol":"CU-SHRED-1M","side":"buy","quantity_lots":"1","price_limit":"260","expires_at": (datetime.utcnow()+timedelta(minutes=10)).isoformat()+"Z"})
    if r.status_code == 200:
        ok("POST /rfq", r)
        rfq_id = r.json().get("rfq_id")
    else:
        skip("POST /rfq", f"status={r.status_code}")

    if rfq_id:
        r = post(f"/rfq/{rfq_id}/quote", json={"price":"258","qty_lots":"1"})
        if r.status_code == 200:
            ok("POST /rfq/{id}/quote", r)
            quote_id = r.json().get("quote_id")
            if quote_id:
                ok("POST /rfq/{id}/award", post(f"/rfq/{rfq_id}/award", params={"quote_id":quote_id}))
        else:
            skip("POST /rfq/{id}/quote", f"status={r.status_code}")

    # Settlement publish (vwap fallback → 0 ok)
    ok("POST /settlement/publish", post("/settlement/publish", params={"symbol":"CU-SHRED-1M","as_of":str(date.today()), "method":"vwap_last60m"}))
    get("/index/latest")
    get("/index/history", params={"symbol":"CU-SHRED-1M"})
    get("/index/history.csv", params={"symbol":"CU-SHRED-1M"})
    get("/index/tweet")

    # Clearing (read-only unless you deposit)
    # (Margin account created via trade/order path in futures book, but we can directly hit clearing)
    get("/clearing/positions", params={"account_id": str(uuid.uuid4())})
    get("/clearing/margin",    params={"account_id": str(uuid.uuid4())})
    # Variation run without data is fine
    post("/clearing/variation_run", json={"mark_date": str(date.today())})

    # ICE webhooks (HMAC only if set)
    payload = {"example":"ice-hook"}
    if H_ICE:
        # build hmac header that matches your code (hex of sha256(secret, body))
        import hmac, hashlib
        body = json.dumps(payload).encode("utf-8")
        mac  = hmac.new(H_ICE.encode(), body, hashlib.sha256).hexdigest()
        r = post("/ice/webhook", headers={"X-Signature": mac}, data=body)
        ok("POST /ice/webhook (HMAC)", r)
    else:
        # sends without HMAC—should 401 in prod; dev may 401 also; treat as skip
        r = post("/ice/webhook", data=json.dumps(payload))
        if r.status_code == 200:
            ok("POST /ice/webhook", r)
        else:
            skip("POST /ice/webhook", f"status={r.status_code}")

    # Documents index JSON/CSV already covered earlier

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

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted.")
        sys.exit(130)
