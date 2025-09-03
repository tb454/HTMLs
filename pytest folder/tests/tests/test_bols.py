# tests/test_bols.py
import uuid
from datetime import datetime, timezone

def _iso_now():
    return datetime.now(tz=timezone.utc).isoformat()

def _make_contract(client):
    payload = {
        "buyer": "Lewis Salvage",
        "seller": "Winski Brothers",
        "material": "Shred Steel",
        "weight_tons": 40.0,
        "price_per_ton": 245.0
    }
    r = client.post("/contracts", json=payload)
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]

def test_create_bol_idempotent_and_list(client):
    # Ensure we have a contract_id to attach this BOL to
    contract_id = _make_contract(client)

    bol_payload = {
        "contract_id": contract_id,
        "buyer": "Lewis Salvage",
        "seller": "Winski Brothers",
        "material": "Shred Steel",
        "weight_tons": 40.0,
        "price_per_unit": 245.0,
        "total_value": 9800.0,
        "carrier": {
            "name": "ABC Trucking Co.",
            "driver": "John Driver",
            "truck_vin": "1FDUF5GY3KDA12345"
        },
        "pickup_signature": {
            "base64": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAA...",  # fake data URL
            "timestamp": _iso_now()
        },
        "pickup_time": _iso_now()
    }

    # Use Idempotency-Key to assert duplicate submits return same result
    idem_key = str(uuid.uuid4())
    r1 = client.post("/bols", json=bol_payload, headers={"Idempotency-Key": idem_key})
    assert r1.status_code in (200, 201), r1.text
    data1 = r1.json()
    assert data1["contract_id"] == contract_id
    assert "bol_id" in data1

    r2 = client.post("/bols", json=bol_payload, headers={"Idempotency-Key": idem_key})
    assert r2.status_code in (200, 201), r2.text
    data2 = r2.json()
    assert data1["bol_id"] == data2["bol_id"]  # idempotency worked

    # List BOLs filtered by contract_id + pagination
    r = client.get("/bols", params={"contract_id": contract_id, "limit": 5, "offset": 0})
    assert r.status_code == 200, r.text
    bols = r.json()
    assert isinstance(bols, list)
    assert any(b["bol_id"] == data1["bol_id"] for b in bols)
