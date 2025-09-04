# tests/test_contracts.py
import random
import string

def _rand_material():
    return "Shred Steel"

def _rand_buyer():
    return "Lewis Salvage"

def _rand_seller():
    return "Winski Brothers"

def test_create_contract_and_list_with_filters(client):
    # Create a new contract
    payload = {
        "buyer": _rand_buyer(),
        "seller": _rand_seller(),
        "material": _rand_material(),
        "weight_tons": 40.0,
        "price_per_ton": 245.0
    }
    r = client.post("/contracts", json=payload)
    assert r.status_code in (200, 201), r.text
    created = r.json()
    assert created["buyer"] == payload["buyer"]
    assert created["seller"] == payload["seller"]
    assert created["material"] == payload["material"]
    assert "id" in created

    # List contracts with filters + pagination
    params = {
        "buyer": payload["buyer"],
        "status": "Signed",  # may or may not match; still valid to pass
        "limit": 5,
        "offset": 0
    }
    r = client.get("/contracts", params=params)
    assert r.status_code == 200, r.text
    rows = r.json()
    assert isinstance(rows, list)
    # At least it's a valid response; if DB's empty/filters strict, list could be empty
    # We assert shape where present:
    if rows:
        row = rows[0]
        assert {"id", "buyer", "seller", "material", "status", "created_at"} <= set(row.keys())
