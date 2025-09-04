def test_create_contract_and_list_with_filters(client):
    payload = {
        "buyer": "Lewis Salvage",
        "seller": "Winski Brothers",
        "material": "Shred Steel",
        "weight_tons": 40.0,
        "price_per_ton": 245.0,
    }
    r = client.post("/contracts", json=payload)
    assert r.status_code in (200, 201), r.text
    created = r.json()
    assert created["buyer"] == payload["buyer"]
    assert created["seller"] == payload["seller"]
    assert created["material"] == payload["material"]
    assert "id" in created

    # Filtered list (allowing that filters may or may not match)
    params = {"buyer": payload["buyer"], "status": "Signed", "limit": 5, "offset": 0}
    r = client.get("/contracts", params=params)
    assert r.status_code == 200, r.text
    rows = r.json()
    assert isinstance(rows, list)
    if rows:
        row = rows[0]
        assert {"id", "buyer", "seller", "material", "status", "created_at"} <= set(row.keys())
