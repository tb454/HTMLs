# tests/test_health.py
def test_healthz(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    data = r.json()
    assert data.get("ok") is True
    assert data.get("service") == "bridge-buyer"
