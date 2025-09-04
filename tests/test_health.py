# tests/test_health.py
def test_healthz(client):
    r = client.get("/healthz")
    assert r.status_code == 200, r.text
    data = r.json()
    # Accept either {"ok": True, "service": "..."} or {"status": "ok"} style
    assert ("ok" in data and data["ok"] is True) or (data.get("status") in {"ok", "healthy", "up"})
