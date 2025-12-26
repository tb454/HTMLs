import os, time, hmac, hashlib
from fastapi.testclient import TestClient

from bridge_buyer_backend import app, current_member_from_request

class DummyReq:
    def __init__(self, session=None, query=None, headers=None):
        self.session = session or {}
        self.query_params = query or {}
        self.headers = headers or {}

def _sig(secret: str, member: str, ts: int) -> str:
    msg = f"{member}:{ts}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()

def test_prod_dev_endpoints_404(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    client = TestClient(app)
    assert client.post("/admin/demo/seed").status_code == 404
    assert client.post("/admin/demo/reset").status_code == 404

def test_prod_admin_override_requires_signature(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    monkeypatch.setenv("ADMIN_OVERRIDE_SECRET", "secret123")

    ts = int(time.time())
    good = "ICE_OVERRIDE_MEMBER"
    headers = {
        "X-Admin-Override-Ts": str(ts),
        "X-Admin-Override-Sig": _sig("secret123", good, ts),
    }

    req = DummyReq(
        session={"role": "admin", "member": "Winski Brothers"},
        query={"member": good},
        headers=headers,
    )
    assert current_member_from_request(req) == good

    # missing signature -> must fall back to session
    req2 = DummyReq(
        session={"role": "admin", "member": "Winski Brothers"},
        query={"member": "EVIL"},
        headers={},
    )
    assert current_member_from_request(req2) == "Winski Brothers"
