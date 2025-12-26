import os
import uuid
import pytest
from fastapi.testclient import TestClient

# Import your FastAPI app module (adjust import path if needed)
from bridge_buyer_backend import app

@pytest.fixture(scope="module")
def client():
    return TestClient(app)

def _login_as(client, username="test", password="test"):
    # non-prod bypass in your /login supports test/test
    r = client.post("/login", json={"username": username, "password": password})
    assert r.status_code == 200

def test_tenant_query_param_cannot_override_session_in_prod_sim(client, monkeypatch):
    """
    We simulate prod rules by setting ENV=production for this test only.
    The resolver should ignore ?member= override unless role=admin.
    """
    monkeypatch.setenv("ENV", "production")
    # log in as buyer (non-admin)
    _login_as(client, "test", "test")

    # set a session member (your login will often set member; if not, we call /me and rely on empty)
    # We directly hit an endpoint that uses current_tenant_id and should not accept member override.
    # If your /me has no member, this test will just confirm it doesn't crash.

    r = client.get("/me?member=EVIL_OVERRIDE")
    assert r.status_code == 200
    me = r.json()
    # In prod mode, query param should not become member unless admin.
    assert me.get("member") != "EVIL_OVERRIDE"

def test_admin_can_override_member_in_prod(client, monkeypatch):
    monkeypatch.setenv("ENV", "production")
    # Fake an admin session by manually setting cookie session is hard; easiest is to call /login with an admin user if you have one.
    # If you don't, skip this test safely.
    # You can create admin via /admin/create_user with X-Setup-Token in CI if desired.
    pytest.skip("Enable once admin login exists in CI.")
