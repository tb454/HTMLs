# tests/test_tenant_isolation.py
import time
import hmac
import hashlib
import pytest
from bridge_buyer_backend import current_member_from_request


class DummyReq:
    def __init__(self, session=None, query=None, headers=None):
        self.session = session or {}
        self.query_params = query or {}
        self.headers = headers or {}


def _sig(secret: str, member: str, ts: int) -> str:
    msg = f"{member}:{ts}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def test_prod_ignores_query_override_for_non_admin(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    req = DummyReq(
        session={"role": "buyer", "member": "Winski Brothers"},
        query={"member": "EVIL_OVERRIDE"},
    )
    assert current_member_from_request(req) == "Winski Brothers"


def test_prod_allows_query_override_for_admin(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    monkeypatch.setenv("ADMIN_OVERRIDE_SECRET", "secret123")  # used by _verify_admin_override

    override = "ICE_AUDIT_OVERRIDE"
    ts = int(time.time())

    req = DummyReq(
        session={"role": "admin", "member": "Winski Brothers"},
        query={"member": override},
        headers={
            "X-Admin-Override-Ts": str(ts),
            "X-Admin-Override-Sig": _sig("secret123", override, ts),
        },
    )
    assert current_member_from_request(req) == override


def test_non_prod_allows_query_override_when_session_missing(monkeypatch):
    monkeypatch.setenv("ENV", "development")
    req = DummyReq(
        session={"role": "buyer"},
        query={"member": "DEV_OVERRIDE"},
    )
    assert current_member_from_request(req) == "DEV_OVERRIDE"


def test_non_prod_prefers_session_when_present(monkeypatch):
    monkeypatch.setenv("ENV", "development")
    req = DummyReq(
        session={"role": "buyer", "member": "SessionTenant"},
        query={"member": "QueryTenant"},
    )
    assert current_member_from_request(req) == "SessionTenant"
