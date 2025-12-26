# tests/test_tenant_isolation.py
import os
import pytest

from bridge_buyer_backend import current_member_from_request


class DummyReq:
    def __init__(self, session=None, query=None):
        self.session = session or {}
        self.query_params = query or {}


def test_prod_ignores_query_override_for_non_admin(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    req = DummyReq(
        session={"role": "buyer", "member": "Winski Brothers"},
        query={"member": "EVIL_OVERRIDE"},
    )
    assert current_member_from_request(req) == "Winski Brothers"


def test_prod_allows_query_override_for_admin(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    req = DummyReq(
        session={"role": "admin", "member": "Winski Brothers"},
        query={"member": "ICE_AUDIT_OVERRIDE"},
    )
    assert current_member_from_request(req) == "ICE_AUDIT_OVERRIDE"


def test_non_prod_allows_query_override_when_session_missing(monkeypatch):
    monkeypatch.setenv("ENV", "development")
    req = DummyReq(
        session={"role": "buyer"},              # no member in session
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

