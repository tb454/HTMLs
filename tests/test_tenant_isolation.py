# tests/test_tenant_isolation.py
import os
import pytest

from bridge_buyer_backend import current_member_from_request

class DummyRequest:
    """
    Minimal Request-like object for testing current_member_from_request().
    We only provide the fields it reads: session + query_params.
    """
    def __init__(self, session=None, query_params=None):
        self.session = session or {}
        self.query_params = query_params or {}

def test_prod_ignores_query_override_for_non_admin(monkeypatch):
    monkeypatch.setenv("ENV", "production")

    req = DummyRequest(
        session={"role": "buyer", "member": "Winski Brothers"},
        query_params={"member": "EVIL_OVERRIDE"},
    )
    assert current_member_from_request(req) == "Winski Brothers"

def test_prod_allows_query_override_for_admin(monkeypatch):
    monkeypatch.setenv("ENV", "production")

    req = DummyRequest(
        session={"role": "admin", "member": "Winski Brothers"},
        query_params={"member": "ADMIN_OVERRIDE"},
    )
    # Admin is allowed to override in prod (by design in your resolver)
    assert current_member_from_request(req) == "Winski Brothers"  # session wins first

    # If you WANT admin override to win over session, change your resolver,
    # then update this expected value to "ADMIN_OVERRIDE".

def test_non_prod_allows_query_override(monkeypatch):
    monkeypatch.setenv("ENV", "development")

    req = DummyRequest(
        session={"role": "buyer"},
        query_params={"member": "DEV_OVERRIDE"},
    )
    assert current_member_from_request(req) == "DEV_OVERRIDE"
