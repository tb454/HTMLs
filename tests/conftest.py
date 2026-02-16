# tests/conftest.py

import os
import sys
import pathlib
import pytest
from fastapi.testclient import TestClient

# -------------------------------------------------------------------
# 0) Environment MUST be set BEFORE importing the backend module
# -------------------------------------------------------------------
os.environ.setdefault("ENV", "ci")
os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/bridge")

# required for your backend to connect DB under pytest
os.environ.setdefault("BRIDGE_TEST_DB", "1")

# required for /login to accept test/test 
os.environ.setdefault("ALLOW_TEST_LOGIN_BYPASS", "1")

# make sure DDL bootstraps run in CI
os.environ.setdefault("BRIDGE_BOOTSTRAP_DDL", "1")
os.environ.setdefault("INIT_DB", "1")

# Tenant + materials behavior for CI
os.environ.setdefault("TEST_TENANT_ID", "00000000-0000-0000-0000-000000000001")
os.environ.setdefault("MATERIALS_AUTO_LEARN", "1")
os.environ.setdefault("BRIDGE_FREE_MODE", "1")

# Kill Stripe / external integrations in CI
os.environ.setdefault("ENABLE_STRIPE", "0")
os.environ.setdefault("DOSSIER_SYNC", "0")

# -------------------------------------------------------------------
# 1) Point Python at repo root
# -------------------------------------------------------------------
THIS_FILE = pathlib.Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[1]  # .../HTMLs
sys.path.insert(0, str(REPO_ROOT))

# -------------------------------------------------------------------
# 2) Import the real backend app
# -------------------------------------------------------------------
import bridge_buyer_backend as backend

app = backend.app

# -------------------------------------------------------------------
# 3) Patch auth/permissions so tests don't get 401/403 on protected routes
# -------------------------------------------------------------------
async def _require_perm_noop(request, perm: str):
    return None

backend.require_perm = _require_perm_noop  # monkey-patch in the module


def _require_admin_noop(request):
    return None

backend._require_admin = _require_admin_noop  # monkey-patch in the module

# -------------------------------------------------------------------
# 4) Shared TestClient fixture (starts FastAPI lifespan/startup once)
# -------------------------------------------------------------------
@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        # Ensure every request carries a tenant for _tenant_or_404/current_tenant_id
        c.headers.update({"X-Tenant-Id": os.environ["TEST_TENANT_ID"]})
        yield c

# -------------------------------------------------------------------
# 5) Seed minimal inventory so contracts/BOL tests don't 409
# -------------------------------------------------------------------
@pytest.fixture(scope="session", autouse=True)
def seed_tenant_and_materials(client: TestClient):
    tid = os.environ["TEST_TENANT_ID"]

    # 1) Ensure tenant exists (current_tenant_id resolves via tenants.slug)
    r = client.post("/admin/demo/seed")
    if r.status_code not in (200, 404):
        raise AssertionError(f"demo seed failed: {r.status_code} {r.text}")

    # 2) Ensure the specific tenant slug -> id row exists for TEST_TENANT_ID
    r2 = client.post(
        "/admin/provision_user",
        json={
            "email": "ci@example.com",
            "username": "ci",
            "org_name": "Winski Brothers",
            "role": "seller",
            "plan": "free",
            "promo": True,
        },
    )
    if r2.status_code not in (200, 201):
        raise AssertionError(f"provision_user failed: {r2.status_code} {r2.text}")

    # 3) Seed minimal materials for tenant so require_material_exists passes
    # materials table uses canonical_name exact match.
    mats = [
        "Shred Steel",
        "Plate & Structural",
        "Heavy Melt Steel",
    ]
    for i, m in enumerate(mats, start=1):
        client.post(
            "/materials",
            json={"canonical_name": m, "display_name": m, "enabled": True, "sort_order": i},
        )
@pytest.fixture(scope="session", autouse=True)
def seed_inventory(client: TestClient):
    payload = {
        "source": "ci",
        "seller": "Winski Brothers",
        "items": [
            {"sku": "Shred Steel", "qty_on_hand": 500.0, "description": "CI seed Shred Steel", "uom": "ton"},
            {"sku": "Plate & Structural", "qty_on_hand": 500.0, "description": "CI seed P&S", "uom": "ton"},
            {"sku": "Heavy Melt Steel", "qty_on_hand": 500.0, "description": "CI seed HMS", "uom": "ton"},
        ],
    }
    r = client.post("/inventory/bulk_upsert", json=payload)
    assert r.status_code == 200, f"Inventory seed failed: {r.status_code} {r.text}"
