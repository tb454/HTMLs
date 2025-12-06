# tests/conftest.py

import os
import sys
import pathlib
import pytest
from fastapi.testclient import TestClient

# --- 1) Point Python at your repo root so we can import the backend ---
THIS_FILE = pathlib.Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[1]  # .../HTMLs
sys.path.insert(0, str(REPO_ROOT))

# --- 2) CI env + DB DSN *before* importing the backend module ---
os.environ.setdefault("ENV", "ci")
# Adjust DB name if your CI service uses a different one
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/bridge",
)
# Optional but harmless: make sure DDL bootstraps run in CI
os.environ.setdefault("BRIDGE_BOOTSTRAP_DDL", "1")
os.environ.setdefault("INIT_DB", "1")
# Kill Stripe / external integrations in CI
os.environ.setdefault("ENABLE_STRIPE", "0")
os.environ.setdefault("DOSSIER_SYNC", "0")

# --- 3) Import the real backend app ---
# If your file is named differently, change this to `import backend as bridge_buyer_backend`
import bridge_buyer_backend as backend

app = backend.app

# --- 4) Patch auth/permissions so tests don't get 401/403 ---

# All your protected endpoints call `await require_perm(request, "perm")`.
# For CI, we just no-op it.
async def _require_perm_noop(request, perm: str):
    return None

backend.require_perm = _require_perm_noop  # monkey-patch in the module


# Admin-only endpoints use `_require_admin(request)`; no-op that too.
def _require_admin_noop(request):
    return None

backend._require_admin = _require_admin_noop  # monkey-patch in the module


# --- 5) Shared TestClient fixture (starts FastAPI lifespan/startup once) ---
@pytest.fixture(scope="session")
def client():
    """
    Session-scoped TestClient so:
      - Startup hooks run once (DDL, indices, etc.)
      - DB schema is bootstrapped once for all tests
    """
    with TestClient(app) as c:
        yield c


# --- 6) Seed minimal inventory so contracts/BOL tests don't 409 ---
@pytest.fixture(scope="session", autouse=True)
def seed_inventory(client: TestClient):
    """
    Make sure there is scrap on hand for Winski so /contracts and /bols
    can run without 'not enough inventory' errors in CI.
    """
    payload = {
        "source": "ci",
        "seller": "Winski Brothers",
        "items": [
            {
                "sku": "Shred Steel",
                "qty_on_hand": 500.0,
                "description": "CI seed Shred Steel",
                "uom": "ton",
            },
            {
                "sku": "Plate & Structural",
                "qty_on_hand": 500.0,
                "description": "CI seed P&S",
                "uom": "ton",
            },
            {
                "sku": "Heavy Melt Steel",
                "qty_on_hand": 500.0,
                "description": "CI seed HMS",
                "uom": "ton",
            },
        ],
    }
    r = client.post("/inventory/bulk_upsert", json=payload)
    # Don't hard-fail CI on this, but assert in case something is really broken
    assert r.status_code == 200, f"Inventory seed failed: {r.status_code} {r.text}"
