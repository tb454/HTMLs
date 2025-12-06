# tests/conftest.py
import os
import sys
import pathlib

import pytest
from fastapi.testclient import TestClient

# --- make sure we can import bridge_buyer_backend from the repo root ---
THIS_FILE = pathlib.Path(__file__).resolve()
HTMLS_ROOT = THIS_FILE.parents[1]
sys.path.insert(0, str(HTMLS_ROOT))

# --- force CI env + test DB BEFORE importing the app ---
os.environ.setdefault("ENV", "ci")
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/test_db",
)

from bridge_buyer_backend import app  # noqa: E402

# ===== CI AUTH BYPASS: ALWAYS-AUTHED USER IN TESTS =====

try:
    # this is the auth dependency used by your endpoints
    from bridge_buyer_backend import get_current_user  # type: ignore
except ImportError:
    # if your helper is named differently, change this import to match
    raise RuntimeError(
        "Import get_current_user from bridge_buyer_backend failed. "
        "Update conftest.py to import your real auth dependency."
    )


class _CiUser:
    """
    Minimal fake user object to satisfy anything that expects a 'user'
    from get_current_user in CI.
    """
    def __init__(self):
        self.id = "00000000-0000-0000-0000-000000000000"
        self.username = "ci-tester"
        self.email = "ci@example.com"
        self.role = "admin"
        self.is_active = True


async def _ci_user_override():
    # In CI we don't care who the user is, just that auth passes.
    return _CiUser()


# override the real dependency ONLY in tests
app.dependency_overrides[get_current_user] = _ci_user_override


# ===== TEST CLIENT + SEED DATA =====

@pytest.fixture(scope="session")
def client():
    """
    Single TestClient for the whole test session.

    - Triggers FastAPI startup (so your DDL bootstrap runs once).
    - Uses the CI auth bypass above so every request is 'logged in'.
    """
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="session", autouse=True)
def seed_inventory(client):
    """
    Seed a little inventory so /contracts has something to reserve
    when tests post Shred Steel contracts.
    """
    payload = {
        "source": "ci",
        "seller": "Winski Brothers",
        "items": [
            {
                "sku": "Shred Steel",
                "qty_on_hand": 100.0,
                "description": "test",
                "uom": "ton",
            }
        ],
    }
    r = client.post("/inventory/adjust", json=payload)
    # don't crash tests if this fails; just print for debugging
    print("seed_inventory status:", r.status_code, r.text)
