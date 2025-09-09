import os
import sys
import pathlib
import pytest
from fastapi.testclient import TestClient

# Put the repo's HTMLs folder on sys.path so imports work
THIS_FILE = pathlib.Path(__file__).resolve()
HTMLS_ROOT = THIS_FILE.parents[1]  # ...\BRidge-html\HTMLs
sys.path.insert(0, str(HTMLS_ROOT))

# Ensure non-production env so TrustedHost allows 'testserver'
os.environ.setdefault("ENV", "ci")

# IMPORTANT: DATABASE_URL must point to your staging DB before running tests
# Example:
# os.environ.setdefault("DATABASE_URL", "postgresql://postgres:PASS@HOST.supabase.co:5432/postgres")

# Import your FastAPI app from the real filename
from bridge_buyer_backend import app  # <-- your file is bridge_buyer_backend.py

@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c

# Auto-seed inventory for tests so ?contracts can reserve without 409
@pytest.fixture(scope="session", autouse=True)
def seed_inventory(client):
    payload = {
        "source": "ci",
        "seller": "Winski Brothers",
        "items": [
            {"sku": "Shred Steel", "qty_on_hand": 100.0, "description": "test", "uom": "ton"}
        ]
    }
    r = client.post("/inventory/bulk_upsert", json=payload)
    assert r.status_code == 200, f"seed_inventory failed: {r.text}"
