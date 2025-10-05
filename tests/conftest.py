# conftest.py
import os, sys, pathlib, pytest
from fastapi.testclient import TestClient

THIS_FILE = pathlib.Path(__file__).resolve()
HTMLS_ROOT = THIS_FILE.parents[1]
sys.path.insert(0, str(HTMLS_ROOT))

# ensure CI env + a real test DB DSN **before** importing the app
os.environ.setdefault("ENV", "ci")
os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/test_db")

from bridge_buyer_backend import app

@pytest.fixture(scope="session")
def client():
    # TestClient triggers FastAPI startup, which runs all your DDL bootstraps
    with TestClient(app) as c:
        yield c

@pytest.fixture(scope="session", autouse=True)
def seed_inventory(client):
    payload = {
        "source": "ci",
        "seller": "Winski Brothers",
        "items": [{"sku": "Shred Steel", "qty_on_hand": 100.0, "description": "test", "uom": "ton"}]
    }
    r = client.post("/inventory/bulk_upsert", json=payload)
    assert r.status_code == 200, f"seed_inventory failed: {r.text}"
