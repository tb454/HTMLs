# conftest.py
import os, sys, pathlib, pytest
from fastapi.testclient import TestClient

# ---------- Path + ENV (BEFORE app import) ----------
THIS_FILE = pathlib.Path(__file__).resolve()
HTMLS_ROOT = THIS_FILE.parents[1]
sys.path.insert(0, str(HTMLS_ROOT))

os.environ.setdefault("ENV", "ci")
os.environ.setdefault("SESSION_SECRET", "ci-test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/test_db")

# ---------- Import app ----------
from bridge_buyer_backend import app  # noqa

# ---------- Find & override auth / CSRF dependencies explicitly ----------
# Adjust the candidate names if yours differ.
AUTH_CANDIDATES = [
    # most common in your codebase/messages
    ("bridge_buyer_backend", "get_current_user"),
    ("bridge_buyer_backend", "current_user"),
    ("bridge_buyer_backend", "require_user"),
    ("bridge_buyer_backend", "require_login"),
    # sometimes split to auth module
    ("auth", "get_current_user"),
    ("auth", "current_user"),
    ("auth", "require_user"),
    ("auth", "require_login"),
]

CSRF_CANDIDATES = [
    ("bridge_buyer_backend", "csrf_protect"),
    ("bridge_buyer_backend", "csrf_protector"),
    ("auth", "csrf_protect"),
    ("auth", "csrf_protector"),
]

auth_dep = None
csrf_dep = None

def _import_attr(mod, name):
    try:
        m = __import__(mod, fromlist=[name])
        return getattr(m, name)
    except Exception:
        return None

for mod, name in AUTH_CANDIDATES:
    auth_dep = _import_attr(mod, name)
    if auth_dep:
        break

for mod, name in CSRF_CANDIDATES:
    csrf_dep = _import_attr(mod, name)
    if csrf_dep:
        break

class _CIUser:
    id = "00000000-0000-0000-0000-000000000000"
    username = "ci.user"
    email = "ci@bridge.local"
    role = "admin"
    is_active = True
    email_verified = True
    tenant_id = "11111111-1111-1111-1111-111111111111"

if auth_dep:
    app.dependency_overrides[auth_dep] = lambda: _CIUser()
if csrf_dep:
    app.dependency_overrides[csrf_dep] = lambda: None

# ---- sanity check: /contracts should not 401 after overrides ----
with TestClient(app) as _probe:
    probe = _probe.post("/contracts", json={
        "buyer": "X",
        "seller": "Y",
        "material": "Z",
        "weight_tons": 1.0,
        "price_per_ton": 1.0,
    })
    if probe.status_code == 401:
        raise RuntimeError(
            "CI auth override did NOT attach. "
            f"Tried auth={[(m,n) for (m,n) in AUTH_CANDIDATES if _import_attr(m,n)]} "
            f"csrf={[(m,n) for (m,n) in CSRF_CANDIDATES if _import_attr(m,n)]}. "
            "Rename the candidate to match your actual dependency or export it from bridge_buyer_backend."
        )

@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        c.headers.update({"X-CI": "true"})
        c.cookies.set("session", "ci-session")
        yield c

@pytest.fixture(scope="session", autouse=True)
def seed_inventory(client):
    payload = {
        "source": "ci",
        "seller": "Winski Brothers",
        "items": [
            {"sku": "Shred Steel", "qty_on_hand": 100.0, "description": "test", "uom": "ton"}
        ],
    }
    r = client.post("/inventory/bulk_upsert", json=payload)
    assert r.status_code == 200, f"seed_inventory failed: {r.text}"
