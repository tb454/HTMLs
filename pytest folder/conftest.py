# tests/conftest.py
import os
import pytest
from starlette.testclient import TestClient

# IMPORTANT: point to a staging DB before running tests in CI or locally.
# os.environ["DATABASE_URL"] = "postgresql://postgres:***@your-staging.supabase.co:5432/postgres"

# Import your FastAPI app
from backend import app  # <-- if your file isn't named backend.py, adjust the import

@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c
