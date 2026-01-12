import os, json
import httpx
from typing import Optional, Dict, Any

DAT_BASE_URL = os.getenv("DAT_BASE_URL", "").rstrip("/")
DAT_API_KEY = os.getenv("DAT_API_KEY", "")
DAT_API_SECRET = os.getenv("DAT_API_SECRET", "")

class DATError(Exception): ...

def _headers() -> Dict[str, str]:
    if not DAT_API_KEY:
        raise DATError("DAT_API_KEY missing")
    # Adjust auth scheme to what DAT gives you (Bearer, Basic, etc.)
    return {
        "Authorization": f"Bearer {DAT_API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

async def get_carrier_by_dot(dot_number: str) -> Dict[str, Any]:
    """
    Look up a carrier by DOT in DAT CarrierWatch.
    Response shape should include authority, insurance, OOS, etc.
    Replace path with your actual endpoint.
    """
    url = f"{DAT_BASE_URL}/carrierwatch/carriers?dot={dot_number}"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url, headers=_headers())
    if r.status_code == 404:
        return {}
    if r.status_code >= 300:
        raise DATError(f"DAT error {r.status_code}: {r.text}")
    return r.json()

async def subscribe_monitoring(external_carrier_id: str) -> Dict[str, Any]:
    """
    Turn on monitoring/alerts for a DAT carrier id.
    """
    url = f"{DAT_BASE_URL}/carrierwatch/monitoring"
    payload = {"carrier_id": external_carrier_id, "monitor": True}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, headers=_headers(), json=payload)
    if r.status_code >= 300:
        raise DATError(f"DAT monitor error {r.status_code}: {r.text}")
    return r.json()

async def fetch_changes_since(since_iso: str) -> Dict[str, Any]:
    """
    Poll DAT for changes (OOS/insurance/etc) since timestamp.
    """
    url = f"{DAT_BASE_URL}/carrierwatch/changes?since={since_iso}"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url, headers=_headers())
    if r.status_code >= 300:
        raise DATError(f"DAT changes error {r.status_code}: {r.text}")
    return r.json()
