import os
from typing import Dict, Any, Optional

FMCSA_SAFER_ENABLED = os.getenv("FMCSA_SAFER_ENABLED", "false").lower() == "true"

class SAFERError(Exception): ...

async def fetch_snapshot(dot_number: Optional[str]=None, mc_number: Optional[str]=None) -> Dict[str, Any]:
    """
    Lightweight wrapper for FMCSA/SAFER baseline.
    For production, point this to your chosen method (official feed, partner API, or your own proxy).
    This stub returns a normalized dict; replace the body with real calls.
    """
    if not FMCSA_SAFER_ENABLED:
        # Return minimal stub (lets you build the pipe first)
        return {
            "source": "fmcsa",
            "dot_number": dot_number,
            "mc_number": mc_number,
            "legal_name": None,
            "dba_name": None,
            "authority_status": None,
            "safety_rating": None,
            "oos": None,
            "insurance_exp": None,
            "address": {"country": "US", "state": None, "city": None, "postal_code": None},
            "raw": {}
        }

    # TODO: implement your real fetch here (via your proxy or paid source).
    # Keep the returned JSON shape stable with keys above.
    raise SAFERError("SAFER integration not yet implemented. Provide a proxy/API and wire it here.")
