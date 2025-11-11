from fastapi import APIRouter, Request, HTTPException
import hmac, hashlib, base64, os

router = APIRouter(prefix="/webhooks", tags=["Integrations"])
ICE_SECRET = os.getenv("ICE_WEBHOOK_SECRET","change-me").encode()

def verify_signature(raw: bytes, sig_b64: str) -> bool:
    mac = hmac.new(ICE_SECRET, raw, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(mac), sig_b64.encode())

@router.post("/ice")
async def ice_webhook(request: Request):
    raw = await request.body()
    sig = request.headers.get("X-ICE-Signature","")
    if not verify_signature(raw, sig):
        raise HTTPException(401, "bad signature")
    # TODO: persist receipt + advance contract status
    return {"ok": True}
