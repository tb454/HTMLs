# indices_builder.py
import os, asyncpg, asyncio
from datetime import datetime, timezone, date
from typing import Dict, Any, Optional

DATABASE_URL = os.getenv("DATABASE_URL")

# ---------- Helpers ----------
async def _fetch_latest(pool, symbol: str) -> Optional[float]:
    q = """
      SELECT price
      FROM reference_prices
      WHERE symbol = $1
      ORDER BY ts_server DESC
      LIMIT 1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(q, symbol)
    return float(row["price"]) if row else None

async def _upsert_close(pool, symbol: str, dt: date, price: float, note: str):
    q = """
      INSERT INTO bridge_index_history (symbol, dt, close_price, source_note)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (symbol, dt)
      DO UPDATE SET close_price = EXCLUDED.close_price,
                    source_note = EXCLUDED.source_note
    """
    async with pool.acquire() as conn:
        await conn.execute(q, symbol, dt, price, note)

# ---------- Built-in defaults (only used if DB-definitions table is empty) ----------
DEFAULT_INDEX_SET = [
    # Copper family
    {"symbol": "BR-CU#1",            "method": "CU_MINUS10_X", "factor": 0.91,  "base_symbol": "COMEX_CU", "notes": "#1 Copper"},
    {"symbol": "BR-CU-BARLEY",       "method": "CU_MINUS10_X", "factor": 0.94,  "base_symbol": "COMEX_CU", "notes": "Barley (Cu BB)"},
    {"symbol": "BR-CU-#2",           "method": "CU_MINUS10_X", "factor": 0.85,  "base_symbol": "COMEX_CU", "notes": "#2 (Birch & Cliff)"},
    {"symbol": "BR-CU-#3",           "method": "CU_MINUS10_X", "factor": 0.83,  "base_symbol": "COMEX_CU", "notes": "#3 (Sheet)"},
    # Aluminum
    {"symbol": "BR-AL6063-OLD",      "method": "AL_X",         "factor": 0.95,  "base_symbol": "COMEX_AL", "notes": "6063 Old Extrusion"},
    {"symbol": "BR-AL6061-NBE",      "method": "AL_X",         "factor": 0.84,  "base_symbol": "COMEX_AL", "notes": "6061 New Bare Extrusion"},
    {"symbol": "BR-AL-MLC-PREP",     "method": "AL_X",         "factor": 0.84,  "base_symbol": "COMEX_AL", "notes": "MLC Prepared"},
    # ICW sample
    {"symbol": "BR-ICW1-THHN80",     "method": "AL_X",         "factor": 2.36,  "base_symbol": "COMEX_AL", "notes": "ICW1 THHN80"},
    # Zinc die cast
    {"symbol": "BR-ZN-DIECAST",      "method": "ZN_X",         "factor": 0.5903,"base_symbol": "LME_ZN",   "notes": "Zinc Die Cast"},
    # Lead & Stainless examples (useful if DB defs missing)
    {"symbol": "BR-PB-CLEAN",        "method": "PB_X",         "factor": 0.718, "base_symbol": "LME_PB",   "notes": "Lead Clean"},
    {"symbol": "BR-SS304",           "method": "NI_X",         "factor": 0.0638,"base_symbol": "LME_NI",   "notes": "Stainless 304"},
]

async def _load_index_definitions(pool):
    # Prefer DB-configured indices if present; else fall back to defaults.
    q = """SELECT symbol, method, factor, base_symbol, coalesce(notes,'') AS notes
           FROM bridge_index_definitions WHERE enabled = true"""
    async with pool.acquire() as conn:
        rows = await conn.fetch(q)
    if rows:
        return [dict(r) for r in rows]
    return DEFAULT_INDEX_SET

# ---------- Compute one ----------
async def _compute_one(pool, cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    method = cfg["method"].strip().upper()
    factor = float(cfg["factor"])
    symbol = cfg["symbol"]
    base   = cfg["base_symbol"].strip().upper()

    # Helper to annotate which base was actually used (handy for ZN fallback)
    def note_used(base_label: str) -> str:
        return f"{base_label} * {factor}"

    # CU_MINUS10_X: (COMEX_CU - 0.10) * factor
    if method == "CU_MINUS10_X":
        cu = await _fetch_latest(pool, "COMEX_CU")
        if cu is None:
            return None
        price = round((cu - 0.10) * factor, 6)
        note = f"(COMEX_CU - 0.10) * {factor}"
        return {"symbol": symbol, "price": price, "note": note}

    # AL_X: COMEX_AL * factor
    if method == "AL_X":
        al = await _fetch_latest(pool, "COMEX_AL")
        if al is None:
            return None
        price = round(al * factor, 6)
        note = note_used("COMEX_AL")
        return {"symbol": symbol, "price": price, "note": note}

    # ZN_X: (LME_ZN else COMEX_ZN) * factor
    if method == "ZN_X":
        zn = await _fetch_latest(pool, "LME_ZN")
        base_label = "LME_ZN"
        if zn is None:
            zn = await _fetch_latest(pool, "COMEX_ZN")
            base_label = "COMEX_ZN"
        if zn is None:
            return None
        price = round(zn * factor, 6)
        note = note_used(base_label)
        return {"symbol": symbol, "price": price, "note": note}

    # PB_X: LME_PB * factor
    if method == "PB_X":
        pb = await _fetch_latest(pool, "LME_PB")
        if pb is None:
            return None
        price = round(pb * factor, 6)
        note = note_used("LME_PB")
        return {"symbol": symbol, "price": price, "note": note}

    # NI_X: LME_NI * factor
    if method == "NI_X":
        ni = await _fetch_latest(pool, "LME_NI")
        if ni is None:
            return None
        price = round(ni * factor, 6)
        note = note_used("LME_NI")
        return {"symbol": symbol, "price": price, "note": note}

    # Unknown method → skip
    return None

# ---------- Entry point ----------
async def run_indices_builder():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=10, max_size=20)
    try:
        dt_utc = datetime.now(timezone.utc).date()
        defs = await _load_index_definitions(pool)
        for cfg in defs:
            res = await _compute_one(pool, cfg)
            if not res:
                continue
            await _upsert_close(
                pool,
                symbol=cfg["symbol"],
                dt=dt_utc,
                price=res["price"],
                note=res["note"] + (f" [{cfg.get('notes','')}]" if cfg.get('notes') else "")
            )
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(run_indices_builder())
