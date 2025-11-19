# bridge_bootstrap_instruments.py
import os, asyncio, re
from databases import Database

DB_URL = os.getenv("DATABASE_URL") or os.getenv("ASYNC_DATABASE_URL")

# crude helper: make a screaming snake-ish code
def slug(s: str) -> str:
    s = s.strip()
    s = s.replace("—", "-").replace("–","-")
    s = re.sub(r"[()]+", "", s)
    s = re.sub(r"[^A-Za-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_").upper()

def infer_base_metal(label: str) -> str:
    u = label.upper()
    if "COPPER" in u or u.startswith("NO. 1 BARE BRIGHT") or "BARE BRIGHT" in u or u.startswith("ICW"):
        return "CU"
    if "AL " in f" {u}" or "ALUMINUM" in u or u.startswith("MLC") or "EXTRUSION" in u or "RADS" in u:
        return "AL"
    if "BRASS" in u:
        return "BRASS"
    if "STAINLESS" in u or "SS " in u:
        return "SS"
    if "LEAD" in u:
        return "PB"
    if "BUSHELING" in u or "SHRED" in u or "HMS" in u or "DRUMS & ROTORS" in u:
        return "FE"
    if "E-SCRAP" in u or "SERVER" in u or "COMPUTER" in u or "BOARD" in u:
        return "E-SCRAP"
    return "OTHER"

def infer_core_code(label: str) -> str:
    u = label.upper()

    # COPPER CORE FAMILIES
    if "BARE BRIGHT" in u or "BARLEY" in u:
        return "CU_BARE_BRIGHT"
    if "NO. 1 COPPER" in u or "BERRY" in u:
        return "CU_NO1"
    if "NO. 2 COPPER" in u or "BIRCH" in u:
        return "CU_NO2"
    if u.startswith("ICW #1") or "ICW #1" in u:
        return "CU_ICW_HIGH"
    if u.startswith("ICW #2") or "ICW #2" in u:
        return "CU_ICW_MED"
    if "ICW LOW GRADE" in u:
        return "CU_ICW_LOW"

    # RADS / ACR
    if "AL/CU RADIATORS" in u and "CLEAN" in u:
        return "AL_CU_RADS_CLEAN"
    if "AL/CU RADIATORS" in u and "DIRTY" in u:
        return "AL_CU_RADS_DIRTY"
    if "RADIATOR ENDS" in u:
        return "CU_RAD_ENDS"

    # ALUMINUM EXTRUSION / CAST / MLC / WHEELS
    if "AL EXTRUSION 6063 10/10" in u:
        return "AL_6063_EXTRUSION_10_10"
    if "EXTRUSION 6063" in u or "AL EXTRUSION 6063" in u:
        return "AL_6063_EXTRUSION"
    if "AL 6061 SOLIDS" in u:
        return "AL_6061_SOLIDS"
    if "MLC (MIXED LOW COPPER)" in u:
        return "AL_MLC"
    if "AL CAST" in u:
        return "AL_CAST"
    if "AL BREAKAGE" in u:
        return "AL_BREAKAGE"
    if "AL CAR WHEELS" in u:
        return "AL_WHEEL_AUTO"
    if "AL TRUCK WHEELS" in u:
        return "AL_WHEEL_TRUCK"

    # STAINLESS
    if "STAINLESS 304" in u:
        return "SS_304"
    if "STAINLESS 316" in u:
        return "SS_316"
    if "STAINLESS BREAKAGE" in u:
        return "SS_BREAKAGE"

    # BRASS / LEAD / BATTERIES
    if "YELLOW BRASS" in u and "TURNINGS" in u:
        return "BRASS_YELLOW_TURNINGS"
    if "YELLOW BRASS" in u:
        return "BRASS_YELLOW"
    if "BRASS SHELLS" in u:
        return "BRASS_SHELLS"
    if "RANGE LEAD" in u:
        return "LEAD_RANGE"
    if "LEAD WHEEL WEIGHTS" in u:
        return "LEAD_WHEEL"
    if "LEAD-ACID BATTERIES (AUTO)" in u:
        return "BATTERY_AUTO"
    if "LEAD-ACID BATTERIES (STEEL CASE" in u:
        return "BATTERY_STEEL_CASE"

    # MOTORS / SEALED UNITS / TRANSFORMERS
    if "MIXED ELECTRIC MOTORS" in u:
        return "MOTORS_MIX"
    if "LOW GRADE/CEILING FANS" in u:
        return "MOTORS_LOW"
    if "LARGE <1000 LB" in u:
        return "MOTORS_LARGE"
    if "SEALED UNITS / COMPRESSORS" in u:
        return "SEALED_UNITS"
    if "TRANSFORMERS (AL/CU)" in u:
        return "TRANSFORMERS_AL_CU"
    if "TRANSFORMERS (COPPER, SMALL)" in u:
        return "TRANSFORMERS_CU_SMALL"
    if "TRANSFORMERS (COPPER, MEDIUM)" in u:
        return "TRANSFORMERS_CU_MED"
    if "TRANSFORMERS (COPPER, LARGE)" in u:
        return "TRANSFORMERS_CU_LARGE"

    # E-SCRAP
    if "E-SCRAP (MIXED)" in u:
        return "E_SCRAP_MIXED"
    if "E-SCRAP (BOARDS)" in u:
        return "E_SCRAP_BOARDS"
    if "E-SCRAP (TV/MONITORS)" in u:
        return "E_SCRAP_TV_MON"
    if "COMPUTER TOWER (COMPLETE)" in u:
        return "E_TOWER_COMPLETE"
    if "SERVERS (COMPLETE)" in u:
        return "E_SERVERS_COMPLETE"
    if "BALLASTS (ELECTRONIC)" in u:
        return "BALLAST_ELECTRONIC"
    if "BALLASTS (MAGNETIC/REGULAR)" in u:
        return "BALLAST_MAGNETIC"

    # Fallback: generic slug
    return slug(u)[:64]

async def main():
    if not DB_URL:
        print("ERROR: DATABASE_URL/ASYNC_DATABASE_URL not set")
        return

    db = Database(DB_URL)
    await db.connect()
    print(f"Connected to {DB_URL}")

    # 1) Get distinct material_canonical values you already defined
    rows = await db.fetch_all("""
        SELECT DISTINCT material_canonical
        FROM vendor_material_map
        WHERE material_canonical NOT LIKE '#%%'   -- skip comment lines if any got through
          AND material_canonical <> ''
    """)
    print(f"Found {len(rows)} distinct canonical labels")

    core_seen = set()
    instr_seen = set()

    async with db.transaction():
        for r in rows:
            label = r["material_canonical"].strip()
            if not label:
                continue

            base = infer_base_metal(label)
            core = infer_core_code(label)
            instr = slug(label)

            # Upsert core
            if core not in core_seen:
                await db.execute("""
                    INSERT INTO scrap_core(core_code, category, description)
                    VALUES (:core, :cat, :desc)
                    ON CONFLICT (core_code) DO NOTHING
                """, {
                    "core": core,
                    "cat": base if base != "OTHER" else "Other",
                    "desc": label,
                })
                core_seen.add(core)

            # Upsert instrument
            if instr not in instr_seen:
                await db.execute("""
                    INSERT INTO scrap_instrument(
                      instrument_code, core_code, material_canonical,
                      base_metal, series_or_grade, form,
                      condition_clean, bale_status, prep_status, recovery_band, notes
                    )
                    VALUES (:instr, :core, :label,
                            :base, NULL, NULL,
                            NULL, NULL, NULL, NULL, NULL)
                    ON CONFLICT (instrument_code) DO NOTHING
                """, {
                    "instr": instr,
                    "core": core,
                    "label": label,
                    "base": base,
                })
                instr_seen.add(instr)

        # 2) Write instrument_code back into vendor_material_map
        await db.execute("""
            UPDATE vendor_material_map m
            SET instrument_code = s.instrument_code
            FROM scrap_instrument s
            WHERE m.material_canonical = s.material_canonical
        """)

    print(f"Bootstrapped {len(core_seen)} cores and {len(instr_seen)} instruments")
    await db.disconnect()
    print("Disconnected.")

if __name__ == "__main__":
    asyncio.run(main())
