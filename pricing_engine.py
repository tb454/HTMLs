from typing import Optional

# ---- Material sets  ----
copperMaterials = ["Barley", "#1", "#2", "#3"]
aluminumMaterials = [
    "AL6061_NewBareExtrusion","AL6061_ClipPlatePipe","AL6063_Secondary","AL6063_OldExtrusion",
    "AL6063_Extrusion10/10","AL_PAS_Baled","AL_Cast_Clean","AL_RADS_CleanBaled","AL_RADS_DirtyBaled",
    "AL_Sheet_CleanBaled","AL_WheelAuto_Clean","AL_WheelChrome_Clean","TruckWheel_Clean",
    "AL_Wire_BareCATV","AL_Wire_NeopreneACSR","AL_Wire_ECPrepared","AL_MLC_Prepared"
]
brassMaterials = ["Brass_Hard","Brass_Red_Ebony","Brass_Red_Semi","Brass_Shaving_AlBronze",
                  "Brass_Shaving_BrassRed","Brass_Shell","Brass_Special","Brass_YellowRegular"]
leadMaterials = ["Lead_Clean","Lead_Range"]
stainlessMaterials = ["SS304","SS316"]
eScrapMaterials = ["E_Scrap_Board"]
insulatedWireMaterials = [
    "ICW_LowGrade_ChristmasLights","ICW_LowGrade_ComputerWire25","ICW_LowGrade_CuCATV",
    "ICW_LowGrade_ExtCords35Up","ICW_LowGrade_MixedWire40Up","ICW1_Heliax57_OpenEyeBaled",
    "ICW1_MCM85HG","ICW1_Romex65_NoWeatherProof","ICW1_THHN80","ICW2_50Cat5TelWire",
    "ICW2_HarnessWireNoFuseBoxes","ICW2_BX_Cable_AL","ICW2_BX_Cable_FE24"
]
miscProductMaterials = [
    "Ballast_Electronic","Ballast_Regular","BatteryAuto_PbAcid","BatterySteelCase_Industrial",
    "Compressor_SealedUnitNoCastIron","EMotorLowGrade_CeilingFanMotors","EMotorMix_NoPumpsPowerToolsFans",
    "EMotorLarge_LessThan1000lbs","Transformer_AluLargeOnly","TransformerCu_AlCu",
    "TransformerCu_SmallPalmSize","TransformerCu_MediumOver200lbs","TransformerCu_Large","Zinc_DieCast"
]
radiatorProductMaterials = [
    "ACReefer_CleanTalkBaled","ACReefer_CleanUnbaled","ACReefer_DirtyTalkBaled",
    "ACReefer_DirtyUnbaled","Radiator_CleanCar","CuBkg_ACReeferEnd"
]

# ---- Multipliers  ----
CU_FACT = {"Barley":0.94, "#1":0.91, "#2":0.85, "#3":0.83}
AL_FACT = {
    "AL6061_NewBareExtrusion":0.84, "AL6061_ClipPlatePipe":0.78, "AL6063_Secondary":0.78,
    "AL6063_OldExtrusion":0.95, "AL6063_Extrusion10/10":0.85, "AL_PAS_Baled":0.82,
    "AL_Cast_Clean":0.72, "AL_RADS_CleanBaled":0.67, "AL_RADS_DirtyBaled":0.46,
    "AL_Sheet_CleanBaled":0.75, "AL_WheelAuto_Clean":0.93, "AL_WheelChrome_Clean":0.80,
    "TruckWheel_Clean":0.72, "AL_Wire_BareCATV":0.43, "AL_Wire_NeopreneACSR":0.54,
    "AL_Wire_ECPrepared":1.03, "AL_MLC_Prepared":0.84
}
BRASS_FACT = {
    "Brass_Hard":0.81, "Brass_Red_Ebony":0.77, "Brass_Red_Semi":0.72,
    "Brass_Shaving_AlBronze":0.57, "Brass_Shaving_BrassRed":0.61,
    "Brass_Shell":0.60, "Brass_Special":0.64, "Brass_YellowRegular":0.61
}
LEAD_FACT = {"Lead_Clean":0.718, "Lead_Range":0.728}
STAIN_FACT = {"SS304":0.0638, "SS316":0.1174}
ICW_FACT = {
    "ICW_LowGrade_ChristmasLights":0.528, "ICW_LowGrade_ComputerWire25":0.448,
    "ICW_LowGrade_CuCATV":1.008, "ICW_LowGrade_ExtCords35Up":0.92,
    "ICW_LowGrade_MixedWire40Up":1.08, "ICW1_Heliax57_OpenEyeBaled":1.536,
    "ICW1_MCM85HG":2.536, "ICW1_Romex65_NoWeatherProof":1.904, "ICW1_THHN80":2.36,
    "ICW2_50Cat5TelWire":1.384, "ICW2_HarnessWireNoFuseBoxes":1.408,
    "ICW2_BX_Cable_AL":1.648, "ICW2_BX_Cable_FE24":0.6
}
MISC_AL_FACT = {
    "Ballast_Electronic":0.12, "Ballast_Regular":0.216, "BatteryAuto_PbAcid":0.216,
    "BatterySteelCase_Industrial":0.192, "Compressor_SealedUnitNoCastIron":0.264,
    "EMotorLowGrade_CeilingFanMotors":0.144, "EMotorMix_NoPumpsPowerToolsFans":0.344,
    "EMotorLarge_LessThan1000lbs":0.312, "Transformer_AluLargeOnly":0.176,
    "TransformerCu_AlCu":0.28, "TransformerCu_SmallPalmSize":0.56,
    "TransformerCu_MediumOver200lbs":0.592, "TransformerCu_Large":0.624
}
RADS_FACT = {
    "ACReefer_CleanTalkBaled":1.608, "ACReefer_CleanUnbaled":1.592,
    "ACReefer_DirtyTalkBaled":1.496, "ACReefer_DirtyUnbaled":1.48,
    "Radiator_CleanCar":1.984, "CuBkg_ACReeferEnd":1.008
}

# ---- Engine ----
async def compute_material_price(fetch_price_fn, category: str, material: str) -> Optional[float]:
    """
    fetch_price_fn(symbol) -> {"price": float}
      Symbols now supported:
        COMEX_CU, COMEX_AL, COMEX_ZN, COMEX_AU, COMEX_AG, COMEX_PL, COMEX_PA
        LME_PB, LME_NI, LME_ZN
    """
    cat = category.strip().lower()

    # Bases (None if missing)
    comex_cu = await fetch_price_fn("COMEX_CU")
    comex_al = await fetch_price_fn("COMEX_AL")
    comex_zn = await fetch_price_fn("COMEX_ZN")
    lme_pb   = await fetch_price_fn("LME_PB")
    lme_ni   = await fetch_price_fn("LME_NI")
    lme_zn   = await fetch_price_fn("LME_ZN")

    cu_base = comex_cu["price"] if comex_cu else None
    al_base = comex_al["price"] if comex_al else None
    zn_base = (lme_zn["price"] if lme_zn else (comex_zn["price"] if comex_zn else None))
    pb_base = lme_pb["price"] if lme_pb else None
    ni_base = lme_ni["price"] if lme_ni else None

    # Rule: COMEX Cu − $0.10 before multipliers for copper family + brass
    cu_minus_10 = (cu_base - 0.10) if cu_base is not None else None

    if cat == "copper" and cu_minus_10 is not None:
        f = CU_FACT.get(material)
        return round(cu_minus_10 * f, 4) if f is not None else None

    if cat == "aluminum" and al_base is not None:
        f = AL_FACT.get(material)
        return round(al_base * f, 4) if f is not None else None

    if cat == "brass" and cu_minus_10 is not None:
        f = BRASS_FACT.get(material)
        return round(cu_minus_10 * f, 4) if f is not None else None

    if cat == "lead" and pb_base is not None:
        f = LEAD_FACT.get(material)
        return round(pb_base * f, 4) if f is not None else None

    if cat == "stainless" and ni_base is not None:
        f = STAIN_FACT.get(material)
        return round(ni_base * f, 4) if f is not None else None

    if cat == "insulated wire" and al_base is not None:
        f = ICW_FACT.get(material)
        return round(al_base * f, 4) if f is not None else None

    if cat == "misc product":
        if material == "Zinc_DieCast" and zn_base is not None:
            return round(zn_base * 0.5903, 4)
        if al_base is not None:
            f = MISC_AL_FACT.get(material)
            return round(al_base * f, 4) if f is not None else None
        return None

    if cat == "radiator product" and al_base is not None:
        f = RADS_FACT.get(material)
        return round(al_base * f, 4) if f is not None else None

    if cat == "e-scrap":
        # Leave paramized (gold/silver/copper contents differ per board).
        return None

    return None
