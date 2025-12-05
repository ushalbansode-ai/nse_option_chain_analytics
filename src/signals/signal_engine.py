import pandas as pd
import numpy as np

def classify_buildup(price_change, oi_change):
    if price_change > 0 and oi_change > 0:
        return "LONG_BUILDUP"
    if price_change < 0 and oi_change > 0:
        return "SHORT_BUILDUP"
    if price_change > 0 and oi_change < 0:
        return "SHORT_COVERING"
    if price_change < 0 and oi_change < 0:
        return "LONG_UNWINDING"
    return "NEUTRAL"


def detect_signal_row(row, underlying_spot):
    signal = None
    reasons = []

    # --- Build-up classification ---
    ce_bu = classify_buildup(row["price_change_CE"], row["oi_change_CE"])
    pe_bu = classify_buildup(row["price_change_PE"], row["oi_change_PE"])

    # --- OI Flip detection ---
    oi_flip = (
        (row["oi_diff_prev"] > 0 and row["oi_diff"] < 0) or
        (row["oi_diff_prev"] < 0 and row["oi_diff"] > 0)
    )

    # --- IV Skew ---
    iv_skew = row["iv_pe"] - row["iv_ce"]

    # --- Market Position ---
    above_strike = underlying_spot > row["strike"]
    below_strike = underlying_spot < row["strike"]

    # ============================
    #          CALL BUY
    # ============================
    if above_strike:
        if ce_bu in ["SHORT_COVERING", "LONG_BUILDUP"] and \
           pe_bu in ["LONG_UNWINDING", "SHORT_BUILDUP"]:
            
            signal = "CALL_BUY"
            reasons.append(f"CE buildup: {ce_bu}, PE: {pe_bu}")

            if oi_flip:
                reasons.append("OI Flip → CE strength")
            if iv_skew < 0:
                reasons.append("CE IV increasing")

    # ============================
    #          PUT BUY
    # ============================
    if below_strike:
        if pe_bu in ["SHORT_COVERING", "LONG_BUILDUP"] and \
           ce_bu in ["LONG_UNWINDING", "SHORT_BUILDUP"]:

            signal = "PUT_BUY"
            reasons.append(f"PE buildup: {pe_bu}, CE: {ce_bu}")

            if oi_flip:
                reasons.append("OI Flip → PE strength")
            if iv_skew > 0:
                reasons.append("PE IV increasing")

    # Final return - ONLY TWO VALUES
    reason_text = "; ".join(reasons) if reasons else ""
    return signal, reason_text
    
