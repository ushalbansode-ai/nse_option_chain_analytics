# -----------------------------------------
# ADVANCED ANALYTICS MODULE (OPTION 3)
# Volume Spike + Reversal + Momentum Pulse
# -----------------------------------------

def analyze_volume_spike(records):
    try:
        spikes = []
        for row in records:
            ce = row.get("CE", {})
            pe = row.get("PE", {})

            ce_vol = ce.get("totalTradedVolume", 0)
            pe_vol = pe.get("totalTradedVolume", 0)
            ce_avg = ce_vol / max(ce.get("totalBuyQuantity", 1), 1)
            pe_avg = pe_vol / max(pe.get("totalBuyQuantity", 1), 1)

            if ce_avg > 5 or pe_avg > 5:  # sudden explosion
                spikes.append({
                    "strike": row["strikePrice"],
                    "ce_volume": ce_vol,
                    "pe_volume": pe_vol,
                    "ce_spike_factor": round(ce_avg, 2),
                    "pe_spike_factor": round(pe_avg, 2)
                })
        return spikes
    except:
        return []


def detect_reversal(records):
    try:
        reversals = []
        for row in records:
            ce = row.get("CE", {})
            pe = row.get("PE", {})

            ce_chg = ce.get("change", 0)
            pe_chg = pe.get("change", 0)

            if ce_chg < -2 and pe_chg > 2:
                reversals.append({
                    "strike": row["strikePrice"],
                    "type": "BEARISH"
                })

            if pe_chg < -2 and ce_chg > 2:
                reversals.append({
                    "strike": row["strikePrice"],
                    "type": "BULLISH"
                })
        return reversals
    except:
        return []


def momentum_pulse(records):
    try:
        pulses = []
        for row in records:
            ce = row.get("CE", {})
            pe = row.get("PE", {})

            ce_iv = ce.get("impliedVolatility", 0)
            pe_iv = pe.get("impliedVolatility", 0)

            if ce_iv > 20 or pe_iv > 20:
                pulses.append({
                    "strike": row["strikePrice"],
                    "ce_iv": ce_iv,
                    "pe_iv": pe_iv
                })
        return pulses
    except:
        return []


def full_option3_analysis(option_chain):
    try:
        records = option_chain.get("records", {}).get("data", [])
        if not records:
            return {"error": "No OC data"}

        return {
            "volume_spikes": analyze_volume_spike(records),
            "reversals": detect_reversal(records),
            "momentum_pulse": momentum_pulse(records)
        }
    except Exception as e:
        return {"error": str(e)}
      
