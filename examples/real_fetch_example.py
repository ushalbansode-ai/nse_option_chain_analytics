#!/usr/bin/env python3
"""
Generate Signals from Derived Option-Chain Data
"""

import os
import pandas as pd
from datetime import datetime

from src.signals.signal_engine import detect_signal_row


# ------------------------------------------
# Load the latest derived output
# ------------------------------------------

def load_derived_csv(symbol):
    path = f"data/derived_{symbol}.csv"
    if not os.path.exists(path):
        print(f"[WARN] Missing: {path}")
        return None
    return pd.read_csv(path)


# ------------------------------------------
# Extract underlying spot from derived_latest.csv
# ------------------------------------------

def load_underlying_spots():
    spot_file = "data/derived_latest.csv"
    if not os.path.exists(spot_file):
        print("[WARN] No derived_latest.csv found")
        return {}

    df = pd.read_csv(spot_file)

    # Example format:
    # symbol, underlying
    # NIFTY, 22110.25
    # BANKNIFTY, 48205.90

    spots = dict(zip(df["symbol"], df["underlying"]))
    return spots


# ------------------------------------------
# Apply signal detection to a symbol
# ------------------------------------------

def process_symbol(symbol, df, underlying_spot):
    print(f"\n▶ Processing {symbol} (spot={underlying_spot})")

    signals = []

    for _, row in df.iterrows():
        sig, reason, score = detect_signal_row(row, underlying_spot)
        signals.append([symbol, row["strike"], sig, score, reason])

    out_df = pd.DataFrame(signals,
                          columns=["symbol", "strike", "signal", "score", "reason"])

    return out_df


# ------------------------------------------
# Main routine
# ------------------------------------------

def main():
    os.makedirs("data", exist_ok=True)

    # Load underlying spot values
    spots = load_underlying_spots()
    if not spots:
        print("❌ ERROR: Missing derived_latest.csv → Cannot continue.")
        return

    all_outputs = []

    # Process NIFTY
    nifty_df = load_derived_csv("NIFTY")
    if nifty_df is not None and "NIFTY" in spots:
        out = process_symbol("NIFTY", nifty_df, spots["NIFTY"])
        all_outputs.append(out)

    # Process BANKNIFTY
    bn_df = load_derived_csv("BANKNIFTY")
    if bn_df is not None and "BANKNIFTY" in spots:
        out = process_symbol("BANKNIFTY", bn_df, spots["BANKNIFTY"])
        all_outputs.append(out)

    if not all_outputs:
        print("❌ No derived data available to process.")
        return

    final_df = pd.concat(all_outputs, ignore_index=True)

    # Add timestamp for debugging
    final_df["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Save output
    out_path = "data/signals_latest.csv"
    final_df.to_csv(out_path, index=False)
    print(f"\n✅ Signals saved → {out_path}")

    # Quick preview in logs
    print("\n📌 Top signal rows:")
    print(final_df.head(10))


if __name__ == "__main__":
    main()
    
