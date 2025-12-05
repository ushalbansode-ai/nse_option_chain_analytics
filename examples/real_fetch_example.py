from nse_chain.fetcher import fetch_snapshot
from nse_chain.analytics import compute_oi_differences, compute_oi_magnets_and_gaps
import pandas as pd
import os

# --------------------------------------------------
# Create data folder if missing
# --------------------------------------------------
os.makedirs("data", exist_ok=True)

# --------------------------------------------------
# INDEX LIST — NIFTY + BANKNIFTY
# --------------------------------------------------
INDEX_LIST = ["NIFTY", "BANKNIFTY"]


all_outputs = []

# --------------------------------------------------
# PROCESS BOTH INDEXES
# --------------------------------------------------
for symbol in INDEX_LIST:
    print(f"\n========================")
    print(f"   FETCHING {symbol}")
    print(f"========================")

    # Fetch raw snapshot
    df = fetch_snapshot(symbol)
    print("\n=== RAW SNAPSHOT ===")
    print(df.head())

    # Compute CE/PE derived analytics
    derived = compute_oi_differences(df)
    print("\n=== DERIVED ANALYTICS ===")
    print(derived.head())

    # Save individual derived results
    derived.to_csv(f"data/derived_{symbol}.csv", index=False)
    print(f"✔ Saved: data/derived_{symbol}.csv")

    # Approx spot using mid-strike
    spot = derived["strike"].median()

    # Compute magnets & gaps
    magnets, gaps = compute_oi_magnets_and_gaps(derived, spot)

    magnets.to_csv(f"data/oi_magnets_{symbol}.csv", index=False)
    gaps.to_csv(f"data/oi_gaps_{symbol}.csv", index=False)

    print(f"✔ Saved: data/oi_magnets_{symbol}.csv")
    print(f"✔ Saved: data/oi_gaps_{symbol}.csv")

    # Keep all data for combined CSV
    derived["symbol"] = symbol
    all_outputs.append(derived)


# --------------------------------------------------
# COMBINE NIFTY + BANKNIFTY OUTPUT
# --------------------------------------------------
final_df = pd.concat(all_outputs, ignore_index=True)
final_df.to_csv("data/derived_latest.csv", index=False)

print("\n\n🎉 DONE — Combined analytics saved to: data/derived_latest.csv")
