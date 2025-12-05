from nse_chain.fetcher import fetch_snapshot
from nse_chain.analytics import compute_oi_differences, compute_oi_magnets_and_gaps
import os

symbol = "NIFTY"      # or BANKNIFTY

# Ensure data folder exists
os.makedirs("data", exist_ok=True)

# 1. Fetch raw snapshot
df = fetch_snapshot(symbol)
print("\n=== RAW SNAPSHOT ===")
print(df.head())

# 2. Derive analytics
derived = compute_oi_differences(df)
print("\n=== DERIVED ANALYTICS ===")
print(derived.head())

# 3. SAVE DERIVED ANALYTICS (important for GitHub workflow)
derived.to_csv("data/derived_latest.csv", index=False)
print("\n✔ Saved: data/derived_latest.csv")

# 4. Compute ATM-based spot for magnets/gaps
spot = derived["strike"].iloc[len(derived)//2]

# 5. Compute OI magnets and gaps
magnets, gaps = compute_oi_magnets_and_gaps(derived, spot)
print("\n=== TOP OI MAGNETS ===")
print(magnets.head())

print("\n=== OI GAPS ===")
print(gaps.head())

# Save magnets and gaps too (optional)
magnets.to_csv("data/oi_magnets.csv", index=False)
gaps.to_csv("data/oi_gaps.csv", index=False)

print("\n✔ All analytics saved into /data/")
