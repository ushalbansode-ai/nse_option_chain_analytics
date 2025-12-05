from nse_chain.utils import load_snapshot_csv
from nse_chain.analytics import compute_oi_differences, compute_oi_magnets_and_gaps

df = load_snapshot_csv("data/sample_snapshot.csv")
spot = df["underlying_spot"].iloc[0] if "underlying_spot" in df else None

derived = compute_oi_differences(df)
print("Derived OI Metrics:\n", derived.head())

if spot:
    magnets, gaps = compute_oi_magnets_and_gaps(derived, spot)
    print("\nTop OI Magnets:\n", magnets.head())
    print("\nOI Gaps:\n", gaps.head())
  
