import pandas as pd

EXPECTED_COLS = [
    "timestamp","underlying","expiry","strike",
    "option_type","ltp","iv","oi","oi_change",
    "volume","ltp_prev"
]

def load_snapshot_csv(path):
    df = pd.read_csv(path)
    missing = set(EXPECTED_COLS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    return df
  
