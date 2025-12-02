import pandas as pd

def detect_reversal(df: pd.DataFrame) -> pd.Series:
    """
    Basic reversal detection using candle pattern:
    Bullish: (Close > Open) after (Close < Open)
    Bearish: reverse of above
    """
    bull = (df["Close"] > df["Open"]) & (df["Close"].shift(1) < df["Open"].shift(1))
    bear = (df["Close"] < df["Open"]) & (df["Close"].shift(1) > df["Open"].shift(1))

    return bull.astype(int) - bear.astype(int)
  
