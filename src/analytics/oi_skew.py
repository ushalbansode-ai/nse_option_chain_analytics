import pandas as pd
from .utils import zscore

def calculate_oi_skew(df: pd.DataFrame) -> pd.Series:
    """
    Computes call-vs-put OI skew as % difference.
    """
    call_oi = df['CE_OI']
    put_oi = df['PE_OI']
    skew = (call_oi - put_oi) / (call_oi + put_oi + 1e-9)
    return zscore(skew)
  
