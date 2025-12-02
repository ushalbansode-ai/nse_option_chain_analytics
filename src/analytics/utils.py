import numpy as np
import pandas as pd

def zscore(series: pd.Series) -> pd.Series:
    """Returns z-score normalized series."""
    return (series - series.mean()) / (series.std() + 1e-9)
  
