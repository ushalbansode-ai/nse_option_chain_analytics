from analytics import calculate_oi_skew, detect_volume_spike, compute_gamma_exposure, detect_reversal
import pandas as pd

def test_smoke():
    df = pd.DataFrame({
        "CE_OI": [100, 120],
        "PE_OI": [80, 130],
        "Volume": [5000, 9000],
        "OI": [2000, 2100],
        "Gamma": [0.5, 0.6],
        "LTP": [150, 152],
        "Open": [100, 120],
        "Close": [110, 115]
    })

    calculate_oi_skew(df)
    detect_volume_spike(df)
    compute_gamma_exposure(df)
    detect_reversal(df)
  
