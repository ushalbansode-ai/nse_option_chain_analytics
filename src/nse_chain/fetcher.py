import requests
import pandas as pd
from datetime import datetime

HEADERS = {
    "user-agent": "Mozilla/5.0",
    "accept-language": "en-US,en;q=0.9",
    "accept": "application/json",
}

def fetch_snapshot(symbol: str):
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"

    session = requests.Session()

    # Step 1: Mandatory homepage request to get cookies
    session.get("https://www.nseindia.com", headers=HEADERS)

    # Step 2: Retry logic (NSE is unstable)
    for attempt in range(3):
        try:
            res = session.get(url, headers=HEADERS, timeout=10)
            if res.status_code == 200:
                data = res.json()
                break
        except:
            continue
    else:
        print("❌ NSE fetch failed 3 times")
        return pd.DataFrame()

    # Validate response
    if "records" not in data or "data" not in data["records"]:
        print("❌ NSE returned invalid format")
        return pd.DataFrame()

    records = data["records"]["data"]

    rows = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for row in records:
        strike = row.get("strikePrice")

        ce = row.get("CE", {})
        pe = row.get("PE", {})

        rows.append({
            "timestamp": timestamp,
            "symbol": symbol,
            "strike": strike,

            # CE
            "ltp_CE": ce.get("lastPrice"),
            "ltp_prev_CE": ce.get("prevClose"),
            "oi_CE": ce.get("openInterest"),
            "oi_change_CE": ce.get("changeinOpenInterest"),
            "iv_ce": ce.get("impliedVolatility"),

            # PE
            "ltp_PE": pe.get("lastPrice"),
            "ltp_prev_PE": pe.get("prevClose"),
            "oi_PE": pe.get("openInterest"),
            "oi_change_PE": pe.get("changeinOpenInterest"),
            "iv_pe": pe.get("impliedVolatility")
        })

    df = pd.DataFrame(rows)
    return df
    
