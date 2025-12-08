#!/usr/bin/env python3

import requests
import time
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

HEADERS = {
    "authority": "www.nseindia.com",
    "pragma": "no-cache",
    "cache-control": "no-cache",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "accept": "*/*",
    "referer": "https://www.nseindia.com/option-chain",
    "accept-language": "en-US,en;q=0.9",
}

API_URL = "https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"

OUT_DIR = Path("data/option_chain_raw")


def create_session():
    s = requests.Session()
    s.headers.update(HEADERS)

    # Very important: fetch homepage once for cookies
    try:
        s.get("https://www.nseindia.com/", timeout=15)
        time.sleep(1)
    except Exception:
        pass

    return s


def fetch_option_chain(symbol):
    s = create_session()
    url = API_URL.format(symbol=symbol)

    for attempt in range(1, 8):
        try:
            r = s.get(url, timeout=15)
        except Exception as e:
            print(f"[{symbol}] Connection error: {e}")
            time.sleep(2)
            continue

        # If HTML returned → NSE blocked
        if r.text.strip().startswith("<"):
            print(f"[{symbol}] NSE returned HTML (blocked). Retrying {attempt}/7...")
            time.sleep(2)
            continue

        try:
            data = r.json()
        except:
            print(f"[{symbol}] JSON parse failed. Retrying {attempt}/7...")
            time.sleep(2)
            continue

        if "records" not in data:
            print(f"[{symbol}] Missing records key. Retrying {attempt}/7...")
            time.sleep(2)
            continue

        print(f"[{symbol}] Successfully fetched")
        return data

    print(f"[{symbol}] FAILED after 7 attempts")
    return None


def flatten_data(data, symbol):
    if data is None:
        return pd.DataFrame()

    rows = []
    recs = data["records"]["data"]

    uv = data["records"].get("underlyingValue", None)

    for r in recs:
        strike = r.get("strikePrice")
        expiry = r.get("expiryDate")

        for side in ["CE", "PE"]:
            opt = r.get(side)
            if not opt:
                continue

            rows.append({
                "symbol": symbol,
                "underlying": uv,
                "expiry": expiry,
                "strike": strike,
                "option_type": "CALL" if side == "CE" else "PUT",
                "oi": opt.get("openInterest"),
                "change_oi": opt.get("changeinOpenInterest"),
                "volume": opt.get("totalTradedVolume"),
                "ltp": opt.get("lastPrice"),
                "iv": opt.get("impliedVolatility"),
            })

    return pd.DataFrame(rows)


def process_symbol(symbol):
    print(f"\nFetching → {symbol}")

    data = fetch_option_chain(symbol)
    df = flatten_data(data, symbol)

    if df.empty:
        print(f"[{symbol}] Empty dataframe")
        return None

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    file = OUT_DIR / f"{symbol}_OC_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(file, index=False)

    print(f"[{symbol}] Saved → {file} ({len(df)} rows)")
    return df


def main():
    symbols = ["NIFTY", "BANKNIFTY"]
    all_df = []

    for s in symbols:
        df = process_symbol(s)
        if df is not None:
            all_df.append(df)

    if all_df:
        final = pd.concat(all_df, ignore_index=True)
        final.to_csv(OUT_DIR / "combined_today.csv", index=False)
        print("\nCombined CSV saved.")


if __name__ == "__main__":
    main()

