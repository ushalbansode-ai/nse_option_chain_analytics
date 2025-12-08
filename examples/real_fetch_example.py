#!/usr/bin/env python3
"""
Real Fetch Example — NSE Option Chain (Working Version)
This version handles:
 - NSE cookie refresh
 - HTML blocking protection
 - indices/equities fallback
 - clean dataframe output
 - multi-symbol CLI input
"""

import requests
import json
import time
import argparse
import pandas as pd
from pathlib import Path


# -------------------------------
# Session Handler (Fixes Blocking)
# -------------------------------
class NSESession:
    def __init__(self):
        self.s = requests.Session()
        self.h = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
            "Connection": "keep-alive",
        }

    def refresh_cookies(self):
        """Visits NSE homepage to get cookies."""
        try:
            print("🔄 Refreshing NSE cookies…")
            self.s.get("https://www.nseindia.com", headers=self.h, timeout=5)
        except Exception:
            pass

    def fetch_json(self, url, symbol, attempt):
        """Internal helper."""
        try:
            r = self.s.get(url, headers=self.h, timeout=10)

            # HTML block → reject
            if "<html" in r.text.lower():
                print(f"[{symbol}] HTML blocked @ {url}, retry {attempt}/8…")
                return None

            data = r.json()
            if "records" not in data:
                print(f"[{symbol}] No 'records' @ {url}, retry {attempt}/8…")
                return None

            return data

        except Exception:
            return None

    def get_chain(self, symbol):
        """Main fetch method with retries + fallback."""
        urls = [
            f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}",
            f"https://www.nseindia.com/api/option-chain?symbol={symbol}",
            f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}",
        ]

        self.refresh_cookies()

        for attempt in range(1, 9):
            for url in urls:
                data = self.fetch_json(url, symbol, attempt)
                if data:
                    print(f"[{symbol}] ✓ Data received successfully")
                    return data

            time.sleep(1.3)

        print(f"[{symbol}] ❌ FAILED after 8 attempts")
        return None


# ------------------------------
# Data Processing
# ------------------------------
def convert_to_dataframe(js, symbol):
    if js is None or "records" not in js:
        print(f"[{symbol}] Empty JSON → returning empty dataframe")
        return pd.DataFrame()

    rows = []
    for item in js["records"]["data"]:
        ce = item.get("CE", {})
        pe = item.get("PE", {})

        rows.append({
            "symbol": symbol,
            "strikePrice": item.get("strikePrice"),
            "expiryDate": item.get("expiryDate"),

            "ce_oi": ce.get("openInterest"),
            "ce_change_oi": ce.get("changeinOpenInterest"),
            "ce_ltp": ce.get("lastPrice"),

            "pe_oi": pe.get("openInterest"),
            "pe_change_oi": pe.get("changeinOpenInterest"),
            "pe_ltp": pe.get("lastPrice"),
        })

    df = pd.DataFrame(rows)
    return df


# ------------------------------
# Save results
# ------------------------------
def save_output(df, symbol):
    Path("output").mkdir(exist_ok=True)

    out_path = f"output/{symbol}_option_chain.csv"
    df.to_csv(out_path, index=False)

    print(f"[{symbol}] Saved → {out_path}")


# ------------------------------
# MAIN
# ------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["NIFTY", "BANKNIFTY"],
        help="Symbols to fetch"
    )
    args = parser.parse_args()

    session = NSESession()

    for sym in args.symbols:
        print(f"\n==============================")
        print(f"Fetching → {sym}")
        print("==============================")

        js = session.get_chain(sym)
        df = convert_to_dataframe(js, sym)

        if df.empty:
            print(f"[{sym}] ⚠ No data fetched")
            continue

        save_output(df, sym)
        print(df.head(10))


# ------------------------------
# ENTRY
# ------------------------------
if __name__ == "__main__":
    main()
    
