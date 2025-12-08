#!/usr/bin/env python3
"""
examples/real_fetch_example.py

Robust NSE option-chain fetch example.

- Retries with polite headers and initial homepage GET to get cookies.
- Detects HTML responses (NSE anti-bot) and retries.
- Handles JSON responses that may lack "records" gracefully.
- Flattens each option-chain row into CE/PE lines and saves CSV per symbol and an aggregate CSV.

Usage:
    python examples/real_fetch_example.py --symbols NIFTY BANKNIFTY
"""

import requests
import time
import json
import argparse
from pathlib import Path
import pandas as pd
from datetime import datetime

# -----------------------
# Config
# -----------------------
DEFAULT_SYMBOLS = ["NIFTY", "BANKNIFTY"]
OUT_DIR = Path("data/option_chain_raw")
REQUEST_TIMEOUT = 15
MAX_RETRIES = 6
RETRY_DELAY = 2.0  # seconds (backoff will multiply this)
SESSION_PAUSE = 0.6  # pause after initial homepage GET

# NSE endpoints to try (indices first then equities)
ENDPOINTS = [
    "https://www.nseindia.com/api/option-chain-indices?symbol={symbol}",
    "https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

# -----------------------
# Helpers
# -----------------------
def polite_session():
    """
    Returns a requests.Session() that has first hit the homepage to create cookies.
    """
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        # a quick homepage GET to generate cookies and headers NSE expects
        resp = s.get("https://www.nseindia.com", timeout=REQUEST_TIMEOUT)
        # ignore resp content; we only need cookies & tone headers
        time.sleep(SESSION_PAUSE)
    except Exception:
        # If homepage fails, session is still useful (we continue and let retries handle it)
        pass
    return s


def is_html_response(resp_text: str) -> bool:
    """Detect if the response seems to be HTML (i.e., NSE blocked request)."""
    t = resp_text.strip().lower()
    return t.startswith("<!doctype") or t.startswith("<html") or "<html" in t


def fetch_snapshot(symbol: str, max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY):
    """
    Fetch option-chain snapshot for a given symbol.
    Tries multiple endpoints (indices/equities). Retries politely on HTML/empty responses.
    Returns parsed JSON 'records' dict OR None on permanent failure.
    """

    session = polite_session()

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        # cycle endpoints (indices first)
        for ep in ENDPOINTS:
            url = ep.format(symbol=symbol)
            try:
                r = session.get(url, timeout=REQUEST_TIMEOUT)
            except Exception as e:
                print(f"[{symbol}] Request error (attempt {attempt}) -> {e}")
                time.sleep(retry_delay * attempt)
                continue

            text = r.text or ""
            # if HTML returned -> likely blocked, retry
            if is_html_response(text):
                print(f"[{symbol}] NSE returned HTML (blocked or captcha page). "
                      f"Attempt {attempt}/{max_retries}. Retrying after backoff...")
                time.sleep(retry_delay * attempt)
                continue

            # attempt to parse JSON
            try:
                data = r.json()
            except Exception as e:
                print(f"[{symbol}] Failed to decode JSON (attempt {attempt}) -> {e}")
                time.sleep(retry_delay * attempt)
                continue

            # Validate structure
            # Typical good response: { "records": { "data": [...], "underlyingValue": ... }, ... }
            if not isinstance(data, dict):
                print(f"[{symbol}] Unexpected response type; retrying (attempt {attempt}).")
                time.sleep(retry_delay * attempt)
                continue

            # Some NSE formats put option chain under top-level 'records'
            if "records" in data and isinstance(data["records"], dict) and "data" in data["records"]:
                print(f"[{symbol}] Snapshot retrieved from endpoint: {url}")
                return data  # return full JSON so caller can extract what it needs

            # Other providers or changed formats could use different keys; try some fallbacks
            # If data has 'filtered' or 'data' directly, attempt to accept it.
            if "filtered" in data and isinstance(data["filtered"], dict) and "data" in data["filtered"]:
                print(f"[{symbol}] Snapshot retrieved using 'filtered' key from endpoint: {url}")
                return {"records": {"data": data["filtered"]["data"], "underlyingValue": data.get("underlyingValue")}}

            if "data" in data and isinstance(data["data"], list):
                print(f"[{symbol}] Snapshot retrieved using top-level 'data' key from endpoint: {url}")
                return {"records": {"data": data["data"], "underlyingValue": data.get("underlyingValue")}}

            # Not acceptable, retry
            print(f"[{symbol}] JSON did not contain expected keys (attempt {attempt}). Retrying...")
            time.sleep(retry_delay * attempt)

        # end for endpoints loop
    # out of attempts
    print(f"[{symbol}] Failed to fetch snapshot after {max_retries} attempts.")
    return None


def flatten_records_to_df(snapshot_json, symbol):
    """
    Flatten NSE 'records' -> 'data' list into a table with one row per option side (CE/PE).
    Returns a pandas.DataFrame.
    """
    if snapshot_json is None:
        return pd.DataFrame()

    records = snapshot_json.get("records", {}).get("data", None)
    underlying_value = snapshot_json.get("records", {}).get("underlyingValue", None)

    if records is None:
        return pd.DataFrame()

    rows = []
    for r in records:
        # strike price key may be 'strikePrice' or 'strike'
        strike = r.get("strikePrice") if r.get("strikePrice") is not None else r.get("strike")
        expiry = r.get("expiryDate") or r.get("expiry") or None

        # flattened CE & PE objects (when present)
        for opt_side in ("CE", "PE"):
            opt = r.get(opt_side)
            if not opt:
                continue

            row = {
                "symbol": symbol,
                "underlyingValue": underlying_value,
                "expiry": opt.get("expiryDate") or expiry or opt.get("expiry"),
                "strike": strike,
                "option_type": "CALL" if opt_side == "CE" else "PUT",
                # common fields found in NSE option object
                "open_interest": opt.get("openInterest") or opt.get("openInterest"),
                "change_in_oi": opt.get("changeinOpenInterest") or opt.get("changeInOpenInterest") or opt.get("changeInOI"),
                "volume": opt.get("totalTradedVolume") or opt.get("totalTradedVolume"),
                "ltp": opt.get("lastPrice") or opt.get("lastTradedPrice") or opt.get("last"),
                "bidprice": opt.get("bidprice"),
                "askprice": opt.get("askPrice") or opt.get("askprice"),
                "iv": opt.get("impliedVolatility") or opt.get("impliedVolatility"),
                "underlying": r.get("underlying") or snapshot_json.get("records", {}).get("underlying") or symbol,
                # raw JSON for debugging if needed (avoid huge payloads by not storing always)
            }
            rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # Normalize numeric columns
    for col in ("open_interest", "change_in_oi", "volume", "ltp", "bidprice", "askprice", "iv"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# -----------------------
# Processing helpers
# -----------------------
def process_symbol(symbol: str, out_dir: Path):
    """
    1) Fetch snapshot JSON
    2) Flatten to DataFrame
    3) Save CSV per symbol and return DataFrame
    """
    print(f"\nFetching → {symbol}")
    snapshot = fetch_snapshot(symbol)
    if snapshot is None:
        print(f"[{symbol}] No snapshot available (NSE may be blocking).")
        return None

    df = flatten_records_to_df(snapshot, symbol)
    if df.empty:
        print(f"[{symbol}] Flattened dataframe is empty — nothing to save.")
        return None

    # save with timestamped filename
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{symbol}_option_chain_{date_str}.csv"
    df.to_csv(out_path, index=False)
    print(f"[{symbol}] Saved option chain CSV: {out_path}  (rows: {len(df)})")
    return df


# -----------------------
# Main
# -----------------------
def main(symbols):
    OUT_DIR = Path("data/option_chain_raw")
    combined = []

    for sym in symbols:
        df = process_symbol(sym, OUT_DIR)
        if df is not None and not df.empty:
            combined.append(df)

    if combined:
        full = pd.concat(combined, ignore_index=True)
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        combined_path = OUT_DIR / f"option_chains_combined_{date_str}.csv"
        full.to_csv(combined_path, index=False)
        print(f"\nCombined CSV saved: {combined_path} (total rows: {len(full)})")
    else:
        print("\nNo option-chain data was saved for the requested symbols.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NSE option chain snapshots (robust).")
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS,
                        help="Space separated list of symbols to fetch (default: NIFTY BANKNIFTY)")
    args = parser.parse_args()

    main(args.symbols)
            
