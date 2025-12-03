"""
NSE fetcher utilities.

Provides:
- fetch_option_chain(symbol)  -> dict
- get_quote(symbol)           -> dict
- get_futures(symbol)         -> dict
"""

from __future__ import annotations
import requests
import time
import json
import datetime
from pathlib import Path
from typing import Dict, Any, Optional

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
}

# -----------------------------------------
# Internal helpers
# -----------------------------------------
def create_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)
    return s


def safe_get(session: requests.Session, url: str, params=None, timeout=12):
    for _ in range(4):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r
        except:
            pass
        time.sleep(0.6)
    raise RuntimeError(f"Failed GET {url}")


# -----------------------------------------
# OPTION CHAIN
# -----------------------------------------
def fetch_option_chain(symbol: str) -> Dict:
    """
    NSE Option-chain fetcher.
    Only works for indices: NIFTY, BANKNIFTY
    """
    s = create_session()
    try:
        s.get("https://www.nseindia.com", timeout=8)
    except:
        pass

    url = "https://www.nseindia.com/api/option-chain-indices"
    return safe_get(s, url, {"symbol": symbol}).json()


# -----------------------------------------
# QUOTE EXTRACTOR (fixed)
# -----------------------------------------
def get_quote(symbol: str = "NIFTY") -> Dict:
    """
    Clean quote wrapper (this is what your main.py uses)
    Returns:
        - underlying price
        - raw OC JSON
    """

    try:
        oc = fetch_option_chain(symbol)

        underlying = (
            oc.get("records", {}).get("underlyingValue")
            or oc.get("underlyingValue")
            or 0
        )

        return {
            "symbol": symbol,
            "underlying": underlying,
            "priceInfo": {"lastPrice": underlying},
            "raw": oc
        }

    except Exception as e:
        return {"symbol": symbol, "error": str(e)}


# -----------------------------------------
# FUTURES (placeholder)
# -----------------------------------------
def get_futures(symbol: str) -> Dict:
    return fetch_option_chain(symbol)
    
