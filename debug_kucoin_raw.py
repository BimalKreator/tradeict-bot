#!/usr/bin/env python3
"""
Test KuCoin Futures Cross Mode using a RAW API payload.
Uses standard ccxt for set_margin_mode/set_leverage, then private_post_orders
with a manually built JSON body so marginMode is sent exactly as "CROSS".
Run from project root: python debug_kucoin_raw.py
"""
import os
import time
import uuid

from dotenv import load_dotenv

load_dotenv()

import ccxt

SYMBOL_CCXT = "BEAT/USDT:USDT"
SYMBOL_MARKET_ID = "BEATUSDTM"
SIZE = 20


def main() -> None:
    print("=" * 60)
    print("KuCoin Futures RAW API Debug (Cross Mode)")
    print("=" * 60)

    api_key = os.environ.get("KUCOIN_API_KEY", "").strip()
    secret = os.environ.get("KUCOIN_SECRET", "").strip()
    passphrase = os.environ.get("KUCOIN_PASSPHRASE", "").strip()
    if not (api_key and secret and passphrase):
        print("ERROR: Missing KUCOIN_API_KEY, KUCOIN_SECRET, or KUCOIN_PASSPHRASE in .env")
        return

    exchange = ccxt.kucoinfutures(
        {
            "apiKey": api_key,
            "secret": secret,
            "password": passphrase,
            "options": {"defaultType": "swap"},
        }
    )

    # Step 1: Set Margin Mode to 'cross' (lowercase) via standard ccxt
    print("\n--- Step 1: Set Margin Mode to 'cross' ---")
    try:
        exchange.load_markets()
        result = exchange.set_margin_mode("cross", SYMBOL_CCXT)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Step 1 FAILED: {type(e).__name__}: {e}")

    # Step 2: Set Leverage to 1 (lowercase params) via standard ccxt
    print("\n--- Step 2: Set Leverage to 1 ---")
    try:
        result = exchange.set_leverage(1, SYMBOL_CCXT, params={"marginMode": "cross"})
        print(f"Result: {result}")
    except Exception as e:
        print(f"Step 2 FAILED: {type(e).__name__}: {e}")

    print("\n--- Sleeping 3 seconds to allow backend to sync... ---")
    time.sleep(3)

    # Step 3 (THE FIX): Place Market Order via RAW payload with marginMode "CROSS" (uppercase)
    print("\n--- Step 3: Place Market Order (RAW) — marginMode 'CROSS' in payload ---")
    try:
        client_oid = str(uuid.uuid4()).replace("-", "")
        payload = {
            "clientOid": client_oid,
            "side": "buy",
            "symbol": SYMBOL_MARKET_ID,
            "type": "market",
            "leverage": 1,
            "size": SIZE,
            "marginMode": "CROSS",
        }
        print(f"Payload: {payload}")
        response = exchange.private_post_orders(payload)
        print("FULL RAW RESPONSE:")
        print(response)
    except Exception as e:
        print(f"Step 3 FAILED: {type(e).__name__}: {e}")

    print("\n" + "=" * 60)
    print("Debug script finished.")
    print("=" * 60)


if __name__ == "__main__":
    main()
