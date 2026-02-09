#!/usr/bin/env python3
"""
Test KuCoin Cross Margin with a CLEAN order: no marginMode or leverage in the order payload.
Account settings are set via API first; order relies on those settings only.
Run from project root: python debug_cross_clean.py
"""
import os
import time

from dotenv import load_dotenv

load_dotenv()

import ccxt

SYMBOL = "BEAT/USDT:USDT"
QUANTITY = 20


def main() -> None:
    print("=" * 60)
    print("KuCoin Cross Margin — Clean Order (no margin params in order)")
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

    # Step 1: Set Margin Mode to 'CROSS' (Uppercase)
    print("\n--- Step 1: Set Margin Mode to 'CROSS' ---")
    try:
        exchange.load_markets()
        result = exchange.set_margin_mode("CROSS", SYMBOL)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Step 1 FAILED: {type(e).__name__}: {e}")

    # Step 2: Set Leverage to 1 (Cross mode supports this; no params in call)
    print("\n--- Step 2: Set Leverage to 1 ---")
    try:
        result = exchange.set_leverage(1, SYMBOL)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Step 2 FAILED: {type(e).__name__}: {e}")

    # Step 3: Sync wait (crucial for backend)
    print("\n--- Step 3: Sleeping 3 seconds for backend sync... ---")
    time.sleep(3)

    # Step 4: Clean Order — params={}, NO marginMode or leverage in payload
    print("\n--- Step 4: Place Market BUY (Qty 20) — CLEAN ORDER params={} ---")
    params = {}
    print(f"Params sent with order: {params} (must be empty)")
    try:
        order = exchange.create_market_order(SYMBOL, "buy", QUANTITY, params)
        print("FULL RAW RESPONSE:")
        print(order)
        oid = order.get("id")
        print(f"\n✅ SUCCESS — Order ID: {oid}")
    except Exception as e:
        print(f"Step 4 FAILED: {type(e).__name__}: {e}")

    print("\n" + "=" * 60)
    print("Debug script finished.")
    print("=" * 60)


if __name__ == "__main__":
    main()
