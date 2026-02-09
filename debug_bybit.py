#!/usr/bin/env python3
"""
Standalone script to diagnose Bybit connection and order execution.
Tests balance, margin mode (if supported), and a small market SELL.
Run from project root: python debug_bybit.py
"""
import os
import traceback

from dotenv import load_dotenv

load_dotenv()

import ccxt

# Use liquid pair; BEAT may not exist on Bybit
SYMBOL = "XRP/USDT:USDT"
SELL_QUANTITY = 20


def main() -> None:
    print("=" * 60)
    print("Bybit Debug Script")
    print("=" * 60)

    api_key = os.environ.get("BYBIT_API_KEY", "").strip()
    secret = os.environ.get("BYBIT_SECRET", "").strip()
    if not (api_key and secret):
        print("ERROR: Missing BYBIT_API_KEY or BYBIT_SECRET in .env")
        return

    exchange = ccxt.bybit(
        {
            "apiKey": api_key,
            "secret": secret,
            "options": {
                "defaultType": "swap",
                "enableUnifiedMargin": True,
                "enableUnifiedAccount": True,
            },
        }
    )

    # Step 1: USDT Balance
    print("\n--- Step 1: Fetch USDT Balance ---")
    try:
        balance = exchange.fetch_balance()
        usdt = balance.get("USDT") or balance.get("usdt") or {}
        if isinstance(usdt, dict):
            total = usdt.get("total")
            free = usdt.get("free")
            used = usdt.get("used")
            print(f"USDT Balance: total={total}, free={free}, used={used}")
        else:
            print(f"USDT raw: {usdt}")
    except Exception as e:
        print(f"Step 1 FAILED: {type(e).__name__}: {e}")

    # Step 2: Try Set Margin Mode to 'cross' (Unified Account may handle differently)
    print("\n--- Step 2: Try Set Margin Mode to 'cross' for " + SYMBOL + " ---")
    try:
        exchange.load_markets()
        result = exchange.set_margin_mode("cross", SYMBOL)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Step 2 (set_margin_mode) — may be expected on Unified Account: {type(e).__name__}: {e}")

    # Step 3: Market SELL (20 XRP) to test execution
    print("\n--- Step 3: Place Market SELL Order (" + str(SELL_QUANTITY) + " " + SYMBOL + ") ---")
    try:
        order = exchange.create_market_order(SYMBOL, "sell", SELL_QUANTITY, {})
        print("FULL RAW RESPONSE:")
        print(order)
        oid = order.get("id")
        print(f"\nOrder ID: {oid}")
    except Exception as e:
        print(f"Step 3 FAILED: {type(e).__name__}: {e}")
        print("FULL TRACEBACK (exact line failing):")
        print(traceback.format_exc())
        print("\nAttempting to fetch open orders to see if it actually worked...")
        try:
            open_orders = exchange.fetch_open_orders(SYMBOL)
            print(f"Open orders for {SYMBOL}: {open_orders}")
        except Exception as e2:
            print(f"fetch_open_orders also failed: {type(e2).__name__}: {e2}")

    print("\n" + "=" * 60)
    print("Debug script finished.")
    print("=" * 60)


if __name__ == "__main__":
    main()
