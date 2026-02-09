#!/usr/bin/env python3
"""
Standalone script to verify KuCoin Futures connection and place a test market order.
Run from project root: python debug_kucoin.py
"""
import os
import time

from dotenv import load_dotenv

load_dotenv()

import ccxt

SYMBOL = "BEAT/USDT:USDT"
QUANTITY = 20  # tokens, ~1 USDT at low price


def main() -> None:
    print("=" * 60)
    print("KuCoin Futures Debug Script")
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

    # Step 1: USDT Balance
    print("\n--- Step 1: Fetch USDT Balance ---")
    try:
        balance = exchange.fetch_balance()
        usdt = balance.get("USDT") or balance.get("usdt") or {}
        if isinstance(usdt, dict):
            total = usdt.get("total")
            free = usdt.get("free")
            print(f"USDT Balance: total={total}, free={free}")
        else:
            print(f"USDT raw: {usdt}")
    except Exception as e:
        print(f"Step 1 FAILED: {type(e).__name__}: {e}")

    # Step 2: Set Margin Mode to 'CROSS' (UPPERCASE — KuCoin requires it per ccxt#25592)
    print("\n--- Step 2: Set Margin Mode to 'CROSS' for " + SYMBOL + " ---")
    try:
        exchange.load_markets()
        result = exchange.set_margin_mode("CROSS", SYMBOL)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Step 2 FAILED: {type(e).__name__}: {e}")

    # Step 3: Set Leverage to 1x with marginMode 'CROSS' (UPPERCASE)
    print("\n--- Step 3: Set Leverage to 1x for " + SYMBOL + " ---")
    try:
        result = exchange.set_leverage(1, SYMBOL, params={"marginMode": "CROSS"})
        print("✅ Leverage Set Success (Cross mode active)")
    except Exception as e:
        print(f"Step 3 FAILED: {type(e).__name__}: {e}")

    print("\n--- Sleeping 3 seconds to allow KuCoin backend to sync... ---")
    time.sleep(3)

    # Step 4: Market Buy Order (Qty 20) — params={'marginMode': 'CROSS'} (UPPERCASE)
    print("\n--- Step 4: Place Market BUY Order (" + str(QUANTITY) + " " + SYMBOL + ") — params={'marginMode': 'CROSS'} ---")
    try:
        params = {"marginMode": "CROSS"}
        order = exchange.create_market_order(SYMBOL, "buy", QUANTITY, params)
        print("FULL RAW RESPONSE:")
        print(order)
        oid = order.get("id")
        print(f"\n✅ SUCCESS — Order ID: {oid}")
        print("\n--- Key fields ---")
        print(f"  id: {order.get('id')}")
        print(f"  status: {order.get('status')}")
        print(f"  symbol: {order.get('symbol')}")
        print(f"  side: {order.get('side')}")
        print(f"  amount: {order.get('amount')}")
    except Exception as e:
        print(f"Step 4 FAILED: {type(e).__name__}: {e}")

    print("\n" + "=" * 60)
    print("Debug script finished.")
    print("=" * 60)


if __name__ == "__main__":
    main()
