#!/usr/bin/env python3
"""
Test Bybit V5 order via RAW API to bypass CCXT create_market_order (e.g. None price conversion bug).
Run from project root: python debug_bybit_raw.py
"""
import os

from dotenv import load_dotenv

load_dotenv()

import ccxt

SYMBOL_CCXT = "XRP/USDT:USDT"
QTY = "20"


def main() -> None:
    print("=" * 60)
    print("Bybit V5 RAW API Debug")
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
                "defaultType": "linear",
                "enableUnifiedAccount": True,
            },
        }
    )

    # Step 1: Fetch Balance
    print("\n--- Step 1: Fetch USDT Balance ---")
    try:
        exchange.load_markets()
        balance = exchange.fetch_balance()
        usdt = balance.get("USDT") or balance.get("usdt") or {}
        if isinstance(usdt, dict):
            print(f"USDT: total={usdt.get('total')}, free={usdt.get('free')}")
        else:
            print(f"USDT raw: {usdt}")
    except Exception as e:
        print(f"Step 1 FAILED: {type(e).__name__}: {e}")

    # Step 2: Raw V5 Order — use market id for symbol
    print("\n--- Step 2: Raw V5 Market SELL (private_post_v5_order_create) ---")
    try:
        market = exchange.market(SYMBOL_CCXT)
        symbol_raw = market["id"]
        payload = {
            "category": "linear",
            "symbol": symbol_raw,
            "side": "Sell",
            "orderType": "Market",
            "qty": QTY,
            "positionIdx": 0,
        }
        print(f"Payload: {payload}")
        response = exchange.private_post_v5_order_create(payload)
        print("FULL RAW RESPONSE:")
        print(response)
    except Exception as e:
        print(f"Step 2 FAILED: {type(e).__name__}: {e}")

    print("\n" + "=" * 60)
    print("Debug script finished.")
    print("=" * 60)


if __name__ == "__main__":
    main()
