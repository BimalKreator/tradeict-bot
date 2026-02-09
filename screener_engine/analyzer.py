"""
Screener Engine: match symbols across KuCoin and Bybit, compute funding spread and direction.
Standard arbitrage: SHORT the exchange with the higher funding rate, LONG the lower.
"""
from __future__ import annotations

from typing import Any


def get_arbitrage_opportunities() -> list[dict[str, Any]]:
    """
    Fetch normalized market data (cached), match symbols on both exchanges, compute gross spread,
    recommended action, and next_funding_time. Returns full list; API filters/sorts/paginates.
    """
    from market_data_service.exchange import fetch_all_market_data

    data = fetch_all_market_data()
    kucoin_list = data.get("kucoin", {}).get("symbols") or []
    bybit_list = data.get("bybit", {}).get("symbols") or []

    kucoin_by_symbol = {row["symbol"]: row for row in kucoin_list}
    bybit_by_symbol = {row["symbol"]: row for row in bybit_list}
    common = set(kucoin_by_symbol) & set(bybit_by_symbol)

    results = []
    for symbol in sorted(common):
        kr = kucoin_by_symbol[symbol].get("funding_rate")
        br = bybit_by_symbol[symbol].get("funding_rate")
        if kr is None or br is None:
            continue
        kucoin_interval = kucoin_by_symbol[symbol].get("funding_interval")
        bybit_interval = bybit_by_symbol[symbol].get("funding_interval")
        print(f"[DEBUG] Checking {symbol}: KuCoin={kucoin_interval}h | Bybit={bybit_interval}h")
        # Strict interval matching: only pair when both have same funding interval (e.g. 8h vs 8h)
        if kucoin_interval is None or bybit_interval is None:
            print(f"⚠️ MISSING DATA (skipped): {symbol} — KuCoin interval={kucoin_interval!r}, Bybit interval={bybit_interval!r} (no valid futures contract or interval)")
            continue
        if kucoin_interval != bybit_interval:
            print(f"❌ MISMATCH: {symbol} (Skipping)")
            continue
        print(f"✅ MATCH: {symbol}")
        gross_spread = abs(kr - br)
        if kr > br:
            action = "KuCoin: Short / Bybit: Long"
        else:
            action = "KuCoin: Long / Bybit: Short"
        next_ft = bybit_by_symbol[symbol].get("next_funding_time") or kucoin_by_symbol[symbol].get("next_funding_time")
        results.append({
            "symbol": symbol,
            "kucoin_rate": kr,
            "bybit_rate": br,
            "kucoin_funding_interval": int(kucoin_interval),
            "bybit_funding_interval": int(bybit_interval),
            "gross_spread": gross_spread,
            "recommended_action": action,
            "next_funding_time": next_ft,
        })
    return results
