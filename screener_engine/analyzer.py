"""
Screener Engine: match symbols across KuCoin and Bybit, compute funding spread and direction.
Standard arbitrage: SHORT the exchange with the higher funding rate, LONG the lower.
"""
from __future__ import annotations

from typing import Any


def get_screener_results() -> list[dict[str, Any]]:
    """
    Fetch normalized market data, match symbols on both exchanges, compute gross spread
    and recommended action. Returns list of opportunities (unsorted; API sorts by spread).
    """
    from market_data_service.exchange import fetch_all_market_data

    data = fetch_all_market_data()
    kucoin_list = data.get("kucoin", {}).get("symbols") or []
    bybit_list = data.get("bybit", {}).get("symbols") or []

    kucoin_by_symbol = {row["symbol"]: row for row in kucoin_list}
    bybit_by_symbol = {row["symbol"]: row for row in bybit_list}
    common = set(kucoin_by_symbol) & set(bybit_by_symbol)

    results = []
    for symbol in common:
        kr = kucoin_by_symbol[symbol].get("funding_rate")
        br = bybit_by_symbol[symbol].get("funding_rate")
        if kr is None or br is None:
            continue
        gross_spread = abs(kr - br)
        # Standard arbitrage: short the higher rate, long the lower rate
        if kr > br:
            action = "KuCoin: Short / Bybit: Long"
        else:
            action = "KuCoin: Long / Bybit: Short"
        results.append({
            "symbol": symbol,
            "kucoin_rate": kr,
            "bybit_rate": br,
            "gross_spread": gross_spread,
            "recommended_action": action,
        })
    return results
