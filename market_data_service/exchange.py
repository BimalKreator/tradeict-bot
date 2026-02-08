"""
Exchange market data via CCXT (public endpoints only).
Fetches USDT perpetuals: symbol, funding rate, next funding time.
Normalizes symbols to BASE/USDT (e.g. BTC/USDT).
"""
import logging
from datetime import datetime, timezone
from typing import Any

import ccxt

logger = logging.getLogger(__name__)

# Normalize CCXT symbol (e.g. "BTC/USDT:USDT" or "XBT/USDT:USDT") to "BTC/USDT" / "XBT/USDT"
def _normalize_symbol(ccxt_symbol: str) -> str:
    if "/" not in ccxt_symbol:
        return ccxt_symbol
    base_quote = ccxt_symbol.split(":")[0]
    return base_quote.strip()


def _row(data: dict, raw_symbol: str) -> dict[str, Any]:
    """Build one result row from CCXT funding data."""
    normalized = _normalize_symbol(raw_symbol)
    next_ts = data.get("fundingTimestamp") or data.get("fundingRateTimestamp") or data.get("timestamp")
    next_funding_time = None
    if next_ts:
        next_funding_time = datetime.fromtimestamp(next_ts / 1000.0, tz=timezone.utc).isoformat()
    return {
        "symbol": normalized,
        "funding_rate": data.get("fundingRate"),
        "next_funding_time": next_funding_time,
    }


def _get_usdt_perp_symbols(exchange: ccxt.Exchange) -> list[str]:
    """Filter exchange markets to USDT-margined perpetuals."""
    usdt_perp_symbols = []
    for s, m in exchange.markets.items():
        if m.get("linear") and m.get("quote") == "USDT" and m.get("type") in ("swap", "future"):
            usdt_perp_symbols.append(s)
    if not usdt_perp_symbols:
        usdt_perp_symbols = [s for s in exchange.symbols if "/USDT" in s and ":USDT" in s]
    return usdt_perp_symbols


# Max symbols to fetch per exchange when bulk API is not available (avoids timeouts/rate limits)
KUCOIN_SYMBOL_LIMIT = 100

def _fetch_kucoin() -> list[dict[str, Any]]:
    """KuCoin futures: no fetch_funding_rates(), use fetch_funding_rate() per symbol (capped)."""
    import time
    exchange = ccxt.kucoinfutures()
    exchange.load_markets()
    symbols = _get_usdt_perp_symbols(exchange)[:KUCOIN_SYMBOL_LIMIT]
    result = []
    for sym in symbols:
        try:
            data = exchange.fetch_funding_rate(sym)
            result.append(_row(data, sym))
            time.sleep(0.05)  # avoid rate limit
        except Exception as e:
            logger.warning("Skip %s: %s", sym, e)
    return result


def _fetch_bybit() -> list[dict[str, Any]]:
    """Bybit: supports fetch_funding_rates() for all symbols."""
    exchange = ccxt.bybit({"options": {"defaultType": "linear"}})
    exchange.load_markets()
    symbols = _get_usdt_perp_symbols(exchange)
    funding = exchange.fetch_funding_rates(symbols)
    return [_row(data, raw_symbol) for raw_symbol, data in funding.items()]


def _fetch_usdt_perpetuals_for_exchange(exchange_id: str) -> list[dict[str, Any]]:
    """
    Load exchange, filter to USDT perpetuals, fetch funding rates.
    Returns list of { symbol, funding_rate, next_funding_time } with normalized symbols.
    Raises on failure (caller should catch to allow other exchange to still work).
    """
    if exchange_id == "kucoin":
        return _fetch_kucoin()
    if exchange_id == "bybit":
        return _fetch_bybit()
    raise ValueError(f"Unknown exchange: {exchange_id}")


def fetch_all_market_data() -> dict[str, Any]:
    """
    Fetch USDT perpetual data from KuCoin and Bybit.
    If one exchange fails, the other still returns data; failed exchange has error message and zero count.
    """
    summary: dict[str, Any] = {
        "kucoin": {"symbols_count": 0, "symbols": [], "error": None},
        "bybit": {"symbols_count": 0, "symbols": [], "error": None},
    }

    for exchange_id in ("kucoin", "bybit"):
        try:
            data = _fetch_usdt_perpetuals_for_exchange(exchange_id)
            summary[exchange_id]["symbols"] = data
            summary[exchange_id]["symbols_count"] = len(data)
        except Exception as e:
            logger.exception("Market data fetch failed for %s", exchange_id)
            summary[exchange_id]["error"] = str(e)
            summary[exchange_id]["symbols_count"] = 0
            summary[exchange_id]["symbols"] = []

    return summary
