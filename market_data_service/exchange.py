"""
Exchange market data via CCXT. Uses API keys from .env when set for private data (balances).
Fetches USDT perpetuals: symbol, funding rate, next funding time, min_qty, lot_size.
Normalizes symbols to BASE/USDT (e.g. BTC/USDT).
"""
import logging
import os
from datetime import datetime, timezone
from typing import Any

import ccxt

logger = logging.getLogger(__name__)

# Options for proper Futures/Swap handling
DEFAULT_OPTIONS = {"defaultType": "swap"}


def _get_kucoin_config() -> dict[str, Any]:
    """Build KuCoin Futures config from env. Uses API keys if set."""
    config: dict[str, Any] = {"options": {**DEFAULT_OPTIONS}}
    api_key = os.environ.get("KUCOIN_API_KEY", "").strip()
    secret = os.environ.get("KUCOIN_SECRET", "").strip()
    passphrase = os.environ.get("KUCOIN_PASSPHRASE", "").strip()
    if api_key and secret and passphrase:
        config["apiKey"] = api_key
        config["secret"] = secret
        config["password"] = passphrase
    return config


def _get_bybit_config() -> dict[str, Any]:
    """Build Bybit config from env (linear = USDT perpetual). Uses API keys if set."""
    config: dict[str, Any] = {"options": {"defaultType": "linear"}}
    api_key = os.environ.get("BYBIT_API_KEY", "").strip()
    secret = os.environ.get("BYBIT_SECRET", "").strip()
    if api_key and secret:
        config["apiKey"] = api_key
        config["secret"] = secret
    return config

# Normalize CCXT symbol (e.g. "BTC/USDT:USDT" or "XBT/USDT:USDT") to "BTC/USDT" / "XBT/USDT"
def _normalize_symbol(ccxt_symbol: str) -> str:
    if "/" not in ccxt_symbol:
        return ccxt_symbol
    base_quote = ccxt_symbol.split(":")[0]
    return base_quote.strip()


def _row(data: dict, raw_symbol: str) -> dict[str, Any]:
    """Build one result row from CCXT funding data. funding_interval in hours (int) or None if unknown."""
    normalized = _normalize_symbol(raw_symbol)
    next_ts = data.get("fundingTimestamp") or data.get("fundingRateTimestamp") or data.get("timestamp")
    next_funding_time = None
    if next_ts:
        next_funding_time = datetime.fromtimestamp(next_ts / 1000.0, tz=timezone.utc).isoformat()

    # Funding interval in hours: (nextFundingTime - timestamp) / 3600000. No default; use None if unknown.
    funding_interval: int | None = None
    ts = data.get("timestamp")
    if next_ts is not None and ts is not None and next_ts > ts:
        raw_h = (next_ts - ts) / 3600000.0
        if raw_h > 0:
            funding_interval = int(round(raw_h))
            if funding_interval < 1:
                funding_interval = 1

    return {
        "symbol": normalized,
        "funding_rate": data.get("fundingRate"),
        "next_funding_time": next_funding_time,
        "funding_interval": funding_interval,
    }


def _enrich_contract_specs(
    row: dict[str, Any],
    exchange: ccxt.Exchange,
    raw_symbol: str,
    exchange_id: str,
) -> None:
    """Add min_qty, lot_size, and funding_interval from exchange market (in-place)."""
    m = exchange.markets.get(raw_symbol)
    if not m:
        row["min_qty"] = None
        row["lot_size"] = None
        return
    limits = m.get("limits") or {}
    amount_limits = limits.get("amount") or {}
    precision = m.get("precision") or {}
    row["min_qty"] = amount_limits.get("min")
    row["lot_size"] = precision.get("amount") or amount_limits.get("min")

    # Funding interval from market['info']: KuCoin = seconds, Bybit V5 = minutes
    info = m.get("info") or {}
    if isinstance(info, dict):
        raw_interval = info.get("fundingInterval")
        if raw_interval is not None:
            try:
                val = int(float(raw_interval))
                if exchange_id.lower() == "kucoin":
                    # KuCoin: fundingInterval is in seconds (e.g. 28800 -> 8h)
                    interval_hours = val / 3600
                else:
                    # Bybit V5: fundingInterval is in minutes (e.g. 480 -> 8h)
                    interval_hours = val / 60
                if interval_hours >= 1:
                    row["funding_interval"] = int(interval_hours)
            except (TypeError, ValueError):
                pass
    # If still None, _row() may have set it from timestamp math; otherwise stays None


def _get_usdt_perp_symbols(exchange: ccxt.Exchange) -> list[str]:
    """
    Strict filter: only USDT-margined perpetual SWAP contracts (no spot, no delivery futures).
    Ensures we do not include spot pairs like API3/USDT.
    """
    usdt_perp_symbols = []
    markets = getattr(exchange, "markets", None) or {}
    for s, m in markets.items():
        if not isinstance(m, dict):
            continue
        # Must be perpetual swap, USDT-margined (linear=True), quote USDT, active
        is_swap = m.get("swap") is True or m.get("type") in ("swap", "future")
        if (
            is_swap
            and m.get("linear") is True
            and m.get("quote") == "USDT"
            and m.get("active", True) is not False
        ):
            usdt_perp_symbols.append(s)
    return usdt_perp_symbols


# Max symbols to fetch per exchange when bulk API is not available (avoids timeouts/rate limits)
KUCOIN_SYMBOL_LIMIT = 100

def _fetch_kucoin() -> list[dict[str, Any]]:
    """KuCoin futures: no fetch_funding_rates(), use fetch_funding_rate() per symbol (capped)."""
    import time
    exchange = ccxt.kucoinfutures(_get_kucoin_config())
    exchange.load_markets()
    symbols = _get_usdt_perp_symbols(exchange)[:KUCOIN_SYMBOL_LIMIT]
    result = []
    for sym in symbols:
        try:
            data = exchange.fetch_funding_rate(sym)
            row = _row(data, sym)
            _enrich_contract_specs(row, exchange, sym, "kucoin")
            result.append(row)
            iv = row.get("funding_interval")
            print(f"{row['symbol']} -> KuCoin Interval: {iv}h" if iv is not None else f"{row['symbol']} -> KuCoin Interval: None")
            time.sleep(0.05)  # avoid rate limit
        except Exception as e:
            logger.warning("Skip %s: %s", sym, e)
    return result


def _fetch_bybit() -> list[dict[str, Any]]:
    """Bybit: supports fetch_funding_rates() for all symbols."""
    exchange = ccxt.bybit(_get_bybit_config())
    exchange.load_markets()
    symbols = _get_usdt_perp_symbols(exchange)
    funding = exchange.fetch_funding_rates(symbols)
    result = []
    for raw_symbol, data in funding.items():
        row = _row(data, raw_symbol)
        _enrich_contract_specs(row, exchange, raw_symbol, "bybit")
        result.append(row)
        iv = row.get("funding_interval")
        print(f"{row['symbol']} -> Bybit Interval: {iv}h" if iv is not None else f"{row['symbol']} -> Bybit Interval: None")
    return result


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


# In-memory cache so /api/screener and pagination don't re-fetch from exchanges every request
_MARKET_DATA_CACHE: dict[str, Any] | None = None
_MARKET_DATA_CACHE_TS: float = 0
MARKET_DATA_CACHE_TTL_SECONDS = 60


def fetch_all_market_data() -> dict[str, Any]:
    """
    Fetch USDT perpetual data from KuCoin and Bybit.
    If one exchange fails, the other still returns data; failed exchange has error message and zero count.
    Uses in-memory cache (TTL) so repeated calls (e.g. pagination) don't hit exchanges.
    """
    import time
    global _MARKET_DATA_CACHE, _MARKET_DATA_CACHE_TS
    now = time.monotonic()
    if _MARKET_DATA_CACHE is not None and (now - _MARKET_DATA_CACHE_TS) < MARKET_DATA_CACHE_TTL_SECONDS:
        return _MARKET_DATA_CACHE

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

    _MARKET_DATA_CACHE = summary
    _MARKET_DATA_CACHE_TS = now
    return summary


def get_mark_prices_for_symbol(normalized_symbol: str) -> dict[str, Any]:
    """
    Fetch latest mark/last price for a symbol from KuCoin and Bybit (public).
    normalized_symbol e.g. "BTC/USDT". Returns { kucoin_price, bybit_price } (None if fetch failed).
    """
    # CCXT futures symbol format
    perp_symbol = normalized_symbol if ":USDT" in normalized_symbol else f"{normalized_symbol}:USDT"
    result: dict[str, Any] = {"kucoin_price": None, "bybit_price": None}

    for exchange_id, key in (("kucoin", "kucoin_price"), ("bybit", "bybit_price")):
        try:
            if exchange_id == "kucoin":
                ex = ccxt.kucoinfutures(_get_kucoin_config())
            else:
                ex = ccxt.bybit(_get_bybit_config())
            ex.load_markets()
            if perp_symbol not in ex.markets:
                # Try to find a matching symbol (e.g. XBT vs BTC)
                alt = next((s for s in ex.markets if _normalize_symbol(s) == normalized_symbol), None)
                if alt is None:
                    continue
                perp_symbol_use = alt
            else:
                perp_symbol_use = perp_symbol
            ticker = ex.fetch_ticker(perp_symbol_use)
            # Prefer mark price for futures; fallback to last
            price = ticker.get("mark") or ticker.get("last")
            if price is not None:
                result[key] = float(price)
        except Exception as e:
            logger.warning("get_mark_prices %s %s: %s", exchange_id, normalized_symbol, e)
    return result


def get_wallet_balance(exchange_name: str) -> dict[str, Any]:
    """
    Fetch wallet balance (Unified/Futures) for the given exchange.
    Returns: total_wallet_balance, available_balance, unrealized_pnl (USDT).
    If keys are missing or auth fails, returns zeros and optional error message (no exception).
    """
    result: dict[str, Any] = {
        "total_wallet_balance": 0.0,
        "available_balance": 0.0,
        "unrealized_pnl": 0.0,
        "error": None,
    }
    try:
        if exchange_name.lower() == "kucoin":
            config = _get_kucoin_config()
            if not config.get("apiKey"):
                result["error"] = "API keys not configured"
                return result
            exchange = ccxt.kucoinfutures(config)
        elif exchange_name.lower() == "bybit":
            config = _get_bybit_config()
            if not config.get("apiKey"):
                result["error"] = "API keys not configured"
                return result
            exchange = ccxt.bybit(config)
        else:
            result["error"] = f"Unknown exchange: {exchange_name}"
            return result

        balance = exchange.fetch_balance()
        # CCXT: balance['USDT'] has 'total', 'free', 'used'; futures may have 'info' with unrealizedPnl
        usdt = balance.get("USDT") or balance.get("usdt") or {}
        if isinstance(usdt, dict):
            result["total_wallet_balance"] = float(usdt.get("total") or 0)
            result["available_balance"] = float(usdt.get("free") or 0)
            result["unrealized_pnl"] = float(usdt.get("unrealizedPnl") or usdt.get("unrealized_pnl") or 0)
        # Some exchanges put unrealized PnL in balance.info
        info = balance.get("info") or {}
        if isinstance(info, dict):
            upnl = info.get("unrealisedPnl") or info.get("unrealizedPnl")
            if upnl is not None:
                result["unrealized_pnl"] = float(upnl)
    except Exception as e:
        logger.warning("get_wallet_balance %s: %s", exchange_name, e)
        result["error"] = str(e)
        result["total_wallet_balance"] = 0.0
        result["available_balance"] = 0.0
        result["unrealized_pnl"] = 0.0
    return result


def _perp_symbol(exchange: ccxt.Exchange, normalized_symbol: str) -> str | None:
    """Return CCXT perp symbol (e.g. BTC/USDT:USDT) for the exchange, or None if not found."""
    perp = f"{normalized_symbol}:USDT" if ":USDT" not in normalized_symbol else normalized_symbol
    if perp in exchange.markets:
        return perp
    alt = next((s for s in exchange.markets if _normalize_symbol(s) == normalized_symbol), None)
    return alt


def place_market_order(
    exchange_name: str,
    normalized_symbol: str,
    side: str,
    amount_base: float,
    leverage: int,
) -> dict[str, Any]:
    """
    Place a market order on the given exchange (futures).
    side: 'buy' or 'sell'. amount_base: order size in tokens (base currency), passed directly to ccxt.
    Returns { "success": bool, "error": str | None, "order_id": str | None }.
    """
    result: dict[str, Any] = {"success": False, "error": None, "order_id": None}
    try:
        if exchange_name.lower() == "kucoin":
            exchange = ccxt.kucoinfutures(_get_kucoin_config())
        elif exchange_name.lower() == "bybit":
            exchange = ccxt.bybit(_get_bybit_config())
        else:
            result["error"] = f"Unknown exchange: {exchange_name}"
            return result
        if not exchange.apiKey:
            result["error"] = "API keys not configured"
            return result
        exchange.load_markets()
        sym = _perp_symbol(exchange, normalized_symbol)
        if not sym:
            result["error"] = f"Symbol {normalized_symbol} not found"
            return result
        if amount_base <= 0:
            result["error"] = "Amount must be positive"
            return result
        side_lower = side.lower() if side else "buy"
        params: dict[str, Any] = {"leverage": leverage}
        order = exchange.create_market_order(sym, side_lower, amount_base, params)
        result["success"] = True
        result["order_id"] = order.get("id") if isinstance(order, dict) else None
    except Exception as e:
        logger.exception("place_market_order %s %s: %s", exchange_name, normalized_symbol, e)
        result["error"] = str(e)
    return result


def close_position(
    exchange_name: str,
    normalized_symbol: str,
    side: str,
    amount_base: float,
) -> dict[str, Any]:
    """
    Close (reduce) a position on the given exchange.
    side: the side that was opened ('buy' or 'sell'); we send the opposite to reduce.
    amount_base: position size in base currency.
    Returns { "success": bool, "error": str | None }.
    """
    result: dict[str, Any] = {"success": False, "error": None}
    try:
        if exchange_name.lower() == "kucoin":
            exchange = ccxt.kucoinfutures(_get_kucoin_config())
        elif exchange_name.lower() == "bybit":
            exchange = ccxt.bybit(_get_bybit_config())
        else:
            result["error"] = f"Unknown exchange: {exchange_name}"
            return result
        if not exchange.apiKey:
            result["error"] = "API keys not configured"
            return result
        exchange.load_markets()
        sym = _perp_symbol(exchange, normalized_symbol)
        if not sym:
            result["error"] = f"Symbol {normalized_symbol} not found"
            return result
        close_side = "sell" if (side or "").lower() == "buy" else "buy"
        params: dict[str, Any] = {"reduceOnly": True}
        exchange.create_market_order(sym, close_side, amount_base, params)
        result["success"] = True
    except Exception as e:
        logger.exception("close_position %s %s: %s", exchange_name, normalized_symbol, e)
        result["error"] = str(e)
    return result
