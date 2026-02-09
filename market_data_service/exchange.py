"""
Exchange market data via CCXT. Uses API keys from .env when set for private data (balances).
Fetches USDT perpetuals: symbol, funding rate, next funding time, min_qty, lot_size.
Normalizes symbols to BASE/USDT (e.g. BTC/USDT).
"""
import logging
import os
import time
import uuid
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
    """Build Bybit config from env. UTA (Unified Trading Account) options for correct V5 API and parsing."""
    config: dict[str, Any] = {
        "options": {
            "defaultType": "swap",
            "enableUnifiedMargin": True,
            "enableUnifiedAccount": True,
        }
    }
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
    symbol_display = row.get("symbol", raw_symbol)
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
                    interval_int = int(interval_hours)
                    row["funding_interval"] = interval_int
                    print(f"--> {symbol_display} {exchange_id} Interval detected: {interval_int}h")
            except (TypeError, ValueError) as e:
                print(f"--> {symbol_display} {exchange_id} fundingInterval parse failed: raw={raw_interval!r} err={e}")
        else:
            # Debug: print raw info for a few symbols when interval key is missing
            if symbol_display in ("BTC/USDT", "ETH/USDT", "API3/USDT"):
                print(f"--> [DEBUG] {symbol_display} {exchange_id} info (no fundingInterval): {info}")
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


def _fetch_kucoin_intervals(exchange: ccxt.Exchange) -> dict[str, int]:
    """
    Fetch funding intervals from KuCoin Futures GET /api/v1/contracts/active.
    Priority 1: fundingInterval (seconds). Priority 2: fundingRateGranularity (milliseconds).
    Returns dict mapping symbol id -> interval_hours (e.g. {'XBTUSDTM': 8, 'API3USDTM': 4}).
    """
    result: dict[str, int] = {}
    try:
        resp = exchange.futurespublic_get_contracts_active()
        data = resp.get("data") if isinstance(resp, dict) else None
        if not isinstance(data, list):
            logger.warning("KuCoin contracts/active: unexpected response shape")
            return result
        for item in data:
            if not isinstance(item, dict):
                continue
            raw_id = item.get("symbol")
            if raw_id is None:
                continue
            interval_seconds: float | None = None
            used_field = None
            raw_val = None
            # Priority 1: fundingInterval (seconds)
            raw_interval = item.get("fundingInterval")
            if raw_interval is not None:
                try:
                    interval_seconds = int(float(raw_interval))
                    used_field = "fundingInterval"
                    raw_val = raw_interval
                except (TypeError, ValueError):
                    pass
            # Priority 2: fundingRateGranularity (milliseconds)
            if interval_seconds is None:
                granularity = item.get("fundingRateGranularity")
                if granularity is not None:
                    try:
                        interval_seconds = int(float(granularity)) / 1000
                        used_field = "fundingRateGranularity"
                        raw_val = granularity
                    except (TypeError, ValueError):
                        pass
            if interval_seconds is None or interval_seconds <= 0:
                continue
            hours = int(interval_seconds / 3600)
            if hours >= 1:
                result[str(raw_id)] = hours
                print(f"KuCoin {raw_id}: {used_field}={raw_val} -> {hours}h")
        print(f"--> KuCoin intervals loaded: {len(result)} contracts from /contracts/active")
    except Exception as e:
        logger.warning("_fetch_kucoin_intervals failed: %s", e)
    return result


def _get_valid_kucoin_futures_ids(exchange: ccxt.Exchange) -> set[str]:
    """Single source of truth: contract IDs from KuCoin GET /api/v1/contracts/active."""
    intervals = _fetch_kucoin_intervals(exchange)
    return set(intervals.keys())


def _fetch_kucoin() -> list[dict[str, Any]]:
    """KuCoin futures: only include symbols that exist in contracts/active (no string-based symbol construction)."""
    import time
    exchange = ccxt.kucoinfutures(_get_kucoin_config())
    exchange.load_markets()
    intervals_map = _fetch_kucoin_intervals(exchange)
    valid_futures_ids = set(intervals_map.keys())
    symbols = _get_usdt_perp_symbols(exchange)[:KUCOIN_SYMBOL_LIMIT]
    result = []
    for sym in symbols:
        try:
            m = exchange.markets.get(sym)
            if not m:
                continue
            raw_id = m.get("id")
            if raw_id is None or str(raw_id) not in valid_futures_ids:
                print(f"[DEBUG] Skipping {sym} (market_id={raw_id}): not in KuCoin contracts/active")
                continue
            data = exchange.fetch_funding_rate(sym)
            row = _row(data, sym)
            _enrich_contract_specs(row, exchange, sym, "kucoin")
            interval_h = intervals_map.get(str(raw_id))
            if interval_h is not None:
                row["funding_interval"] = interval_h
                print(f"--> {row['symbol']} kucoin Interval detected: {interval_h}h")
            else:
                row["funding_interval"] = None
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
    Place a market order. KuCoin: PURE RAW API only (no create_order). Bybit: Raw V5.
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
        if amount_base <= 0:
            result["error"] = "Amount must be positive"
            return result
        side_lower = side.lower() if side else "buy"
        user_leverage = int(leverage) if leverage is not None and leverage > 0 else 1
        print(f"[DEBUG] Applying User Leverage: {user_leverage}x")

        if exchange.id == "kucoinfutures":
            # 1. Gatekeeper: validate symbol and get official symbol_id
            target_base = normalized_symbol.split("/")[0] if "/" in normalized_symbol else normalized_symbol
            found_market = None
            for m in exchange.markets.values():
                if not isinstance(m, dict):
                    continue
                if (
                    m.get("base") == target_base
                    and m.get("quote") == "USDT"
                    and m.get("swap") is True
                    and m.get("active", True) is not False
                ):
                    found_market = m
                    break
            if not found_market:
                print(f"[WARN] Skipping {normalized_symbol}: Not tradable on KuCoin Futures.")
                result["error"] = f"{normalized_symbol} is not in the active KuCoin Futures list. Trade skipped."
                return result
            sym = found_market["symbol"]
            symbol_id = found_market["id"]
            qty_str = exchange.amount_to_precision(sym, amount_base)
            print(f"[DEBUG] KuCoin gatekeeper: using real id {symbol_id!r}")

            # Setup (margin, leverage) on main connection
            try:
                exchange.futuresprivate_post_position_changemarginmode(
                    {"symbol": symbol_id, "marginMode": "CROSS"}
                )
                print(f"[DEBUG] Cross mode set OK.")
            except Exception as e:
                print(f"[DEBUG] Setup marginMode failed (continue anyway): {e}")
            try:
                exchange.private_post_position_update_user_leverage(
                    {"symbol": symbol_id, "leverage": str(user_leverage)}
                )
            except Exception as e:
                print(f"[DEBUG] Setup leverage failed (continue anyway): {e}")
            time.sleep(2)

            payload = {
                "clientOid": exchange.uuid(),
                "side": side_lower,
                "symbol": symbol_id,
                "type": "market",
                "size": qty_str,
            }
            print(f"[DEBUG] KuCoin RAW order payload (no marginMode/leverage): {payload}")
            print(f"[DEBUG] Corrected Method: private_post_orders")

            # 2. Execution loop: main connection first, then fresh instance on 900001
            order: dict[str, Any] | None = None
            for attempt in range(2):
                if attempt == 0:
                    print(f"[DEBUG] Executing via Main Connection")
                    try:
                        raw_response = exchange.private_post_orders(payload)
                        order_id = (raw_response.get("data") or {}).get("orderId") or raw_response.get("orderId")
                        order = {
                            "id": order_id,
                            "symbol": sym,
                            "status": "closed",
                            "info": raw_response,
                        }
                        break
                    except Exception as e:
                        err_str = str(e).lower()
                        code_val = getattr(e, "code", None) or getattr(e, "error", None)
                        if code_val is not None and hasattr(code_val, "__str__"):
                            code_str = str(code_val)
                        else:
                            code_str = ""
                        # KuCoin 900001 = "Trading pair does not exist" (stale connection); retry with fresh instance
                        if (
                            code_str == "900001"
                            or "900001" in err_str
                            or "trading pair" in err_str
                            and "does not exist" in err_str
                        ):
                            print(f"[WARN] Error 900001 on main connection. Switching to FRESH connection...")
                            continue
                        raise
                else:
                    print(f"[DEBUG] Executing via Fresh Connection")
                    temp_config = _get_kucoin_config()
                    temp_exchange = ccxt.kucoinfutures(temp_config)
                    try:
                        # New clientOid for fresh attempt
                        payload["clientOid"] = temp_exchange.uuid()
                        raw_response = temp_exchange.private_post_orders(payload)
                        order_id = (raw_response.get("data") or {}).get("orderId") or raw_response.get("orderId")
                        order = {
                            "id": order_id,
                            "symbol": sym,
                            "status": "closed",
                            "info": raw_response,
                        }
                    finally:
                        if hasattr(temp_exchange, "close"):
                            temp_exchange.close()
                    break

            if order is None:
                result["error"] = "KuCoin order failed after retry (symbol rejected)."
                return result

        elif exchange.id == "bybit":
            sym = _perp_symbol(exchange, normalized_symbol)
            if not sym:
                print(f"[DEBUG] Symbol resolution failed: normalized={normalized_symbol!r} -> no perp symbol on exchange")
                result["error"] = f"Symbol {normalized_symbol} not found"
                return result
            qty_str = exchange.amount_to_precision(sym, amount_base)
            print(f"[DEBUG] Applying User Leverage: {user_leverage}x | qty_str: {qty_str}")
            print(f"[DEBUG] PURE RAW EXECUTION for {exchange.id}.")
            market = exchange.market(sym)
            payload = {
                "category": "linear",
                "symbol": market["id"],
                "side": side_lower.capitalize(),
                "orderType": "Market",
                "qty": qty_str,
                "positionIdx": 0,
            }
            print(f"[DEBUG] Bybit Payload: {payload}")
            raw_response = exchange.private_post_v5_order_create(payload)
            order = {
                "id": raw_response["result"]["orderId"],
                "symbol": sym,
                "status": "closed",
                "info": raw_response,
            }

        else:
            sym = _perp_symbol(exchange, normalized_symbol)
            if not sym:
                result["error"] = f"Symbol {normalized_symbol} not found"
                return result
            order = exchange.create_market_order(
                sym, side_lower, amount_base, {"leverage": user_leverage}
            )

        print(f"[DEBUG] Exchange Response: {order}")
        print(f"✅ Order Placed! ID: {order.get('id')} | Status: {order.get('status')}")
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
        if exchange.id == "kucoinfutures":
            market = exchange.market(sym)
            valid_kucoin_ids = _get_valid_kucoin_futures_ids(exchange)
            if market.get("id") is None or str(market.get("id")) not in valid_kucoin_ids:
                print(f"[DEBUG] close_position: invalid futures symbol {normalized_symbol!r} (market_id={market.get('id')!r})")
                result["error"] = f"Invalid futures symbol ({normalized_symbol}): not in KuCoin contracts/active"
                return result
        close_side = "sell" if (side or "").lower() == "buy" else "buy"
        params: dict[str, Any] = {"reduceOnly": True}
        if exchange.id == "kucoinfutures":
            params["marginMode"] = "cross"
        exchange.create_market_order(sym, close_side, amount_base, params)
        result["success"] = True
    except Exception as e:
        logger.exception("close_position %s %s: %s", exchange_name, normalized_symbol, e)
        result["error"] = str(e)
    return result
