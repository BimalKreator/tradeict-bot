"""
Trade Execution Engine: dual-exchange atomic logic with rollback on failure.
Quantity is in TOKENS (base currency) for perfect hedge; passed directly to ccxt as amount.
Testing limits: max 5 tokens, max 20 USDT notional.
"""
from __future__ import annotations

from typing import Any

# Testing phase limits
MAX_TOKENS = 5.0
MAX_NOTIONAL_USDT = 20.0


class TradeExecutor:
    """
    Executes dual trades (KuCoin then Bybit). quantity = token quantity (same on both exchanges).
    If Bybit fails, closes KuCoin position (rollback).
    """

    def __init__(self) -> None:
        pass

    def _get_directions_and_prices(self, symbol: str) -> tuple[str, str, float | None, float | None]:
        """Get kucoin_direction, bybit_direction, kucoin_entry_price, bybit_entry_price."""
        from market_data_service.exchange import fetch_all_market_data, get_mark_prices_for_symbol

        data = fetch_all_market_data()
        kucoin_list = data.get("kucoin", {}).get("symbols") or []
        bybit_list = data.get("bybit", {}).get("symbols") or []
        kucoin_by = {r["symbol"]: r for r in kucoin_list}
        bybit_by = {r["symbol"]: r for r in bybit_list}
        kr = kucoin_by.get(symbol, {}).get("funding_rate")
        br = bybit_by.get(symbol, {}).get("funding_rate")
        if kr is not None and br is not None:
            if kr > br:
                kucoin_direction, bybit_direction = "Short", "Long"
            else:
                kucoin_direction, bybit_direction = "Long", "Short"
        else:
            kucoin_direction, bybit_direction = "Long", "Short"

        prices = get_mark_prices_for_symbol(symbol)
        kucoin_price = prices.get("kucoin_price")
        bybit_price = prices.get("bybit_price")
        return kucoin_direction, bybit_direction, kucoin_price, bybit_price

    def _validate_amount_limit(
        self,
        quantity: float,
        kucoin_price: float | None,
        bybit_price: float | None,
    ) -> tuple[bool, str]:
        """quantity = token quantity. Enforce max 5 tokens and max 20 USDT notional."""
        if quantity <= 0:
            return False, "Token quantity must be positive"
        if quantity > MAX_TOKENS:
            return False, f"Token quantity {quantity} exceeds limit ({MAX_TOKENS} tokens)"
        pk = float(kucoin_price or 0)
        pb = float(bybit_price or 0)
        max_price = max(pk, pb)
        if max_price > 0 and (quantity * max_price) > MAX_NOTIONAL_USDT:
            return False, f"Notional {quantity * max_price:.2f} USDT exceeds limit ({MAX_NOTIONAL_USDT} USDT)"
        return True, ""

    def _validate_balance(
        self,
        quantity: float,
        leverage: int,
        kucoin_price: float | None,
        bybit_price: float | None,
    ) -> tuple[bool, str]:
        """Margin = (Token Quantity * Mark Price) / Leverage. Must be <= available balance on each."""
        from market_data_service.exchange import get_wallet_balance

        pk = float(kucoin_price or 0)
        pb = float(bybit_price or 0)
        if pk <= 0 or pb <= 0:
            return False, "Could not get mark price"
        margin_k = (quantity * pk) / leverage if leverage > 0 else float("inf")
        margin_b = (quantity * pb) / leverage if leverage > 0 else float("inf")

        kucoin = get_wallet_balance("kucoin")
        bybit = get_wallet_balance("bybit")
        if kucoin.get("error"):
            return False, f"KuCoin: {kucoin['error']}"
        if bybit.get("error"):
            return False, f"Bybit: {bybit['error']}"
        avail_k = float(kucoin.get("available_balance") or 0)
        avail_b = float(bybit.get("available_balance") or 0)
        if margin_k > avail_k:
            return False, f"Insufficient balance: margin {margin_k:.2f} > KuCoin available {avail_k:.2f} USDT"
        if margin_b > avail_b:
            return False, f"Insufficient balance: margin {margin_b:.2f} > Bybit available {avail_b:.2f} USDT"
        return True, ""

    def execute_dual_trade(
        self,
        symbol: str,
        quantity: float,
        leverage: int,
        simulate_failure: bool = False,
    ) -> dict[str, Any]:
        """
        quantity = token quantity (e.g. 1 API3, 10 XRP). Passed directly as amount to create_market_order.
        """
        from market_data_service.exchange import close_position, place_market_order

        import database

        kucoin_direction, bybit_direction, kucoin_price, bybit_price = self._get_directions_and_prices(symbol)
        price_k = float(kucoin_price or 0)
        price_b = float(bybit_price or 0)
        if price_k <= 0 or price_b <= 0:
            return {"success": False, "status": None, "message": "Could not get mark price for symbol", "logs": []}

        ok, msg = self._validate_amount_limit(quantity, kucoin_price, bybit_price)
        if not ok:
            return {"success": False, "status": None, "message": msg, "logs": [msg]}

        ok, msg = self._validate_balance(quantity, leverage, kucoin_price, bybit_price)
        if not ok:
            return {"success": False, "status": None, "message": msg, "logs": [msg]}

        kucoin_side = "sell" if kucoin_direction == "Short" else "buy"
        bybit_side = "sell" if bybit_direction == "Short" else "buy"
        logs: list[str] = []

        # Step 1: Place order on KuCoin — amount = token quantity (no conversion)
        res_a = place_market_order("kucoin", symbol, kucoin_side, quantity, leverage)
        if not res_a.get("success"):
            logs.append(f"[KuCoin] Place order — FAILED: {res_a.get('error', 'Unknown')}")
            return {"success": False, "status": None, "message": f"KuCoin order failed: {res_a.get('error')}", "logs": logs}
        logs.append(f"[KuCoin] Place {kucoin_direction} order: {quantity} tokens, {leverage}x — OK")

        # Step 2: Place order on Bybit — same token quantity
        exchange_b_ok = True
        if simulate_failure:
            logs.append("[Bybit] Place order — FAILED (simulated)")
            exchange_b_ok = False
        else:
            res_b = place_market_order("bybit", symbol, bybit_side, quantity, leverage)
            if not res_b.get("success"):
                logs.append(f"[Bybit] Place order — FAILED: {res_b.get('error', 'Unknown')}")
                exchange_b_ok = False
            else:
                logs.append(f"[Bybit] Place {bybit_direction} order: {quantity} tokens, {leverage}x — OK")

        if not exchange_b_ok:
            close_res = close_position("kucoin", symbol, kucoin_side, quantity)
            if close_res.get("success"):
                logs.append("[KuCoin] Rollback: close position — OK")
            else:
                logs.append(f"[KuCoin] Rollback: close position — FAILED: {close_res.get('error', 'Unknown')}")
            database.insert_trade(
                token_group_id=symbol,
                quantity=quantity,
                leverage=leverage,
                kucoin_direction=kucoin_direction,
                bybit_direction=bybit_direction,
                kucoin_entry_price=kucoin_price,
                bybit_entry_price=bybit_price,
                status="FAILED_ROLLBACK",
            )
            return {
                "success": False,
                "status": "FAILED_ROLLBACK",
                "message": "Trade Failed! First Order Rolled Back.",
                "logs": logs,
            }

        database.insert_trade(
            token_group_id=symbol,
            quantity=quantity,
            leverage=leverage,
            kucoin_direction=kucoin_direction,
            bybit_direction=bybit_direction,
            kucoin_entry_price=kucoin_price,
            bybit_entry_price=bybit_price,
            status="OPEN",
        )
        logs.append("Trade logged as OPEN")
        return {
            "success": True,
            "status": "OPEN",
            "message": "Trade Successful",
            "logs": logs,
        }
