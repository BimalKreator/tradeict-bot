"""
Trade Execution Engine: dual-exchange atomic logic with rollback on failure.
Performs REAL trades via ccxt when simulate_failure is False. Testing limit: 20 USDT.
"""
from __future__ import annotations

from typing import Any

# Hard limit for testing phase (USDT per order)
MAX_ORDER_USDT = 20.0


class TradeExecutor:
    """
    Executes dual trades (KuCoin then Bybit). If Bybit fails, closes KuCoin position (rollback).
    Real orders via ccxt when simulate_failure is False.
    """

    def __init__(self) -> None:
        pass

    def _required_margin(self, quantity: float, leverage: int) -> float:
        if leverage <= 0:
            return float("inf")
        return quantity / leverage

    def _validate_balance(self, quantity: float, leverage: int) -> tuple[bool, str]:
        from market_data_service.exchange import get_wallet_balance

        margin = self._required_margin(quantity, leverage)
        kucoin = get_wallet_balance("kucoin")
        bybit = get_wallet_balance("bybit")
        if kucoin.get("error"):
            return False, f"KuCoin: {kucoin['error']}"
        if bybit.get("error"):
            return False, f"Bybit: {bybit['error']}"
        avail_k = float(kucoin.get("available_balance") or 0)
        avail_b = float(bybit.get("available_balance") or 0)
        if margin > avail_k:
            return False, f"Insufficient balance: margin {margin} > KuCoin available {avail_k} USDT"
        if margin > avail_b:
            return False, f"Insufficient balance: margin {margin} > Bybit available {avail_b} USDT"
        return True, ""

    def _validate_amount_limit(self, quantity: float) -> tuple[bool, str]:
        if quantity > MAX_ORDER_USDT:
            return False, f"Order size {quantity} USDT exceeds testing limit ({MAX_ORDER_USDT} USDT)"
        if quantity <= 0:
            return False, "Order size must be positive"
        return True, ""

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

    def execute_dual_trade(
        self,
        symbol: str,
        quantity: float,
        leverage: int,
        simulate_failure: bool = False,
    ) -> dict[str, Any]:
        """
        Execute order on KuCoin (A) then Bybit (B). Real orders via ccxt unless simulate_failure.
        Safety: quantity must be <= 20 USDT. If B fails, close A position (rollback).
        """
        from market_data_service.exchange import close_position, place_market_order

        import database

        ok, msg = self._validate_amount_limit(quantity)
        if not ok:
            return {"success": False, "status": None, "message": msg, "logs": [msg]}

        ok, msg = self._validate_balance(quantity, leverage)
        if not ok:
            return {"success": False, "status": None, "message": msg, "logs": [msg]}

        kucoin_direction, bybit_direction, kucoin_price, bybit_price = self._get_directions_and_prices(symbol)
        kucoin_side = "sell" if kucoin_direction == "Short" else "buy"
        bybit_side = "sell" if bybit_direction == "Short" else "buy"
        price_k = float(kucoin_price or 0)
        price_b = float(bybit_price or 0)
        if price_k <= 0 or price_b <= 0:
            return {"success": False, "status": None, "message": "Could not get mark price for symbol", "logs": []}

        amount_kucoin = quantity / price_k
        amount_bybit = quantity / price_b
        logs: list[str] = []

        # Step 1: Place order on Exchange A (KuCoin) — real
        res_a = place_market_order("kucoin", symbol, kucoin_side, quantity, leverage, price_k)
        if not res_a.get("success"):
            logs.append(f"[KuCoin] Place order — FAILED: {res_a.get('error', 'Unknown')}")
            return {"success": False, "status": None, "message": f"KuCoin order failed: {res_a.get('error')}", "logs": logs}
        logs.append(f"[KuCoin] Place {kucoin_direction} order: {quantity} USDT, {leverage}x — OK")

        # Step 2: Place order on Exchange B (Bybit) — real or simulated failure
        exchange_b_ok = True
        if simulate_failure:
            logs.append("[Bybit] Place order — FAILED (simulated)")
            exchange_b_ok = False
        else:
            res_b = place_market_order("bybit", symbol, bybit_side, quantity, leverage, price_b)
            if not res_b.get("success"):
                logs.append(f"[Bybit] Place order — FAILED: {res_b.get('error', 'Unknown')}")
                exchange_b_ok = False
            else:
                logs.append(f"[Bybit] Place {bybit_direction} order: {quantity} USDT, {leverage}x — OK")

        if not exchange_b_ok:
            # Rollback: close position on Exchange A
            close_res = close_position("kucoin", symbol, kucoin_side, amount_kucoin)
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
