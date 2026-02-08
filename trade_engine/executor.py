"""
Trade Execution Engine: dual-exchange atomic logic with rollback on failure.
"""
from __future__ import annotations

from typing import Any

# Mock balance (must match main.py for validation)
MOCK_BALANCE_USDT = 1000.0


class TradeExecutor:
    """
    Executes dual trades (Exchange A then B). If B fails, rolls back A and logs FAILED_ROLLBACK.
    All order placement is mocked.
    """

    def __init__(self) -> None:
        pass

    def _required_margin(self, quantity: float, leverage: int) -> float:
        if leverage <= 0:
            return float("inf")
        return quantity / leverage

    def _validate_balance(self, quantity: float, leverage: int) -> tuple[bool, str]:
        margin = self._required_margin(quantity, leverage)
        if margin > MOCK_BALANCE_USDT:
            return False, f"Insufficient balance: margin {margin} > {MOCK_BALANCE_USDT} USDT"
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

    def _place_order_mock(self, exchange: str, symbol: str, side: str, quantity: float, leverage: int) -> None:
        """Mock place order on exchange. Raises on simulate_failure when exchange is Bybit."""
        pass  # Mock success; raise only when caller requests simulate_failure for Exchange B

    def _close_position_mock(self, exchange: str, symbol: str) -> None:
        """Mock close position on exchange (rollback)."""
        pass

    def execute_dual_trade(
        self,
        symbol: str,
        quantity: float,
        leverage: int,
        simulate_failure: bool = False,
    ) -> dict[str, Any]:
        """
        Execute order on Exchange A (KuCoin), then Exchange B (Bybit).
        If simulate_failure is True, Exchange B raises; then rollback A and log FAILED_ROLLBACK.
        Otherwise log OPEN.
        """
        import database

        ok, msg = self._validate_balance(quantity, leverage)
        if not ok:
            return {"success": False, "status": None, "message": msg, "logs": [msg]}

        kucoin_direction, bybit_direction, kucoin_price, bybit_price = self._get_directions_and_prices(symbol)
        logs: list[str] = []

        # Step 1: Place order on Exchange A (KuCoin) — mock success
        logs.append(f"[KuCoin] Place {kucoin_direction} order: {quantity} USDT, {leverage}x — OK")
        exchange_a_ok = True

        # Step 2: Place order on Exchange B (Bybit) — mock or simulate failure
        if simulate_failure:
            logs.append("[Bybit] Place order — FAILED (simulated)")
            exchange_b_ok = False
        else:
            logs.append(f"[Bybit] Place {bybit_direction} order: {quantity} USDT, {leverage}x — OK")
            exchange_b_ok = True

        if not exchange_b_ok:
            # Rollback: close position on Exchange A
            logs.append("[KuCoin] Rollback: close position — OK")
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

        # Success: both orders placed
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
