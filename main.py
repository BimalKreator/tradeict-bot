"""
Funding Arbitrage Bot — FastAPI backend.
Serves dashboard UI and API. Internal system time: IST (Indian Standard Time).
"""
import os
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Load .env before other imports that may use env vars
load_dotenv()

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

# IST timezone for internal use
IST = ZoneInfo("Asia/Kolkata")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Set process timezone to IST for internal system time."""
    # Ensure datetime operations use IST when no tz is specified
    os.environ.setdefault("TZ", "Asia/Kolkata")
    yield
    # cleanup if needed
    pass


app = FastAPI(
    title="Funding Arbitrage Bot",
    description="Kucoin & Bybit Funding Arbitrage",
    version="0.1.0",
    lifespan=lifespan,
)

# Templates and static
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

static_dir = os.path.join(BASE_DIR, "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Serve the main dashboard UI."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/health")
async def health():
    """Health check; confirms server and timezone."""
    from datetime import datetime
    return {
        "status": "ok",
        "timezone": "IST",
        "time_ist": datetime.now(IST).isoformat(),
    }


@app.get("/api/market-data")
def get_market_data():
    """
    Fetch USDT perpetual market data from KuCoin and Bybit (public endpoints).
    Returns symbol counts per exchange; if one fails, the other still works.
    """
    from market_data_service.exchange import fetch_all_market_data
    data = fetch_all_market_data()
    return {
        "kucoin": {"symbols_count": data["kucoin"]["symbols_count"], "error": data["kucoin"]["error"]},
        "bybit": {"symbols_count": data["bybit"]["symbols_count"], "error": data["bybit"]["error"]},
    }


@app.get("/api/screener")
def get_screener(
    page: int = 1,
    limit: int = 10,
    search: str | None = None,
    sort_by: str = "optimal",
):
    """
    Return funding arbitrage opportunities with server-side pagination, search, and sort.
    Uses cached market data (no re-fetch from exchanges per request).
    """
    from screener_engine.analyzer import get_arbitrage_opportunities

    full = get_arbitrage_opportunities()

    # 1. Filter by search (token name / symbol)
    if search and search.strip():
        q = search.strip().lower()
        full = [r for r in full if q in r["symbol"].lower()]

    # 2. Sort: optimal = interval asc then spread desc; spread = gross_spread desc; interval = next_funding_time asc
    if sort_by == "optimal":
        # Primary: funding_interval ascending (1h, 2h, 4h, 8h); None -> 999 at bottom. Secondary: gross_spread descending.
        full.sort(
            key=lambda x: (
                x.get("kucoin_funding_interval") or x.get("bybit_funding_interval") or 999,
                -x["gross_spread"],
            )
        )
    elif sort_by == "interval":
        def _interval_key(r):
            nt = r.get("next_funding_time")
            return (nt is None, nt or "")

        full.sort(key=_interval_key)
    else:
        full.sort(key=lambda x: x["gross_spread"], reverse=True)

    total_items = len(full)
    total_pages = max(1, (total_items + limit - 1) // limit) if limit > 0 else 1
    page = max(1, min(page, total_pages))
    start = (page - 1) * limit
    data = full[start : start + limit]

    return {
        "data": data,
        "total_pages": total_pages,
        "current_page": page,
        "total_items": total_items,
    }


# Fallback when API keys not set (trade preview shows 0 and user can still see UI)
TRADE_PREVIEW_MOCK_BALANCE_USDT = 1000.0


@app.get("/api/trade-preview/{symbol:path}")
def get_trade_preview(symbol: str):
    """
    Trade preview for a symbol: mark prices and real wallet balances (from .env API keys).
    If keys are missing or invalid, returns 0 balance so UI still works.
    """
    from market_data_service.exchange import get_mark_prices_for_symbol, get_wallet_balance

    symbol = symbol.strip()
    if not symbol or "/" not in symbol:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="Invalid symbol")
    prices = get_mark_prices_for_symbol(symbol)
    kucoin_bal = get_wallet_balance("kucoin")
    bybit_bal = get_wallet_balance("bybit")
    kucoin_balance = kucoin_bal["available_balance"] if not kucoin_bal.get("error") else 0.0
    bybit_balance = bybit_bal["available_balance"] if not bybit_bal.get("error") else 0.0
    return {
        "symbol": symbol,
        "kucoin_price": prices.get("kucoin_price"),
        "bybit_price": prices.get("bybit_price"),
        "kucoin_balance": kucoin_balance,
        "bybit_balance": bybit_balance,
    }


@app.get("/api/test-connection")
def test_connection():
    """
    Verify API keys by fetching balance from both KuCoin and Bybit.
    Returns status "ok" with message if both succeed, else "error" with failure details.
    """
    from market_data_service.exchange import get_wallet_balance

    kucoin = get_wallet_balance("kucoin")
    bybit = get_wallet_balance("bybit")
    errors = []
    if kucoin.get("error"):
        errors.append(f"KuCoin: {kucoin['error']}")
    if bybit.get("error"):
        errors.append(f"Bybit: {bybit['error']}")
    if not errors:
        return {"status": "ok", "message": "Connected! KuCoin & Bybit Ready."}
    return {"status": "error", "message": "Error: " + "; ".join(errors)}


class ExecuteTradeRequest(BaseModel):
    symbol: str
    quantity: float
    leverage: int = 1
    simulate_failure: bool = False


@app.post("/api/execute-trade")
def execute_trade(body: ExecuteTradeRequest):
    """
    Execute dual trade (KuCoin then Bybit). Mock orders only.
    Body: { symbol, quantity, leverage, simulate_failure?: bool }
    Returns result with success, status, message, logs.
    """
    from fastapi import HTTPException
    from trade_engine.executor import TradeExecutor

    symbol = (body.symbol or "").strip()
    if not symbol or "/" not in symbol:
        raise HTTPException(status_code=400, detail="Invalid symbol")
    if body.quantity <= 0 or body.leverage <= 0:
        raise HTTPException(status_code=400, detail="quantity and leverage must be positive")

    executor = TradeExecutor()
    result = executor.execute_dual_trade(
        symbol=symbol,
        quantity=body.quantity,
        leverage=body.leverage,
        simulate_failure=body.simulate_failure,
    )
    return result


@app.get("/api/trades")
def get_trades(limit: int = 5):
    """Return last N trades for Recent Trades section."""
    import database
    trades = database.get_recent_trades(limit=min(50, max(1, limit)))
    return {"trades": trades}
