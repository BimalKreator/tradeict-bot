"""
Funding Arbitrage Bot — FastAPI backend.
Serves dashboard UI and API. Internal system time: IST (Indian Standard Time).
"""
import os
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

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

# Templates and static (if added later)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

if os.path.isdir(os.path.join(BASE_DIR, "static")):
    app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")


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
    sort_by: str = "spread",
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

    # 2. Sort: spread = gross_spread desc; interval = next_funding_time asc (nulls last)
    if sort_by == "interval":
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


# Mock balances for trade preview UI (no real trading yet)
TRADE_PREVIEW_MOCK_BALANCE_USDT = 1000.0


@app.get("/api/trade-preview/{symbol:path}")
def get_trade_preview(symbol: str):
    """
    Trade preview for a symbol: mark prices and mock balances.
    Used by the Trade Preview Modal (manual trade UI).
    """
    from market_data_service.exchange import get_mark_prices_for_symbol

    symbol = symbol.strip()
    if not symbol or "/" not in symbol:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="Invalid symbol")
    prices = get_mark_prices_for_symbol(symbol)
    return {
        "symbol": symbol,
        "kucoin_price": prices.get("kucoin_price"),
        "bybit_price": prices.get("bybit_price"),
        "kucoin_balance": TRADE_PREVIEW_MOCK_BALANCE_USDT,
        "bybit_balance": TRADE_PREVIEW_MOCK_BALANCE_USDT,
    }
