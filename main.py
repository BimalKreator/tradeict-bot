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
