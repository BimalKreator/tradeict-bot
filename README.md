# Funding Arbitrage Bot (Kucoin & Bybit)

Phase 0: Project foundation and architecture.

## Structure

- **market_data_service** — fetch data from exchanges
- **screener_engine** — filter funding arbitrage opportunities
- **trade_engine** — execute orders
- **dashboard** — UI layer

## Run the server

Using a virtual environment (recommended):

```bash
python3 -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

One-liner (Unix, from project root):

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt && .venv/bin/uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

- Dashboard: http://localhost:8000
- Health (IST): http://localhost:8000/api/health
