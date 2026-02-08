"""
SQLite database for trade history.
"""
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "trades.db")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create trades table if it does not exist."""
    conn = get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_group_id TEXT NOT NULL,
                entry_time TEXT NOT NULL,
                quantity REAL NOT NULL,
                leverage INTEGER NOT NULL,
                kucoin_direction TEXT NOT NULL,
                bybit_direction TEXT NOT NULL,
                kucoin_entry_price REAL,
                bybit_entry_price REAL,
                status TEXT NOT NULL
            )
        """)
        conn.commit()
    finally:
        conn.close()


def insert_trade(
    token_group_id: str,
    quantity: float,
    leverage: int,
    kucoin_direction: str,
    bybit_direction: str,
    kucoin_entry_price: float | None,
    bybit_entry_price: float | None,
    status: str,
) -> int:
    """Insert a trade and return its id."""
    conn = get_connection()
    try:
        entry_time = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            """INSERT INTO trades (
                token_group_id, entry_time, quantity, leverage,
                kucoin_direction, bybit_direction, kucoin_entry_price, bybit_entry_price, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                token_group_id,
                entry_time,
                quantity,
                leverage,
                kucoin_direction,
                bybit_direction,
                kucoin_entry_price,
                bybit_entry_price,
                status,
            ),
        )
        conn.commit()
        return cur.lastrowid or 0
    finally:
        conn.close()


def get_recent_trades(limit: int = 5) -> list[dict[str, Any]]:
    """Return the last N trades, newest first."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, token_group_id, entry_time, quantity, leverage, kucoin_direction, bybit_direction, kucoin_entry_price, bybit_entry_price, status FROM trades ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# Ensure table exists on import
init_db()
