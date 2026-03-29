"""
data/fetcher.py
────────────────
Fetches OHLCV candles from OKX via ccxt.
Returns ET-indexed DataFrames with Title-case columns (Open/High/Low/Close/Volume)
so the rest of the strategy files work unchanged.
"""
import asyncio
import logging

import ccxt
import pandas as pd
import pytz
from functools import lru_cache

logger = logging.getLogger("fetcher")
ET = pytz.timezone("America/New_York")

INTERVAL_MAP = {"1m": "1m", "3m": "3m", "1h": "1h"}
LIMIT_MAP    = {"1m": 500, "3m": 200, "1h": 48}


@lru_cache(maxsize=1)
def _get_exchange() -> ccxt.okx:
    return ccxt.okx({"enableRateLimit": True, "options": {"defaultType": "spot"}})


async def get_candles(symbol: str, interval: str, period: str = "1d") -> pd.DataFrame:
    """
    Fetch OHLCV from OKX. period arg kept for API compat but ignored.
    Returns ET-indexed DataFrame: Open, High, Low, Close, Volume
    """
    if interval not in INTERVAL_MAP:
        logger.warning(f"Unknown interval: {interval}")
        return pd.DataFrame()

    tf    = INTERVAL_MAP[interval]
    limit = LIMIT_MAP.get(interval, 200)

    try:
        exchange = _get_exchange()
        raw = await asyncio.to_thread(exchange.fetch_ohlcv, symbol, tf, limit=limit)

        if not raw:
            return pd.DataFrame()

        df = pd.DataFrame(raw, columns=["ts", "Open", "High", "Low", "Close", "Volume"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df = df.set_index("ts")
        df.index = df.index.tz_convert(ET)
        return df.astype(float).dropna()

    except ccxt.BadSymbol:
        logger.warning(f"OKX: symbol not found — {symbol}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"OKX fetch error {symbol}/{interval}: {e}")
        return pd.DataFrame()
