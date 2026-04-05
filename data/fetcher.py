"""
data/fetcher.py
────────────────
Fetches OHLCV candles from OKX via ccxt.
Returns ET-indexed DataFrames with Title-case columns (Open/High/Low/Close/Volume).

No API key needed for public market data.
Free tier: generous rate limits, perfect for this strategy.
"""

import asyncio
import logging
from datetime import datetime

import ccxt.async_support as ccxt
import pandas as pd
import pytz

logger = logging.getLogger("fetcher")
ET     = pytz.timezone("America/New_York")
UTC    = pytz.utc

# Interval mapping for ccxt (OKX uses: 1m, 3m, 5m, 15m, 1h, 4h, 1d)
INTERVAL_MAP = {
    "1m": "1m",
    "3m": "3m",
    "1h": "1h",  # FIXED: OKX uses lowercase 'h'
}

# How many bars to fetch per interval
LIMIT_MAP = {
    "1m": 500,
    "3m": 200,
    "1h": 48,
}

# Single OKX instance shared across calls
_exchange = None


def _get_exchange():
    """Lazy-load OKX exchange instance."""
    global _exchange
    if _exchange is None:
        _exchange = ccxt.okx({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot',  # spot market
            }
        })
    return _exchange


async def get_candles(symbol: str, interval: str, period: str = "1d") -> pd.DataFrame:
    """
    Fetch OHLCV from OKX.
    Returns ET-indexed DataFrame: Open, High, Low, Close, Volume
    
    symbol format: BTC/USDT, ETH/USDT, SOL/USDT
    """
    if interval not in INTERVAL_MAP:
        logger.warning(f"Unknown interval: {interval}")
        return pd.DataFrame()

    timeframe = INTERVAL_MAP[interval]
    limit     = LIMIT_MAP.get(interval, 200)
    exchange  = _get_exchange()

    try:
        # Fetch OHLCV: [[timestamp, open, high, low, close, volume], ...]
        ohlcv = await exchange.fetch_ohlcv(
            symbol=symbol,
            timeframe=timeframe,
            limit=limit
        )

        if not ohlcv:
            logger.warning(f"OKX: no data for {symbol}/{interval}")
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(ohlcv, columns=["ts", "Open", "High", "Low", "Close", "Volume"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df = df.set_index("ts")
        df.index = df.index.tz_convert(ET)
        df = df.astype(float).dropna()

        logger.debug(f"OKX: {symbol}/{interval} → {len(df)} bars")
        return df

    except ccxt.RateLimitExceeded:
        logger.warning(f"OKX rate limit for {symbol}/{interval} — triggering global backoff")
        # Notify cache to back off ALL requests
        from data.cache import _cache
        _cache.trigger_backoff()
        return pd.DataFrame()

    except ccxt.NetworkError as e:
        logger.error(f"OKX network error {symbol}/{interval}: {e}")
        return pd.DataFrame()

    except Exception as e:
        logger.error(f"OKX fetch error {symbol}/{interval}: {e}")
        return pd.DataFrame()


async def cleanup():
    """Close the exchange connection on shutdown."""
    global _exchange
    if _exchange:
        await _exchange.close()
        _exchange = None
