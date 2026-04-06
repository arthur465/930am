"""
data/fetcher.py
────────────────
Unified candle fetcher. Routes based on symbol type:
  - Crypto ("BTC/USDT")  → Coinalyze (primary) → OKX fallback
  - Stocks ("AAPL")      → yfinance

Returns ET-indexed DataFrames with Title-case columns: Open, High, Low, Close, Volume
"""

import asyncio
import logging
from datetime import datetime

import pandas as pd
import pytz

logger = logging.getLogger("fetcher")
ET  = pytz.timezone("America/New_York")
UTC = pytz.utc

INTERVAL_MAP_OKX = {"1m": "1m", "1h": "1h"}
LIMIT_MAP_OKX    = {"1m": 500,  "1h": 48}

_okx_exchange = None


# ── Symbol routing ─────────────────────────────────────────────────────────────

def is_crypto(symbol: str) -> bool:
    return "/" in symbol


# ── OKX fallback for crypto ────────────────────────────────────────────────────

def _get_okx():
    global _okx_exchange
    if _okx_exchange is None:
        import ccxt.async_support as ccxt
        _okx_exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        })
    return _okx_exchange


async def _get_candles_okx(symbol: str, interval: str) -> pd.DataFrame:
    if interval not in INTERVAL_MAP_OKX:
        return pd.DataFrame()
    import ccxt.async_support as ccxt
    exchange = _get_okx()
    try:
        ohlcv = await exchange.fetch_ohlcv(
            symbol=symbol,
            timeframe=INTERVAL_MAP_OKX[interval],
            limit=LIMIT_MAP_OKX.get(interval, 200),
        )
        if not ohlcv:
            return pd.DataFrame()
        df = pd.DataFrame(ohlcv, columns=["ts", "Open", "High", "Low", "Close", "Volume"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df = df.set_index("ts")
        df.index = df.index.tz_convert(ET)
        return df.astype(float).dropna()
    except ccxt.RateLimitExceeded:
        logger.warning(f"OKX rate limit for {symbol}/{interval}")
        from data.cache import _cache
        _cache.trigger_backoff()
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"OKX fetch error {symbol}/{interval}: {e}")
        return pd.DataFrame()


# ── Coinalyze for crypto ───────────────────────────────────────────────────────

async def _get_candles_coinalyze(symbol: str, interval: str) -> pd.DataFrame:
    """Fetch OHLCV + buy/sell vol from Coinalyze. Returns empty if key not set."""
    from data.coinalyze_fetcher import get_candles_crypto
    return await get_candles_crypto(symbol, interval)


# ── yfinance for stocks ────────────────────────────────────────────────────────

YFINANCE_INTERVAL_MAP = {"1m": "1m", "1h": "1h"}
YFINANCE_PERIOD_MAP   = {"1m": "1d", "1h": "5d"}


def _fetch_stock_sync(symbol: str, interval: str) -> pd.DataFrame:
    try:
        import yfinance as yf
        period   = YFINANCE_PERIOD_MAP.get(interval, "1d")
        yf_iv    = YFINANCE_INTERVAL_MAP.get(interval, "1m")
        ticker   = yf.Ticker(symbol)
        df       = ticker.history(period=period, interval=yf_iv, prepost=False, auto_adjust=True)

        if df.empty:
            return pd.DataFrame()

        # Normalize columns
        df = df.rename(columns={
            "Open": "Open", "High": "High", "Low": "Low",
            "Close": "Close", "Volume": "Volume",
        })
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()

        # Convert index to ET
        if df.index.tz is None:
            df.index = df.index.tz_localize("America/New_York")
        else:
            df.index = df.index.tz_convert(ET)

        return df.astype(float).dropna()

    except Exception as e:
        logger.error(f"yfinance fetch error {symbol}/{interval}: {e}")
        return pd.DataFrame()


async def _get_candles_stock(symbol: str, interval: str) -> pd.DataFrame:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch_stock_sync, symbol, interval)


# ── Public API ─────────────────────────────────────────────────────────────────

async def get_candles(symbol: str, interval: str, period: str = "1d") -> pd.DataFrame:
    """
    Unified entry point.
      Crypto → Coinalyze (with OKX fallback)
      Stocks → yfinance
    """
    if is_crypto(symbol):
        df = await _get_candles_coinalyze(symbol, interval)
        if df.empty:
            logger.debug(f"Coinalyze empty for {symbol}/{interval} — trying OKX fallback")
            df = await _get_candles_okx(symbol, interval)
        return df
    else:
        return await _get_candles_stock(symbol, interval)


async def cleanup():
    global _okx_exchange
    if _okx_exchange:
        await _okx_exchange.close()
        _okx_exchange = None
