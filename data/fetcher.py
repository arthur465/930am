"""
data/fetcher.py
────────────────
Fetches OHLCV candles from Polygon.io REST API.
Supports both stocks (SPY, TSLA, AMZN...) and crypto (BTC, ETH...).
Returns ET-indexed DataFrames with Title-case columns (Open/High/Low/Close/Volume).

Requires POLYGON_API_KEY in environment.
Free tier: unlimited calls, slight rate limit — we throttle accordingly.
Sign up at https://polygon.io (free tier works for this strategy).
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta

import httpx
import pandas as pd
import pytz

logger = logging.getLogger("fetcher")
ET     = pytz.timezone("America/New_York")
UTC    = pytz.utc

API_KEY  = os.getenv("POLYGON_API_KEY", "")
BASE_URL = "https://api.polygon.io"
TIMEOUT  = 15

# Polygon timespan mapping
INTERVAL_MAP = {
    "1m":  ("minute", 1),
    "3m":  ("minute", 3),
    "1h":  ("hour",   1),
}

# How many bars to fetch per interval
LIMIT_MAP = {
    "1m": 500,
    "3m": 200,
    "1h": 48,
}

# Crypto symbols — bot uses short names, Polygon uses X:BTCUSD format
CRYPTO_SYMBOLS = {"BTC", "ETH", "XRP", "SOL", "DOGE", "LINK", "AVAX"}


def _polygon_ticker(symbol: str) -> str:
    """Convert internal symbol name to Polygon ticker format."""
    sym = symbol.upper().replace("/USDT", "").replace("/USD", "")
    if sym in CRYPTO_SYMBOLS:
        return f"X:{sym}USD"
    return sym  # stocks already correct: SPY, TSLA, AMZN etc


def _is_crypto(symbol: str) -> bool:
    sym = symbol.upper().replace("/USDT", "").replace("/USD", "")
    return sym in CRYPTO_SYMBOLS


async def get_candles(symbol: str, interval: str, period: str = "1d") -> pd.DataFrame:
    """
    Fetch OHLCV from Polygon.io.
    Returns ET-indexed DataFrame: Open, High, Low, Close, Volume
    """
    if not API_KEY:
        logger.error("POLYGON_API_KEY not set — cannot fetch candles.")
        return pd.DataFrame()

    if interval not in INTERVAL_MAP:
        logger.warning(f"Unknown interval: {interval}")
        return pd.DataFrame()

    timespan, multiplier = INTERVAL_MAP[interval]
    limit                = LIMIT_MAP.get(interval, 200)
    ticker               = _polygon_ticker(symbol)

    # Date range — go back far enough to capture enough bars across weekends
    now_utc   = datetime.now(UTC)
    days_back = 7 if interval == "1h" else 3
    from_dt   = (now_utc - timedelta(days=days_back)).strftime("%Y-%m-%d")
    to_dt     = now_utc.strftime("%Y-%m-%d")

    url = (
        f"{BASE_URL}/v2/aggs/ticker/{ticker}/range"
        f"/{multiplier}/{timespan}/{from_dt}/{to_dt}"
    )

    params = {
        "adjusted": "true",
        "sort":     "asc",
        "limit":    limit,
        "apiKey":   API_KEY,
    }

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        results = data.get("results", [])
        if not results:
            status = data.get("status", "unknown")
            logger.warning(f"Polygon: no results for {ticker}/{interval} (status={status})")
            return pd.DataFrame()

        df = pd.DataFrame(results).rename(columns={
            "t": "ts", "o": "Open", "h": "High",
            "l": "Low",  "c": "Close", "v": "Volume",
        })[["ts", "Open", "High", "Low", "Close", "Volume"]]

        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df = df.set_index("ts")
        df.index = df.index.tz_convert(ET)
        df = df.astype(float).dropna()

        if len(df) > limit:
            df = df.tail(limit)

        logger.debug(f"Polygon: {ticker}/{interval} → {len(df)} bars")
        return df

    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        if code == 403:
            logger.error(f"Polygon 403 for {ticker} — check API key or plan")
        elif code == 429:
            logger.warning(f"Polygon 429 for {ticker}/{interval} — triggering global backoff")
            # Import here to avoid circular import; triggers backoff for ALL pending calls
            from data.cache import _cache
            _cache.trigger_backoff()
        else:
            logger.error(f"Polygon HTTP {code} for {ticker}/{interval}")
        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Polygon fetch error {ticker}/{interval}: {e}")
        return pd.DataFrame()
