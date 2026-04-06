"""
data/coinalyze_fetcher.py
──────────────────────────
Coinalyze API client.

Used for:
  1. Crypto OHLCV candles with buy/sell volume breakdown
     (higher quality than free exchange APIs)
  2. CVD confirmation on BOS candle
     (buy ratio > threshold on long BOS, < threshold on short BOS)

API docs: https://api.coinalyze.net/v1/doc/
Rate limit: 40 calls / minute (free tier)
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

import aiohttp
import pandas as pd
import pytz

from config import COINALYZE_API_KEY, COINALYZE_EXCHANGE, CVD_MIN_RATIO

logger = logging.getLogger("coinalyze")
ET  = pytz.timezone("America/New_York")
UTC = pytz.utc

BASE_URL = "https://api.coinalyze.net/v1"

SYMBOL_MAP = {
    "BTC/USDT": "BTCUSDT",
    "ETH/USDT": "ETHUSDT",
}

EXCHANGE_MAP = {
    "binance":  "A",
    "okx":      "0",
    "bybit":    "1",
    "coinbase": "C",
}

INTERVAL_MAP = {
    "1m": "1min",
    "1h": "1hour",
}

# Lookback sizes
LOOKBACK_MAP = {
    "1m": 120,   # 2 hours of 1m bars
    "1h": 48,    # 48 hours of 1h bars
}

_session: Optional[aiohttp.ClientSession] = None
_rate_sem: Optional[asyncio.Semaphore]    = None
_reset_task: Optional[asyncio.Task]       = None


def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


async def _get_sem() -> asyncio.Semaphore:
    global _rate_sem, _reset_task
    if _rate_sem is None:
        _rate_sem   = asyncio.Semaphore(40)
        _reset_task = asyncio.create_task(_sem_reset_loop())
    return _rate_sem


async def _sem_reset_loop():
    while True:
        await asyncio.sleep(60)
        if _rate_sem:
            for _ in range(40):
                try:
                    _rate_sem.release()
                except ValueError:
                    break


def _build_symbol(base: str, futures: bool = True) -> str:
    code     = SYMBOL_MAP.get(base, base.replace("/", ""))
    exch     = EXCHANGE_MAP.get(COINALYZE_EXCHANGE, "A")
    suffix   = "_PERP" if futures else ""
    return f"{code}{suffix}.{exch}"


async def _fetch(endpoint: str, params: dict) -> Optional[list]:
    """Raw GET helper. Returns parsed JSON list or None on error."""
    if not COINALYZE_API_KEY:
        return None

    sem = await _get_sem()
    await sem.acquire()

    session = _get_session()
    url     = f"{BASE_URL}/{endpoint}"
    params  = {"api_key": COINALYZE_API_KEY, **params}

    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 429:
                retry = int(r.headers.get("Retry-After", 60))
                logger.warning(f"Coinalyze 429 — retry after {retry}s")
                await asyncio.sleep(retry)
                return None
            if r.status == 401:
                logger.error("Coinalyze: invalid API key")
                return None
            if r.status != 200:
                logger.error(f"Coinalyze {r.status} on {endpoint}")
                return None
            return await r.json()
    except aiohttp.ClientError as e:
        logger.error(f"Coinalyze network error: {e}")
        return None
    except Exception as e:
        logger.error(f"Coinalyze fetch error: {e}", exc_info=True)
        return None


# ── Public: OHLCV ─────────────────────────────────────────────────────────────

async def get_candles_crypto(symbol: str, interval: str, futures: bool = True) -> pd.DataFrame:
    """
    Fetch OHLCV + buy/sell volume from Coinalyze.
    Returns ET-indexed DataFrame with columns:
      Open, High, Low, Close, Volume, BuyVolume, Trades, BuyTrades
    Falls back to empty DataFrame if key not set or error.
    """
    if not COINALYZE_API_KEY:
        return pd.DataFrame()

    if interval not in INTERVAL_MAP:
        return pd.DataFrame()

    coinalyze_sym      = _build_symbol(symbol, futures)
    coinalyze_interval = INTERVAL_MAP[interval]
    lookback           = LOOKBACK_MAP.get(interval, 120)

    now      = datetime.now(UTC)
    from_ts  = int((now - timedelta(minutes=lookback)).timestamp())
    to_ts    = int(now.timestamp())

    data = await _fetch("ohlcv-history", {
        "symbols":  coinalyze_sym,
        "interval": coinalyze_interval,
        "from":     from_ts,
        "to":       to_ts,
    })

    if not data:
        return pd.DataFrame()

    history = data[0].get("history", []) if data else []
    if not history:
        return pd.DataFrame()

    df = pd.DataFrame(history).rename(columns={
        "t": "ts", "o": "Open", "h": "High", "l": "Low",
        "c": "Close", "v": "Volume", "bv": "BuyVolume",
        "tx": "Trades", "btx": "BuyTrades",
    })

    df["ts"]    = pd.to_datetime(df["ts"], unit="s", utc=True)
    df          = df.set_index("ts")
    df.index    = df.index.tz_convert(ET)

    num_cols = [c for c in ["Open", "High", "Low", "Close", "Volume", "BuyVolume", "Trades", "BuyTrades"]
                if c in df.columns]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    df           = df.dropna(subset=["Open", "High", "Low", "Close"])

    logger.debug(f"Coinalyze {coinalyze_sym}/{coinalyze_interval} → {len(df)} bars")
    return df


# ── Public: CVD Confirmation ───────────────────────────────────────────────────

async def get_cvd_confirmation(
    symbol: str,
    direction: str,
    bos_candle_time,       # pd.Timestamp or datetime
    futures: bool = True,
) -> Tuple[bool, float]:
    """
    Check buy/sell ratio on the BOS candle via Coinalyze.

    Returns:
        (confirmed: bool, buy_ratio: float)
        confirmed = True if order flow aligns with the BOS direction.
        Falls back to (True, 0.5) if no data (pass-through).
    """
    if not COINALYZE_API_KEY or CVD_MIN_RATIO <= 0:
        return True, 0.5

    df = await get_candles_crypto(symbol, "1m", futures)
    if df.empty or "BuyVolume" not in df.columns:
        return True, 0.5

    # Find the BOS candle or the one right after it
    try:
        post = df[df.index >= bos_candle_time]
        candle = post.iloc[0] if not post.empty else df.iloc[-1]
    except Exception:
        return True, 0.5

    total_vol = float(candle["Volume"]) or 1.0
    buy_vol   = float(candle.get("BuyVolume", 0))
    ratio     = buy_vol / total_vol

    if direction == "long":
        confirmed = ratio >= CVD_MIN_RATIO
    else:
        confirmed = ratio <= (1.0 - CVD_MIN_RATIO)

    logger.info(
        f"CVD confirm {symbol} {direction}: buy_ratio={ratio:.3f} "
        f"threshold={'≥' if direction == 'long' else '≤'}"
        f"{CVD_MIN_RATIO if direction == 'long' else 1-CVD_MIN_RATIO:.2f} "
        f"→ {'✅' if confirmed else '❌'}"
    )
    return confirmed, round(ratio, 3)


# ── Cleanup ────────────────────────────────────────────────────────────────────

async def cleanup():
    global _session, _rate_sem, _reset_task
    if _reset_task:
        _reset_task.cancel()
        try:
            await _reset_task
        except asyncio.CancelledError:
            pass
        _reset_task = None
    _rate_sem = None
    if _session and not _session.closed:
        await _session.close()
        _session = None
