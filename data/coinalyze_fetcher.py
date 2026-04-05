"""
data/coinalyze_fetcher.py
──────────────────────────
Fetches high-quality candlestick data from Coinalyze API.
Used for precise trade execution tracking (entry/exit points).

Coinalyze provides professional-grade OHLCV data with:
  - Better data quality than free exchange APIs
  - Accurate tick-level precision
  - Buy/sell volume breakdown for order flow analysis
  - Clean, reliable candles

API Docs: https://api.coinalyze.net/v1/doc/
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import aiohttp
import pandas as pd
import pytz

from config import COINALYZE_API_KEY, COINALYZE_EXCHANGE

logger = logging.getLogger("coinalyze")
ET = pytz.timezone("America/New_York")
UTC = pytz.utc

# Coinalyze API base URL
BASE_URL = "https://api.coinalyze.net/v1"

# Symbol mapping: OKX format -> Coinalyze symbol format
# Format: SYMBOL_PERP.EXCHANGE or SYMBOL.EXCHANGE
# Examples: BTCUSDT_PERP.A (Binance futures), BTCUSDT.A (Binance spot)
SYMBOL_MAP = {
    "BTC/USDT": "BTCUSDT",
    "ETH/USDT": "ETHUSDT", 
    "SOL/USDT": "SOLUSDT",
    "AVAX/USDT": "AVAXUSDT",
    "LINK/USDT": "LINKUSDT",
    "ARB/USDT": "ARBUSDT",
}

# Exchange codes (from /v1/exchanges endpoint)
EXCHANGE_MAP = {
    "binance": "A",
    "okx": "0",
    "bybit": "1",
    "coinbase": "C",
}

# Interval mapping (Coinalyze uses: 1min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 12hour, daily)
INTERVAL_MAP = {
    "1m": "1min",
    "3m": "1min",  # Coinalyze doesn't have 3m, use 1m and aggregate if needed
    "5m": "5min",
    "15m": "15min",
    "1h": "1hour",
}

# Rate limit: 40 calls per minute
_rate_limiter = None
_session: Optional[aiohttp.ClientSession] = None


class RateLimiter:
    """Simple rate limiter for Coinalyze API (40 calls/min)."""
    
    def __init__(self, calls_per_minute: int = 40):
        self.calls_per_minute = calls_per_minute
        self.semaphore = asyncio.Semaphore(calls_per_minute)
        self.reset_task = None
        
    async def start(self):
        """Start the rate limiter reset loop."""
        self.reset_task = asyncio.create_task(self._reset_loop())
        
    async def _reset_loop(self):
        """Reset semaphore every minute."""
        while True:
            await asyncio.sleep(60)
            # Release all permits
            for _ in range(self.calls_per_minute):
                try:
                    self.semaphore.release()
                except ValueError:
                    break
                    
    async def acquire(self):
        """Acquire a permit to make an API call."""
        await self.semaphore.acquire()
        
    async def stop(self):
        """Stop the rate limiter."""
        if self.reset_task:
            self.reset_task.cancel()
            try:
                await self.reset_task
            except asyncio.CancelledError:
                pass


def _get_session() -> aiohttp.ClientSession:
    """Get or create aiohttp session."""
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


async def _get_rate_limiter() -> RateLimiter:
    """Get or create rate limiter."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiter(calls_per_minute=40)
        await _rate_limiter.start()
    return _rate_limiter


def _build_symbol(base_symbol: str, is_futures: bool = True) -> str:
    """
    Convert OKX symbol format to Coinalyze format.
    
    Args:
        base_symbol: Symbol in OKX format (e.g., "BTC/USDT")
        is_futures: True for futures (_PERP suffix), False for spot
    
    Returns:
        Coinalyze symbol format (e.g., "BTCUSDT_PERP.A")
    """
    base = SYMBOL_MAP.get(base_symbol, base_symbol.replace("/", ""))
    exchange_code = EXCHANGE_MAP.get(COINALYZE_EXCHANGE, "A")
    
    if is_futures:
        return f"{base}_PERP.{exchange_code}"
    else:
        return f"{base}.{exchange_code}"


async def get_execution_candles(
    symbol: str,
    interval: str,
    lookback_minutes: int = 10,
    is_futures: bool = True,
) -> pd.DataFrame:
    """
    Fetch recent high-quality OHLCV candles for trade execution analysis.
    
    Args:
        symbol: Symbol in OKX format (e.g., "BTC/USDT")
        interval: Timeframe (1m, 5m, 15m, 1h)
        lookback_minutes: How many minutes of data to fetch
        is_futures: True for futures market, False for spot
    
    Returns:
        DataFrame with columns: Open, High, Low, Close, Volume, BuyVolume, Trades, BuyTrades
        Index is ET-aware datetime
    """
    if not COINALYZE_API_KEY:
        logger.warning("COINALYZE_API_KEY not set — skipping enhanced execution data")
        return pd.DataFrame()
    
    if interval not in INTERVAL_MAP:
        logger.warning(f"Unsupported interval: {interval}")
        return pd.DataFrame()
    
    coinalyze_symbol = _build_symbol(symbol, is_futures)
    coinalyze_interval = INTERVAL_MAP[interval]
    
    # Calculate time range
    now = datetime.now(UTC)
    from_time = now - timedelta(minutes=lookback_minutes)
    
    # Convert to UNIX timestamps (seconds)
    from_ts = int(from_time.timestamp())
    to_ts = int(now.timestamp())
    
    try:
        # Rate limiting
        limiter = await _get_rate_limiter()
        await limiter.acquire()
        
        # Make API request
        session = _get_session()
        url = f"{BASE_URL}/ohlcv-history"
        params = {
            "api_key": COINALYZE_API_KEY,
            "symbols": coinalyze_symbol,
            "interval": coinalyze_interval,
            "from": from_ts,
            "to": to_ts,
        }
        
        async with session.get(url, params=params) as response:
            if response.status == 429:
                # Rate limit hit
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Coinalyze rate limit — retry after {retry_after}s")
                await asyncio.sleep(retry_after)
                return pd.DataFrame()
            
            if response.status == 401:
                logger.error("Coinalyze: Invalid API key")
                return pd.DataFrame()
            
            if response.status != 200:
                logger.error(f"Coinalyze API error: {response.status}")
                return pd.DataFrame()
            
            data = await response.json()
            
            if not data or len(data) == 0:
                logger.warning(f"Coinalyze: No data for {coinalyze_symbol}/{coinalyze_interval}")
                return pd.DataFrame()
            
            # Parse response: [{"symbol": "...", "history": [{t, o, h, l, c, v, bv, tx, btx}, ...]}]
            symbol_data = data[0]
            history = symbol_data.get("history", [])
            
            if not history:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(history)
            
            # Rename columns to match our standard format
            df = df.rename(columns={
                "t": "ts",
                "o": "Open",
                "h": "High", 
                "l": "Low",
                "c": "Close",
                "v": "Volume",
                "bv": "BuyVolume",
                "tx": "Trades",
                "btx": "BuyTrades",
            })
            
            # Convert timestamp to datetime
            df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True)
            df = df.set_index("ts")
            df.index = df.index.tz_convert(ET)
            
            # Ensure numeric types
            numeric_cols = ["Open", "High", "Low", "Close", "Volume", "BuyVolume", "Trades", "BuyTrades"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            df = df.dropna()
            
            logger.debug(f"Coinalyze: {coinalyze_symbol}/{coinalyze_interval} → {len(df)} bars")
            return df
    
    except aiohttp.ClientError as e:
        logger.error(f"Coinalyze network error: {e}")
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"Coinalyze fetch error: {e}", exc_info=True)
        return pd.DataFrame()


async def get_trade_context(
    symbol: str,
    entry_time: datetime,
    lookback_minutes: int = 5,
    is_futures: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Get detailed market context around a trade entry using Coinalyze data.
    
    Returns dict with:
        - entry_candle: The candle where trade was entered
        - buy_sell_ratio: Ratio of buy volume to total volume
        - avg_trade_size: Average trade size
        - recent_candles: Last N candles for context
    """
    df = await get_execution_candles(
        symbol=symbol,
        interval="1m",
        lookback_minutes=lookback_minutes,
        is_futures=is_futures,
    )
    
    if df.empty:
        return None
    
    # Find the candle closest to entry time
    entry_time_et = entry_time.astimezone(ET) if entry_time.tzinfo else ET.localize(entry_time)
    
    # Get candles around entry
    mask = df.index <= entry_time_et
    if not mask.any():
        return None
    
    entry_candle = df[mask].iloc[-1]
    
    # Calculate buy/sell metrics
    total_vol = float(entry_candle["Volume"]) if entry_candle["Volume"] > 0 else 1.0
    buy_vol = float(entry_candle.get("BuyVolume", 0))
    buy_ratio = buy_vol / total_vol if total_vol > 0 else 0.5
    
    # Calculate average trade size
    total_trades = float(entry_candle.get("Trades", 0))
    avg_size = total_vol / total_trades if total_trades > 0 else 0
    
    return {
        "entry_candle": entry_candle.to_dict(),
        "buy_sell_ratio": buy_ratio,
        "avg_trade_size": avg_size,
        "total_volume": total_vol,
        "buy_volume": buy_vol,
        "sell_volume": total_vol - buy_vol,
        "recent_candles": df.tail(5).to_dict("records"),
    }


async def cleanup():
    """Close aiohttp session and stop rate limiter."""
    global _session, _rate_limiter
    
    if _rate_limiter:
        await _rate_limiter.stop()
        _rate_limiter = None
    
    if _session and not _session.closed:
        await _session.close()
        _session = None