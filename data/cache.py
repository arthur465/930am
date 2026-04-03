"""
data/cache.py
─────────────
TTL cache + rate limiter for OKX API calls.

Problem solved:
    scanner.py calls get_candles() 4-5x per symbol per scan cycle.
    6 symbols = 24-30 API calls/min → unnecessary load.

Solution:
    - Cache every response for TTL seconds (default 45s for 1m, 120s for 1h)
    - Rate limiter: max N calls per minute with token bucket
    - Duplicate calls within TTL hit cache instantly (0 API calls)

Result:
    First scan cycle: 1 real call per (symbol, timeframe)
    Subsequent cycles: 0 calls if within TTL
    Total real calls per minute: ~6-8 instead of 24-30
"""

import asyncio
import logging
import time
from typing import Optional

import pandas as pd

logger = logging.getLogger("cache")

# TTL per timeframe — how long a response stays valid (seconds)
# 1m data: 50s (slightly under 1 candle period)
# 3m data: 185s (3m candles literally can't change faster — saves huge API budget)
# 1h data: 300s (rarely changes mid-scan)
TTL_MAP = {
    "1m": 50,
    "3m": 185,
    "1h": 300,
}
DEFAULT_TTL = 60

# Rate limiter — OKX public endpoints = 20 calls per 2s = 600/min
# Using 30/60 to stay safely under with plenty of headroom
RATE_LIMIT_CALLS  = 30    # max real API calls per window
RATE_LIMIT_WINDOW = 60.0  # seconds


class _CacheEntry:
    __slots__ = ("data", "expires_at")
    def __init__(self, data: pd.DataFrame, ttl: float):
        self.data       = data
        self.expires_at = time.monotonic() + ttl

    def is_valid(self) -> bool:
        return time.monotonic() < self.expires_at


class CandleCache:
    """
    Drop-in cache wrapper around get_candles().
    Thread/async-safe via asyncio.Lock per key.
    """

    def __init__(self):
        self._cache: dict[str, _CacheEntry] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

        # Token bucket rate limiter
        self._tokens      = float(RATE_LIMIT_CALLS)
        self._last_refill = time.monotonic()

        # Global 429 backoff — when one call hits rate limit, ALL calls pause
        self._backoff_until: float = 0.0
        self._backoff_seconds: float = 15.0

    # ── Public API ────────────────────────────────────────────────────────────

    async def get(
        self,
        symbol: str,
        interval: str,
        fetch_fn,           # the real get_candles coroutine
        period: str = "1d",
        force: bool = False,
    ) -> pd.DataFrame:
        """
        Return cached candles if valid, else fetch and cache.

        Args:
            symbol:   ticker symbol
            interval: "1m" | "3m" | "1h"
            fetch_fn: async function(symbol, interval, period) → DataFrame
            period:   passed through to fetch_fn
            force:    bypass cache and force a fresh fetch
        """
        key = f"{symbol}:{interval}"

        # Per-key lock prevents duplicate in-flight requests
        async with self._global_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
        lock = self._locks[key]

        async with lock:
            entry = self._cache.get(key)
            if not force and entry and entry.is_valid():
                logger.debug(f"[CACHE HIT]  {key}")
                return entry.data

            # Respect global 429 backoff before attempting any real call
            now = time.monotonic()
            if now < self._backoff_until:
                wait = self._backoff_until - now
                logger.warning(f"[GLOBAL BACKOFF] {key} — waiting {wait:.1f}s (429 cooldown)")
                await asyncio.sleep(wait)

            # Rate limit before real call
            await self._wait_for_token(key)

            logger.debug(f"[CACHE MISS] {key} — fetching from API")
            data = await fetch_fn(symbol, interval, period)

            # If fetcher returned empty due to 429, set global backoff and return stale cache
            if data.empty and entry:
                logger.warning(f"[CACHE STALE] {key} — using stale data after failed fetch")
                self._backoff_until = time.monotonic() + self._backoff_seconds
                return entry.data

            ttl = TTL_MAP.get(interval, DEFAULT_TTL)
            self._cache[key] = _CacheEntry(data, ttl)
            return data

    def trigger_backoff(self):
        """Call this when a 429 is received — pauses all subsequent cache misses."""
        self._backoff_until = time.monotonic() + self._backoff_seconds
        logger.warning(f"[GLOBAL BACKOFF SET] All API calls paused for {self._backoff_seconds:.0f}s")

    def invalidate(self, symbol: str = None, interval: str = None):
        """Manually invalidate cache entries."""
        if symbol and interval:
            self._cache.pop(f"{symbol}:{interval}", None)
        elif symbol:
            keys = [k for k in self._cache if k.startswith(f"{symbol}:")]
            for k in keys:
                self._cache.pop(k, None)
        else:
            self._cache.clear()

    def stats(self) -> dict:
        now = time.monotonic()
        total  = len(self._cache)
        valid  = sum(1 for e in self._cache.values() if e.is_valid())
        return {"total_keys": total, "valid": valid, "expired": total - valid}

    # ── Rate limiter (token bucket) ───────────────────────────────────────────

    async def _wait_for_token(self, key: str):
        """Block until a rate limit token is available."""
        while True:
            now = time.monotonic()
            elapsed = now - self._last_refill
            # Refill tokens proportionally to time passed
            self._tokens = min(
                float(RATE_LIMIT_CALLS),
                self._tokens + elapsed * (RATE_LIMIT_CALLS / RATE_LIMIT_WINDOW)
            )
            self._last_refill = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return

            wait_time = (1.0 - self._tokens) / (RATE_LIMIT_CALLS / RATE_LIMIT_WINDOW)
            logger.info(f"[RATE LIMIT] {key} — waiting {wait_time:.1f}s for token")
            await asyncio.sleep(wait_time + 0.1)


# ── Module-level singleton ────────────────────────────────────────────────────
_cache = CandleCache()


async def get_candles_cached(
    symbol: str,
    interval: str,
    period: str = "1d",
    force: bool = False,
) -> pd.DataFrame:
    """
    Cached drop-in replacement for get_candles().
    Import this instead of get_candles() in scanner.py.

    Usage:
        from data.cache import get_candles_cached as get_candles
    """
    from data.fetcher import get_candles as _real_fetch
    return await _cache.get(symbol, interval, _real_fetch, period, force)


def get_cache_stats() -> dict:
    return _cache.stats()


def invalidate_cache(symbol: str = None, interval: str = None):
    _cache.invalidate(symbol, interval)
