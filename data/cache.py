"""
data/cache.py
─────────────
TTL cache + rate limiter for candle fetch calls.
Prevents hammering APIs on every 60s scan cycle.
"""

import asyncio
import logging
import time

import pandas as pd

logger = logging.getLogger("cache")

TTL_MAP = {
    "1m": 50,    # just under one candle period
    "1h": 300,   # rarely changes mid-scan
}
DEFAULT_TTL = 60

RATE_LIMIT_CALLS  = 20
RATE_LIMIT_WINDOW = 60.0
BACKOFF_SECONDS   = 30.0


class _CacheEntry:
    __slots__ = ("data", "expires_at")
    def __init__(self, data: pd.DataFrame, ttl: float):
        self.data       = data
        self.expires_at = time.monotonic() + ttl

    def is_valid(self) -> bool:
        return time.monotonic() < self.expires_at


class CandleCache:

    def __init__(self):
        self._cache: dict[str, _CacheEntry] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._global_lock   = asyncio.Lock()
        self._tokens        = float(RATE_LIMIT_CALLS)
        self._last_refill   = time.monotonic()
        self._backoff_until = 0.0

    async def get(self, symbol, interval, fetch_fn, period="1d", force=False) -> pd.DataFrame:
        key = f"{symbol}:{interval}"

        async with self._global_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
        lock = self._locks[key]

        async with lock:
            entry = self._cache.get(key)
            if not force and entry and entry.is_valid():
                logger.debug(f"[CACHE HIT]  {key}")
                return entry.data

            now = time.monotonic()
            if now < self._backoff_until:
                wait = self._backoff_until - now
                logger.warning(f"[BACKOFF] {key} — waiting {wait:.1f}s")
                await asyncio.sleep(wait)

            await self._wait_for_token(key)

            logger.debug(f"[CACHE MISS] {key} — fetching")
            data = await fetch_fn(symbol, interval, period)

            if data.empty and entry:
                logger.warning(f"[STALE]  {key} — serving stale after failed fetch")
                self._backoff_until = time.monotonic() + BACKOFF_SECONDS
                return entry.data

            ttl = TTL_MAP.get(interval, DEFAULT_TTL)
            self._cache[key] = _CacheEntry(data, ttl)
            return data

    def trigger_backoff(self):
        self._backoff_until = time.monotonic() + BACKOFF_SECONDS
        logger.warning(f"[GLOBAL BACKOFF SET] paused {BACKOFF_SECONDS:.0f}s")

    def invalidate(self, symbol=None, interval=None):
        if symbol and interval:
            self._cache.pop(f"{symbol}:{interval}", None)
        elif symbol:
            for k in [k for k in self._cache if k.startswith(f"{symbol}:")]:
                self._cache.pop(k, None)
        else:
            self._cache.clear()

    def stats(self) -> dict:
        total = len(self._cache)
        valid = sum(1 for e in self._cache.values() if e.is_valid())
        return {"total_keys": total, "valid": valid, "expired": total - valid}

    async def _wait_for_token(self, key: str):
        while True:
            now     = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                float(RATE_LIMIT_CALLS),
                self._tokens + elapsed * (RATE_LIMIT_CALLS / RATE_LIMIT_WINDOW),
            )
            self._last_refill = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            wait = (1.0 - self._tokens) / (RATE_LIMIT_CALLS / RATE_LIMIT_WINDOW)
            logger.info(f"[RATE LIMIT] {key} — waiting {wait:.1f}s")
            await asyncio.sleep(wait + 0.1)


_cache = CandleCache()


async def get_candles_cached(symbol: str, interval: str, period: str = "1d", force: bool = False) -> pd.DataFrame:
    from data.fetcher import get_candles as _real_fetch
    return await _cache.get(symbol, interval, _real_fetch, period, force)


def get_cache_stats() -> dict:
    return _cache.stats()


def invalidate_cache(symbol=None, interval=None):
    _cache.invalidate(symbol, interval)
