"""
Nitro BOS Scanner — Main
Strategy: OR (9:30–9:40) → Volatility → BOS → Enter immediately
Assets:   AAPL, GOOGL, AMZN, TSLA, NVDA, SPY, QQQ, BTC, ETH
Session:  9:30 – 11:00 AM ET (stocks: Mon–Fri only via yfinance; crypto: daily)
"""
import asyncio
import logging
import sys
from datetime import datetime, timedelta

import pytz

from config import SCAN_INTERVAL_SECONDS, SCAN_END_HOUR, SCAN_END_MIN
from analysis.scanner import NitroScanner
from notifications.telegram_bot import send_startup, send_stats
from execution.stats_tracker import format_summary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")
ET = pytz.timezone("America/New_York")


def _seconds_until_session(now_et: datetime) -> float:
    target = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    if now_et >= target:
        target += timedelta(days=1)
    return (target - now_et).total_seconds()


def _in_session(now_et: datetime) -> bool:
    h, m = now_et.hour, now_et.minute
    after_open  = (h == 9 and m >= 30) or h >= 10
    before_stop = h < SCAN_END_HOUR or (h == SCAN_END_HOUR and m <= SCAN_END_MIN)
    return after_open and before_stop


async def main():
    logger.info("=" * 55)
    logger.info("  Nitro BOS Scanner")
    logger.info("  Strategy: OR → Vol → BOS → Enter (no FVG)")
    logger.info("  Assets: AAPL GOOGL AMZN TSLA NVDA SPY QQQ BTC ETH")
    logger.info("  Session: 9:30 – 11:00 AM ET")
    logger.info("=" * 55)

    await send_startup()
    scanner       = NitroScanner()
    session_closed = False

    while True:
        now_et = datetime.now(ET)

        if _in_session(now_et):
            session_closed = False
            logger.info(f"Scanning... {now_et.strftime('%I:%M:%S %p ET')}")
            await scanner.scan()
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

        else:
            if not session_closed:
                logger.info("Session ended — closing trades + sending stats")
                await scanner.close_session()
                logger.info(f"\n{format_summary()}")
                session_closed = True

            secs  = _seconds_until_session(now_et)
            hours = int(secs // 3600)
            mins  = int((secs % 3600) // 60)
            logger.info(f"Outside session — sleeping {hours}h {mins}m until next open")
            await asyncio.sleep(min(secs, 60))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown.")
        from data.fetcher import cleanup as cleanup_fetcher
        from data.coinalyze_fetcher import cleanup as cleanup_coinalyze
        asyncio.run(cleanup_fetcher())
        asyncio.run(cleanup_coinalyze())
