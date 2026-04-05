"""
Nitro BOS+FVG Scanner — Main
Strategy: Opening Range → BOS → FVG retest (1m/3m) → Paper Trade
Session:  9:30–11:00 AM ET only. Sleeps outside that window.
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
    """
    Returns seconds until next 9:30 AM ET session open.
    If we're already past 11am, sleep until 9:30 tomorrow.
    """
    target = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    if now_et >= target:
        target += timedelta(days=1)
    # Crypto runs 24/7 - no weekend skip
    return (target - now_et).total_seconds()


def _in_session(now_et: datetime) -> bool:
    # Crypto runs 24/7 - no weekend restriction
    h, m = now_et.hour, now_et.minute
    after_open  = (h == 9 and m >= 30) or h >= 10
    before_stop = h < SCAN_END_HOUR or (h == SCAN_END_HOUR and m <= SCAN_END_MIN)
    return after_open and before_stop


async def main():
    logger.info("=" * 55)
    logger.info("  Nitro BOS + FVG Scanner")
    logger.info("  Entry: FVG retest on 1m or 3m TF")
    logger.info("  Session: 9:30 – 11:00 AM ET (7 days/week)")
    logger.info("=" * 55)

    await send_startup()
    scanner = NitroScanner()
    session_closed = False

    while True:
        now_et = datetime.now(ET)

        if _in_session(now_et):
            session_closed = False
            logger.info(f"Scanning... {now_et.strftime('%I:%M:%S %p ET')}")
            await scanner.scan()
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

        else:
            # Session just ended — close trades, send stats, then sleep
            if not session_closed:
                logger.info("Session ended — closing trades + sending stats")
                await scanner.close_session()
                summary = format_summary()
                logger.info(f"\n{summary}")
                session_closed = True

            secs = _seconds_until_session(now_et)
            hours = int(secs // 3600)
            mins  = int((secs % 3600) // 60)
            logger.info(f"Outside session — sleeping {hours}h {mins}m until next open")
            # Sleep in chunks so we wake up right at 9:30
            await asyncio.sleep(min(secs, 60))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown.")
        # Clean up connections
        from data.fetcher import cleanup as cleanup_okx
        from data.coinalyze_fetcher import cleanup as cleanup_coinalyze
        asyncio.run(cleanup_okx())
        asyncio.run(cleanup_coinalyze())
