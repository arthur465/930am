"""
analysis/trade_tracker.py
After an alert fires, tracks the live trade and notifies when:
  - TP is hit → 🎯 WIN
  - SL is hit → 🛑 LOSS
  - Max hold time expires (2 hours) → ⏰ EXPIRED (no result)

One active trade per symbol at a time.
"""
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional

import pytz

from data.fetcher import get_candles
from notifications.telegram_bot import send_outcome_alert

logger = logging.getLogger("tracker")
ET = pytz.timezone("America/New_York")

MAX_HOLD_MINUTES = 120   # Auto-expire after 2 hours if neither TP nor SL hit


@dataclass
class ActiveTrade:
    symbol:    str
    direction: str
    entry:     float
    sl:        float
    tp:        float
    rr:        float
    opened_at: datetime = field(default_factory=lambda: datetime.now(ET))
    closed:    bool = False


class TradeTracker:

    def __init__(self):
        self._trades: Dict[str, ActiveTrade] = {}

    def register(
        self,
        symbol: str,
        direction: str,
        entry: float,
        sl: float,
        tp: float,
        rr: float,
    ) -> None:
        """Called right after an alert fires."""
        self._trades[symbol] = ActiveTrade(
            symbol=symbol,
            direction=direction,
            entry=entry,
            sl=sl,
            tp=tp,
            rr=rr,
        )
        logger.info(f"Tracking trade: {symbol} {direction.upper()} entry={entry:.4f} sl={sl:.4f} tp={tp:.4f}")

    async def check_outcomes(self) -> None:
        """Poll all active trades and fire outcome alerts."""
        if not self._trades:
            return

        open_trades = [t for t in self._trades.values() if not t.closed]
        if not open_trades:
            return

        tasks = [self._check_trade(t) for t in open_trades]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _check_trade(self, trade: ActiveTrade) -> None:
        try:
            candles = await asyncio.to_thread(get_candles, trade.symbol, "1m", "1d")
            if candles.empty:
                return

            # Only look at candles formed after entry
            post_entry = candles[candles.index > trade.opened_at]
            if post_entry.empty:
                return

            # Use high/low of each candle to determine hits
            for _, row in post_entry.iterrows():
                high = float(row["High"])
                low  = float(row["Low"])

                if trade.direction == "long":
                    if high >= trade.tp:
                        await self._close(trade, "win", trade.tp)
                        return
                    if low <= trade.sl:
                        await self._close(trade, "loss", trade.sl)
                        return
                else:
                    if low <= trade.tp:
                        await self._close(trade, "win", trade.tp)
                        return
                    if high >= trade.sl:
                        await self._close(trade, "loss", trade.sl)
                        return

            # Check max hold time
            elapsed = datetime.now(ET) - trade.opened_at
            if elapsed >= timedelta(minutes=MAX_HOLD_MINUTES):
                current_price = float(candles["Close"].iloc[-1])
                await self._close(trade, "expired", current_price)

        except Exception as e:
            logger.error(f"Trade tracker error for {trade.symbol}: {e}", exc_info=True)

    async def _close(self, trade: ActiveTrade, outcome: str, exit_price: float) -> None:
        trade.closed = True
        del self._trades[trade.symbol]

        # Calculate actual P&L in R multiples
        risk = abs(trade.entry - trade.sl)
        if risk > 0:
            if trade.direction == "long":
                r_multiple = (exit_price - trade.entry) / risk
            else:
                r_multiple = (trade.entry - exit_price) / risk
        else:
            r_multiple = 0.0

        duration = datetime.now(ET) - trade.opened_at
        minutes  = int(duration.total_seconds() / 60)

        logger.info(
            f"Trade closed: {trade.symbol} | {outcome.upper()} | "
            f"exit={exit_price:.4f} | {r_multiple:+.2f}R | {minutes}m"
        )

        await send_outcome_alert(
            symbol=trade.symbol,
            direction=trade.direction,
            entry=trade.entry,
            exit_price=exit_price,
            sl=trade.sl,
            tp=trade.tp,
            outcome=outcome,
            r_multiple=r_multiple,
            duration_minutes=minutes,
        )
