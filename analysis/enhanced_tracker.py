"""
analysis/enhanced_tracker.py
─────────────────────────────
Enhanced trade tracker that uses Coinalyze API for precision execution data.

Fallback to OKX if Coinalyze unavailable.
Tracks buy/sell volume imbalance at entry/exit for post-trade analysis.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, Any

import pytz

from data.fetcher import get_candles as get_okx_candles
from data.coinalyze_fetcher import get_execution_candles, get_trade_context
from notifications.telegram_bot import send_outcome_alert
from config import COINALYZE_API_KEY

logger = logging.getLogger("enhanced_tracker")
ET = pytz.timezone("America/New_York")

MAX_HOLD_MINUTES = 120   # Auto-expire after 2 hours


@dataclass
class EnhancedTrade:
    symbol:    str
    direction: str
    entry:     float
    sl:        float
    tp:        float
    rr:        float
    opened_at: datetime = field(default_factory=lambda: datetime.now(ET))
    closed:    bool = False
    
    # Enhanced fields from Coinalyze
    entry_context: Optional[Dict[str, Any]] = None
    exit_context:  Optional[Dict[str, Any]] = None
    

class EnhancedTradeTracker:
    """
    Enhanced trade tracker with Coinalyze integration.
    
    Improvements over basic tracker:
      - Uses higher-quality Coinalyze data for TP/SL detection
      - Captures buy/sell volume at entry/exit
      - Records average trade size
      - Provides richer post-trade analytics
    """
    
    def __init__(self):
        self._trades: Dict[str, EnhancedTrade] = {}
        self._use_coinalyze = bool(COINALYZE_API_KEY)
        
        if self._use_coinalyze:
            logger.info("EnhancedTracker: Using Coinalyze for execution data")
        else:
            logger.info("EnhancedTracker: Coinalyze API key not set, using OKX only")
    
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
        trade = EnhancedTrade(
            symbol=symbol,
            direction=direction,
            entry=entry,
            sl=sl,
            tp=tp,
            rr=rr,
        )
        
        # Capture entry context asynchronously
        if self._use_coinalyze:
            asyncio.create_task(self._capture_entry_context(trade))
        
        self._trades[symbol] = trade
        logger.info(
            f"Tracking enhanced trade: {symbol} {direction.upper()} "
            f"entry={entry:.4f} sl={sl:.4f} tp={tp:.4f}"
        )
    
    async def _capture_entry_context(self, trade: EnhancedTrade) -> None:
        """Fetch market context at entry time using Coinalyze."""
        try:
            context = await get_trade_context(
                symbol=trade.symbol,
                entry_time=trade.opened_at,
                lookback_minutes=5,
            )
            
            if context:
                trade.entry_context = context
                buy_ratio = context.get("buy_sell_ratio", 0)
                logger.info(
                    f"{trade.symbol} | Entry context: "
                    f"buy/sell={buy_ratio:.2%} "
                    f"vol={context.get('total_volume', 0):.0f}"
                )
        except Exception as e:
            logger.error(f"Failed to capture entry context for {trade.symbol}: {e}")
    
    async def check_outcomes(self) -> None:
        """Poll all active trades and fire outcome alerts."""
        if not self._trades:
            return
        
        open_trades = [t for t in self._trades.values() if not t.closed]
        if not open_trades:
            return
        
        tasks = [self._check_trade(t) for t in open_trades]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _check_trade(self, trade: EnhancedTrade) -> None:
        """Check if TP/SL hit using Coinalyze data (fallback to OKX)."""
        try:
            # Try Coinalyze first for better precision
            if self._use_coinalyze:
                candles = await get_execution_candles(
                    symbol=trade.symbol,
                    interval="1m",
                    lookback_minutes=15,
                )
                
                # Fallback to OKX if Coinalyze fails
                if candles.empty:
                    logger.debug(f"{trade.symbol} | Coinalyze unavailable, using OKX")
                    candles = await asyncio.to_thread(get_okx_candles, trade.symbol, "1m", "1d")
            else:
                candles = await asyncio.to_thread(get_okx_candles, trade.symbol, "1m", "1d")
            
            if candles.empty:
                return
            
            # Only look at candles formed after entry
            post_entry = candles[candles.index > trade.opened_at]
            if post_entry.empty:
                return
            
            # Check TP/SL hits
            for idx, row in post_entry.iterrows():
                high = float(row["High"])
                low  = float(row["Low"])
                
                if trade.direction == "long":
                    if high >= trade.tp:
                        await self._close(trade, "win", trade.tp, idx)
                        return
                    if low <= trade.sl:
                        await self._close(trade, "loss", trade.sl, idx)
                        return
                else:
                    if low <= trade.tp:
                        await self._close(trade, "win", trade.tp, idx)
                        return
                    if high >= trade.sl:
                        await self._close(trade, "loss", trade.sl, idx)
                        return
            
            # Check max hold time
            elapsed = datetime.now(ET) - trade.opened_at
            if elapsed >= timedelta(minutes=MAX_HOLD_MINUTES):
                current_price = float(candles["Close"].iloc[-1])
                await self._close(trade, "expired", current_price, candles.index[-1])
        
        except Exception as e:
            logger.error(f"Enhanced tracker error for {trade.symbol}: {e}", exc_info=True)
    
    async def _close(
        self,
        trade: EnhancedTrade,
        outcome: str,
        exit_price: float,
        exit_time: datetime,
    ) -> None:
        """Close trade and capture exit context."""
        trade.closed = True
        
        # Capture exit context if using Coinalyze
        if self._use_coinalyze:
            try:
                exit_ctx = await get_trade_context(
                    symbol=trade.symbol,
                    entry_time=exit_time,
                    lookback_minutes=5,
                )
                trade.exit_context = exit_ctx
            except Exception as e:
                logger.error(f"Failed to capture exit context: {e}")
        
        del self._trades[trade.symbol]
        
        # Calculate P&L
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
        
        # Build enhanced log message
        log_msg = (
            f"Enhanced trade closed: {trade.symbol} | {outcome.upper()} | "
            f"exit={exit_price:.4f} | {r_multiple:+.2f}R | {minutes}m"
        )
        
        # Add buy/sell ratio if available
        if trade.entry_context:
            entry_buy = trade.entry_context.get("buy_sell_ratio", 0)
            log_msg += f" | entry_buy={entry_buy:.1%}"
        
        if trade.exit_context:
            exit_buy = trade.exit_context.get("buy_sell_ratio", 0)
            log_msg += f" exit_buy={exit_buy:.1%}"
        
        logger.info(log_msg)
        
        # Send Telegram alert
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
    
    def get_trade(self, symbol: str) -> Optional[EnhancedTrade]:
        """Get active trade for symbol."""
        return self._trades.get(symbol)
    
    def has_active_trade(self, symbol: str) -> bool:
        """Check if symbol has an active trade."""
        return symbol in self._trades and not self._trades[symbol].closed
