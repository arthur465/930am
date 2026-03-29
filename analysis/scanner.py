"""
analysis/scanner.py
────────────────────
Per-symbol state machine. Runs through each step in order:
  Volatility → OR → BOS → FVG → Retest → RR → Alert → Paper Trade → Track

Entry executes on the FVG retest, on whatever TF the best FVG was found (3m > 1m).
"""
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, Optional

import pytz

from config import ALL_SYMBOLS, MIN_RR, OR_END_HOUR, OR_END_MIN
from data.fetcher import get_candles
from analysis.opening_range import build_opening_range, OpeningRange
from analysis.bos_detector import detect_bos, BOSResult
from analysis.fvg_detector import (
    find_fvgs, get_best_fvg, FVG,
    is_retesting, get_1h_tp, calculate_rr,
)
from analysis.volatility import or_range_is_valid, atr_is_valid
from execution.paper_trader import PaperTrader
from execution.stats_tracker import record_trade, format_summary
from notifications.telegram_bot import send_setup_alert, send_outcome_alert, send_stats

logger = logging.getLogger("scanner")
ET = pytz.timezone("America/New_York")


@dataclass
class SymbolState:
    opening_range: Optional[OpeningRange] = None
    bos:           Optional[BOSResult]    = None
    active_fvg:    Optional[FVG]          = None
    alerted:       bool                   = False
    skipped_vol:   bool                   = False


class NitroScanner:

    def __init__(self):
        self._states: Dict[str, SymbolState] = {}
        self._last_reset: Optional[date]     = None
        self._paper = PaperTrader()
        self._reset_if_new_day()

    def _reset_if_new_day(self):
        today = datetime.now(ET).date()
        if today != self._last_reset:
            logger.info(f"Daily reset — {today}")
            self._states     = {sym: SymbolState() for sym in ALL_SYMBOLS}
            self._last_reset = today

    async def scan(self):
        self._reset_if_new_day()
        now_et = datetime.now(ET)

        for sym in ALL_SYMBOLS:
            await self._scan_symbol(sym, now_et)
            await asyncio.sleep(0.3)

    async def close_session(self):
        """Called at 11am — force close any open paper trades and send stats."""
        logger.info("Session ending — closing all open trades")
        # get latest price for each symbol to close at market
        for sym in ALL_SYMBOLS:
            if self._paper.already_in_trade(sym):
                try:
                    candles = await get_candles(sym, "1m")
                    if not candles.empty:
                        price = float(candles["Close"].iloc[-1])
                        results = self._paper.force_close_all(price)
                        for r in results:
                            record_trade(r)
                            await send_outcome_alert(**r)
                except Exception as e:
                    logger.error(f"Force close error {sym}: {e}")

        summary = format_summary()
        await send_stats(summary)

    async def _scan_symbol(self, symbol: str, now_et: datetime):
        state = self._states[symbol]

        if state.alerted or state.skipped_vol:
            # Still check open trades for TP/SL even after alert fired
            await self._check_trade_outcome(symbol)
            return

        try:
            candles_1m = await get_candles(symbol, "1m")
            if candles_1m.empty:
                return

            # ── Step 1: Build opening range (9:30–9:39) ─────────────────────
            if state.opening_range is None:
                result = build_opening_range(candles_1m)
                if result:
                    state.opening_range = result
                    logger.info(f"{symbol} | OR built: H={result.high:.2f}  L={result.low:.2f}")
                return

            # ── Wait for OR window to close (9:40) ──────────────────────────
            if now_et.hour < OR_END_HOUR or (now_et.hour == OR_END_HOUR and now_et.minute < OR_END_MIN):
                return

            # ── Step 2: Volatility check ─────────────────────────────────────
            if state.bos is None and not state.skipped_vol:
                or_valid, or_range_pct = or_range_is_valid(
                    state.opening_range.high, state.opening_range.low
                )
                if not or_valid:
                    state.skipped_vol = True
                    logger.info(f"{symbol} | Skipped — OR too tight ({or_range_pct:.3f}%)")
                    return

                candles_1h = await get_candles(symbol, "1h")
                atr_valid, atr_ratio = atr_is_valid(candles_1h)
                if not atr_valid:
                    state.skipped_vol = True
                    logger.info(f"{symbol} | Skipped — ATR compressed (ratio={atr_ratio:.2f})")
                    return

                logger.info(f"{symbol} | Vol OK — OR={or_range_pct:.3f}%  ATR ratio={atr_ratio:.2f}")

            # ── Step 3: Detect BOS ───────────────────────────────────────────
            if state.bos is None:
                bos = detect_bos(candles_1m, state.opening_range.high, state.opening_range.low)
                if bos:
                    state.bos = bos
                    logger.info(f"{symbol} | BOS {bos.direction.upper()} @ {bos.bos_price:.2f}")
                return

            # ── Step 4: Find best FVG after BOS ─────────────────────────────
            candles_3m = await get_candles(symbol, "3m")
            fvgs_1m = find_fvgs(candles_1m, state.bos.bos_candle_time, state.bos.direction, "1m")
            fvgs_3m = find_fvgs(candles_3m, state.bos.bos_candle_time, state.bos.direction, "3m") \
                      if not candles_3m.empty else []

            best_fvg = get_best_fvg(fvgs_1m, fvgs_3m)
            if not best_fvg:
                return

            state.active_fvg = best_fvg

            # ── Step 5: Wait for retest on the FVG's timeframe ──────────────
            current_price = float(candles_1m["Close"].iloc[-1])
            if not is_retesting(current_price, best_fvg):
                logger.debug(
                    f"{symbol} | {best_fvg.timeframe} FVG awaiting retest "
                    f"[price={current_price:.2f}  fvg={best_fvg.bottom:.2f}–{best_fvg.top:.2f}]"
                )
                return

            # ── Step 6: RR check ─────────────────────────────────────────────
            candles_1h = await get_candles(symbol, "1h")
            tp = get_1h_tp(candles_1h, current_price, state.bos.direction)
            if tp is None:
                logger.info(f"{symbol} | Retest confirmed but no 1H TP level found")
                return

            rr = calculate_rr(current_price, best_fvg, tp, state.bos.direction)
            if rr < MIN_RR:
                logger.info(f"{symbol} | RR {rr:.1f} < {MIN_RR} min — skipping")
                return

            # ── ALL CONDITIONS MET ───────────────────────────────────────────
            state.alerted = True
            sl = best_fvg.bottom if state.bos.direction == "long" else best_fvg.top

            logger.info(
                f"🚨 ALERT: {symbol} {state.bos.direction.upper()} "
                f"entry={current_price:.2f}  sl={sl:.2f}  tp={tp:.2f}  rr={rr:.1f}R  "
                f"FVG={best_fvg.timeframe}"
            )

            await send_setup_alert(
                symbol=symbol,
                direction=state.bos.direction,
                entry=current_price,
                sl=sl,
                tp=tp,
                fvg=best_fvg,
                or_high=state.opening_range.high,
                or_low=state.opening_range.low,
                rr=rr,
            )

            # ── Paper trade entry ────────────────────────────────────────────
            self._paper.open_trade(
                symbol=symbol,
                direction=state.bos.direction,
                entry=current_price,
                fvg_top=best_fvg.top,
                fvg_bottom=best_fvg.bottom,
                fvg_tf=best_fvg.timeframe,
                tp=tp,
            )

        except Exception as e:
            logger.error(f"{symbol} | scan error: {e}", exc_info=True)

    async def _check_trade_outcome(self, symbol: str):
        """Poll open trade for TP/SL hit using latest candle."""
        if not self._paper.already_in_trade(symbol):
            return
        try:
            candles = await get_candles(symbol, "1m")
            if candles.empty:
                return
            last = candles.iloc[-1]
            result = self._paper.check_and_close(
                symbol,
                current_high=float(last["High"]),
                current_low=float(last["Low"]),
            )
            if result:
                record_trade(result)
                await send_outcome_alert(**result)
        except Exception as e:
            logger.error(f"Trade outcome check error {symbol}: {e}")
