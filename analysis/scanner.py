"""
analysis/scanner.py
────────────────────
Per-symbol state machine — simplified for direct BOS entry.

Flow per symbol each scan cycle:
  1. Build OR (9:30–9:39 candles)
  2. Wait for 9:40 OR window to close
  3. Volatility check (OR range + ATR)
  4. Detect BOS (first strong 1m close above/below OR H/L)
  5. [Crypto only] Coinalyze CVD confirmation
  6. Enter at BOS close price
     - SL = just inside the broken OR boundary
     - TP = TARGET_R × risk (1.5R)
     - 1H swing level shown in alert as reference
  7. Track trade outcome (TP / SL hit)
"""
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, Optional

import pytz

from config import (
    ALL_SYMBOLS, TARGET_R, SL_BUFFER_PCT,
    OR_END_HOUR, OR_END_MIN,
)
from data.cache import get_candles_cached as get_candles
from data.fetcher import is_crypto
from analysis.opening_range import build_opening_range, OpeningRange
from analysis.bos_detector import detect_bos, BOSResult
from analysis.levels import get_1h_swing_level
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
            await asyncio.sleep(1.0)

    async def close_session(self):
        """Called at session end — force-close open paper trades and send stats."""
        for sym in ALL_SYMBOLS:
            if self._paper.already_in_trade(sym):
                try:
                    candles = await get_candles(sym, "1m")
                    if not candles.empty:
                        price   = float(candles["Close"].iloc[-1])
                        results = self._paper.force_close_all(price)
                        for r in results:
                            record_trade(r)
                            await send_outcome_alert(**r)
                except Exception as e:
                    logger.error(f"Force close error {sym}: {e}")

        summary = format_summary()
        await send_stats(summary)

    # ── Per-symbol state machine ───────────────────────────────────────────────

    async def _scan_symbol(self, symbol: str, now_et: datetime):
        state = self._states[symbol]

        # Already fired alert or failed vol — just monitor open trade if any
        if state.alerted or state.skipped_vol:
            await self._check_trade_outcome(symbol)
            return

        try:
            candles_1m = await get_candles(symbol, "1m")
            if candles_1m.empty:
                return

            # ── Step 1: Build OR (9:30–9:39) ─────────────────────────────────
            if state.opening_range is None:
                result = build_opening_range(candles_1m)
                if result:
                    state.opening_range = result
                    logger.info(f"{symbol} | OR built: H={result.high:.4f}  L={result.low:.4f}")
                return

            # ── Step 2: Wait for OR window to fully close (9:40) ─────────────
            if now_et.hour < OR_END_HOUR or (now_et.hour == OR_END_HOUR and now_et.minute < OR_END_MIN):
                return

            # ── Step 3: Volatility check ──────────────────────────────────────
            if state.bos is None and not state.skipped_vol:
                or_valid, or_pct = or_range_is_valid(
                    state.opening_range.high, state.opening_range.low
                )
                if not or_valid:
                    state.skipped_vol = True
                    logger.info(f"{symbol} | Skipped — OR too tight ({or_pct:.3f}%)")
                    return

                candles_1h = await get_candles(symbol, "1h")
                atr_valid, atr_ratio = atr_is_valid(candles_1h)
                if not atr_valid:
                    state.skipped_vol = True
                    logger.info(f"{symbol} | Skipped — ATR compressed (ratio={atr_ratio:.2f})")
                    return

                logger.info(f"{symbol} | Vol OK — OR={or_pct:.3f}%  ATR={atr_ratio:.2f}")

            # ── Step 4: BOS detection ─────────────────────────────────────────
            if state.bos is None:
                bos = detect_bos(candles_1m, state.opening_range.high, state.opening_range.low)
                if bos:
                    state.bos = bos
                    logger.info(
                        f"{symbol} | BOS {bos.direction.upper()} @ {bos.bos_price:.4f}  "
                        f"candle H={bos.bos_candle_high:.4f} L={bos.bos_candle_low:.4f}"
                    )
                    # BOS just detected — fall through to entry below
                else:
                    return

            # ── Step 5: [Crypto only] Coinalyze CVD confirmation ──────────────
            if is_crypto(symbol):
                from data.coinalyze_fetcher import get_cvd_confirmation
                cvd_ok, buy_ratio = await get_cvd_confirmation(
                    symbol, state.bos.direction, state.bos.bos_candle_time
                )
                if not cvd_ok:
                    logger.info(
                        f"{symbol} | BOS confirmed but CVD contra-flow "
                        f"(buy_ratio={buy_ratio:.3f}) — skipping"
                    )
                    # Don't set alerted; re-check next scan in case we want to let it slide
                    # Actually: one BOS per day, so skip it cleanly
                    state.skipped_vol = True
                    return

            # ── Step 6: Compute entry / SL / TP ──────────────────────────────
            entry = state.bos.bos_price
            or_h  = state.opening_range.high
            or_l  = state.opening_range.low

            if state.bos.direction == "long":
                sl   = or_h * (1 - SL_BUFFER_PCT)
            else:
                sl   = or_l * (1 + SL_BUFFER_PCT)

            risk   = abs(entry - sl)
            if risk <= 0:
                logger.warning(f"{symbol} | Risk = 0 — skipping")
                return

            if state.bos.direction == "long":
                tp = entry + TARGET_R * risk
            else:
                tp = entry - TARGET_R * risk

            rr = round(abs(tp - entry) / risk, 2)

            # 1H swing level as reference (shown in alert, not used for TP)
            candles_1h  = await get_candles(symbol, "1h")
            swing_level = get_1h_swing_level(candles_1h, entry, state.bos.direction)

            # ── ALL CONDITIONS MET — fire alert ───────────────────────────────
            state.alerted = True

            logger.info(
                f"🚨 ENTRY: {symbol} {state.bos.direction.upper()} "
                f"entry={entry:.4f}  sl={sl:.4f}  tp={tp:.4f}  rr={rr}R  "
                f"swing={swing_level}"
            )

            await send_setup_alert(
                symbol=symbol,
                direction=state.bos.direction,
                entry=entry,
                sl=sl,
                tp=tp,
                rr=rr,
                or_high=or_h,
                or_low=or_l,
                swing_level=swing_level,
            )

            # ── Paper trade ───────────────────────────────────────────────────
            self._paper.open_trade(
                symbol=symbol,
                direction=state.bos.direction,
                entry=entry,
                sl=sl,
                tp=tp,
            )

        except Exception as e:
            logger.error(f"{symbol} | scan error: {e}", exc_info=True)

    async def _check_trade_outcome(self, symbol: str):
        if not self._paper.already_in_trade(symbol):
            return
        try:
            candles = await get_candles(symbol, "1m")
            if candles.empty:
                return
            last   = candles.iloc[-1]
            result = self._paper.check_and_close(
                symbol,
                current_high=float(last["High"]),
                current_low=float(last["Low"]),
            )
            if result:
                record_trade(result)
                await send_outcome_alert(**result)
                self._states[symbol].alerted = False
                logger.info(f"{symbol} | Trade closed — ready for new setups")
        except Exception as e:
            logger.error(f"Trade outcome check error {symbol}: {e}")
