"""
execution/paper_trader.py
──────────────────────────
Pure log-only paper trader. No exchange connection needed.
Tracks open trades in memory, checks TP/SL each scan cycle,
closes at session end (11am).

When you're ready to go live on Phemex, the execution
layer plugs in here — this file becomes the router.

Entry:  FVG retest confirmed
SL:     just beyond the FVG edge (bottom for long, top for short)
TP:     nearest 1H high (long) or 1H low (short)
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("paper_trader")

TRADE_SIZE_USDT = float(os.getenv("PAPER_TRADE_SIZE_USDT", "100"))
SL_BUFFER_PCT   = 0.0015   # 0.15% buffer beyond FVG edge for SL


@dataclass
class OpenTrade:
    symbol:    str
    direction: str          # 'long' | 'short'
    entry:     float
    sl:        float
    tp:        float
    fvg_tf:    str          # '1m' or '3m'
    size_usdt: float
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class PaperTrader:

    def __init__(self):
        self._open: dict[str, OpenTrade] = {}
        logger.info("Paper trader ready (log-only mode)")

    def already_in_trade(self, symbol: str) -> bool:
        return symbol in self._open

    def open_trade(
        self,
        symbol:     str,
        direction:  str,
        entry:      float,
        fvg_top:    float,
        fvg_bottom: float,
        fvg_tf:     str,
        tp:         float,
    ) -> Optional[OpenTrade]:
        if self.already_in_trade(symbol):
            logger.info(f"[paper] Already in {symbol} — skipping")
            return None

        sl = fvg_bottom * (1 - SL_BUFFER_PCT) if direction == "long" \
             else fvg_top * (1 + SL_BUFFER_PCT)

        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        rr     = round(reward / risk, 2) if risk > 0 else 0

        trade = OpenTrade(
            symbol=symbol,
            direction=direction,
            entry=entry,
            sl=sl,
            tp=tp,
            fvg_tf=fvg_tf,
            size_usdt=TRADE_SIZE_USDT,
        )

        self._open[symbol] = trade
        logger.info(
            f"[paper] OPEN — {symbol} {direction.upper()} "
            f"entry={entry:.4f}  sl={sl:.4f}  tp={tp:.4f}  "
            f"RR={rr}  FVG={fvg_tf}  size=${TRADE_SIZE_USDT}"
        )
        return trade

    def check_and_close(
        self,
        symbol:       str,
        current_high: float,
        current_low:  float,
    ) -> Optional[dict]:
        trade = self._open.get(symbol)
        if not trade:
            return None

        outcome = exit_price = None

        if trade.direction == "long":
            if current_high >= trade.tp:
                outcome, exit_price = "win",  trade.tp
            elif current_low <= trade.sl:
                outcome, exit_price = "loss", trade.sl
        else:
            if current_low <= trade.tp:
                outcome, exit_price = "win",  trade.tp
            elif current_high >= trade.sl:
                outcome, exit_price = "loss", trade.sl

        return self._close(trade, outcome, exit_price) if outcome else None

    def force_close_all(self, current_price: float) -> list[dict]:
        return [
            self._close(trade, "expired", current_price)
            for trade in list(self._open.values())
        ]

    def _close(self, trade: OpenTrade, outcome: str, exit_price: float) -> dict:
        risk = abs(trade.entry - trade.sl)
        if risk > 0:
            r_mult = ((exit_price - trade.entry) / risk) if trade.direction == "long" \
                     else ((trade.entry - exit_price) / risk)
        else:
            r_mult = 0.0

        minutes = int((datetime.now(timezone.utc) - trade.opened_at).total_seconds() / 60)
        del self._open[trade.symbol]

        result = {
            "symbol":    trade.symbol,
            "direction": trade.direction,
            "fvg_tf":    trade.fvg_tf,
            "entry":     trade.entry,
            "exit":      exit_price,
            "sl":        trade.sl,
            "tp":        trade.tp,
            "outcome":   outcome,
            "r_mult":    round(r_mult, 2),
            "minutes":   minutes,
            "size_usdt": trade.size_usdt,
        }

        emoji = {"win": "🎯", "loss": "🛑", "expired": "⏰"}[outcome]
        logger.info(
            f"[paper] {emoji} CLOSED — {trade.symbol} {outcome.upper()} "
            f"exit={exit_price:.4f}  {r_mult:+.2f}R  {minutes}m"
        )
        return result
