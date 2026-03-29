"""
execution/paper_trader.py
──────────────────────────
Places trades on OKX's built-in paper trading environment.
Uses x-simulated-trading: 1 header — real OKX API, fake money.

Entry:  FVG retest confirmed (price inside FVG zone on 1m or 3m)
SL:     just beyond the FVG edge (bottom for long, top for short)
TP:     nearest 1H high (long) or 1H low (short)

Requires OKX API keys with "Simulated Trading" enabled on okx.com.
Set in .env: OKX_API_KEY, OKX_SECRET, OKX_PASSWORD
Optional:    PAPER_TRADE_SIZE_USDT (default 100)
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import ccxt

logger = logging.getLogger("paper_trader")

TRADE_SIZE_USDT = float(os.getenv("PAPER_TRADE_SIZE_USDT", "100"))
SL_BUFFER_PCT   = 0.0015   # 0.15% buffer beyond FVG edge for SL


@dataclass
class OpenTrade:
    symbol:       str
    direction:    str        # 'long' | 'short'
    entry:        float
    sl:           float
    tp:           float
    fvg_tf:       str        # '1m' or '3m' — which TF the FVG was on
    size_usdt:    float
    opened_at:    datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    order_id:     Optional[str] = None


class PaperTrader:

    def __init__(self):
        api_key  = os.getenv("OKX_API_KEY", "")
        secret   = os.getenv("OKX_SECRET", "")
        password = os.getenv("OKX_PASSWORD", "")

        self._exchange = None
        self._open: dict[str, OpenTrade] = {}   # symbol → OpenTrade

        if not all([api_key, secret, password]):
            logger.warning("OKX credentials not set — paper trader in log-only mode")
            return

        try:
            self._exchange = ccxt.okx({
                "apiKey":          api_key,
                "secret":          secret,
                "password":        password,
                "enableRateLimit": True,
                "options":         {"defaultType": "swap"},
                "headers":         {"x-simulated-trading": "1"},
            })
            self._exchange.load_markets()
            logger.info("OKX paper trader ready ✅")
        except Exception as e:
            logger.error(f"OKX paper trader init failed: {e}")
            self._exchange = None

    def already_in_trade(self, symbol: str) -> bool:
        return symbol in self._open

    def open_trade(
        self,
        symbol: str,
        direction: str,
        entry: float,
        fvg_top: float,
        fvg_bottom: float,
        fvg_tf: str,
        tp: float,
    ) -> Optional[OpenTrade]:
        """
        Called when FVG retest confirmed.
        SL = just beyond the FVG edge.
        TP = 1H level passed in from scanner.
        """
        if self.already_in_trade(symbol):
            logger.info(f"[paper] Already in {symbol} trade — skipping")
            return None

        # SL just outside the FVG
        if direction == "long":
            sl = fvg_bottom * (1 - SL_BUFFER_PCT)
        else:
            sl = fvg_top * (1 + SL_BUFFER_PCT)

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

        # Try to place on OKX paper trading
        if self._exchange:
            try:
                okx_symbol = symbol.replace("/", "-") + "-SWAP"
                side       = "buy" if direction == "long" else "sell"
                # Size in contracts (OKX BTC-USDT-SWAP = 0.01 BTC/contract)
                contracts  = round(TRADE_SIZE_USDT / entry / 0.01)
                contracts  = max(1, contracts)

                order = self._exchange.create_market_order(
                    symbol=okx_symbol,
                    side=side,
                    amount=contracts,
                    params={"tdMode": "cross", "posSide": "long" if direction == "long" else "short"},
                )
                trade.order_id = order.get("id")
                logger.info(f"[paper] OKX order placed: {order.get('id')} | {contracts} contracts")
            except Exception as e:
                logger.warning(f"[paper] OKX order failed (logging only): {e}")

        self._open[symbol] = trade
        logger.info(
            f"[paper] TRADE OPEN — {symbol} {direction.upper()} "
            f"entry={entry:.2f}  sl={sl:.2f}  tp={tp:.2f}  "
            f"RR={rr}  FVG={fvg_tf}  size=${TRADE_SIZE_USDT}"
        )
        return trade

    def check_and_close(self, symbol: str, current_high: float, current_low: float) -> Optional[dict]:
        """
        Call each scan cycle with latest candle high/low.
        Returns close dict if trade closed, else None.
        """
        trade = self._open.get(symbol)
        if not trade:
            return None

        outcome    = None
        exit_price = None

        if trade.direction == "long":
            if current_high >= trade.tp:
                outcome, exit_price = "win", trade.tp
            elif current_low <= trade.sl:
                outcome, exit_price = "loss", trade.sl
        else:
            if current_low <= trade.tp:
                outcome, exit_price = "win", trade.tp
            elif current_high >= trade.sl:
                outcome, exit_price = "loss", trade.sl

        if outcome:
            return self._close(trade, outcome, exit_price)
        return None

    def force_close_all(self, current_price: float) -> list[dict]:
        """Force close all open trades at session end (11am)."""
        results = []
        for symbol, trade in list(self._open.items()):
            results.append(self._close(trade, "expired", current_price))
        return results

    def _close(self, trade: OpenTrade, outcome: str, exit_price: float) -> dict:
        risk = abs(trade.entry - trade.sl)
        if risk > 0:
            r_mult = ((exit_price - trade.entry) / risk) if trade.direction == "long" \
                     else ((trade.entry - exit_price) / risk)
        else:
            r_mult = 0.0

        duration = datetime.now(timezone.utc) - trade.opened_at
        minutes  = int(duration.total_seconds() / 60)

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
            f"[paper] {emoji} TRADE CLOSED — {trade.symbol} {outcome.upper()} "
            f"exit={exit_price:.2f}  {r_mult:+.2f}R  {minutes}m"
        )
        return result
