"""
analysis/volatility.py
Two volatility filters applied before any trade is considered:

1. OR Range Filter — if the 9:30–9:40 range is too tight, the open is dead/choppy.
   Skip the symbol entirely for the day.

2. ATR Filter — if current ATR is compressed vs its rolling average, momentum
   is too thin for clean BOS moves.
"""
import logging

import numpy as np
import pandas as pd

from config import OR_MIN_RANGE_PCT, ATR_MIN_RATIO, ATR_LOOKBACK

logger = logging.getLogger("volatility")


# ── OR Range Filter ───────────────────────────────────────────────────────────

def or_range_is_valid(or_high: float, or_low: float) -> tuple[bool, float]:
    """
    Returns (is_valid, range_pct).
    If range < OR_MIN_RANGE_PCT of price, the opening is too tight — skip.
    """
    if or_low == 0:
        return False, 0.0

    range_pct = (or_high - or_low) / or_low * 100
    is_valid  = range_pct >= OR_MIN_RANGE_PCT

    if not is_valid:
        logger.info(
            f"OR range too tight: {range_pct:.3f}% < {OR_MIN_RANGE_PCT}% min — skipping symbol"
        )
    return is_valid, round(range_pct, 3)


# ── ATR Filter ────────────────────────────────────────────────────────────────

def _compute_atr(candles: pd.DataFrame, period: int) -> pd.Series:
    """True Range → rolling ATR."""
    high  = candles["High"]
    low   = candles["Low"]
    close = candles["Close"].shift(1)

    tr = pd.concat([
        high - low,
        (high - close).abs(),
        (low  - close).abs(),
    ], axis=1).max(axis=1)

    return tr.rolling(period).mean()


def atr_is_valid(candles_1h: pd.DataFrame) -> tuple[bool, float]:
    """
    Returns (is_valid, atr_ratio).
    Compares the most recent ATR to the average of the last ATR_LOOKBACK values.
    If current < ATR_MIN_RATIO * average, volatility is compressed — skip.
    """
    if len(candles_1h) < ATR_LOOKBACK * 2:
        # Not enough history — don't block the trade, just pass
        return True, 1.0

    atr_series = _compute_atr(candles_1h, ATR_LOOKBACK).dropna()

    if len(atr_series) < 2:
        return True, 1.0

    current_atr = float(atr_series.iloc[-1])
    avg_atr     = float(atr_series.iloc[-ATR_LOOKBACK:].mean())

    if avg_atr == 0:
        return True, 1.0

    ratio    = current_atr / avg_atr
    is_valid = ratio >= ATR_MIN_RATIO

    if not is_valid:
        logger.info(
            f"ATR compressed: ratio={ratio:.2f} < {ATR_MIN_RATIO} min — skipping symbol"
        )

    return is_valid, round(ratio, 2)
