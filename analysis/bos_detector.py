"""
analysis/bos_detector.py
Detects a clean Break of Structure after the opening range.

Rules:
  - First candle to close above OR high (long) or below OR low (short)
  - Must be a STRONG candle: body >= 55% of range, minimal wicks
  - Must NOT be preceded by choppy price action (too many direction flips)
"""
import logging
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from config import STRONG_BODY_PCT, MAX_WICK_PCT, CHOP_LOOKBACK, MAX_CHOP_FLIPS

logger = logging.getLogger("bos")


@dataclass
class BOSResult:
    direction: str           # 'long' or 'short'
    bos_candle_time: object  # pd.Timestamp
    bos_price: float         # close of the BOS candle


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_strong_candle(row: pd.Series, direction: str) -> bool:
    """Body >= STRONG_BODY_PCT of range, wick in trade direction <= MAX_WICK_PCT."""
    candle_range = row["High"] - row["Low"]
    if candle_range == 0:
        return False

    body = abs(row["Close"] - row["Open"])
    if body / candle_range < STRONG_BODY_PCT:
        return False

    if direction == "long":
        # Upper wick should be small (no rejection above)
        upper_wick = row["High"] - max(row["Open"], row["Close"])
        if upper_wick / candle_range > MAX_WICK_PCT:
            return False
    else:
        # Lower wick should be small (no rejection below)
        lower_wick = min(row["Open"], row["Close"]) - row["Low"]
        if lower_wick / candle_range > MAX_WICK_PCT:
            return False

    return True


def _is_choppy(candles_before: pd.DataFrame) -> bool:
    """
    Returns True if the recent candles flip direction too many times.
    We look at the last CHOP_LOOKBACK candles and count how many times
    the close direction alternates.
    """
    if len(candles_before) < CHOP_LOOKBACK:
        return False

    recent = candles_before.tail(CHOP_LOOKBACK)
    directions = [1 if row["Close"] >= row["Open"] else -1 for _, row in recent.iterrows()]

    flips = sum(1 for i in range(1, len(directions)) if directions[i] != directions[i - 1])
    return flips >= MAX_CHOP_FLIPS


# ── Public API ────────────────────────────────────────────────────────────────

def detect_bos(
    candles_1m: pd.DataFrame,
    or_high: float,
    or_low: float,
) -> Optional[BOSResult]:
    """
    Scan 1m candles after 9:40 for the first clean BOS.
    Returns None if no valid BOS found yet.
    """
    try:
        post_or = candles_1m.between_time("09:40", "23:59")
    except Exception:
        return None

    if post_or.empty:
        return None

    for i, (idx, row) in enumerate(post_or.iterrows()):
        # Check chop on candles leading up to this one
        candles_before = candles_1m[candles_1m.index < idx]
        if _is_choppy(candles_before):
            continue

        # Bullish BOS
        if row["Close"] > or_high and _is_strong_candle(row, "long"):
            logger.info(f"BOS LONG confirmed @ {row['Close']:.4f}  ({idx})")
            return BOSResult("long", idx, float(row["Close"]))

        # Bearish BOS
        if row["Close"] < or_low and _is_strong_candle(row, "short"):
            logger.info(f"BOS SHORT confirmed @ {row['Close']:.4f}  ({idx})")
            return BOSResult("short", idx, float(row["Close"]))

    return None
