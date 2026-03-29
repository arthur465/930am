"""
analysis/opening_range.py
Builds the 9:30–9:40 ET opening range high/low.
"""
import logging
from dataclasses import dataclass
from typing import Optional

import pandas as pd

logger = logging.getLogger("opening_range")

MIN_OR_CANDLES = 5  # Need at least 5 of the 10 possible 1m candles


@dataclass
class OpeningRange:
    high: float
    low: float


def build_opening_range(candles_1m: pd.DataFrame) -> Optional[OpeningRange]:
    """
    Extract OR high/low from 9:30–9:39 1m candles.
    Returns None if not enough candles yet.
    """
    try:
        or_candles = candles_1m.between_time("09:30", "09:39")
    except Exception:
        return None

    if len(or_candles) < MIN_OR_CANDLES:
        return None

    high = float(or_candles["High"].max())
    low  = float(or_candles["Low"].min())

    logger.info(f"Opening range built — H: {high:.4f}  L: {low:.4f}  ({len(or_candles)} candles)")
    return OpeningRange(high=high, low=low)
