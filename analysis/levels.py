"""
analysis/levels.py
───────────────────
Key level detection for take-profit referencing.

get_1h_swing_level:
    Finds the nearest 1H swing high (for longs) or swing low (for shorts)
    beyond the current price. Shown in alerts as a discretionary extension target.
    Actual TP is always set to TARGET_R × risk.
"""
import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger("levels")


def get_1h_swing_level(
    candles_1h: pd.DataFrame,
    current_price: float,
    direction: str,
    min_distance_pct: float = 0.001,
) -> Optional[float]:
    """
    Returns the nearest 1H swing high (long) or swing low (short) that is
    at least min_distance_pct away from current price.
    """
    if candles_1h.empty or len(candles_1h) < 2:
        return None
    try:
        if direction == "long":
            min_level  = current_price * (1 + min_distance_pct)
            candidates = [float(r["High"]) for _, r in candles_1h.iterrows() if float(r["High"]) >= min_level]
            return min(candidates) if candidates else None
        else:
            max_level  = current_price * (1 - min_distance_pct)
            candidates = [float(r["Low"]) for _, r in candles_1h.iterrows() if float(r["Low"]) <= max_level]
            return max(candidates) if candidates else None
    except Exception as e:
        logger.error(f"get_1h_swing_level error: {e}")
        return None
